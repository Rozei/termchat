"""TermChat — minimal asynchronous TCP chat server.

Single-process asyncio event loop handling concurrent connections.
Supports named rooms, broadcasts, and direct messages over a tiny
line-based text protocol.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, Optional, Set

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("termchat")


@dataclass
class Client:
    name: str
    writer: asyncio.StreamWriter
    rooms: Set[str] = field(default_factory=set)


class ChatServer:
    """Holds the live state of the chat: connected clients and rooms.

    All mutations go through `self.lock`. The protected critical sections
    are intentionally tiny — we copy out the recipient list under the lock
    and release it before doing the (potentially slow) network writes.
    """

    def __init__(self) -> None:
        self.clients: Dict[str, Client] = {}
        self.rooms: Dict[str, Set[str]] = defaultdict(set)
        self.lock = asyncio.Lock()

    async def register(self, name: str, writer: asyncio.StreamWriter) -> Optional[Client]:
        async with self.lock:
            if name in self.clients:
                return None
            client = Client(name=name, writer=writer)
            self.clients[name] = client
            return client

    async def unregister(self, client: Client) -> None:
        async with self.lock:
            for room in list(client.rooms):
                self.rooms[room].discard(client.name)
                if not self.rooms[room]:
                    del self.rooms[room]
            self.clients.pop(client.name, None)

    async def join(self, client: Client, room: str) -> None:
        async with self.lock:
            self.rooms[room].add(client.name)
            client.rooms.add(room)
        await self._send(client, f"joined #{room}")

    async def broadcast(self, room: str, sender: str, msg: str) -> None:
        async with self.lock:
            recipients = [
                self.clients[name]
                for name in self.rooms.get(room, set())
                if name != sender and name in self.clients
            ]
        for client in recipients:
            await self._send(client, f"#{room} <{sender}> {msg}")

    async def direct(self, sender: str, target: str, msg: str) -> bool:
        async with self.lock:
            client = self.clients.get(target)
        if client is None:
            return False
        await self._send(client, f"DM <{sender}> {msg}")
        return True

    async def _send(self, client: Client, line: str) -> None:
        try:
            client.writer.write((line + "\n").encode())
            await client.writer.drain()
        except (ConnectionResetError, BrokenPipeError):
            log.warning("write to %s failed", client.name)


async def handle_client(
    server: ChatServer,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    addr = writer.get_extra_info("peername")
    log.info("connection from %s", addr)

    writer.write(b"username: ")
    await writer.drain()

    name_bytes = await reader.readline()
    name = name_bytes.decode(errors="replace").strip()
    if not name:
        writer.close()
        return

    client = await server.register(name, writer)
    if client is None:
        writer.write(b"username taken\n")
        await writer.drain()
        writer.close()
        return

    log.info("registered %s from %s", name, addr)
    await server._send(
        client,
        f"welcome {name}. commands: /join <room>, /room <room> <text>, /msg <user> <text>, /quit",
    )

    try:
        while True:
            line_bytes = await reader.readline()
            if not line_bytes:
                break
            line = line_bytes.decode(errors="replace").strip()
            if not line:
                continue
            if line == "/quit":
                break
            elif line.startswith("/join "):
                room = line[len("/join ") :].strip()
                if room:
                    await server.join(client, room)
            elif line.startswith("/msg "):
                rest = line[len("/msg ") :].split(" ", 1)
                if len(rest) == 2:
                    target, msg = rest
                    ok = await server.direct(name, target, msg)
                    if not ok:
                        await server._send(client, f"no such user: {target}")
            elif line.startswith("/room "):
                rest = line[len("/room ") :].split(" ", 1)
                if len(rest) == 2:
                    room, msg = rest
                    if room in client.rooms:
                        await server.broadcast(room, name, msg)
                    else:
                        await server._send(client, f"join #{room} first")
            else:
                await server._send(client, "unknown command")
    except asyncio.CancelledError:
        pass
    finally:
        log.info("disconnect %s", name)
        await server.unregister(client)
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def serve(host: str, port: int) -> None:
    server = ChatServer()

    async def cb(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await handle_client(server, reader, writer)

    srv = await asyncio.start_server(cb, host, port)
    log.info("listening on %s:%d", host, port)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    async with srv:
        await stop.wait()
        log.info("shutting down")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=8765)
    args = p.parse_args()
    asyncio.run(serve(args.host, args.port))


if __name__ == "__main__":
    main()
