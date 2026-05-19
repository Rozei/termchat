"""TermChat — minimal terminal client.

Connects to the server over TCP, prints incoming lines to stdout,
and forwards stdin lines as outgoing messages.
"""
from __future__ import annotations

import argparse
import asyncio
import sys


async def reader_loop(reader: asyncio.StreamReader) -> None:
    while True:
        line = await reader.readline()
        if not line:
            print("(disconnected)")
            return
        sys.stdout.write(line.decode(errors="replace"))
        sys.stdout.flush()


async def writer_loop(writer: asyncio.StreamWriter) -> None:
    loop = asyncio.get_running_loop()
    while True:
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if not line:
            return
        writer.write(line.encode())
        await writer.drain()


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    args = p.parse_args()

    reader, writer = await asyncio.open_connection(args.host, args.port)
    rt = asyncio.create_task(reader_loop(reader))
    wt = asyncio.create_task(writer_loop(writer))
    _, pending = await asyncio.wait({rt, wt}, return_when=asyncio.FIRST_COMPLETED)
    for t in pending:
        t.cancel()
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
