"""Async unit + integration tests for the TermChat server."""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from server import ChatServer  # noqa: E402


class FakeWriter:
    """Minimal stand-in for asyncio.StreamWriter used in unit tests."""

    def __init__(self) -> None:
        self.buf: list[str] = []

    def write(self, data: bytes) -> None:
        self.buf.append(data.decode())

    async def drain(self) -> None:
        return


@pytest.mark.asyncio
async def test_register_unique_names():
    s = ChatServer()
    a = await s.register("alice", FakeWriter())
    b = await s.register("alice", FakeWriter())
    assert a is not None
    assert b is None


@pytest.mark.asyncio
async def test_broadcast_excludes_sender():
    s = ChatServer()
    wa, wb = FakeWriter(), FakeWriter()
    alice = await s.register("alice", wa)
    bob = await s.register("bob", wb)
    await s.join(alice, "general")
    await s.join(bob, "general")
    wa.buf.clear()
    wb.buf.clear()

    await s.broadcast("general", "alice", "hi")

    assert any("hi" in line and "alice" in line for line in wb.buf)
    assert wa.buf == []


@pytest.mark.asyncio
async def test_direct_message_to_known_user():
    s = ChatServer()
    wa, wb = FakeWriter(), FakeWriter()
    await s.register("alice", wa)
    await s.register("bob", wb)

    ok = await s.direct("alice", "bob", "ping")

    assert ok is True
    assert any("ping" in line and "alice" in line for line in wb.buf)


@pytest.mark.asyncio
async def test_direct_message_to_unknown_user():
    s = ChatServer()
    await s.register("alice", FakeWriter())
    ok = await s.direct("alice", "ghost", "ping")
    assert ok is False


@pytest.mark.asyncio
async def test_unregister_cleans_rooms():
    s = ChatServer()
    alice = await s.register("alice", FakeWriter())
    await s.join(alice, "general")
    assert "general" in s.rooms
    await s.unregister(alice)
    assert "general" not in s.rooms
    assert "alice" not in s.clients


@pytest.mark.asyncio
async def test_concurrent_registrations_are_safe():
    s = ChatServer()
    names = [f"user{i}" for i in range(20)]
    results = await asyncio.gather(*[s.register(n, FakeWriter()) for n in names])
    assert all(r is not None for r in results)
    assert len(s.clients) == 20
