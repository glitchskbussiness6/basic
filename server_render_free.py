import argparse
import asyncio
import json
import os
import struct
from dataclasses import dataclass, field
from typing import Dict, Set

LEN_STRUCT = struct.Struct("!I")
TYPE_FRAME = ord("J")
TYPE_CTRL = ord("C")
MAX_FRAME_BYTES = 16 * 1024 * 1024

HTTP_METHODS = (b"GET ", b"HEAD", b"POST", b"PUT ", b"DELE", b"OPTI", b"PATC", b"CONN", b"TRAC")


class ProbeConnectionError(Exception):
    pass


async def _send_http_ok_and_close(writer: asyncio.StreamWriter):
    try:
        body = b"OK\n"
        payload = (
            b"HTTP/1.1 200 OK\r\n"
            b"Content-Type: text/plain; charset=utf-8\r\n"
            + f"Content-Length: {len(body)}\r\n".encode("ascii")
            + b"Connection: close\r\n\r\n"
            + body
        )
        writer.write(payload)
        await writer.drain()
    except Exception:
        pass


async def read_msg(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter | None = None,
    initial_handshake: bool = False,
):
    hdr = await reader.readexactly(LEN_STRUCT.size)
    (length,) = LEN_STRUCT.unpack(hdr)
    if length <= 0 or length > MAX_FRAME_BYTES:
        if initial_handshake and writer is not None:
            # Drain any immediate trailing bytes (e.g., rest of HTTP request line/headers)
            try:
                await asyncio.wait_for(reader.read(4096), timeout=0.05)
            except Exception:
                pass
            if hdr in HTTP_METHODS or all(32 <= b <= 126 or b in (9, 10, 13) for b in hdr):
                await _send_http_ok_and_close(writer)
        raise ProbeConnectionError(f"Non-protocol or invalid length: {length}")
    payload = await reader.readexactly(length)
    return payload[0], payload[1:]


async def write_msg(writer: asyncio.StreamWriter, msg_type: int, body: bytes):
    frame = bytes([msg_type]) + body
    writer.write(LEN_STRUCT.pack(len(frame)) + frame)
    await writer.drain()


@dataclass
class RoomState:
    room_id: str
    host: asyncio.StreamWriter | None = None
    controllers: Set[asyncio.StreamWriter] = field(default_factory=set)


class RelayServer:
    def __init__(self):
        self.rooms: Dict[str, RoomState] = {}
        self.lock = asyncio.Lock()

    async def get_or_create_room(self, room_id: str) -> RoomState:
        async with self.lock:
            if room_id not in self.rooms:
                self.rooms[room_id] = RoomState(room_id=room_id)
            return self.rooms[room_id]

    async def broadcast_to_controllers(self, room: RoomState, msg_type: int, body: bytes):
        dead = []
        for ctrl in list(room.controllers):
            try:
                await write_msg(ctrl, msg_type, body)
            except Exception:
                dead.append(ctrl)
        if dead:
            async with self.lock:
                for ctrl in dead:
                    room.controllers.discard(ctrl)
                    try:
                        ctrl.close()
                    except Exception:
                        pass

    async def send_to_host(self, room: RoomState, msg_type: int, body: bytes):
        host = room.host
        if host is None:
            return
        try:
            await write_msg(host, msg_type, body)
        except Exception:
            async with self.lock:
                if room.host is host:
                    room.host = None
            try:
                host.close()
            except Exception:
                pass

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info("peername")
        role = None
        room_id = "default"
        room: RoomState | None = None
        registered = False
        try:
            msg_type, body = await read_msg(reader, writer=writer, initial_handshake=True)
            if msg_type != TYPE_CTRL:
                await write_msg(writer, TYPE_CTRL, json.dumps({"type": "error", "message": "Expected register frame"}).encode())
                return
            reg = json.loads(body.decode("utf-8"))
            if reg.get("type") != "register":
                await write_msg(writer, TYPE_CTRL, json.dumps({"type": "error", "message": "First message must be register"}).encode())
                return

            role = str(reg.get("role", "")).strip().lower()
            room_id = str(reg.get("room", "default")).strip() or "default"
            if role not in ("host", "controller"):
                await write_msg(writer, TYPE_CTRL, json.dumps({"type": "error", "message": "Invalid role"}).encode())
                return

            room = await self.get_or_create_room(room_id)
            async with self.lock:
                if role == "host":
                    if room.host is not None and room.host is not writer:
                        try:
                            room.host.close()
                        except Exception:
                            pass
                    room.host = writer
                else:
                    room.controllers.add(writer)
            registered = True

            print(f"[Relay] {role} connected in room='{room_id}' from {peer}", flush=True)
            await write_msg(
                writer,
                TYPE_CTRL,
                json.dumps({
                    "type": "registered",
                    "role": role,
                    "room": room_id,
                    "host_connected": room.host is not None,
                    "controllers_count": len(room.controllers),
                }).encode("utf-8"),
            )

            presence = json.dumps({
                "type": "peer_status",
                "room": room_id,
                "host_connected": room.host is not None,
                "controllers_count": len(room.controllers),
            }).encode("utf-8")
            await self.broadcast_to_controllers(room, TYPE_CTRL, presence)
            if role == "controller":
                await self.send_to_host(room, TYPE_CTRL, presence)

            while True:
                m_type, m_body = await read_msg(reader, writer=writer, initial_handshake=False)
                if m_type == TYPE_CTRL:
                    try:
                        parsed = json.loads(m_body.decode("utf-8"))
                        if parsed.get("type") == "ping":
                            await write_msg(writer, TYPE_CTRL, json.dumps({"type": "pong"}).encode("utf-8"))
                            continue
                    except Exception:
                        pass

                if role == "host":
                    await self.broadcast_to_controllers(room, m_type, m_body)
                else:
                    await self.send_to_host(room, m_type, m_body)

        except (
            asyncio.IncompleteReadError,
            ConnectionResetError,
            BrokenPipeError,
            OSError,
            ProbeConnectionError,
        ):
            pass
        except Exception as exc:
            print(f"[Relay] Error ({role}, room='{room_id}', peer={peer}): {exc}", flush=True)
        finally:
            if room is not None:
                async with self.lock:
                    if role == "host" and room.host is writer:
                        room.host = None
                    elif role == "controller":
                        room.controllers.discard(writer)
                if registered:
                    presence = json.dumps({
                        "type": "peer_status",
                        "room": room_id,
                        "host_connected": room.host is not None,
                        "controllers_count": len(room.controllers),
                    }).encode("utf-8")
                    await self.broadcast_to_controllers(room, TYPE_CTRL, presence)
                    if role == "controller":
                        await self.send_to_host(room, TYPE_CTRL, presence)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            if registered:
                print(f"[Relay] {role} left room='{room_id}' ({peer})", flush=True)


async def run_relay(host: str, port: int):
    relay = RelayServer()
    server = await asyncio.start_server(relay.handle_client, host=host, port=port, backlog=256)
    addrs = ", ".join(str(s.getsockname()) for s in (server.sockets or []))
    print(f"[Relay] Render relay listening on {addrs}", flush=True)
    async with server:
        await server.serve_forever()


def main():
    parser = argparse.ArgumentParser(description="Render Relay Server (Host <-> Controller)")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "10000")))
    args = parser.parse_args()
    asyncio.run(run_relay(args.host, args.port))


if __name__ == "__main__":
    main()
