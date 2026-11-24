import argparse
import asyncio
import base64
import hashlib
import json
import os
import struct
from dataclasses import dataclass, field
from typing import Dict, Set

LEN_STRUCT = struct.Struct("!I")
TYPE_FRAME = ord("J")
TYPE_CTRL = ord("C")
MAX_FRAME_BYTES = 16 * 1024 * 1024
WS_GUID = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class WebSocketClosed(Exception):
    pass


def _ws_accept_key(sec_key: str) -> str:
    digest = hashlib.sha1(sec_key.strip().encode("ascii") + WS_GUID).digest()
    return base64.b64encode(digest).decode("ascii")


async def _read_http_headers(reader: asyncio.StreamReader):
    raw = await reader.readuntil(b"\r\n\r\n")
    lines = raw.decode("latin-1").split("\r\n")
    req_line = lines[0] if lines else ""
    headers = {}
    for line in lines[1:]:
        if ":" in line:
            k, v = line.split(":", 1)
            headers[k.strip().lower()] = v.strip()
    return req_line, headers


async def _send_http_response(writer: asyncio.StreamWriter, status: str, body: bytes):
    resp = (
        f"HTTP/1.1 {status}\r\n".encode("ascii")
        + b"Content-Type: text/plain; charset=utf-8\r\n"
        + f"Content-Length: {len(body)}\r\n".encode("ascii")
        + b"Connection: close\r\n\r\n"
        + body
    )
    writer.write(resp)
    await writer.drain()


async def ws_recv_frame(reader: asyncio.StreamReader) -> bytes:
    while True:
        hdr = await reader.readexactly(2)
        b1, b2 = hdr[0], hdr[1]
        opcode = b1 & 0x0F
        masked = (b2 & 0x80) != 0
        length = b2 & 0x7F

        if length == 126:
            (length,) = struct.unpack("!H", await reader.readexactly(2))
        elif length == 127:
            (length,) = struct.unpack("!Q", await reader.readexactly(8))

        if length > MAX_FRAME_BYTES:
            raise ValueError(f"WebSocket payload too large: {length}")

        mask_key = await reader.readexactly(4) if masked else b""
        payload = await reader.readexactly(length) if length > 0 else b""

        if masked and payload:
            payload = bytes(b ^ mask_key[i & 3] for i, b in enumerate(payload))

        if opcode == 0x8:  # Close frame
            raise WebSocketClosed("Peer sent close frame")
        if opcode == 0x9:  # Ping frame -> ignore or handled at app level
            continue
        if opcode == 0xA:  # Pong frame
            continue
        if opcode in (0x1, 0x2):  # Text or Binary frame
            return payload


async def ws_send_binary(writer: asyncio.StreamWriter, data: bytes):
    length = len(data)
    header = bytearray([0x82])  # FIN=1, opcode=2 (binary)
    if length < 126:
        header.append(length)
    elif length <= 0xFFFF:
        header.append(126)
        header.extend(struct.pack("!H", length))
    else:
        header.append(127)
        header.extend(struct.pack("!Q", length))
    writer.write(bytes(header) + data)
    await writer.drain()


async def read_msg(reader: asyncio.StreamReader):
    raw = await ws_recv_frame(reader)
    if len(raw) < 1:
        raise ValueError("Empty relay message")
    return raw[0], raw[1:]


async def write_msg(writer: asyncio.StreamWriter, msg_type: int, body: bytes):
    await ws_send_binary(writer, bytes([msg_type]) + body)


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
            req_line, headers = await asyncio.wait_for(_read_http_headers(reader), timeout=10.0)
            upgrade = headers.get("upgrade", "").lower()
            sec_key = headers.get("sec-websocket-key", "")

            if upgrade != "websocket" or not sec_key:
                # Normal HTTP health check / browser visit -> return 200 OK cleanly
                await _send_http_response(writer, "200 OK", b"Render WebSocket Relay Online\n")
                return

            accept_key = _ws_accept_key(sec_key)
            handshake_resp = (
                b"HTTP/1.1 101 Switching Protocols\r\n"
                b"Upgrade: websocket\r\n"
                b"Connection: Upgrade\r\n"
                + f"Sec-WebSocket-Accept: {accept_key}\r\n\r\n".encode("ascii")
            )
            writer.write(handshake_resp)
            await writer.drain()

            msg_type, body = await read_msg(reader)
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

            print(f"[Relay] {role} connected via WebSocket in room='{room_id}' from {peer}", flush=True)
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
                m_type, m_body = await read_msg(reader)
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
            asyncio.LimitOverrunError,
            asyncio.TimeoutError,
            ConnectionResetError,
            BrokenPipeError,
            OSError,
            WebSocketClosed,
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
    print(f"[Relay] Render WebSocket relay listening on {addrs}", flush=True)
    async with server:
        await server.serve_forever()


def main():
    parser = argparse.ArgumentParser(description="Render WebSocket Relay Server (Host <-> Controller)")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "10000")))
    args = parser.parse_args()
    asyncio.run(run_relay(args.host, args.port))


if __name__ == "__main__":
    main()
