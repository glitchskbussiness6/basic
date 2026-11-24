import argparse
import asyncio
import json
import os
from collections import deque

from aiohttp import web

TYPE_FRAME = ord("J")
TYPE_CTRL = ord("C")
MAX_FRAME_BYTES = 16 * 1024 * 1024

ROOMS = {}


class Peer:
    def __init__(self, ws):
        self.ws = ws
        self.ctrl = deque()
        self.frame = None
        self.ev = asyncio.Event()
        self.task = asyncio.create_task(self.run())

    def push(self, msg_type, body):
        if msg_type == TYPE_FRAME:
            self.frame = body
        else:
            self.ctrl.append((msg_type, body))
        self.ev.set()

    async def run(self):
        try:
            while True:
                await self.ev.wait()
                self.ev.clear()
                while self.ctrl:
                    t, b = self.ctrl.popleft()
                    await self.ws.send_bytes(bytes([t]) + b)
                if self.frame is not None:
                    f = self.frame
                    self.frame = None
                    await self.ws.send_bytes(bytes([TYPE_FRAME]) + f)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            print(f"[Relay] Sender error: {exc}", flush=True)
            try:
                await self.ws.close()
            except Exception:
                pass

    def close(self):
        self.task.cancel()


def get_room(room_id):
    if room_id not in ROOMS:
        ROOMS[room_id] = {"host": None, "controllers": set()}
    return ROOMS[room_id]


def presence(room_id, room):
    return json.dumps({
        "type": "peer_status",
        "room": room_id,
        "host_connected": room["host"] is not None,
        "controllers_count": len(room["controllers"]),
    }).encode("utf-8")


def broadcast_controllers(room, msg_type, body):
    for ctrl in list(room["controllers"]):
        ctrl.push(msg_type, body)


def send_host(room, msg_type, body):
    if room["host"] is not None:
        room["host"].push(msg_type, body)


async def health(request):
    return web.Response(text="relay ok")


async def ws_handler(request):
    ws = web.WebSocketResponse(heartbeat=20, max_msg_size=MAX_FRAME_BYTES)
    await ws.prepare(request)
    peer = Peer(ws)
    addr = request.headers.get("X-Forwarded-For", request.remote)
    role = None
    room_id = "default"
    room = None
    try:
        first = await ws.receive(timeout=30)
        if first.type != web.WSMsgType.BINARY or first.data[0] != TYPE_CTRL:
            peer.push(TYPE_CTRL, json.dumps({"type": "error", "message": "Expected register frame"}).encode())
            await asyncio.sleep(0.2)
            return ws
        reg = json.loads(first.data[1:].decode("utf-8"))
        if reg.get("type") != "register":
            peer.push(TYPE_CTRL, json.dumps({"type": "error", "message": "First message must be register"}).encode())
            await asyncio.sleep(0.2)
            return ws
        role = str(reg.get("role", "")).strip().lower()
        room_id = str(reg.get("room", "default")).strip() or "default"
        if role not in ("host", "controller"):
            peer.push(TYPE_CTRL, json.dumps({"type": "error", "message": "Invalid role"}).encode())
            await asyncio.sleep(0.2)
            return ws

        room = get_room(room_id)
        if role == "host":
            old = room["host"]
            if old is not None and old is not peer:
                try:
                    await old.ws.close()
                except Exception:
                    pass
                old.close()
            room["host"] = peer
        else:
            room["controllers"].add(peer)

        print(f"[Relay] {role} connected in room='{room_id}' from {addr}", flush=True)
        peer.push(TYPE_CTRL, json.dumps({
            "type": "registered",
            "role": role,
            "room": room_id,
            "host_connected": room["host"] is not None,
            "controllers_count": len(room["controllers"]),
        }).encode("utf-8"))

        pres = presence(room_id, room)
        broadcast_controllers(room, TYPE_CTRL, pres)
        if role == "controller":
            send_host(room, TYPE_CTRL, pres)

        async for msg in ws:
            if msg.type != web.WSMsgType.BINARY or len(msg.data) < 1:
                continue
            m_type, m_body = msg.data[0], msg.data[1:]
            if m_type == TYPE_CTRL:
                try:
                    parsed = json.loads(m_body.decode("utf-8"))
                    if parsed.get("type") == "ping":
                        peer.push(TYPE_CTRL, json.dumps({
                            "type": "pong",
                            "host_connected": room["host"] is not None,
                            "controllers_count": len(room["controllers"]),
                        }).encode("utf-8"))
                        continue
                except Exception:
                    pass
            if role == "host":
                broadcast_controllers(room, m_type, m_body)
            else:
                send_host(room, m_type, m_body)
    except Exception as exc:
        print(f"[Relay] Error ({role}, room='{room_id}', peer={addr}): {exc}", flush=True)
    finally:
        peer.close()
        if room is not None:
            if role == "host" and room["host"] is peer:
                room["host"] = None
            elif role == "controller":
                room["controllers"].discard(peer)
            pres = presence(room_id, room)
            broadcast_controllers(room, TYPE_CTRL, pres)
            if role == "controller":
                send_host(room, TYPE_CTRL, pres)
        print(f"[Relay] {role or 'unknown'} left room='{room_id}' ({addr})", flush=True)
    return ws


def main():
    parser = argparse.ArgumentParser(description="Render Relay Server (WebSocket)")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "10000")))
    args = parser.parse_args()

    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/ws", ws_handler)
    print(f"[Relay] WebSocket relay listening on {args.host}:{args.port} (path /ws)", flush=True)
    web.run_app(app, host=args.host, port=args.port, print=None)


if __name__ == "__main__":
    main()
