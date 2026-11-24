import argparse
import json
import os

from aiohttp import web

TYPE_FRAME = ord("J")
TYPE_CTRL = ord("C")
MAX_FRAME_BYTES = 16 * 1024 * 1024

ROOMS = {}


def get_room(room_id):
    if room_id not in ROOMS:
        ROOMS[room_id] = {"host": None, "controllers": set()}
    return ROOMS[room_id]


async def send(ws, msg_type, body):
    await ws.send_bytes(bytes([msg_type]) + body)


def presence(room_id, room):
    return json.dumps({
        "type": "peer_status",
        "room": room_id,
        "host_connected": room["host"] is not None,
        "controllers_count": len(room["controllers"]),
    }).encode("utf-8")


async def broadcast_controllers(room, msg_type, body):
    for ctrl in list(room["controllers"]):
        try:
            await send(ctrl, msg_type, body)
        except Exception:
            room["controllers"].discard(ctrl)


async def send_host(room, msg_type, body):
    host = room["host"]
    if host is None:
        return
    try:
        await send(host, msg_type, body)
    except Exception:
        if room["host"] is host:
            room["host"] = None


async def health(request):
    return web.Response(text="relay ok")


async def ws_handler(request):
    ws = web.WebSocketResponse(heartbeat=20, max_msg_size=MAX_FRAME_BYTES)
    await ws.prepare(request)
    peer = request.headers.get("X-Forwarded-For", request.remote)
    role = None
    room_id = "default"
    room = None
    try:
        first = await ws.receive(timeout=30)
        if first.type != web.WSMsgType.BINARY or first.data[0] != TYPE_CTRL:
            await send(ws, TYPE_CTRL, json.dumps({"type": "error", "message": "Expected register frame"}).encode())
            return ws
        reg = json.loads(first.data[1:].decode("utf-8"))
        if reg.get("type") != "register":
            await send(ws, TYPE_CTRL, json.dumps({"type": "error", "message": "First message must be register"}).encode())
            return ws
        role = str(reg.get("role", "")).strip().lower()
        room_id = str(reg.get("room", "default")).strip() or "default"
        if role not in ("host", "controller"):
            await send(ws, TYPE_CTRL, json.dumps({"type": "error", "message": "Invalid role"}).encode())
            return ws

        room = get_room(room_id)
        if role == "host":
            old = room["host"]
            if old is not None and old is not ws:
                try:
                    await old.close()
                except Exception:
                    pass
            room["host"] = ws
        else:
            room["controllers"].add(ws)

        print(f"[Relay] {role} connected in room='{room_id}' from {peer}", flush=True)
        await send(ws, TYPE_CTRL, json.dumps({
            "type": "registered",
            "role": role,
            "room": room_id,
            "host_connected": room["host"] is not None,
            "controllers_count": len(room["controllers"]),
        }).encode("utf-8"))

        pres = presence(room_id, room)
        await broadcast_controllers(room, TYPE_CTRL, pres)
        if role == "controller":
            await send_host(room, TYPE_CTRL, pres)

        async for msg in ws:
            if msg.type != web.WSMsgType.BINARY or len(msg.data) < 1:
                continue
            m_type, m_body = msg.data[0], msg.data[1:]
            if m_type == TYPE_CTRL:
                try:
                    parsed = json.loads(m_body.decode("utf-8"))
                    if parsed.get("type") == "ping":
                        await send(ws, TYPE_CTRL, json.dumps({"type": "pong"}).encode("utf-8"))
                        continue
                except Exception:
                    pass
            if role == "host":
                await broadcast_controllers(room, m_type, m_body)
            else:
                await send_host(room, m_type, m_body)
    except Exception as exc:
        print(f"[Relay] Error ({role}, room='{room_id}', peer={peer}): {exc}", flush=True)
    finally:
        if room is not None:
            if role == "host" and room["host"] is ws:
                room["host"] = None
            elif role == "controller":
                room["controllers"].discard(ws)
            pres = presence(room_id, room)
            await broadcast_controllers(room, TYPE_CTRL, pres)
            if role == "controller":
                await send_host(room, TYPE_CTRL, pres)
        print(f"[Relay] {role or 'unknown'} left room='{room_id}' ({peer})", flush=True)
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
