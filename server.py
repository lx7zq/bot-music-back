from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from collections import defaultdict

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

state: dict = defaultdict(lambda: {
    "now_playing": None,
    "queue": [],
    "is_playing": False,
    "is_paused": False,
    "volume": 50,
    "guild_name": "",
    "channel_name": "",
})

clients: list[WebSocket] = []
pending_commands: dict = {}   # guild_id → {command, ...extra}

# bot.py (_handle_dashboard_cmd) รู้จักแค่
# skip|pause|resume|stop|restart|volume|add_song|remove_song
# 'prev' จากปุ่ม ⏮ ในเว็บ → แปลงเป็น 'restart' (เล่นเพลงปัจจุบันใหม่)
ACTION_ALIASES = {"prev": "restart"}


def _store_command(guild_id: str, action: str, data: dict) -> dict:
    command = ACTION_ALIASES.get(action, action)
    entry = {"command": command, **{k: v for k, v in data.items() if k != "command"}}
    pending_commands[guild_id] = entry
    return entry


async def broadcast(data: dict):
    dead = []
    for ws in clients:
        try:
            await ws.send_json(data)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.remove(ws)


# ── WebSocket ──────────────────────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.append(websocket)
    await websocket.send_json({"type": "full_state", "state": dict(state)})
    try:
        while True:
            data = await websocket.receive_json()
            guild_id = data.get("guild_id")
            action = data.get("action")
            # เก็บ command พร้อม extra data (เช่น value ของ volume)
            _store_command(guild_id, action, data)
            if action == "volume":
                state[guild_id]["volume"] = data.get("value", 50)
                await broadcast({"type": "state_update", "guild_id": guild_id, "state": dict(state[guild_id])})
    except WebSocketDisconnect:
        if websocket in clients:
            clients.remove(websocket)


# ── Bot → Server ───────────────────────────────────────────────────────────
@app.post("/update")
async def update(request: Request):
    data = await request.json()
    guild_id = str(data.get("guild_id"))
    s = state[guild_id]
    s.update({k: v for k, v in data.items() if k != "guild_id"})
    await broadcast({"type": "state_update", "guild_id": guild_id, "state": dict(s)})
    return {"ok": True}


@app.get("/poll/{guild_id}")
async def poll(guild_id: str):
    """bot.py poll มาที่นี่ทุก 1 วินาที"""
    entry = pending_commands.pop(guild_id, None)
    if entry is None:
        return {"command": None}
    return entry   # { command, guild_id, value?, query?, index? }


# ── Dashboard → Bot (control commands via HTTP fallback) ─────────────────
# frontend ยิงมาที่นี่ถ้า WS ไม่พร้อม (Render sleep / env scheme ผิด / firewall)
@app.post("/command")
async def command(request: Request):
    data = await request.json()
    guild_id = str(data.get("guild_id"))
    action = data.get("action") or data.get("command", "")
    entry = _store_command(guild_id, action, data)
    if entry["command"] == "volume":
        state[guild_id]["volume"] = data.get("value", 50)
        await broadcast({"type": "state_update", "guild_id": guild_id, "state": dict(state[guild_id])})
    return {"ok": True}


# ── Dashboard → Bot (add/remove song) ─────────────────────────────────────
@app.post("/add_song")
async def add_song(request: Request):
    data = await request.json()
    guild_id = str(data.get("guild_id"))
    pending_commands[guild_id] = {"command": "add_song", "query": data.get("query", "")}
    return {"ok": True}


@app.post("/remove_song")
async def remove_song(request: Request):
    data = await request.json()
    guild_id = str(data.get("guild_id"))
    pending_commands[guild_id] = {"command": "remove_song", "index": data.get("index", 0)}
    return {"ok": True}


@app.get("/state")
async def get_state():
    return dict(state)

