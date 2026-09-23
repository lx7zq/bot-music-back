from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import hmac
import json
import os
import secrets
from collections import defaultdict

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Dashboard keys (per-guild) ─────────────────────────────────────────────
# กุญแจต่อดิสสำหรับปุ่มแดงบนเว็บ (stop/clear/volume/remove/leave)
# ไม่มี login — ใครถือลิงก์ ?key= ถูกต้อง = เคยผ่านด่าน DJ ในดิสมาตอนขอลิงก์
KEY_FILE = os.environ.get(
    "DASHBOARD_KEYS_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard_keys.json"),
)
BOT_SECRET = os.environ.get("BOT_SECRET", "change-me-in-prod")

# แอ็กชันที่ต้องมี key (ปุ่มแดง) — นอกนั้น (ดู/pause/skip/ขอเพลง) เปิดสาธารณะ
RED_ACTIONS = {"stop", "volume", "remove_song"}


def _load_keys() -> dict:
    try:
        with open(KEY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return dict(data) if isinstance(data, dict) else {}
    except (FileNotFoundError, ValueError):
        return {}


def _save_keys(keys: dict) -> None:
    tmp = KEY_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(keys, f)
    os.replace(tmp, KEY_FILE)


dashboard_keys: dict = _load_keys()


def get_or_create_key(guild_id: str) -> str:
    key = dashboard_keys.get(guild_id)
    if not key:
        key = secrets.token_urlsafe(32)
        dashboard_keys[guild_id] = key
        _save_keys(dashboard_keys)
    return key


def rotate_key(guild_id: str) -> str:
    key = secrets.token_urlsafe(32)
    dashboard_keys[guild_id] = key
    _save_keys(dashboard_keys)
    return key


def check_key(guild_id: str, key: str | None) -> bool:
    expected = dashboard_keys.get(guild_id)
    if not expected or not key:
        return False
    return hmac.compare_digest(expected, key)


def _check_bot_secret(x_bot_secret: str | None) -> None:
    if not hmac.compare_digest(x_bot_secret or "", BOT_SECRET):
        raise HTTPException(status_code=403, detail="forbidden")

state: dict = defaultdict(lambda: {
    "now_playing": None,
    "queue": [],
    "is_playing": False,
    "is_paused": False,
    "volume": 50,
    "guild_name": "",
    "channel_name": "",
})

clients: list[dict] = []  # {ws, guild_id|None}
pending_commands: dict = {}   # guild_id → {command, ...extra}

# bot.py (_handle_dashboard_cmd) รู้จักแค่
# skip|pause|resume|stop|restart|volume|add_song|remove_song
# 'prev' จากปุ่ม ⏮ ในเว็บ → แปลงเป็น 'restart' (เล่นเพลงปัจจุบันใหม่)
ACTION_ALIASES = {"prev": "restart"}


def _store_command(guild_id: str, action: str, data: dict) -> dict:
    command = ACTION_ALIASES.get(action, action)
    # ปุ่มแดงต้องมี key ถูกต้อง — กันคนสุ่ม guild_id มากดล้าง/เร่งเสียงดิสคนอื่น
    if command in RED_ACTIONS and not check_key(guild_id, data.get("key")):
        raise HTTPException(status_code=403, detail="dashboard key required")
    entry = {"command": command, **{k: v for k, v in data.items() if k not in ("command", "key")}}
    pending_commands[guild_id] = entry
    return entry


async def broadcast(data: dict, guild_id: str | None = None):
    dead = []
    for client in clients:
        ws = client["ws"]
        # state_update ส่งเฉพาะ client ที่เปิด guild นั้น (client ไม่มี filter = admin/debug รับหมด)
        if guild_id and client.get("guild_id") and client["guild_id"] != guild_id:
            continue
        try:
            await ws.send_json(data)
        except Exception:
            dead.append(client)
    for client in dead:
        if client in clients:
            clients.remove(client)


# ── WebSocket ──────────────────────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    # client ใหม่ (useGuildSocket) ส่ง ?guild_id= มาด้วย → ได้เห็นเฉพาะดิสตัวเอง
    # client เก่า/ไม่มี param → ได้ {} (ไม่ broadcast รวมทุกดิสแล้ว)
    want_guild = websocket.query_params.get("guild_id")
    client = {"ws": websocket, "guild_id": want_guild}
    clients.append(client)
    if want_guild:
        own = state.get(want_guild)
        await websocket.send_json(
            {"type": "full_state", "state": {want_guild: dict(own)} if own else {}}
        )
    else:
        await websocket.send_json({"type": "full_state", "state": {}})
    try:
        while True:
            data = await websocket.receive_json()
            guild_id = data.get("guild_id")
            action = data.get("action")
            try:
                # เก็บ command พร้อม extra data (เช่น value ของ volume)
                _store_command(guild_id, action, data)
            except HTTPException as e:
                await websocket.send_json({"type": "error", "message": e.detail})
                continue
            if action == "volume":
                state[guild_id]["volume"] = data.get("value", 50)
                await broadcast({"type": "state_update", "guild_id": guild_id, "state": dict(state[guild_id])}, guild_id=guild_id)
    except WebSocketDisconnect:
        if client in clients:
            clients.remove(client)


# ── Bot → Server ───────────────────────────────────────────────────────────
@app.post("/update")
async def update(request: Request):
    data = await request.json()
    guild_id = str(data.get("guild_id"))
    s = state[guild_id]
    s.update({k: v for k, v in data.items() if k != "guild_id"})
    await broadcast({"type": "state_update", "guild_id": guild_id, "state": dict(s)}, guild_id=guild_id)
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
        await broadcast({"type": "state_update", "guild_id": guild_id, "state": dict(state[guild_id])}, guild_id=guild_id)
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
    if not check_key(guild_id, data.get("key")):
        raise HTTPException(status_code=403, detail="dashboard key required")
    pending_commands[guild_id] = {"command": "remove_song", "index": data.get("index", 0)}
    return {"ok": True}


# ── Dashboard capability (หน้าเว็บถามว่าปุ่มแดงกดได้ไหม) ──────────────────
@app.get("/capability/{guild_id}")
async def capability(guild_id: str, key: str = ""):
    """key ถูก → can_control=true (ปุ่มแดงเปิด) / ไม่มี key → ดู+ขอเพลงได้อย่างเดียว"""
    return {"guild_id": guild_id, "can_control": check_key(guild_id, key)}


# ── Internal (bot เรียกใช้ ต้องมี X-Bot-Secret) ────────────────────────────
@app.get("/internal/key/{guild_id}")
async def internal_key(guild_id: str, x_bot_secret: str | None = Header(default=None)):
    _check_bot_secret(x_bot_secret)
    return {"guild_id": guild_id, "key": get_or_create_key(guild_id)}


@app.post("/internal/rotate/{guild_id}")
async def internal_rotate(guild_id: str, x_bot_secret: str | None = Header(default=None)):
    _check_bot_secret(x_bot_secret)
    return {"guild_id": guild_id, "key": rotate_key(guild_id)}


@app.get("/state")
async def get_state(x_bot_secret: str | None = Header(default=None)):
    """เหลือไว้ให้ bot/admin debug เท่านั้น — ต้องมี secret (เดิมเปิดสาธารณะทุกดิส)"""
    _check_bot_secret(x_bot_secret)
    return dict(state)


@app.get("/state/{guild_id}")
async def get_guild_state(guild_id: str):
    """สถานะดิสเดียวแบบอ่านอย่างเดียว (ข้อมูลสาธารณะบนหน้า dashboard อยู่แล้ว)"""
    s = state.get(guild_id)
    return dict(s) if s else {}

