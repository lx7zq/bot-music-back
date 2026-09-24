from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException, Header, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import hmac
import json
import os
import secrets
import time
import uuid
from collections import defaultdict
from datetime import date, timedelta

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
    _touch_guild(guild_id)  # กันเหนียวอีกชั้น (หลักอยู่ที่ /poll)
    s = state[guild_id]
    s.update({k: v for k, v in data.items() if k != "guild_id"})
    await broadcast({"type": "state_update", "guild_id": guild_id, "state": dict(s)}, guild_id=guild_id)
    return {"ok": True}


@app.get("/poll/{guild_id}")
async def poll(guild_id: str):
    """bot.py poll มาที่นี่ทุก 1 วินาที — ประทับ first_seen ดิสใหม่ตรงนี้ (~1วิหลังเชิญบอท)"""
    _touch_guild(guild_id)
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


# ── Billing: subscriptions (49฿/ดิส/30วัน) ─────────────────────────────────
# semi-manual: ลูกค้าส่งสลิป → ค้างใน pending → เจ้าของกด ✅/❌ ในดิส
# ช่อง verify_slip() เตรียมไว้เสียบ SlipOK ทีหลัง (ตอนนี้ตรวจมือ 100%)
SUBS_FILE = os.environ.get(
    "SUBSCRIPTIONS_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "subscriptions.json"),
)
SLIPS_DIR = os.environ.get(
    "SLIPS_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "slips"),
)
PLAN_DAYS = int(os.environ.get("PLAN_DAYS", "30") or 30)
GRACE_DAYS = int(os.environ.get("BILLING_GRACE_DAYS", "3") or 3)
TRIAL_DAYS = int(os.environ.get("TRIAL_DAYS", "30") or 0)  # 0 = ปิด trial ขายตรง
PLAN_PRICE = float(os.environ.get("PLAN_PRICE", "49") or 49)
PROMPTPAY_ID = os.environ.get("PROMPTPAY_ID", "")
os.makedirs(SLIPS_DIR, exist_ok=True)


def _load_subs() -> dict:
    try:
        with open(SUBS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return dict(data) if isinstance(data, dict) else {}
    except (FileNotFoundError, ValueError):
        return {}


def _save_subs(subs: dict) -> None:
    tmp = SUBS_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(subs, f)
    os.replace(tmp, SUBS_FILE)


subs: dict = _load_subs()
# pending_id → {pending_id, guild_id, guild_name, filename, status, created_at, notified}
pending_slips: dict = {}


def _today() -> date:
    return date.today()


def _touch_guild(guild_id: str) -> None:
    """ประทับ first_seen ครั้งแรกที่เห็นดิสนี้ — เตะออกแล้วเชิญใหม่ไม่รี (กันปั๊ม trial)"""
    if not guild_id:
        return
    entry = subs.get(guild_id)
    if entry is None:
        subs[guild_id] = {"first_seen": _today().isoformat(), "history": []}
        _save_subs(subs)
    elif not entry.get("first_seen"):
        entry["first_seen"] = _today().isoformat()
        _save_subs(subs)


def _trial_left(entry: dict) -> int:
    if TRIAL_DAYS <= 0:
        return 0
    first = entry.get("first_seen")
    if not first:
        return TRIAL_DAYS
    try:
        left = TRIAL_DAYS - (_today() - date.fromisoformat(first)).days
    except ValueError:
        return 0
    return max(0, left)


def sub_status(guild_id: str) -> dict:
    """จ่ายอยู่ไหม — นับ grace + trial ให้อัตโนมัติ"""
    entry = subs.get(guild_id, {})
    paid_until = entry.get("paid_until")
    if paid_until:
        try:
            until = date.fromisoformat(paid_until)
        except ValueError:
            until = None
        if until:
            today = _today()
            if today <= until:
                return {"paid": True, "paid_until": paid_until, "in_grace": False,
                        "trial": False, "trial_left": 0, "trial_expired": False}
            if today <= until + timedelta(days=GRACE_DAYS):
                return {"paid": False, "paid_until": paid_until, "in_grace": True,
                        "trial": False, "trial_left": 0, "trial_expired": False}
            return {"paid": False, "paid_until": paid_until, "in_grace": False,
                    "trial": False, "trial_left": 0, "trial_expired": False}
    left = _trial_left(entry)
    if left > 0:
        return {"paid": False, "paid_until": None, "in_grace": False,
                "trial": True, "trial_left": left, "trial_expired": False}
    if TRIAL_DAYS > 0 and entry.get("first_seen"):
        return {"paid": False, "paid_until": None, "in_grace": False,
                "trial": False, "trial_left": 0, "trial_expired": True}
    return {"paid": False, "paid_until": None, "in_grace": False,
            "trial": False, "trial_left": 0, "trial_expired": False}


def _sub_ok(st: dict) -> bool:
    return bool(st["paid"] or st["in_grace"] or st["trial"])


def extend_subscription(guild_id: str, days: int = PLAN_DAYS) -> str:
    """ต่ออายุ — เริ่มนับจากวันหมดของเดิมถ้ายังไม่หมด (ไม่โกงลูกค้า)"""
    entry = subs.get(guild_id, {})
    try:
        base = max(_today(), date.fromisoformat(entry.get("paid_until", "2000-01-01")))
    except ValueError:
        base = _today()
    new_until = (base + timedelta(days=days)).isoformat()
    hist = entry.get("history", [])
    hist.append({"date": _today().isoformat(), "days": days, "until": new_until})
    subs[guild_id] = {"paid_until": new_until, "history": hist[-20:]}
    _save_subs(subs)
    return new_until


async def verify_slip(_image_bytes: bytes) -> dict:
    """ช่องเสียบ auto-verify ทีหลัง (เช่น SlipOK) — ตอนนี้คืน manual เสมอ"""
    return {"auto": False, "reason": "manual-review"}


@app.get("/internal/subscription/{guild_id}")
async def internal_subscription(guild_id: str, x_bot_secret: str | None = Header(default=None)):
    _check_bot_secret(x_bot_secret)
    st = sub_status(guild_id)
    # grace + trial = ยังเล่นได้ (บอทนับ ok เป็นผ่าน)
    return {"guild_id": guild_id, **st, "ok": _sub_ok(st)}


@app.get("/billing/status/{guild_id}")
async def billing_status(guild_id: str):
    st = sub_status(guild_id)
    return {"guild_id": guild_id, **st}


@app.get("/billing/pending/{pending_id}")
async def billing_pending_status(pending_id: str):
    """หน้าเว็บ poll ใบนี้หลังส่งสลิป — รู้เองว่า approve/reject แล้วโดยไม่ต้องกดรีเฟรช"""
    p = pending_slips.get(pending_id)
    if not p:
        raise HTTPException(status_code=404, detail="not found")
    return {
        "pending_id": pending_id,
        "status": p["status"],
        "guild_id": p["guild_id"],
        "paid_until": subs.get(p["guild_id"], {}).get("paid_until"),
    }


@app.get("/billing/config")
async def billing_config():
    """ราคา+พร้อมเพย์ให้หน้า /pricing (ไม่ hardcode ลง repo frontend)"""
    return {
        "price": PLAN_PRICE,
        "plan_days": PLAN_DAYS,
        "grace_days": GRACE_DAYS,
        "trial_days": TRIAL_DAYS,
        "promptpay_id": PROMPTPAY_ID,
    }


@app.get("/public/stats")
async def public_stats():
    """สถิติสาธารณะโชว์หน้า landing (นับเฉพาะดิสที่บอทเคยเห็น)"""
    guilds = len(state)
    playing = sum(1 for s in state.values() if s.get("is_playing"))
    listeners = sum(int(s.get("listeners") or 0) for s in state.values())
    pings = [s.get("ping_ms") for s in state.values() if isinstance(s.get("ping_ms"), (int, float))]
    return {
        "guilds": guilds,
        "playing": playing,
        "listeners": listeners,
        "ping_ms": round(max(pings)) if pings else None,
    }


@app.get("/public/live")
async def public_live(limit: int = 5):
    """เพลงที่กำลังเล่นตอนนี้แบบนิรนาม — มีแค่ชื่อ+ปก ไม่มีชื่อดิส/คนขอ"""
    live = []
    for s in state.values():
        song = s.get("now_playing")
        if s.get("is_playing") and song:
            live.append({"title": song.get("title", "?"), "thumbnail": song.get("thumbnail", "")})
        if len(live) >= max(1, min(limit, 10)):
            break
    return {"live": live}


@app.post("/billing/submit")
async def billing_submit(guild_id: str, guild_name: str = "", slip: UploadFile = File(...)):
    """ลูกค้าอัปโหลดสลิป — ตรวจมือ: เก็บไฟล์ + ลง pending ให้เจ้าของกดในดิส"""
    if not guild_id.strip():
        raise HTTPException(status_code=400, detail="guild_id required")
    content = await slip.read()
    if not content or len(content) > 5 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="invalid slip image")
    pending_id = uuid.uuid4().hex[:12]
    ext = (slip.filename or "").rsplit(".", 1)[-1].lower()[:4] or "png"
    if ext not in ("png", "jpg", "jpeg", "webp"):
        ext = "png"
    path = os.path.join(SLIPS_DIR, f"{pending_id}.{ext}")
    with open(path, "wb") as f:
        f.write(content)
    await verify_slip(content)  # ตอนนี้ manual เสมอ — อนาคตเสียบ SlipOK ตรงนี้
    pending_slips[pending_id] = {
        "pending_id": pending_id,
        "guild_id": guild_id.strip(),
        "guild_name": guild_name.strip(),
        "filename": os.path.basename(path),
        "status": "pending",
        "created_at": time.strftime("%Y-%m-%d %H:%M"),
        "notified": False,
    }
    return {"ok": True, "pending_id": pending_id, "status": "pending"}


@app.get("/internal/pending")
async def internal_pending(x_bot_secret: str | None = Header(default=None)):
    _check_bot_secret(x_bot_secret)
    return {"pending": [p for p in pending_slips.values() if p["status"] == "pending"]}


@app.post("/internal/pending/{pending_id}/notified")
async def internal_notified(pending_id: str, x_bot_secret: str | None = Header(default=None)):
    _check_bot_secret(x_bot_secret)
    if pending_id in pending_slips:
        pending_slips[pending_id]["notified"] = True
    return {"ok": True}


@app.get("/internal/slip/{pending_id}")
async def internal_slip(pending_id: str, x_bot_secret: str | None = Header(default=None)):
    _check_bot_secret(x_bot_secret)
    from fastapi.responses import FileResponse
    p = pending_slips.get(pending_id)
    if not p:
        raise HTTPException(status_code=404, detail="not found")
    path = os.path.join(SLIPS_DIR, p["filename"])
    if not os.path.isfile(path):
        raise HTTPException(status_code=404, detail="file gone")
    return FileResponse(path)


@app.post("/internal/billing/approve")
async def internal_approve(request: Request, x_bot_secret: str | None = Header(default=None)):
    _check_bot_secret(x_bot_secret)
    data = await request.json()
    p = pending_slips.get(data.get("pending_id", ""))
    if not p or p["status"] != "pending":
        raise HTTPException(status_code=404, detail="pending not found")
    new_until = extend_subscription(p["guild_id"])
    p["status"] = "approved"
    # ลบรูปสลิปหลังตรวจเสร็จ (ไม่เก็บข้อมูลลูกค้าไว้)
    try:
        os.remove(os.path.join(SLIPS_DIR, p["filename"]))
    except OSError:
        pass
    return {"ok": True, "guild_id": p["guild_id"], "paid_until": new_until}


@app.post("/internal/billing/reject")
async def internal_reject(request: Request, x_bot_secret: str | None = Header(default=None)):
    _check_bot_secret(x_bot_secret)
    data = await request.json()
    p = pending_slips.get(data.get("pending_id", ""))
    if not p or p["status"] != "pending":
        raise HTTPException(status_code=404, detail="pending not found")
    p["status"] = "rejected"
    try:
        os.remove(os.path.join(SLIPS_DIR, p["filename"]))
    except OSError:
        pass
    return {"ok": True}
# ── Dashboard capability (หน้าเว็บถามว่าปุ่มแดงกดได้ไหม) ──────────────────
@app.get("/capability/{guild_id}")
async def capability(guild_id: str, key: str = ""):
    """key ถูก → can_control=true (ปุ่มแดงเปิด) / ไม่มี key → ดู+ขอเพลงได้อย่างเดียว"""
    st = sub_status(guild_id)
    return {
        "guild_id": guild_id,
        "can_control": check_key(guild_id, key),
        "sub_ok": _sub_ok(st),
        "paid_until": st["paid_until"],
        "trial": st["trial"],
        "trial_left": st["trial_left"],
    }


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

