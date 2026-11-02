# language: Python, file: main.py, target: Render Web Service
import asyncio
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import time
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from pathlib import Path

import asyncpg
import jwt
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("tracker")

DATABASE_URL = os.environ["DATABASE_URL"]
JWT_SECRET = os.environ["JWT_SECRET"]
BOT_KEY = os.environ["BOT_KEY"]

BASE_DIR = Path(__file__).parent
TOKEN_TTL = 30 * 86400
MAX_TARGETS = 50
MAX_STATE_BYTES = 5 * 1024 * 1024
STALE_SESSION_SEC = 24 * 3600
ID_RE = re.compile(r"^\d{17,20}$")
UID_RE = re.compile(r"^\d{5,30}$")
NAME_RE = re.compile(r"^[\w.\-]{3,32}$")
EMPTY_STATE = {"targets": {}, "global_feed": []}

state_lock = asyncio.Lock()

def format_duration(sec):
    h, m, s = int(sec // 3600), int((sec % 3600) // 60), int(sec % 60)
    if h > 0:
        return f"{h}ч {m}м"
    if m > 0:
        return f"{m}м {s}с"
    return f"{s}с"

def format_total(sec):
    sec = int(sec)
    h, m, s = sec // 3600, (sec % 3600) // 60, sec % 60
    if h > 0:
        return f"{h}ч {m}м {s}с"
    if m > 0:
        return f"{m}м {s}с"
    return f"{s}с"

def parse_dur(text):
    total = 0
    for pat, mul in ((r"(\d+)\s*ч", 3600), (r"(\d+)\s*м", 60), (r"(\d+)\s*с", 1)):
        m = re.search(pat, text or "")
        if m:
            total += int(m.group(1)) * mul
    return total

def compute_closed_sec(sessions):
    total = 0.0
    for s in sessions:
        if s.get("dur") != "В сети":
            total += parse_dur(s.get("dur", ""))
    return total

def voice_sec_for(t, now_ts):
    u = t.get("user") or {}
    stored = float(u.get("voice_sec") or 0)
    closed = compute_closed_sec(t.get("sessions") or [])
    base = max(stored, closed)
    active = next((s for s in (t.get("sessions") or []) if s.get("dur") == "В сети"), None)
    if active and active.get("start_ts"):
        delta = max(0.0, now_ts - float(active["start_ts"]))
        if delta > STALE_SESSION_SEC:
            return base + 0.0
        return base + delta
    return base

def is_online(t, now_ts):
    for s in (t.get("sessions") or []):
        if s.get("dur") == "В сети" and s.get("start_ts"):
            if (now_ts - float(s["start_ts"])) < STALE_SESSION_SEC:
                return True
    return False

def display_target(data):
    if not data:
        return data
    now_ts = time.time()
    result = {**data}
    sessions = []
    for s in (data.get("sessions") or []):
        s2 = dict(s)
        if s2.get("dur") == "В сети" and s2.get("start_ts"):
            if (now_ts - float(s2["start_ts"])) > STALE_SESSION_SEC:
                s2["dur"] = format_duration(now_ts - float(s2["start_ts"]))
                s2["per"] = ""
                s2.pop("start_ts", None)
        sessions.append(s2)
    result["sessions"] = sessions
    sec = voice_sec_for(data, now_ts)
    u = dict(result.get("user") or {})
    u["voice_sec"] = int(sec)
    u["voice"] = format_total(sec)
    result["user"] = u
    return result

def merge_known(old, new):
    if not old:
        return new
        
    # Защита от старых записей, сохраненных в виде строк (двойной JSON)
    if isinstance(old, str):
        try:
            old = json.loads(old)
        except Exception:
            old = {}
    if not isinstance(old, dict):
        old = {}

    result = dict(new)
    
    seen = set()
    merged_media = []
    for m in (new.get("media") or []) + (old.get("media") or []):
        url = m.get("s") if isinstance(m, dict) else None
        if url and url not in seen:
            seen.add(url)
            merged_media.append(m)
    result["media"] = merged_media[:100]
    
    seen = set()
    merged_nicks = []
    for n in (new.get("nicks") or []) + (old.get("nicks") or []):
        if isinstance(n, list) and len(n) >= 3:
            key = (n[0], n[2])
            if key not in seen:
                seen.add(key)
                merged_nicks.append(n)
    result["nicks"] = merged_nicks[:100]
    
    new_sessions = new.get("sessions") or []
    old_sessions = old.get("sessions") or []
    new_open = [s for s in new_sessions if s.get("dur") == "В сети"]
    closed = [s for s in new_sessions if s.get("dur") != "В сети"]
    seen = {(s.get("srv"), s.get("ch"), s.get("per")) for s in closed}
    for s in old_sessions:
        if s.get("dur") == "В сети":
            continue
        key = (s.get("srv"), s.get("ch"), s.get("per"))
        if key not in seen:
            closed.append(s)
            seen.add(key)
    result["sessions"] = (new_open + closed)[:500]
    
    old_u = old.get("user") or {}
    new_u = result.get("user") or {}
    old_vs = float(old_u.get("voice_sec") or 0)
    new_vs = float(new_u.get("voice_sec") or 0)
    new_u["voice_sec"] = max(old_vs, new_vs)
    result["user"] = new_u
    result["last_seen"] = max(old.get("last_seen", 0), new.get("last_seen", 0))
    return result


def hash_pw(pw: str) -> str:
    salt = secrets.token_bytes(16)
    h = hashlib.scrypt(pw.encode(), salt=salt, n=2**14, r=8, p=1, dklen=32)
    return f"scrypt${salt.hex()}${h.hex()}"

def verify_pw(pw: str, stored: str) -> bool:
    if stored.startswith("scrypt$"):
        _, salt, h = stored.split("$")
        calc = hashlib.scrypt(pw.encode(), salt=bytes.fromhex(salt), n=2**14, r=8, p=1, dklen=32)
        return hmac.compare_digest(calc.hex(), h)
    return hmac.compare_digest(hashlib.sha256(pw.encode()).hexdigest(), stored)

def is_legacy(stored: str) -> bool:
    return not stored.startswith("scrypt$")

_fails: dict[str, list[float]] = {}

def _client_ip(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for", "")
    return (fwd.split(",")[0].strip() or (request.client.host if request.client else "?"))

def rl_check(key: str):
    now = time.time()
    q = [t for t in _fails.get(key, []) if now - t < 300]
    _fails[key] = q
    if len(q) >= 8:
        raise HTTPException(429, "Слишком много попыток. Подождите пару минут.")

def rl_fail(key: str):
    if len(_fails) > 5000:
        _fails.clear()
    _fails.setdefault(key, []).append(time.time())

def make_token(uid: int) -> str:
    return jwt.encode({"uid": uid, "exp": int(time.time()) + TOKEN_TTL}, JWT_SECRET, "HS256")

async def current_user(request: Request) -> int:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(401, "Нужен вход")
    try:
        return int(jwt.decode(auth[7:], JWT_SECRET, algorithms=["HS256"])["uid"])
    except Exception:
        raise HTTPException(401, "Сессия устарела. Войдите снова.")

async def bot_only(request: Request):
    key = request.headers.get("X-Bot-Key", "")
    if not hmac.compare_digest(key.encode(), BOT_KEY.encode()):
        raise HTTPException(403, "Forbidden")

class AuthData(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=1, max_length=128)

class FeedItem(BaseModel):
    user: str = Field(default="", max_length=100)
    user_id: str = Field(max_length=32)
    url: str = Field(max_length=500)
    time: str = Field(default="", max_length=40)

class FeedIn(BaseModel):
    items: list[FeedItem] = Field(max_length=200)

class TrackIn(BaseModel):
    items: dict[str, dict] = Field(default_factory=dict)

def parse_ts(s: str) -> datetime:
    try:
        d = datetime.fromisoformat(s)
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)

FEED_INSERT = ("INSERT INTO feed (user_id, username, url, ts) VALUES ($1, $2, $3, $4) "
               "ON CONFLICT DO NOTHING")

def feed_row(r) -> dict:
    return {"id": r["id"], "type": "avatar", "user": r["username"], "user_id": r["user_id"],
            "url": r["url"], "time": r["ts"].isoformat()}

class TargetData(BaseModel):
    target_id: str = Field(max_length=32)
    target_name: str = Field(default="", max_length=100)

async def _init_conn(conn):
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DATABASE_URL, init=_init_conn, min_size=1, max_size=5)
    async with app.state.pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username TEXT UNIQUE,
                password TEXT
            );
            CREATE TABLE IF NOT EXISTS targets (
                id SERIAL PRIMARY KEY,
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                target_id TEXT,
                target_name TEXT
            );
            CREATE TABLE IF NOT EXISTS bot_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                data JSONB
            );
            CREATE TABLE IF NOT EXISTS feed (
                id BIGSERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                username TEXT,
                url TEXT NOT NULL,
                ts TIMESTAMPTZ NOT NULL,
                UNIQUE (user_id, url, ts)
            );
            CREATE TABLE IF NOT EXISTS known_users (
                id TEXT PRIMARY KEY,
                data JSONB NOT NULL,
                last_seen TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS known_users_last_seen ON known_users (last_seen DESC);
            ALTER TABLE targets DROP CONSTRAINT IF EXISTS targets_target_id_key;
            CREATE UNIQUE INDEX IF NOT EXISTS targets_user_target_uq ON targets (user_id, target_id);
        ''')
        row = await conn.fetchrow("SELECT data FROM bot_state WHERE id=1")
    app.state.store = row["data"] if row and isinstance(row["data"], dict) else dict(EMPTY_STATE)
    app.state.store.pop("known_users", None)
    old_feed = app.state.store.get("global_feed") or []
    if old_feed:
        rows = []
        for f in reversed(old_feed):
            try:
                rows.append((str(f["user_id"]), str(f.get("user", "")), str(f["url"]), parse_ts(f.get("time", ""))))
            except Exception:
                continue
        app.state.store["global_feed"] = []
        async with app.state.pool.acquire() as conn:
            await conn.executemany(FEED_INSERT, rows)
            await conn.execute(
                "INSERT INTO bot_state (id, data) VALUES (1, $1) ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data",
                app.state.store,
            )
    yield
    await app.state.pool.close()

app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

@app.middleware("http")
async def security_headers(request: Request, call_next):
    resp = await call_next(request)
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["Referrer-Policy"] = "no-referrer"
    return resp

@app.get("/")
async def index():
    return FileResponse(BASE_DIR / "index.html")

@app.post("/api/register")
async def register(data: AuthData, request: Request):
    username = data.username.strip()
    if not NAME_RE.match(username):
        raise HTTPException(400, "Логин: 3–32 символа (буквы, цифры, _ . -)")
    if len(data.password) < 6:
        raise HTTPException(400, "Пароль минимум 6 символов")
    pw_hash = await asyncio.to_thread(hash_pw, data.password)
    async with request.app.state.pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                "INSERT INTO users (username, password) VALUES ($1, $2) RETURNING id, username",
                username, pw_hash,
            )
        except asyncpg.UniqueViolationError:
            raise HTTPException(400, "Пользователь уже существует")
    return {"status": "ok", "user_id": row["id"], "username": row["username"], "token": make_token(row["id"])}

@app.post("/api/login")
async def login(data: AuthData, request: Request):
    username = data.username.strip()
    rl_key = f"{_client_ip(request)}|{username.lower()}"
    rl_check(rl_key)
    async with request.app.state.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id, username, password FROM users WHERE username=$1", username)
        ok = bool(row) and await asyncio.to_thread(verify_pw, data.password, row["password"])
        if not ok:
            rl_fail(rl_key)
            raise HTTPException(401, "Неверный логин или пароль")
        if is_legacy(row["password"]):
            new_hash = await asyncio.to_thread(hash_pw, data.password)
            await conn.execute("UPDATE users SET password=$1 WHERE id=$2", new_hash, row["id"])
    _fails.pop(rl_key, None)
    return {"status": "ok", "user_id": row["id"], "username": row["username"], "token": make_token(row["id"])}

@app.get("/api/my_targets")
async def get_user_targets(request: Request, uid: int = Depends(current_user)):
    async with request.app.state.pool.acquire() as conn:
        rows = await conn.fetch("SELECT target_id, target_name FROM targets WHERE user_id=$1 ORDER BY id", uid)
    return [{"target_id": r["target_id"], "target_name": r["target_name"]} for r in rows]

@app.post("/api/add_target")
async def add_target(data: TargetData, request: Request, uid: int = Depends(current_user)):
    tid = data.target_id.strip()
    if not ID_RE.match(tid):
        raise HTTPException(400, "Discord ID — это 17–20 цифр")
    async with request.app.state.pool.acquire() as conn:
        count = await conn.fetchval("SELECT count(*) FROM targets WHERE user_id=$1", uid)
        if count >= MAX_TARGETS:
            raise HTTPException(400, f"Максимум целей: {MAX_TARGETS}")
        try:
            await conn.execute(
                "INSERT INTO targets (user_id, target_id, target_name) VALUES ($1, $2, $3)",
                uid, tid, data.target_name.strip() or tid,
            )
        except asyncpg.UniqueViolationError:
            raise HTTPException(400, "Этот ID уже отслеживается")
        except asyncpg.ForeignKeyViolationError:
            raise HTTPException(401, "Сессия устарела. Войдите снова.")
    return {"status": "ok"}

@app.delete("/api/remove_target/{target_id}")
async def remove_target(target_id: str, request: Request, uid: int = Depends(current_user)):
    async with request.app.state.pool.acquire() as conn:
        await conn.execute("DELETE FROM targets WHERE user_id=$1 AND target_id=$2", uid, target_id)
    return {"status": "ok"}

@app.get("/api/data")
async def get_data(request: Request, uid: int = Depends(current_user)):
    async with request.app.state.pool.acquire() as conn:
        rows = await conn.fetch("SELECT target_id FROM targets WHERE user_id=$1", uid)
        feed = await conn.fetch("SELECT id, user_id, username, url, ts FROM feed ORDER BY id DESC LIMIT 100")
    mine = {r["target_id"] for r in rows}
    store = request.app.state.store
    return {
        "targets": {k: v for k, v in store.get("targets", {}).items() if k in mine},
        "global_feed": [feed_row(r) for r in feed],
    }

@app.get("/api/user/{user_id}")
async def get_user(user_id: str, request: Request, uid: int = Depends(current_user)):
    if not UID_RE.match(user_id):
        raise HTTPException(400, "Неверный ID")
    store = request.app.state.store
    target = store.get("targets", {}).get(user_id)
    if target:
        return {"target": display_target(target), "is_known": True, "is_target": True}
    async with request.app.state.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT data FROM known_users WHERE id=$1", user_id)
    if not row:
        raise HTTPException(404, "Пользователь ещё не встречался боту")
    return {"target": display_target(row["data"]), "is_known": True, "is_target": False}

@app.get("/api/top_voice")
async def top_voice(request: Request, limit: int = 100, uid: int = Depends(current_user)):
    limit = max(1, min(limit, 200))
    now_ts = time.time()
    store = request.app.state.store
    out = []
    for tid, t in (store.get("targets") or {}).items():
        sec = voice_sec_for(t, now_ts)
        if sec <= 0:
            continue
        u = t.get("user") or {}
        out.append({"id": tid, "name": u.get("display") or u.get("username") or tid,
                    "avatar": u.get("avatar_url") or "", "sec": int(sec), "online": is_online(t, now_ts)})
    out.sort(key=lambda x: x["sec"], reverse=True)
    return {"items": out[:limit]}

@app.get("/api/feed")
async def get_feed(request: Request, before: int = 0, limit: int = 100, uid: int = Depends(current_user)):
    limit = max(1, min(limit, 200))
    async with request.app.state.pool.acquire() as conn:
        if before > 0:
            rows = await conn.fetch(
                "SELECT id, user_id, username, url, ts FROM feed WHERE id < $1 ORDER BY id DESC LIMIT $2", before, limit)
        else:
            rows = await conn.fetch("SELECT id, user_id, username, url, ts FROM feed ORDER BY id DESC LIMIT $1", limit)
    return {"items": [feed_row(r) for r in rows], "has_more": len(rows) == limit}

@app.get("/api/bot_targets", dependencies=[Depends(bot_only)])
async def get_all_targets(request: Request):
    async with request.app.state.pool.acquire() as conn:
        rows = await conn.fetch("SELECT DISTINCT target_id FROM targets")
    return [r["target_id"] for r in rows]

@app.get("/api/bot_state", dependencies=[Depends(bot_only)])
async def bot_state(request: Request):
    return request.app.state.store

@app.post("/api/feed", dependencies=[Depends(bot_only)])
async def add_feed(data: FeedIn, request: Request):
    rows = [(i.user_id, i.user, i.url, parse_ts(i.time)) for i in data.items if i.url.startswith("http")]
    if rows:
        async with request.app.state.pool.acquire() as conn:
            await conn.executemany(FEED_INSERT, rows)
    return {"status": "ok", "received": len(rows)}

@app.post("/api/track", dependencies=[Depends(bot_only)])
async def track_known(data: TrackIn, request: Request):
    if not data.items:
        return {"status": "ok", "count": 0}
    if len(data.items) > 1000:
        raise HTTPException(400, "Слишком много записей за раз")
    valid = {uid: kdata for uid, kdata in data.items.items() if UID_RE.match(uid) and isinstance(kdata, dict)}
    if not valid:
        return {"status": "ok", "count": 0}
    async with request.app.state.pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, data FROM known_users WHERE id = ANY($1::text[])", list(valid.keys()))
    existing = {r["id"]: r["data"] for r in rows}
    
    to_upsert = []
    for uid, kdata in valid.items():
        merged = merge_known(existing.get(uid), kdata)
        # ИСПРАВЛЕНИЕ: передаем dict напрямую, без json.dumps
        to_upsert.append((uid, merged))
        
    async with request.app.state.pool.acquire() as conn:
        await conn.executemany(
            "INSERT INTO known_users (id, data, last_seen) VALUES ($1, $2, now()) "
            "ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data, last_seen = now()",
            to_upsert
        )
    return {"status": "ok", "count": len(to_upsert)}

@app.post("/api/update", dependencies=[Depends(bot_only)])
async def update_data(request: Request):
    raw = await request.body()
    if len(raw) > MAX_STATE_BYTES:
        raise HTTPException(413, "Слишком большое состояние")
    try:
        new_data = json.loads(raw)
    except ValueError:
        raise HTTPException(400, "Некорректный JSON")
    if not isinstance(new_data, dict) or not isinstance(new_data.get("targets"), dict):
        raise HTTPException(400, "Нет поля targets")
    new_data.pop("known_users", None)
    new_data["global_feed"] = []

    async with state_lock:
        store = request.app.state.store
        async with request.app.state.pool.acquire() as conn:
            if not new_data["targets"] and store.get("targets"):
                if await conn.fetchval("SELECT count(*) FROM targets") > 0:
                    return {"status": "ignored_empty"}
            if new_data == store:
                return {"status": "unchanged"}
            await conn.execute(
                "INSERT INTO bot_state (id, data) VALUES (1, $1) "
                "ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data",
                new_data,
            )
        request.app.state.store = new_data
    return {"status": "ok"}

@app.get("/api/ping")
async def ping():
    return {"status": "alive"}