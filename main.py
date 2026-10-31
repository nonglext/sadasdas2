# language: Python, file: main.py, target: Render Web Service
# Env: DATABASE_URL, JWT_SECRET (длинная случайная строка), BOT_KEY (общий ключ с local_bot.py)
# Запуск: uvicorn main:app --host 0.0.0.0 --port $PORT   (один воркер)
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
ID_RE = re.compile(r"^\d{17,20}$")
NAME_RE = re.compile(r"^[\w.\-]{3,32}$")
EMPTY_STATE = {"targets": {}, "global_feed": []}

state_lock = asyncio.Lock()


# ───────────────────────── пароли ─────────────────────────
def hash_pw(pw: str) -> str:
    salt = secrets.token_bytes(16)
    h = hashlib.scrypt(pw.encode(), salt=salt, n=2**14, r=8, p=1, dklen=32)
    return f"scrypt${salt.hex()}${h.hex()}"


def verify_pw(pw: str, stored: str) -> bool:
    if stored.startswith("scrypt$"):
        _, salt, h = stored.split("$")
        calc = hashlib.scrypt(pw.encode(), salt=bytes.fromhex(salt), n=2**14, r=8, p=1, dklen=32)
        return hmac.compare_digest(calc.hex(), h)
    # старые аккаунты (sha256 без соли) — проверяем и перехешируем при входе
    return hmac.compare_digest(hashlib.sha256(pw.encode()).hexdigest(), stored)


def is_legacy(stored: str) -> bool:
    return not stored.startswith("scrypt$")


# ───────────────────────── rate-limit входа ─────────────────────────
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


# ───────────────────────── авторизация ─────────────────────────
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


# ───────────────────────── модели ─────────────────────────
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


# ───────────────────────── приложение ─────────────────────────
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
            ALTER TABLE targets DROP CONSTRAINT IF EXISTS targets_target_id_key;
            CREATE UNIQUE INDEX IF NOT EXISTS targets_user_target_uq ON targets (user_id, target_id);
        ''')
        row = await conn.fetchrow("SELECT data FROM bot_state WHERE id=1")
    app.state.store = row["data"] if row and isinstance(row["data"], dict) else dict(EMPTY_STATE)
    # перенос старой ленты из JSON-состояния в отдельную таблицу (без лимита)
    old_feed = app.state.store.get("global_feed") or []
    if old_feed:
        rows = []
        for f in reversed(old_feed):  # от старых к новым, чтобы id шли по времени
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
        log.info("Лента перенесена в таблицу: %d записей", len(rows))
    log.info("Старт. Целей в сохранённом состоянии: %d", len(app.state.store.get("targets", {})))
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


# ───────────────────────── аккаунты ─────────────────────────
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


# ───────────────────────── цели пользователя ─────────────────────────
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


# ───────────────────────── данные для сайта ─────────────────────────
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


@app.get("/api/feed")
async def get_feed(request: Request, before: int = 0, limit: int = 100, uid: int = Depends(current_user)):
    """Вся лента без лимита, постранично: before — id последней загруженной записи."""
    limit = max(1, min(limit, 200))
    async with request.app.state.pool.acquire() as conn:
        if before > 0:
            rows = await conn.fetch(
                "SELECT id, user_id, username, url, ts FROM feed WHERE id < $1 ORDER BY id DESC LIMIT $2", before, limit)
        else:
            rows = await conn.fetch("SELECT id, user_id, username, url, ts FROM feed ORDER BY id DESC LIMIT $1", limit)
    return {"items": [feed_row(r) for r in rows], "has_more": len(rows) == limit}


# ───────────────────────── эндпоинты бота ─────────────────────────
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
    new_data["global_feed"] = []  # лента живёт в отдельной таблице

    async with state_lock:
        store = request.app.state.store
        async with request.app.state.pool.acquire() as conn:
            # пустое состояние принимаем, только если целей в БД действительно нет
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