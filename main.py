# language: Python, file: main.py, target: Render Web Service
import os
import asyncpg
import hashlib
import json
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from contextlib import asynccontextmanager

DATABASE_URL = os.getenv("DATABASE_URL")

# Глобальная переменная для быстрого доступа
STORE_DATA = {"targets": {}, "global_feed": []}

class AuthData(BaseModel):
    username: str
    password: str

class TargetData(BaseModel):
    target_id: str
    target_name: str

def hash_pw(pw): return hashlib.sha256(pw.encode()).hexdigest()

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DATABASE_URL)
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
                target_id TEXT UNIQUE,
                target_name TEXT
            );
            -- Таблица для хранения состояния бота (JSON)
            CREATE TABLE IF NOT EXISTS bot_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                data JSONB
            );
        ''')
    yield
    await app.state.pool.close()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/", response_class=HTMLResponse)
async def index(): return FileResponse("index.html")

# --- АВТОРИЗАЦИЯ ---
@app.post("/api/register")
async def register(data: AuthData, request: Request):
    async with request.app.state.pool.acquire() as conn:
        try:
            row = await conn.fetchrow(
                "INSERT INTO users (username, password) VALUES ($1, $2) RETURNING id, username",
                data.username, hash_pw(data.password)
            )
            return {"status": "ok", "user_id": row["id"], "username": row["username"]}
        except asyncpg.UniqueViolationError:
            raise HTTPException(400, "Пользователь уже существует")

@app.post("/api/login")
async def login(data: AuthData, request: Request):
    async with request.app.state.pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, username FROM users WHERE username=$1 AND password=$2",
            data.username, hash_pw(data.password)
        )
        if row: return {"status": "ok", "user_id": row["id"], "username": row["username"]}
        raise HTTPException(401, "Неверный логин или пароль")

# --- УПРАВЛЕНИЕ ЦЕЛЯМИ ---
@app.get("/api/my_targets/{user_id}")
async def get_user_targets(user_id: int, request: Request):
    async with request.app.state.pool.acquire() as conn:
        rows = await conn.fetch("SELECT target_id, target_name FROM targets WHERE user_id=$1", user_id)
        return [{"target_id": r["target_id"], "target_name": r["target_name"]} for r in rows]

@app.post("/api/add_target/{user_id}")
async def add_target(user_id: int, data: TargetData, request: Request):
    async with request.app.state.pool.acquire() as conn:
        try:
            await conn.execute(
                "INSERT INTO targets (user_id, target_id, target_name) VALUES ($1, $2, $3)",
                user_id, data.target_id, data.target_name
            )
            return {"status": "ok"}
        except asyncpg.UniqueViolationError:
            raise HTTPException(400, "Этот ID уже отслеживается")

@app.delete("/api/remove_target/{user_id}/{target_id}")
async def remove_target(user_id: int, target_id: str, request: Request):
    async with request.app.state.pool.acquire() as conn:
        await conn.execute("DELETE FROM targets WHERE user_id=$1 AND target_id=$2", user_id, target_id)
        return {"status": "ok"}

# --- ДЛЯ БОТА ---
@app.get("/api/bot_targets")
async def get_all_targets(request: Request):
    async with request.app.state.pool.acquire() as conn:
        rows = await conn.fetch("SELECT target_id FROM targets")
        return [r["target_id"] for r in rows]

@app.post("/api/update")
async def update_data(request: Request):
    global STORE_DATA
    STORE_DATA = await request.json()
    
    # СОХРАНЯЕМ ВСЁ В БАЗУ (JSONB)
    async with request.app.state.pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO bot_state (id, data) VALUES (1, $1)
            ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data
            """,
            json.dumps(STORE_DATA)
        )
    return {"status": "ok"}

@app.get("/api/data")
async def get_data(request: Request):
    global STORE_DATA
    # Если память пуста (после перезапуска), читаем из базы
    if not STORE_DATA.get("targets"):
        async with request.app.state.pool.acquire() as conn:
            row = await conn.fetchrow("SELECT data FROM bot_state WHERE id=1")
            if row and row["data"]:
                STORE_DATA = json.loads(row["data"])
    return STORE_DATA

@app.get("/api/ping")
async def ping(): return {"status": "alive"}