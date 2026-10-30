# language: Python, file: main.py, target: Render Web Service
# pip install fastapi uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn, sqlite3, hashlib, os

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

STORE_DATA = {"targets": {}} # Сюда бот шлет данные

def init_db():
    conn = sqlite3.connect('tracker.db')
    c = conn.cursor()
    # Таблица пользователей
    c.execute('''CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, username TEXT UNIQUE, password TEXT)''')
    # Таблица целей (привязка к пользователю)
    c.execute('''CREATE TABLE IF NOT EXISTS targets (id INTEGER PRIMARY KEY, user_id INTEGER, target_id TEXT UNIQUE, target_name TEXT)''')
    conn.commit()
    conn.close()

init_db()

def hash_pw(pw): return hashlib.sha256(pw.encode()).hexdigest()

class AuthData(BaseModel):
    username: str
    password: str

class TargetData(BaseModel):
    target_id: str
    target_name: str

@app.get("/", response_class=HTMLResponse)
async def index(): return FileResponse("index.html")

# --- АВТОРИЗАЦИЯ ---
@app.post("/api/register")
async def register(data: AuthData):
    conn = sqlite3.connect('tracker.db')
    c = conn.cursor()
    try:
        c.execute("INSERT INTO users (username, password) VALUES (?, ?)", (data.username, hash_pw(data.password)))
        conn.commit()
        user_id = c.lastrowid
        return {"status": "ok", "user_id": user_id, "username": data.username}
    except sqlite3.IntegrityError:
        raise HTTPException(400, "Пользователь уже существует")
    finally:
        conn.close()

@app.post("/api/login")
async def login(data: AuthData):
    conn = sqlite3.connect('tracker.db')
    c = conn.cursor()
    c.execute("SELECT id, username FROM users WHERE username=? AND password=?", (data.username, hash_pw(data.password)))
    row = c.fetchone()
    conn.close()
    if row: return {"status": "ok", "user_id": row[0], "username": row[1]}
    raise HTTPException(401, "Неверный логин или пароль")

# --- УПРАВЛЕНИЕ ЦЕЛЯМИ ---
@app.get("/api/my_targets/{user_id}")
async def get_user_targets(user_id: int):
    conn = sqlite3.connect('tracker.db')
    c = conn.cursor()
    c.execute("SELECT target_id, target_name FROM targets WHERE user_id=?", (user_id,))
    rows = c.fetchall()
    conn.close()
    return [{"target_id": r[0], "target_name": r[1]} for r in rows]

@app.post("/api/add_target/{user_id}")
async def add_target(user_id: int, data: TargetData):
    conn = sqlite3.connect('tracker.db')
    c = conn.cursor()
    try:
        c.execute("INSERT INTO targets (user_id, target_id, target_name) VALUES (?, ?, ?)", (user_id, data.target_id, data.target_name))
        conn.commit()
        return {"status": "ok"}
    except sqlite3.IntegrityError:
        raise HTTPException(400, "Этот ID уже отслеживается")
    finally:
        conn.close()

@app.delete("/api/remove_target/{user_id}/{target_id}")
async def remove_target(user_id: int, target_id: str):
    conn = sqlite3.connect('tracker.db')
    c = conn.cursor()
    c.execute("DELETE FROM targets WHERE user_id=? AND target_id=?", (user_id, target_id))
    conn.commit()
    conn.close()
    return {"status": "ok"}

# --- ДЛЯ БОТА ---
@app.get("/api/bot_targets")
async def get_all_targets():
    """Бот забирает список ВСЕХ ID, которые нужно мониторить"""
    conn = sqlite3.connect('tracker.db')
    c = conn.cursor()
    c.execute("SELECT target_id FROM targets")
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]

@app.post("/api/update")
async def update_data(request: Request):
    global STORE_DATA
    STORE_DATA = await request.json()
    return {"status": "ok"}

@app.get("/api/data")
async def get_data(): return STORE_DATA

@app.get("/api/ping")
async def ping(): return {"status": "alive"}

if __name__ == "__main__": uvicorn.run(app, host="0.0.0.0", port=10000)