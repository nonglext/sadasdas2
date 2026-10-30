# language: Python, file: main.py, target: Render Web Service
# pip install fastapi uvicorn

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Тут мы храним данные
STORE_DATA = {
    "user": None, "lastVoice": None, "servers": [], 
    "nicks": [], "media": [], "friends": [], "sessions": []
}

# Отдаем HTML страницу
@app.get("/", response_class=HTMLResponse)
async def index():
    return FileResponse("index.html")

# Отдаем данные для фронтенда
@app.get("/api/data")
async def get_data():
    return STORE_DATA

# Сюда локальный бот будет присылать данные
@app.post("/api/update")
async def update_data(request: Request):
    global STORE_DATA
    data = await request.json()
    # Просто перезаписываем данные
    STORE_DATA = data
    return {"status": "ok"}

# Пинг для Render, чтобы сервер не засыпал
@app.get("/api/ping")
async def ping():
    return {"status": "alive"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=10000)