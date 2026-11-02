# language: Python, file: local_bot.py, target: Local PC
# pip install discord.py-self aiohttp
import asyncio
import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timezone

import aiohttp
import discord

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bot")


def read_secret(env_name, filename):
    v = os.getenv(env_name)
    if v:
        return v.strip()
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            return f.readline().strip()
    return ""


TOKEN = read_secret("DISCORD_TOKEN", "token.txt")
BOT_KEY = read_secret("BOT_KEY", "bot_key.txt")
RENDER_URL = os.getenv("RENDER_URL", "https://tracker-dppb.onrender.com").rstrip("/")

PUSH_EVERY = 5
TARGETS_EVERY = 5
PROFILE_EVERY = 30
FETCH_EVERY = 600
KNOWN_MAX = 5000                          # сколько known_users держим в памяти
LIMITS = {"nicks": 100, "media": 100, "sessions": 500, "friends": 50}

client = discord.Client()
http: aiohttp.ClientSession = None

STATE = {"targets": {}, "known_users": {}, "global_feed": []}
LAST_VALUES, VOICE_START, VOICE_INFO, VOICE_TOTAL = {}, {}, {}, {}
FRIENDS_TRACKER, LAST_FETCH = {}, {}
NO_PROFILE_WARNED = set()
PENDING_FEED = []
RECENT_FEED = []
state_loaded = False
is_initialized = False
started = False
last_sent_hash = None


# ───────────────────────── утилиты ─────────────────────────
def now_iso():
    return datetime.now(timezone.utc).isoformat()


def hm(dt):
    return dt.astimezone().strftime("%H:%M")


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


def backfill_voice(t):
    total, by_srv = 0, {}
    for s in t["sessions"]:
        if s.get("dur") == "В сети":
            continue
        sec = parse_dur(s.get("dur"))
        total += sec
        by_srv[s.get("srv", "-")] = by_srv.get(s.get("srv", "-"), 0) + sec
    t["user"]["voice_sec"] = total
    t["user"]["voice"] = format_total(total)
    t["voice_srv"] = by_srv
    t["user"]["voice_v2"] = True


def blank_target(tid):
    return {
        "user": {"username": "unknown", "display": "unknown", "id": tid, "avatar_url": "",
                 "reg": "unknown", "voice": "0с", "voice_sec": 0, "views": 0},
        "lastVoice": {"server": "-", "channel": "-", "left": "-", "icon": ""},
        "servers": [], "nicks": [], "media": [], "friends": [], "sessions": [], "voice_srv": {},
    }


def ensure_target(tid):
    t = STATE["targets"].setdefault(tid, blank_target(tid))
    for k, v in blank_target(tid).items():
        t.setdefault(k, v)
    for k, v in blank_target(tid)["user"].items():
        t["user"].setdefault(k, v)
    if not t["user"].get("voice_v2"):
        backfill_voice(t)
    LAST_VALUES.setdefault(tid, {"username": "", "display": "", "avatar": ""})
    VOICE_START.setdefault(tid, None)
    VOICE_INFO.setdefault(tid, None)
    VOICE_TOTAL.setdefault(tid, float(t["user"].get("voice_sec", 0) or 0))
    return t


def drop_target(tid):
    STATE["targets"].pop(tid, None)
    for d in (LAST_VALUES, VOICE_START, VOICE_INFO, VOICE_TOTAL, FRIENDS_TRACKER, LAST_FETCH):
        d.pop(tid, None)


def trim(tid):
    t = STATE["targets"][tid]
    for k in ("nicks", "media", "sessions", "friends"):
        del t[k][LIMITS[k]:]


def add_media_history(tid, url):
    if not url:
        return
    media = STATE["targets"][tid]["media"]
    if not media or media[0]["s"] != url:
        media.insert(0, {"s": url})
        trim(tid)


def add_nick_history(tid, name):
    if not name:
        return
    nicks = STATE["targets"][tid]["nicks"]
    if not nicks or nicks[0][0] != name:
        nicks.insert(0, [name, "Глобальное имя", now_iso(), 0])
        trim(tid)


def clean_sessions(tid):
    seen, out = False, []
    for s in STATE["targets"][tid]["sessions"]:
        if s.get("dur") == "В сети":
            if seen:
                continue
            seen = True
        out.append(s)
    STATE["targets"][tid]["sessions"] = out


def update_friend_data(tid, member, additional_sec=0):
    tid, fid = str(tid), str(member.id)
    friends = STATE["targets"][tid]["friends"]
    mins = additional_sec / 60
    today = datetime.now(timezone.utc).strftime("%d.%m")
    for f in friends:
        if f.get("id") == fid:
            f["min"] = f.get("min", 0) + mins
            f["last"], f["u"] = today, member.name
            f["avatar"] = str(member.display_avatar.url)
            return
    friends.insert(0, {"id": fid, "u": member.name, "avatar": str(member.display_avatar.url),
                       "min": mins, "last": today})
    trim(tid)


# ───────────────────────── known users ─────────────────────────
def track_known(user, guild=None, member=None):
    """Собирает данные любого не-бота, которого видит бот. Обновляет STATE['known_users']."""
    if user is None or getattr(user, "bot", False):
        return
    uid = str(user.id)
    now = datetime.now(timezone.utc)
    known = STATE["known_users"]
    k = known.get(uid)
    if k is None:
        k = blank_target(uid)
        known[uid] = k
        k["user"]["username"] = user.name
        k["user"]["display"] = user.global_name or user.name
        k["user"]["avatar_url"] = str(user.display_avatar.url)
        if user.created_at:
            k["user"]["reg"] = user.created_at.strftime("%d.%m.%Y, %H:%M:%S")
        if k["user"]["avatar_url"]:
            k["media"].insert(0, {"s": k["user"]["avatar_url"]})
        k["nicks"].insert(0, [k["user"]["display"], "Глобальное имя", now_iso(), 0])
        k["last_seen"] = now.timestamp()
    else:
        cur_u = user.name
        cur_d = user.global_name or user.name
        cur_a = str(user.display_avatar.url)
        if cur_u != k["user"]["username"]:
            k["user"]["username"] = cur_u
        if cur_d != k["user"]["display"]:
            k["nicks"].insert(0, [cur_d, "Глобальное имя", now_iso(), 0])
            del k["nicks"][LIMITS["nicks"]:]
            k["user"]["display"] = cur_d
        if cur_a and cur_a != k["user"]["avatar_url"]:
            k["media"].insert(0, {"s": cur_a})
            del k["media"][LIMITS["media"]:]
            k["user"]["avatar_url"] = cur_a
            if (uid, cur_a) not in RECENT_FEED:
                RECENT_FEED.append((uid, cur_a))
                del RECENT_FEED[:-50]
                PENDING_FEED.append({"user": cur_u, "user_id": uid, "url": cur_a, "time": now_iso()})
        k["last_seen"] = now.timestamp()

    if guild is not None:
        icon = str(guild.icon.url) if guild.icon else ""
        sv = next((s for s in k["servers"] if s.get("n") == guild.name), None)
        if sv is None:
            k["servers"].append({"n": guild.name, "m": guild.member_count,
                                 "v": sum(len(vc.members) for vc in guild.voice_channels),
                                 "icon": icon, "vs": 0})
        else:
            sv["m"] = guild.member_count
            sv["v"] = sum(len(vc.members) for vc in guild.voice_channels)
            sv["icon"] = icon
        if member is not None and member.voice and member.voice.channel:
            vc = member.voice.channel
            active = next((s for s in k["sessions"] if s.get("dur") == "В сети"), None)
            if active is None:
                k["sessions"].insert(0, {"srv": guild.name, "ch": vc.name,
                                         "srv_icon": icon, "dur": "В сети",
                                         "per": f"с {hm(now)}", "start_ts": time.time()})
                del k["sessions"][LIMITS["sessions"]:]
            k["lastVoice"] = {"server": guild.name, "channel": vc.name,
                              "icon": icon, "left": "Сейчас в сети"}


def evict_known():
    """Оставляем KNOWN_MAX самых свежих по last_seen."""
    known = STATE["known_users"]
    if len(known) <= KNOWN_MAX:
        return
    items = sorted(known.items(), key=lambda kv: kv[1].get("last_seen", 0), reverse=True)
    STATE["known_users"] = dict(items[:KNOWN_MAX])


# ───────────────────────── голосовые сессии (для targets) ─────────────────────────
def start_session(tid, guild, vc, now, start_ts=None):
    icon = str(guild.icon.url) if guild.icon else ""
    VOICE_START[tid] = datetime.fromtimestamp(start_ts, timezone.utc) if start_ts else now
    VOICE_INFO[tid] = {"srv": guild.name, "ch": vc.name, "icon": icon}
    if start_ts:
        return
    t = STATE["targets"][tid]
    t["sessions"].insert(0, {"srv": guild.name, "ch": vc.name, "srv_icon": icon,
                             "dur": "В сети", "per": f"с {hm(now)}", "start_ts": time.time()})
    t["lastVoice"] = {"server": guild.name, "channel": vc.name, "icon": icon, "left": "Сейчас в сети"}
    clean_sessions(tid)
    trim(tid)


def finish_session(tid, now):
    t = STATE["targets"][tid]
    active = next((s for s in t["sessions"] if s.get("dur") == "В сети"), None)
    start, info = VOICE_START.get(tid), VOICE_INFO.get(tid)
    if start is None and active and active.get("start_ts"):
        start = datetime.fromtimestamp(active["start_ts"], timezone.utc)
        if active.get("seen_ts"):
            now = datetime.fromtimestamp(active["seen_ts"], timezone.utc)
    if info is None and active:
        info = {"srv": active.get("srv", "-"), "ch": active.get("ch", "-"), "icon": active.get("srv_icon", "")}
    t["sessions"] = [s for s in t["sessions"] if s.get("dur") != "В сети"]
    if start and info:
        dur = max(0.0, (now - start).total_seconds())
        VOICE_TOTAL[tid] = VOICE_TOTAL.get(tid, 0) + dur
        t["user"]["voice_sec"] = VOICE_TOTAL[tid]
        t["user"]["voice"] = format_total(VOICE_TOTAL[tid])
        t["voice_srv"][info["srv"]] = t["voice_srv"].get(info["srv"], 0) + dur
        t["sessions"].insert(0, {"srv": info["srv"], "ch": info["ch"], "srv_icon": info["icon"],
                                 "dur": format_duration(dur), "per": f"{hm(start)} – {hm(now)}"})
        t["lastVoice"] = {"server": info["srv"], "channel": info["ch"], "icon": info["icon"],
                          "left": now.astimezone().strftime("%d.%m.%Y, %H:%M:%S")}
        trim(tid)
    VOICE_START[tid] = None
    VOICE_INFO[tid] = None


def flush_friends(tid, now, guild):
    for fid, started_at in list(FRIENDS_TRACKER.get(tid, {}).items()):
        m = guild.get_member(int(fid))
        if m:
            update_friend_data(tid, m, (now - started_at).total_seconds())
    FRIENDS_TRACKER.pop(tid, None)


# ───────────────────────── загрузка / синхронизация ─────────────────────────
async def load_state():
    global STATE, state_loaded
    try:
        async with http.get(f"{RENDER_URL}/api/bot_state") as r:
            if r.status != 200:
                log.warning("[Init] /api/bot_state вернул %s", r.status)
                return False
            data = await r.json()
    except Exception as e:
        log.warning("[Init] Не удалось загрузить историю: %s", e)
        return False
    if isinstance(data, dict) and isinstance(data.get("targets"), dict):
        STATE = {"targets": data["targets"],
                 "known_users": data.get("known_users") or {},
                 "global_feed": []}
        for tid in list(STATE["targets"]):
            ensure_target(tid)
            clean_sessions(tid)
        evict_known()
    state_loaded = True
    log.info("[Init] История загружена. Целей: %d, известных: %d",
             len(STATE["targets"]), len(STATE["known_users"]))
    return True


async def push_state():
    global last_sent_hash
    if not (state_loaded and is_initialized):
        return
    evict_known()
    body = json.dumps(STATE, sort_keys=True, ensure_ascii=False).encode()
    h = hashlib.md5(body).hexdigest()
    if h == last_sent_hash:
        return
    try:
        async with http.post(f"{RENDER_URL}/api/update", data=body,
                             headers={"Content-Type": "application/json"}) as r:
            if r.status == 200:
                last_sent_hash = h
            else:
                log.warning("[Sync] /api/update вернул %s", r.status)
    except Exception as e:
        log.warning("[Sync] push_state: %s", type(e).__name__)


async def push_feed():
    while PENDING_FEED:
        batch = PENDING_FEED[:100]
        try:
            async with http.post(f"{RENDER_URL}/api/feed", json={"items": batch}) as r:
                if r.status != 200:
                    log.warning("[Feed] /api/feed вернул %s", r.status)
                    return
        except Exception as e:
            log.warning("[Feed] %s", type(e).__name__)
            return
        del PENDING_FEED[:len(batch)]


async def ping_render():
    try:
        async with http.get(f"{RENDER_URL}/api/ping"):
            pass
    except Exception:
        pass


async def sync_targets():
    global is_initialized
    if not state_loaded and not await load_state():
        return
    try:
        async with http.get(f"{RENDER_URL}/api/bot_targets") as r:
            if r.status != 200:
                log.warning("[Target] /api/bot_targets вернул %s (проверь BOT_KEY)", r.status)
                return
            wanted = {str(x) for x in await r.json()}
    except Exception as e:
        log.warning("[Target] sync_targets: %s", type(e).__name__)
        return
    new = wanted - set(STATE["targets"])
    for tid in new:
        log.info("[Target] Добавлен ID: %s", tid)
        ensure_target(tid)
    for tid in set(STATE["targets"]) - wanted:
        log.info("[Target] Удалён ID: %s", tid)
        drop_target(tid)
    is_initialized = True
    if new:
        for tid in new:
            await check_one(tid)
        await push_state()


# ───────────────────────── проверка targets ─────────────────────────
def apply_profile(tid, user):
    t = STATE["targets"][tid]
    cur_u, cur_d, cur_a = user.name, user.global_name or user.name, str(user.display_avatar.url)
    t["user"].update(username=cur_u, display=cur_d, avatar_url=cur_a)
    if t["user"]["reg"] == "unknown" and user.created_at:
        t["user"]["reg"] = user.created_at.strftime("%d.%m.%Y, %H:%M:%S")
    last = LAST_VALUES[tid]
    if last["username"] and cur_u != last["username"]:
        add_nick_history(tid, cur_u)
    if last["display"] and cur_d != last["display"]:
        add_nick_history(tid, cur_d)
    if last["avatar"] and cur_a != last["avatar"]:
        add_media_history(tid, cur_a)
    last.update(username=cur_u, display=cur_d, avatar=cur_a)


def cached_user(uid):
    u = client.get_user(uid)
    if u:
        return u
    for g in client.guilds:
        m = g.get_member(uid)
        if m:
            return getattr(m, "_user", m)
    return None


async def refresh_profile(tid):
    uid = int(tid)
    user = cached_user(uid)
    if user is None and time.time() - LAST_FETCH.get(tid, 0) > FETCH_EVERY:
        LAST_FETCH[tid] = time.time()
        try:
            user = await client.fetch_user(uid)
        except Exception as e:
            log.debug("[Profile] fetch_user(%s): %s", tid, type(e).__name__)
    if user is None:
        if tid not in NO_PROFILE_WARNED:
            NO_PROFILE_WARNED.add(tid)
            log.info("[Profile] %s: пока недоступен (ждём общий сервер/кэш)", tid)
        return
    NO_PROFILE_WARNED.discard(tid)
    apply_profile(tid, user)


def check_voice(tid):
    t = STATE["targets"][tid]
    now = datetime.now(timezone.utc)
    uid = int(tid)
    for guild in client.guilds:
        member = guild.get_member(uid)
        if not (member and member.voice and member.voice.channel):
            continue
        vc = member.voice.channel
        if VOICE_START.get(tid) is None:
            active = next((s for s in t["sessions"] if s.get("dur") == "В сети"), None)
            start_session(tid, guild, vc, now, active.get("start_ts", time.time()) if active else None)
        FRIENDS_TRACKER.setdefault(tid, {})
        for m in vc.members:
            if m.id == member.id or m.bot:
                continue
            fid = str(m.id)
            if fid not in FRIENDS_TRACKER[tid]:
                FRIENDS_TRACKER[tid][fid] = now
                update_friend_data(tid, m, 0)
            else:
                live = (now - FRIENDS_TRACKER[tid][fid]).total_seconds()
                if live > 15:
                    update_friend_data(tid, m, live)
                    FRIENDS_TRACKER[tid][fid] = now
        total = VOICE_TOTAL.get(tid, 0) + (now - VOICE_START[tid]).total_seconds()
        t["user"]["voice"] = format_total(total)
        for s_ in t["sessions"]:
            if s_.get("dur") == "В сети":
                s_["seen_ts"] = time.time()
                break
        return
    if VOICE_START.get(tid) or any(s.get("dur") == "В сети" for s in t["sessions"]):
        finish_session(tid, now)
        FRIENDS_TRACKER.pop(tid, None)


def refresh_servers(tid):
    t = STATE["targets"][tid]
    uid = int(tid)
    vs = dict(t["voice_srv"])
    start, info = VOICE_START.get(tid), VOICE_INFO.get(tid)
    if start and info:
        live = (datetime.now(timezone.utc) - start).total_seconds()
        vs[info["srv"]] = vs.get(info["srv"], 0) + live
    out, seen = [], set()
    for g in client.guilds:
        if g.get_member(uid):
            seen.add(g.name)
            out.append({"n": g.name, "m": g.member_count,
                        "v": sum(len(vc.members) for vc in g.voice_channels),
                        "icon": str(g.icon.url) if g.icon else "",
                        "vs": int(vs.get(g.name, 0))})
    for name, sec in vs.items():
        if name not in seen and sec > 0:
            icon = next((x.get("srv_icon", "") for x in t["sessions"] if x.get("srv") == name and x.get("srv_icon")), "")
            out.append({"n": name, "m": 0, "v": 0, "icon": icon, "vs": int(sec), "gone": True})
    out.sort(key=lambda x: -x["vs"])
    t["servers"] = out


async def check_one(tid):
    try:
        await refresh_profile(tid)
        check_voice(tid)
        refresh_servers(tid)
    except Exception:
        log.exception("[Check] ошибка для %s", tid)


async def check_targets():
    for tid in list(STATE["targets"]):
        await check_one(tid)


async def run_forever(name, fn, interval):
    while True:
        try:
            await fn()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("[%s] %s: %s", name, type(e).__name__, e)
        await asyncio.sleep(interval)


# ───────────────────────── события Discord ─────────────────────────
@client.event
async def on_ready():
    global http, started
    if started:
        log.info("Переподключение выполнено.")
        return
    started = True
    log.info("Бот %s запущен.", client.user)
    http = aiohttp.ClientSession(headers={"X-Bot-Key": BOT_KEY},
                                 timeout=aiohttp.ClientTimeout(total=60))
    for _ in range(6):
        if await load_state():
            break
        await asyncio.sleep(10)
    # прогреваем known_users тем, что уже в кэше
    for g in client.guilds:
        for m in g.members:
            try:
                track_known(getattr(m, "_user", m), guild=g, member=m)
            except Exception:
                continue
    # в фоне пробуем дотянуть полный список участников крупных гильдий
    async def chunk_all():
        for g in client.guilds:
            if g.member_count and g.member_count > 50000:
                continue
            try:
                await g.chunk()
                for m in g.members:
                    track_known(getattr(m, "_user", m), guild=g, member=m)
            except Exception:
                continue
    client.loop.create_task(chunk_all())

    loop = client.loop
    loop.create_task(run_forever("sync", push_state, PUSH_EVERY))
    loop.create_task(run_forever("feed", push_feed, 5))
    loop.create_task(run_forever("ping", ping_render, 600))
    loop.create_task(run_forever("targets", sync_targets, TARGETS_EVERY))
    loop.create_task(run_forever("check", check_targets, PROFILE_EVERY))


@client.event
async def on_message(message):
    if message.author.bot:
        return
    try:
        track_known(message.author, guild=message.guild)
    except Exception:
        pass


@client.event
async def on_member_join(member):
    if member.bot:
        return
    try:
        track_known(getattr(member, "_user", member), guild=member.guild, member=member)
    except Exception:
        pass


@client.event
async def on_presence_update(before, after):
    if after.bot:
        return
    try:
        track_known(after)
    except Exception:
        pass


@client.event
async def on_user_update(before, after):
    if after.bot:
        return
    uid = str(after.id)
    # в ленту — только если у нас есть общий сервер
    if before.avatar != after.avatar and any(g.get_member(after.id) for g in client.guilds):
        new_url = str(after.display_avatar.url)
        if new_url and (uid, new_url) not in RECENT_FEED:
            RECENT_FEED.append((uid, new_url))
            del RECENT_FEED[:-50]
            PENDING_FEED.append({"user": after.name, "user_id": uid, "url": new_url, "time": now_iso()})
    try:
        track_known(after)
    except Exception:
        pass
    if uid in STATE["targets"]:
        ensure_target(uid)
        t = STATE["targets"][uid]
        if before.avatar != after.avatar:
            url = str(after.display_avatar.url)
            t["user"]["avatar_url"] = url
            add_media_history(uid, url)
            LAST_VALUES[uid]["avatar"] = url
        if before.global_name != after.global_name:
            new_disp = after.global_name or after.name
            t["user"]["display"] = new_disp
            add_nick_history(uid, new_disp)
            LAST_VALUES[uid]["display"] = new_disp


@client.event
async def on_voice_state_update(member, before, after):
    tid = str(member.id)
    now = datetime.now(timezone.utc)
    moved = before.channel != after.channel

    # known_users: обновить всех, кто был/стал в канале
    if not member.bot and moved:
        try:
            if after.channel is not None:
                track_known(getattr(member, "_user", member), guild=after.channel.guild, member=member)
            else:
                track_known(getattr(member, "_user", member), guild=before.channel.guild if before.channel else None)
        except Exception:
            pass

    if tid in STATE["targets"] and moved:
        ensure_target(tid)
        if before.channel is not None:
            flush_friends(tid, now, before.channel.guild)
            finish_session(tid, now)
        if after.channel is not None:
            start_session(tid, after.channel.guild, after.channel, now)
            FRIENDS_TRACKER[tid] = {}
            for m in after.channel.members:
                if m.id != member.id and not m.bot:
                    FRIENDS_TRACKER[tid][str(m.id)] = now
                    update_friend_data(tid, m, 0)

    if not moved or member.bot:
        return

    if after.channel is not None:
        for ttid in list(STATE["targets"]):
            if ttid == tid:
                continue
            tm = after.channel.guild.get_member(int(ttid))
            if tm and tm.voice and tm.voice.channel == after.channel:
                FRIENDS_TRACKER.setdefault(ttid, {})[tid] = now
                update_friend_data(ttid, member, 0)
    if before.channel is not None:
        for ttid in list(STATE["targets"]):
            if ttid == tid:
                continue
            tm = before.channel.guild.get_member(int(ttid))
            if tm and tm.voice and tm.voice.channel == before.channel:
                st = FRIENDS_TRACKER.get(ttid, {}).pop(tid, None)
                if st:
                    update_friend_data(ttid, member, (now - st).total_seconds())


if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("Нет токена: задай DISCORD_TOKEN или положи token.txt")
    if not BOT_KEY:
        raise SystemExit("Нет ключа сервера: задай BOT_KEY или положи bot_key.txt (тот же, что на Render)")
    client.run(TOKEN)