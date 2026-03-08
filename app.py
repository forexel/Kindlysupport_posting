import json
import logging
import os
import re
import secrets
import sqlite3
import time
import ipaddress
import base64
import urllib.parse
import urllib.error
import urllib.request
import csv
import io
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from threading import Thread
from typing import Any, Optional
from uuid import uuid4
from pathlib import Path

from fastapi import Cookie, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # optional in local dev until installed
    psycopg = None
    dict_row = None


DB_PATH = os.getenv("APP_DB_PATH", "kindlysupport.db")
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_BACKEND = "postgres" if DATABASE_URL.startswith(("postgres://", "postgresql://")) else "sqlite"
UTC = timezone.utc
MOSCOW_TZ = timezone(timedelta(hours=3))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper().strip()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("kindlysupport")


def now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def now_msk() -> datetime:
    return datetime.now(tz=MOSCOW_TZ)


def parse_to_utc_iso(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="scheduled_for required")
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        raise HTTPException(status_code=400, detail="scheduled_for must be ISO datetime, e.g. 2026-03-08T10:00:00+03:00")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=MOSCOW_TZ)
    return dt.astimezone(UTC).isoformat()


def validate_public_http_url(value: str) -> str:
    raw = (value or "").strip()
    parsed = urllib.parse.urlparse(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise HTTPException(status_code=400, detail="Only public http/https URLs are allowed")
    host = (parsed.hostname or "").strip().lower()
    if host in {"localhost", "127.0.0.1", "::1"}:
        raise HTTPException(status_code=400, detail="localhost URLs are not allowed")
    try:
        ip = ipaddress.ip_address(host)
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast:
            raise HTTPException(status_code=400, detail="Private IP URLs are not allowed")
    except ValueError:
        pass
    return raw


def load_env_file(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    for raw in open(path, "r", encoding="utf-8"):
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


load_env_file()

ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "admin@example.com").strip()
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "change-me").strip()
SESSION_COOKIE = "session_id"
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "").strip()
OPENROUTER_TEXT_MODEL = os.getenv("OPENROUTER_TEXT_MODEL", "openai/gpt-4o-mini").strip()
OPENROUTER_VISION_MODEL = os.getenv("OPENROUTER_VISION_MODEL", "meta-llama/llama-4-scout").strip()
OPENROUTER_IMAGE_MODEL = os.getenv("OPENROUTER_IMAGE_MODEL", "openai/gpt-5-image-mini").strip()
OPENROUTER_SITE_URL = os.getenv("OPENROUTER_SITE_URL", "http://localhost:8000").strip()
OPENROUTER_APP_NAME = os.getenv("OPENROUTER_APP_NAME", "KindlySupport Posting").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
TELEGRAM_PREVIEW_CHAT = os.getenv("TELEGRAM_PREVIEW_CHAT", "@Yudin_Finance").strip()
TELEGRAM_PUBLISH_CHAT = os.getenv("TELEGRAM_PUBLISH_CHAT", "-1002383010494").strip()
TELEGRAM_BOT_USERNAME = os.getenv("TELEGRAM_BOT_USERNAME", "@ForCreatingTestsBot").strip()
VK_ACCESS_TOKEN = os.getenv("VK_ACCESS_TOKEN", "").strip()
VK_GROUP_ID = os.getenv("VK_GROUP_ID", "").strip()
VK_API_VERSION = os.getenv("VK_API_VERSION", "5.199").strip()
INSTAGRAM_GRAPH_VERSION = os.getenv("INSTAGRAM_GRAPH_VERSION", "v22.0").strip()
INSTAGRAM_ACCESS_TOKEN = os.getenv("INSTAGRAM_ACCESS_TOKEN", "").strip()
INSTAGRAM_IG_USER_ID = os.getenv("INSTAGRAM_IG_USER_ID", "").strip()
PINTEREST_ACCESS_TOKEN = os.getenv("PINTEREST_ACCESS_TOKEN", "").strip()
PINTEREST_BOARD_ID = os.getenv("PINTEREST_BOARD_ID", "").strip()
ENABLE_INSTAGRAM = os.getenv("ENABLE_INSTAGRAM", "0").strip() in {"1", "true", "yes"}
ENABLE_PINTEREST = os.getenv("ENABLE_PINTEREST", "0").strip() in {"1", "true", "yes"}
ENABLE_VK = os.getenv("ENABLE_VK", "0").strip() in {"1", "true", "yes"}
SESSION_TTL_DAYS = int(os.getenv("SESSION_TTL_DAYS", "30"))
APP_TIMEZONE = os.getenv("APP_TIMEZONE", "Europe/Moscow").strip()
APP_BASE_URL = os.getenv("APP_BASE_URL", "http://localhost:8000").strip()
APP_SECURE_COOKIES = os.getenv("APP_SECURE_COOKIES", "0").strip() in {"1", "true", "yes"}
TRUSTED_HOSTS = [x.strip() for x in os.getenv("TRUSTED_HOSTS", "localhost,127.0.0.1").split(",") if x.strip()]
CORS_ALLOW_ORIGINS = [x.strip() for x in os.getenv("CORS_ALLOW_ORIGINS", APP_BASE_URL).split(",") if x.strip()]
ENABLE_DAILY_AUTOPREVIEW = os.getenv("ENABLE_DAILY_AUTOPREVIEW", "1").strip() in {"1", "true", "yes"}
DAILY_AUTOPREVIEW_HOUR_MSK = int(os.getenv("DAILY_AUTOPREVIEW_HOUR_MSK", "9"))
DAILY_AUTOPREVIEW_MINUTE_MSK = int(os.getenv("DAILY_AUTOPREVIEW_MINUTE_MSK", "0"))
TELEGRAM_MODE = os.getenv("TELEGRAM_MODE", "webhook").strip().lower()
FRONTEND_DIST_DIR = Path(os.getenv("FRONTEND_DIST_DIR", "Content Platform Web App/dist")).resolve()

rate_bucket: dict[str, list[float]] = {}


class DBCursorProxy:
    def __init__(self, inner: Any):
        self.inner = inner
        self.lastrowid = getattr(inner, "lastrowid", None)

    def fetchone(self) -> Any:
        row = self.inner.fetchone()
        if row is None:
            return None
        if isinstance(row, dict):
            return row
        try:
            return dict(row)
        except Exception:
            return row

    def fetchall(self) -> list[Any]:
        rows = self.inner.fetchall()
        out = []
        for row in rows:
            if isinstance(row, dict):
                out.append(row)
            else:
                try:
                    out.append(dict(row))
                except Exception:
                    out.append(row)
        return out


class DBConnProxy:
    def __init__(self, conn: Any, backend: str):
        self.conn = conn
        self.backend = backend

    def _sql(self, sql: str) -> str:
        if self.backend == "postgres":
            return sql.replace("?", "%s")
        return sql

    def execute(self, sql: str, params: Any = None) -> DBCursorProxy:
        cur = self.conn.cursor() if self.backend == "postgres" else self.conn
        if params is None:
            inner = cur.execute(self._sql(sql))
        else:
            inner = cur.execute(self._sql(sql), params)
        return DBCursorProxy(inner)

    def executescript(self, script: str) -> None:
        if self.backend == "postgres":
            cur = self.conn.cursor()
            for stmt in [s.strip() for s in script.split(";") if s.strip()]:
                cur.execute(stmt)
            return
        self.conn.executescript(script)

    def commit(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()


@contextmanager
def db():
    if DB_BACKEND == "postgres":
        if psycopg is None:
            raise RuntimeError("psycopg is required for DATABASE_URL postgres mode")
        conn = psycopg.connect(DATABASE_URL, row_factory=dict_row, autocommit=False)
        proxy = DBConnProxy(conn, "postgres")
    else:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        proxy = DBConnProxy(conn, "sqlite")
    try:
        yield proxy
        proxy.commit()
    finally:
        proxy.close()


def init_db() -> None:
    with db() as conn:
        if DB_BACKEND == "postgres":
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
                    csrf_token TEXT,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS app_kv (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS telegram_user_states (
                    user_id BIGINT PRIMARY KEY,
                    state TEXT NOT NULL,
                    payload_json TEXT,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS posts (
                    id BIGSERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    text_body TEXT NOT NULL,
                    source_url TEXT,
                    source_kind TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'draft',
                    recognized_text TEXT,
                    image_scenarios_json TEXT NOT NULL DEFAULT '[]',
                    selected_scenario TEXT,
                    image_prompt TEXT,
                    final_image_url TEXT,
                    telegram_caption TEXT,
                    preview_payload_json TEXT,
                    last_regen_instruction TEXT,
                    scheduled_for TEXT,
                    preview_message_id TEXT,
                    published_message_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS image_generation_logs (
                    id BIGSERIAL PRIMARY KEY,
                    post_id BIGINT,
                    model TEXT NOT NULL,
                    prompt TEXT NOT NULL,
                    size TEXT,
                    raw_response_json TEXT,
                    input_tokens INTEGER,
                    output_tokens INTEGER,
                    reasoning_tokens INTEGER,
                    image_tokens INTEGER,
                    estimated_cost_usd DOUBLE PRECISION,
                    status TEXT NOT NULL,
                    error TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS phrases (
                    id BIGSERIAL PRIMARY KEY,
                    text_body TEXT NOT NULL UNIQUE,
                    topic TEXT,
                    is_published INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS films (
                    id BIGSERIAL PRIMARY KEY,
                    title TEXT NOT NULL,
                    year INTEGER,
                    country TEXT,
                    description TEXT,
                    tags TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_posts_status_updated ON posts(status, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_posts_scheduled_for ON posts(scheduled_for);
                CREATE INDEX IF NOT EXISTS idx_phrases_published_id ON phrases(is_published, id);
                CREATE INDEX IF NOT EXISTS idx_image_logs_post_created ON image_generation_logs(post_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_films_title ON films(title);
                """
            )
        else:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
                    csrf_token TEXT,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS app_kv (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS telegram_user_states (
                    user_id INTEGER PRIMARY KEY,
                    state TEXT NOT NULL,
                    payload_json TEXT,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    text_body TEXT NOT NULL,
                    source_url TEXT,
                    source_kind TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'draft',
                    recognized_text TEXT,
                    image_scenarios_json TEXT NOT NULL DEFAULT '[]',
                    selected_scenario TEXT,
                    image_prompt TEXT,
                    final_image_url TEXT,
                    telegram_caption TEXT,
                    preview_payload_json TEXT,
                    last_regen_instruction TEXT,
                    scheduled_for TEXT,
                    preview_message_id TEXT,
                    published_message_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS image_generation_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    post_id INTEGER,
                    model TEXT NOT NULL,
                    prompt TEXT NOT NULL,
                    size TEXT,
                    raw_response_json TEXT,
                    input_tokens INTEGER,
                    output_tokens INTEGER,
                    reasoning_tokens INTEGER,
                    image_tokens INTEGER,
                    estimated_cost_usd REAL,
                    status TEXT NOT NULL,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(post_id) REFERENCES posts(id)
                );
                CREATE TABLE IF NOT EXISTS phrases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    text_body TEXT NOT NULL UNIQUE,
                    topic TEXT,
                    is_published INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS films (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    year INTEGER,
                    country TEXT,
                    description TEXT,
                    tags TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_posts_status_updated ON posts(status, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_posts_scheduled_for ON posts(scheduled_for);
                CREATE INDEX IF NOT EXISTS idx_phrases_published_id ON phrases(is_published, id);
                CREATE INDEX IF NOT EXISTS idx_image_logs_post_created ON image_generation_logs(post_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_films_title ON films(title);
                """
            )
            # lightweight migration for older sqlite db
            for col_def in (
                "preview_message_id TEXT",
                "published_message_id TEXT",
                "csrf_token TEXT",
            ):
                try:
                    if "token" in col_def:
                        conn.execute(f"ALTER TABLE sessions ADD COLUMN {col_def}")
                    else:
                        conn.execute(f"ALTER TABLE posts ADD COLUMN {col_def}")
                except Exception:
                    pass
        if DB_BACKEND == "postgres":
            conn.execute("ALTER TABLE sessions ADD COLUMN IF NOT EXISTS csrf_token TEXT")
            conn.execute("ALTER TABLE phrases ADD COLUMN IF NOT EXISTS topic TEXT")
        else:
            try:
                conn.execute("ALTER TABLE phrases ADD COLUMN topic TEXT")
            except Exception:
                pass


def ensure_auth(session_id: Optional[str]) -> None:
    if session_id == "telegram-internal":
        return
    if not session_id:
        raise HTTPException(status_code=401, detail="auth required")
    with db() as conn:
        row = conn.execute(
            "SELECT session_id, expires_at FROM sessions WHERE session_id = ?",
            (session_id,),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=401, detail="invalid session")
    if row["expires_at"] <= now_iso():
        raise HTTPException(status_code=401, detail="session expired")


def kv_get(key: str) -> Optional[str]:
    with db() as conn:
        row = conn.execute("SELECT value FROM app_kv WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


def kv_set(key: str, value: str) -> None:
    with db() as conn:
        if DB_BACKEND == "postgres":
            conn.execute(
                "INSERT INTO app_kv(key, value, updated_at) VALUES (?, ?, ?) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at",
                (key, value, now_iso()),
            )
        else:
            conn.execute(
                "INSERT INTO app_kv(key, value, updated_at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
                (key, value, now_iso()),
            )


def settings_key(name: str) -> str:
    return f"setting:{name}"


def setting_get(name: str, default: str = "") -> str:
    value = kv_get(settings_key(name))
    if value is None:
        return default
    return value


def setting_set(name: str, value: str) -> None:
    kv_set(settings_key(name), value or "")


def bool_from_str(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def runtime_openrouter_key() -> str:
    return setting_get("openrouter_api_key", OPENROUTER_API_KEY)


def runtime_openrouter_text_model() -> str:
    return setting_get("openrouter_text_model", OPENROUTER_TEXT_MODEL)


def runtime_openrouter_vision_model() -> str:
    return setting_get("openrouter_vision_model", OPENROUTER_VISION_MODEL)


def runtime_openrouter_image_model() -> str:
    return setting_get("openrouter_image_model", OPENROUTER_IMAGE_MODEL)


def runtime_telegram_token() -> str:
    return setting_get("telegram_bot_token", TELEGRAM_BOT_TOKEN)


def runtime_telegram_preview_chat() -> str:
    return setting_get("telegram_preview_chat", TELEGRAM_PREVIEW_CHAT)


def runtime_telegram_publish_chat() -> str:
    return setting_get("telegram_publish_chat", TELEGRAM_PUBLISH_CHAT)


def runtime_telegram_webhook_secret() -> str:
    return setting_get("telegram_webhook_secret", TELEGRAM_WEBHOOK_SECRET)


def runtime_instagram_token() -> str:
    return setting_get("instagram_access_token", INSTAGRAM_ACCESS_TOKEN)


def runtime_instagram_user_id() -> str:
    return setting_get("instagram_ig_user_id", INSTAGRAM_IG_USER_ID)


def runtime_instagram_graph_version() -> str:
    return setting_get("instagram_graph_version", INSTAGRAM_GRAPH_VERSION)


def runtime_pinterest_token() -> str:
    return setting_get("pinterest_access_token", PINTEREST_ACCESS_TOKEN)


def runtime_pinterest_board_id() -> str:
    return setting_get("pinterest_board_id", PINTEREST_BOARD_ID)


def runtime_vk_token() -> str:
    return setting_get("vk_access_token", VK_ACCESS_TOKEN)


def runtime_vk_group_id() -> str:
    return setting_get("vk_group_id", VK_GROUP_ID)


def runtime_vk_version() -> str:
    return setting_get("vk_api_version", VK_API_VERSION)


def runtime_enable_instagram() -> bool:
    return bool_from_str(setting_get("enable_instagram", "1" if ENABLE_INSTAGRAM else "0"))


def runtime_enable_pinterest() -> bool:
    return bool_from_str(setting_get("enable_pinterest", "1" if ENABLE_PINTEREST else "0"))


def runtime_enable_vk() -> bool:
    return bool_from_str(setting_get("enable_vk", "1" if ENABLE_VK else "0"))


def tg_admin_user_id() -> Optional[int]:
    env_val = os.getenv("TELEGRAM_ADMIN_USER_ID", "").strip()
    if env_val:
        try:
            return int(env_val)
        except Exception:
            return None
    stored = kv_get("telegram_admin_user_id")
    if not stored:
        return None
    try:
        return int(stored)
    except Exception:
        return None


def tg_state_set(user_id: int, state: str, payload: Optional[dict[str, Any]] = None) -> None:
    payload_json = json.dumps(payload or {}, ensure_ascii=False)
    with db() as conn:
        if DB_BACKEND == "postgres":
            conn.execute(
                "INSERT INTO telegram_user_states(user_id, state, payload_json, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT (user_id) DO UPDATE SET state = EXCLUDED.state, payload_json = EXCLUDED.payload_json, updated_at = EXCLUDED.updated_at",
                (user_id, state, payload_json, now_iso()),
            )
        else:
            conn.execute(
                "INSERT INTO telegram_user_states(user_id, state, payload_json, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT(user_id) DO UPDATE SET state = excluded.state, payload_json = excluded.payload_json, updated_at = excluded.updated_at",
                (user_id, state, payload_json, now_iso()),
            )


def tg_state_get(user_id: int) -> Optional[dict[str, Any]]:
    with db() as conn:
        row = conn.execute("SELECT state, payload_json FROM telegram_user_states WHERE user_id = ?", (user_id,)).fetchone()
    if not row:
        return None
    return {"state": row["state"], "payload": json.loads(row["payload_json"] or "{}")}


def tg_state_clear(user_id: int) -> None:
    with db() as conn:
        conn.execute("DELETE FROM telegram_user_states WHERE user_id = ?", (user_id,))


def upsert_phrase_text(text: str, is_published: int = 0) -> dict[str, Any]:
    phrase = (text or "").strip()
    if not phrase:
        raise HTTPException(status_code=400, detail="empty phrase")
    now = now_iso()
    with db() as conn:
        existing = conn.execute(
            "SELECT id, text_body, topic, is_published, created_at, updated_at FROM phrases WHERE text_body = ?",
            (phrase,),
        ).fetchone()
        if existing:
            if int(existing["is_published"]) != int(is_published):
                conn.execute(
                    "UPDATE phrases SET is_published = ?, updated_at = ? WHERE id = ?",
                    (int(is_published), now, existing["id"]),
                )
            row = conn.execute(
                "SELECT id, text_body, topic, is_published, created_at, updated_at FROM phrases WHERE id = ?",
                (existing["id"],),
            ).fetchone()
            return dict(row)
        if DB_BACKEND == "postgres":
            row = conn.execute(
                """
                INSERT INTO phrases(text_body, topic, is_published, created_at, updated_at)
                VALUES (?, NULL, ?, ?, ?)
                RETURNING id, text_body, topic, is_published, created_at, updated_at
                """,
                (phrase, int(is_published), now, now),
            ).fetchone()
            return dict(row)
        cur = conn.execute(
            "INSERT OR IGNORE INTO phrases(text_body, topic, is_published, created_at, updated_at) VALUES (?, NULL, ?, ?, ?)",
            (phrase, int(is_published), now, now),
        )
        phrase_id = cur.lastrowid
        row = conn.execute(
            "SELECT id, text_body, topic, is_published, created_at, updated_at FROM phrases WHERE id = ?",
            (phrase_id,),
        ).fetchone()
        return dict(row)


def row_to_post(row: Any) -> dict[str, Any]:
    return {
        "id": row["id"],
        "title": row["title"],
        "text_body": row["text_body"],
        "source_url": row["source_url"],
        "source_kind": row["source_kind"],
        "status": row["status"],
        "recognized_text": row["recognized_text"],
        "image_scenarios": json.loads(row["image_scenarios_json"] or "[]"),
        "selected_scenario": row["selected_scenario"],
        "image_prompt": row["image_prompt"],
        "final_image_url": row["final_image_url"],
        "telegram_caption": row["telegram_caption"],
        "preview_payload": json.loads(row["preview_payload_json"]) if row["preview_payload_json"] else None,
        "last_regen_instruction": row["last_regen_instruction"],
        "scheduled_for": row["scheduled_for"],
        "preview_message_id": row.get("preview_message_id") if isinstance(row, dict) else row["preview_message_id"],
        "published_message_id": row.get("published_message_id") if isinstance(row, dict) else row["published_message_id"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def fetch_post(post_id: int) -> dict[str, Any]:
    with db() as conn:
        row = conn.execute("SELECT * FROM posts WHERE id = ?", (post_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="post not found")
    return row_to_post(row)


def update_post(post_id: int, **fields: Any) -> None:
    if not fields:
        return
    fields["updated_at"] = now_iso()
    keys = list(fields.keys())
    sql = f"UPDATE posts SET {', '.join(f'{k} = ?' for k in keys)} WHERE id = ?"
    vals = [json.dumps(v, ensure_ascii=False) if k.endswith("_json") else v for k, v in fields.items()]
    vals.append(post_id)
    with db() as conn:
        conn.execute(sql, vals)


def http_json(
    method: str,
    url: str,
    payload: Optional[dict[str, Any]] = None,
    headers: Optional[dict[str, str]] = None,
    timeout: int = 30,
    retries: int = 2,
) -> dict[str, Any]:
    body = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=body, method=method.upper())
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    if body is not None and "Content-Type" not in (headers or {}):
        req.add_header("Content-Type", "application/json")
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", errors="ignore")
            if e.code in {429, 500, 502, 503, 504} and attempt < retries:
                time.sleep(0.35 * (attempt + 1))
                continue
            raise HTTPException(status_code=502, detail=f"HTTP {e.code}: {detail[:1200]}")
        except Exception as e:
            if attempt < retries:
                time.sleep(0.35 * (attempt + 1))
                continue
            raise HTTPException(status_code=502, detail=f"HTTP request failed: {str(e)[:800]}")


def telegram_api(method: str, payload: dict[str, Any]) -> dict[str, Any]:
    tg_token = runtime_telegram_token()
    if not tg_token:
        raise HTTPException(status_code=400, detail="TELEGRAM_BOT_TOKEN not set")
    url = f"https://api.telegram.org/bot{tg_token}/{method}"
    return http_json("POST", url, payload)


def answer_callback(callback_query_id: str, text: str = "") -> None:
    if not runtime_telegram_token():
        return
    try:
        telegram_api("answerCallbackQuery", {"callback_query_id": callback_query_id, "text": text[:200]})
    except Exception:
        pass


def tg_keyboard(rows: list[list[tuple[str, str]]]) -> dict[str, Any]:
    return {
        "inline_keyboard": [
            [{"text": txt, "callback_data": cb} for txt, cb in row]
            for row in rows
        ]
    }


def build_preview_keyboard(post_id: int) -> dict[str, Any]:
    return tg_keyboard(
        [
            [("Опубликовать", f"ks:pub:{post_id}")],
            [("Перегенерировать", f"ks:regen:{post_id}")],
            [("Отмена", f"ks:cancel:{post_id}")],
        ]
    )


def build_manual_create_keyboard() -> dict[str, Any]:
    return tg_keyboard(
        [
            [("Сгенерить пост вручную", "ks:manual:0")],
            [("Добавить фразы", "ks:addphrases:0")],
        ]
    )


def telegram_file_data_url(file_id: str) -> str:
    tg_token = runtime_telegram_token()
    if not tg_token:
        raise HTTPException(status_code=400, detail="TELEGRAM_BOT_TOKEN not set")
    file_meta = telegram_api("getFile", {"file_id": file_id})
    file_path = ((file_meta.get("result") or {}).get("file_path") or "").strip()
    if not file_path:
        raise HTTPException(status_code=502, detail="Telegram getFile: empty file_path")
    file_url = f"https://api.telegram.org/file/bot{tg_token}/{file_path}"
    with urllib.request.urlopen(file_url, timeout=30) as resp:
        content = resp.read()
    ext = (file_path.rsplit(".", 1)[-1] if "." in file_path else "jpg").lower()
    mime = "image/jpeg"
    if ext == "png":
        mime = "image/png"
    elif ext == "webp":
        mime = "image/webp"
    elif ext in {"jpg", "jpeg"}:
        mime = "image/jpeg"
    b64 = base64.b64encode(content).decode("ascii")
    return f"data:{mime};base64,{b64}"


def telegram_send_preview(post: dict[str, Any]) -> Optional[dict[str, Any]]:
    if not runtime_telegram_token():
        return None
    chat_id = runtime_telegram_preview_chat()
    caption = post.get("telegram_caption") or generate_caption(post["title"], post["text_body"])
    keyboard = build_preview_keyboard(int(post["id"]))
    if post.get("final_image_url"):
        payload = {
            "chat_id": chat_id,
            "photo": post["final_image_url"],
            "caption": caption[:1024],
            "reply_markup": keyboard,
        }
        if len(caption) > 1024:
            payload["caption"] = caption[:1000] + "..."
        return telegram_api("sendPhoto", payload)
    return telegram_api(
        "sendMessage",
        {"chat_id": chat_id, "text": caption, "reply_markup": keyboard, "disable_web_page_preview": True},
    )


def telegram_send_publish(post: dict[str, Any]) -> Optional[dict[str, Any]]:
    if not runtime_telegram_token():
        return None
    chat_id = runtime_telegram_publish_chat()
    caption = post.get("telegram_caption") or generate_caption(post["title"], post["text_body"])
    if post.get("final_image_url"):
        return telegram_api(
            "sendPhoto",
            {"chat_id": chat_id, "photo": post["final_image_url"], "caption": caption[:1024]},
        )
    return telegram_api("sendMessage", {"chat_id": chat_id, "text": caption, "disable_web_page_preview": True})


def send_telegram_text(chat_id: str | int, text: str, reply_markup: Optional[dict[str, Any]] = None) -> Optional[dict[str, Any]]:
    if not runtime_telegram_token():
        return None
    payload: dict[str, Any] = {"chat_id": chat_id, "text": text}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return telegram_api("sendMessage", payload)


def instagram_publish_post(post: dict[str, Any]) -> dict[str, Any]:
    instagram_token = runtime_instagram_token()
    instagram_user_id = runtime_instagram_user_id()
    graph_version = runtime_instagram_graph_version()
    if not instagram_token or not instagram_user_id:
        raise HTTPException(status_code=400, detail="Instagram credentials not configured")
    if not post.get("final_image_url"):
        raise HTTPException(status_code=400, detail="Post has no final_image_url")
    base = f"https://graph.facebook.com/{graph_version}/{instagram_user_id}"
    create = http_json(
        "POST",
        f"{base}/media",
        {
            "image_url": post["final_image_url"],
            "caption": (post.get("telegram_caption") or "")[:2200],
            "access_token": instagram_token,
        },
    )
    creation_id = create.get("id")
    if not creation_id:
        raise HTTPException(status_code=502, detail=f"Instagram create media failed: {create}")
    publish = http_json(
        "POST",
        f"{base}/media_publish",
        {"creation_id": creation_id, "access_token": instagram_token},
    )
    return {"create": create, "publish": publish}


def pinterest_publish_post(post: dict[str, Any]) -> dict[str, Any]:
    if not runtime_enable_pinterest():
        raise HTTPException(status_code=503, detail="Pinterest publishing temporarily disabled")
    pinterest_token = runtime_pinterest_token()
    pinterest_board_id = runtime_pinterest_board_id()
    if not pinterest_token or not pinterest_board_id:
        raise HTTPException(status_code=400, detail="Pinterest credentials not configured")
    if not post.get("final_image_url"):
        raise HTTPException(status_code=400, detail="Post has no final_image_url")
    payload = {
        "board_id": pinterest_board_id,
        "title": post["title"][:100],
        "description": (post.get("telegram_caption") or "")[:500],
        "link": post.get("source_url") or OPENROUTER_SITE_URL,
        "media_source": {
            "source_type": "image_url",
            "url": post["final_image_url"],
        },
    }
    res = http_json(
        "POST",
        "https://api.pinterest.com/v5/pins",
        payload,
        headers={"Authorization": f"Bearer {pinterest_token}"},
    )
    return res


def vk_publish_post(post: dict[str, Any]) -> dict[str, Any]:
    if not runtime_enable_vk():
        raise HTTPException(status_code=503, detail="VK publishing temporarily disabled")
    vk_token = runtime_vk_token()
    vk_group_id = runtime_vk_group_id()
    vk_version = runtime_vk_version()
    if not vk_token or not vk_group_id:
        raise HTTPException(status_code=400, detail="VK credentials not configured")
    if not post.get("final_image_url"):
        raise HTTPException(status_code=400, detail="Post has no final_image_url")
    caption = (post.get("telegram_caption") or generate_post_caption(post))[:3500]
    wall_post = http_json(
        "POST",
        "https://api.vk.com/method/wall.post",
        {
            "owner_id": f"-{vk_group_id}",
            "from_group": 1,
            "message": caption,
            "attachments": post["final_image_url"],
            "v": vk_version,
            "access_token": vk_token,
        },
    )
    return wall_post


def generate_caption(title: str, text_body: str) -> str:
    return f"Притча: {title}\n{text_body}\n\n@kindlysupport"


def generate_post_caption(post: dict[str, Any]) -> str:
    source_kind = (post.get("source_kind") or "").strip()
    title = (post.get("title") or "").strip()
    text_body = (post.get("text_body") or "").strip()
    if source_kind == "phrase":
        # For phrase-posting mode: start with the phrase as the title, then channel handle.
        if text_body and text_body != title:
            return f"{title}\n\n{text_body}\n\n@kindlysupport"
        return f"{title}\n\n@kindlysupport"
    return generate_caption(title, text_body)


def phrase_id_from_post(post: dict[str, Any]) -> Optional[int]:
    source_kind = (post.get("source_kind") or "").strip()
    source_url = (post.get("source_url") or "").strip()
    if source_kind != "phrase" or not source_url.startswith("phrase:"):
        return None
    try:
        return int(source_url.split(":", 1)[1])
    except Exception:
        return None


def mark_phrase_published_if_linked(post: dict[str, Any]) -> None:
    phrase_id = phrase_id_from_post(post)
    if not phrase_id:
        return
    with db() as conn:
        conn.execute(
            "UPDATE phrases SET is_published = 1, updated_at = ? WHERE id = ?",
            (now_iso(), phrase_id),
        )


def default_image_prompt(title: str, scenario: str, extra_instruction: str = "") -> str:
    base = (
        "Create a realistic square 1080x1080 image for a social media post. "
        "Calm aesthetic, natural scene, subtle cinematic lighting, no text on image, photorealistic."
    )
    overlay = (
        "The app will add a dark overlay and typography later. Focus only on background composition."
    )
    prompt = f"{base} {overlay} Title context: {title}. Visual scenario: {scenario}."
    if extra_instruction:
        prompt += f" Admin note: {extra_instruction}."
    return prompt


def estimate_cost_usd(model: str, usage: dict[str, Any]) -> Optional[float]:
    input_t = usage.get("input_tokens") or usage.get("prompt_tokens") or 0
    output_t = usage.get("output_tokens") or usage.get("completion_tokens") or 0
    reasoning_t = usage.get("reasoning_tokens") or 0
    image_t = usage.get("image_tokens") or 0
    # Approximate. OpenRouter pricing can vary and may bill separate token classes.
    if model == "openai/gpt-5-image-mini":
        return round((input_t / 1_000_000) * 2.5 + (output_t / 1_000_000) * 2.0 + (reasoning_t / 1_000_000) * 8.0, 6)
    if model == "openai/gpt-4o-mini":
        return round((input_t / 1_000_000) * 0.15 + (output_t / 1_000_000) * 0.60, 6)
    if model == "meta-llama/llama-4-scout":
        return round((input_t / 1_000_000) * 0.08 + (output_t / 1_000_000) * 0.30, 6)
    if image_t:
        # Unknown image-token-only models (e.g. 0/0 + $X/M tokens)
        return None
    return None


def openrouter_chat(model: str, messages: list[dict[str, Any]], extra_body: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    api_key = runtime_openrouter_key()
    if not api_key:
        raise HTTPException(status_code=400, detail="OPENROUTER_API_KEY not set")
    body = {
        "model": model,
        "messages": messages,
        "usage": {"include": True},
    }
    if extra_body:
        body.update(extra_body)
    req = urllib.request.Request(
        "https://openrouter.ai/api/v1/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": OPENROUTER_SITE_URL,
            "X-Title": OPENROUTER_APP_NAME,
        },
        method="POST",
    )
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", errors="ignore")
            if e.code in {429, 500, 502, 503, 504} and attempt < 2:
                time.sleep(0.5 * (attempt + 1))
                continue
            raise HTTPException(status_code=502, detail=f"OpenRouter HTTP {e.code}: {detail[:800]}")
        except Exception as e:
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))
                continue
            raise HTTPException(status_code=502, detail=f"OpenRouter network error: {str(e)[:600]}")
    raise HTTPException(status_code=502, detail="OpenRouter request failed")


def openrouter_generate_text(prompt: str) -> str:
    res = openrouter_chat(
        runtime_openrouter_text_model(),
        [{"role": "user", "content": prompt}],
    )
    return (res.get("choices") or [{}])[0].get("message", {}).get("content", "")


def openrouter_extract_text_from_image(image_url: str) -> str:
    res = openrouter_chat(
        runtime_openrouter_vision_model(),
        [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "Извлеки все самостоятельные фразы/высказывания с изображения.\n"
                            "Правила:\n"
                            "- Каждую фразу верни с новой строки.\n"
                            "- Включай вопросы и цитаты как отдельные фразы.\n"
                            "- Верни только текст фраз, без заголовков, пояснений и markdown.\n"
                            "- Игнорируй служебные элементы интерфейса (например: 'Поделиться', 'Вопрос сессии').\n"
                            "- Если одна фраза разбита на несколько строк, склей её в одну строку.\n"
                            "- Убирай нумерацию вида '1.'/'2.' в начале строк.\n"
                            "- Не пиши фразы вроде 'Текст с изображения', 'Вот что видно на изображении' и т.п.\n"
                            "- Если фразы прочитать нельзя, верни пустую строку."
                        ),
                    },
                    {"type": "image_url", "image_url": {"url": image_url}},
                ],
            }
        ],
    )
    return (res.get("choices") or [{}])[0].get("message", {}).get("content", "")


def openrouter_generate_image(post_id: int, prompt: str, size: str = "1024x1024") -> dict[str, Any]:
    started = time.time()
    model = runtime_openrouter_image_model()
    status = "ok"
    error = None
    raw = {}
    usage = {}
    image_url = None
    try:
        raw = openrouter_chat(
            model,
            [{"role": "user", "content": prompt}],
            extra_body={
                "modalities": ["image", "text"],
                "image": {"size": size},
            },
        )
        usage = raw.get("usage") or {}
        msg = (raw.get("choices") or [{}])[0].get("message", {})
        content = msg.get("content")
        # Providers vary; try common shapes.
        if isinstance(content, list):
            for item in content:
                if isinstance(item, dict):
                    if item.get("type") == "image_url" and item.get("image_url", {}).get("url"):
                        image_url = item["image_url"]["url"]
                        break
                    if item.get("type") == "output_image" and item.get("url"):
                        image_url = item["url"]
                        break
        if not image_url:
            image_url = raw.get("image_url") or raw.get("output", {}).get("image_url")
        if not image_url:
            raise RuntimeError("No image URL in response")
        return {"image_url": image_url, "raw": raw, "usage": usage, "latency_sec": round(time.time() - started, 2)}
    except Exception as e:
        status = "error"
        error = str(e)
        raise
    finally:
        with db() as conn:
            cost = estimate_cost_usd(model, usage if isinstance(usage, dict) else {})
            conn.execute(
                """
                INSERT INTO image_generation_logs
                (post_id, model, prompt, size, raw_response_json, input_tokens, output_tokens, reasoning_tokens, image_tokens,
                 estimated_cost_usd, status, error, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    post_id,
                    model,
                    prompt,
                    size,
                    json.dumps(raw, ensure_ascii=False)[:200000] if raw else None,
                    (usage or {}).get("input_tokens") or (usage or {}).get("prompt_tokens"),
                    (usage or {}).get("output_tokens") or (usage or {}).get("completion_tokens"),
                    (usage or {}).get("reasoning_tokens"),
                    (usage or {}).get("image_tokens"),
                    cost,
                    status,
                    error[:2000] if error else None,
                    now_iso(),
                ),
            )


def extract_from_url(url: str) -> tuple[str, str]:
    url = validate_public_http_url(url)
    lower = url.lower()
    if lower.endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDF не поддерживается. Только ссылка на HTML или картинку.")
    if lower.endswith((".png", ".jpg", ".jpeg", ".webp")):
        return "url_image", openrouter_extract_text_from_image(url)
    # MVP stub for HTML extraction. Replace with readability parser later.
    return "url_html", f"Текст, извлечённый из HTML-ссылки (заглушка)\nИсточник: {url}"


def extract_phrases_from_ocr_text(ocr_text: str) -> list[str]:
    raw = (ocr_text or "").strip()
    if not raw:
        return []

    # Strip common wrappers from vision-model responses.
    raw = re.sub(r"^```[a-zA-Z]*\s*", "", raw)
    raw = re.sub(r"\s*```$", "", raw)

    bad_patterns = [
        r"^#+\s*extracted text\b[:：]?\s*$",
        r"^#+\s*текст с изображения\b[:：]?\s*$",
        r"^the visible text .*?:\s*$",
        r"^visible text .*?:\s*$",
        r"^текст с изображения\b[:：]?\s*$",
        r"^вот (?:что|текст) .*изображени[яи]\b[:：]?\s*$",
        r"^extracted text\b[:：]?\s*$",
        r"^text\b[:：]?\s*$",
        r"^answer\b[:：]?\s*$",
        r"^поделиться$",
        r"^вопрос сессии$",
    ]

    cleaned_lines: list[str] = []
    for line in raw.splitlines():
        s = line.strip()
        if not s:
            continue
        s = s.lstrip("•-—* \t").strip()
        s = s.strip("\"'«»")
        if not s:
            continue
        low = s.lower()
        if any(re.match(p, low, flags=re.IGNORECASE) for p in bad_patterns):
            continue
        if len(s) <= 2:
            continue
        cleaned_lines.append(s)

    if not cleaned_lines:
        return []

    # If model returns a single line with numbered list, split it.
    if len(cleaned_lines) == 1:
        one = cleaned_lines[0]
        parts = re.split(r"(?:^|\s)(?:\d{1,2}[\.\)]\s+)", one)
        parts = [p.strip() for p in parts if p and p.strip()]
        if len(parts) > 1:
            cleaned_lines = parts

    # For multi-phrase pages keep separate lines; for single-card keep one line.
    candidates = cleaned_lines
    if len(cleaned_lines) == 1:
        candidates = [re.sub(r"\s+", " ", cleaned_lines[0]).strip()]

    phrases: list[str] = []
    seen: set[str] = set()
    for c in candidates:
        c = re.sub(r"\s+", " ", c).strip(" .,:;-\t")
        if not c:
            continue
        low = c.lower()
        if low in seen:
            continue
        # Drop leftovers if response is still meta text.
        if "extracted text" in low or "visible text" in low:
            continue
        if low in {"поделиться", "вопрос сессии"}:
            continue
        if c.endswith(":"):
            continue
        seen.add(low)
        phrases.append(c)
    return phrases


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline'; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data: https:; "
            "connect-src 'self'; "
            "base-uri 'self'; frame-ancestors 'none'; form-action 'self'"
        )
        return response


class RequestContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        req_id = request.headers.get("x-request-id") or str(uuid4())
        request.state.request_id = req_id
        path = request.url.path
        if path.startswith("/api/") and path not in {"/api/login", "/api/telegram/webhook"}:
            check_rate_limit(request, f"api:{request.method}:{path}", per_minute=240)
        if request.method.upper() not in {"GET", "HEAD", "OPTIONS"} and path.startswith("/api/"):
            if path not in {"/api/login", "/api/telegram/webhook"}:
                csrf_cookie = request.cookies.get("csrf_token", "")
                csrf_header = request.headers.get("x-csrf-token", "")
                if not csrf_cookie or not csrf_header or csrf_cookie != csrf_header:
                    return JSONResponse(status_code=403, content={"detail": "csrf token mismatch"})
        start = time.time()
        try:
            response = await call_next(request)
        except HTTPException:
            logger.exception("http_error request_id=%s path=%s", req_id, request.url.path)
            raise
        except Exception:
            logger.exception("unhandled request_id=%s path=%s", req_id, request.url.path)
            raise
        latency_ms = int((time.time() - start) * 1000)
        response.headers["X-Request-ID"] = req_id
        logger.info("request_id=%s method=%s path=%s status=%s latency_ms=%s", req_id, request.method, request.url.path, response.status_code, latency_ms)
        return response


def check_rate_limit(request: Request, key_suffix: str = "default", per_minute: int = 120) -> None:
    ip = request.client.host if request.client else "unknown"
    now = time.time()
    bucket_key = f"{ip}:{key_suffix}"
    arr = [x for x in rate_bucket.get(bucket_key, []) if now - x <= 60]
    if len(arr) >= per_minute:
        raise HTTPException(status_code=429, detail="rate limit exceeded")
    arr.append(now)
    rate_bucket[bucket_key] = arr


def ensure_csrf(request: Request, session_id: Optional[str], allow_telegram_internal: bool = False) -> None:
    if allow_telegram_internal and session_id == "telegram-internal":
        return
    if request.method.upper() in {"GET", "HEAD", "OPTIONS"}:
        return
    csrf_cookie = request.cookies.get("csrf_token", "")
    csrf_header = request.headers.get("x-csrf-token", "")
    if not csrf_cookie or not csrf_header or csrf_cookie != csrf_header:
        raise HTTPException(status_code=403, detail="csrf token mismatch")


app = FastAPI(title="Kindlysupport Posting MVP")
app.add_middleware(TrustedHostMiddleware, allowed_hosts=TRUSTED_HOSTS or ["localhost", "127.0.0.1"])
app.add_middleware(GZipMiddleware, minimum_size=1200)
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "X-CSRF-Token", "X-Request-ID"],
)
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(RequestContextMiddleware)
if (FRONTEND_DIST_DIR / "assets").exists():
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIST_DIR / "assets")), name="frontend-assets")


@app.on_event("startup")
def startup() -> None:
    init_db()
    start_background_scheduler()


def run_scheduled_publications() -> int:
    with db() as conn:
        rows = conn.execute(
            """
            SELECT id FROM posts
            WHERE status = 'scheduled'
              AND scheduled_for IS NOT NULL
              AND scheduled_for <= ?
            ORDER BY scheduled_for ASC
            LIMIT 20
            """,
            (now_iso(),),
        ).fetchall()
    processed = 0
    for row in rows:
        try:
            publish_now_internal(int(row["id"]))
            processed += 1
        except Exception:
            logger.exception("scheduled_publish_failed post_id=%s", row["id"])
    return processed


def run_daily_phrase_preview() -> bool:
    if not ENABLE_DAILY_AUTOPREVIEW:
        return False
    now_local = now_msk()
    if (now_local.hour, now_local.minute) < (DAILY_AUTOPREVIEW_HOUR_MSK, DAILY_AUTOPREVIEW_MINUTE_MSK):
        return False
    today_key = now_local.strftime("%Y-%m-%d")
    if kv_get("daily_preview_last_date") == today_key:
        return False
    with db() as conn:
        phrase = conn.execute(
            "SELECT id FROM phrases WHERE coalesce(is_published,0)=0 ORDER BY id ASC LIMIT 1"
        ).fetchone()
    if not phrase:
        kv_set("daily_preview_last_date", today_key)
        return False
    try:
        post = create_post_from_phrase(int(phrase["id"]), session_id="telegram-internal")
        preview_req = _mock_request({"scenario": "", "regen_instruction": ""})
        import asyncio
        asyncio.run(create_preview(int(post["id"]), preview_req, session_id="telegram-internal"))
        kv_set("daily_preview_last_date", today_key)
        return True
    except Exception:
        logger.exception("daily_preview_failed phrase_id=%s", phrase["id"])
        return False


def scheduler_loop() -> None:
    while True:
        try:
            run_scheduled_publications()
            run_daily_phrase_preview()
        except Exception:
            logger.exception("scheduler_loop_error")
        time.sleep(20)


def start_background_scheduler() -> None:
    if getattr(start_background_scheduler, "_started", False):
        return
    thread = Thread(target=scheduler_loop, daemon=True, name="ks-scheduler")
    thread.start()
    start_background_scheduler._started = True


@app.get("/", response_class=HTMLResponse)
def index() -> Any:
    index_file = FRONTEND_DIST_DIR / "index.html"
    if index_file.exists():
        return FileResponse(str(index_file))
    raise HTTPException(status_code=503, detail="Frontend dist not found")


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "ok": True,
        "time_utc": now_iso(),
        "time_msk": now_msk().isoformat(),
        "timezone": APP_TIMEZONE,
        "db": DB_PATH,
        "db_backend": DB_BACKEND,
        "telegram_mode": TELEGRAM_MODE,
    }


@app.post("/api/login")
async def login(request: Request, response: Response) -> dict[str, Any]:
    check_rate_limit(request, "login", per_minute=20)
    payload = await request.json()
    if payload.get("email") != ADMIN_EMAIL or payload.get("password") != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="invalid credentials")
    sid = secrets.token_urlsafe(32)
    csrf_token = secrets.token_urlsafe(24)
    created = now_iso()
    expires = datetime.now(tz=UTC).timestamp() + SESSION_TTL_DAYS * 24 * 3600
    expires_iso = datetime.fromtimestamp(expires, tz=UTC).isoformat()
    with db() as conn:
        conn.execute(
            "INSERT INTO sessions(session_id, csrf_token, created_at, expires_at) VALUES (?, ?, ?, ?)",
            (sid, csrf_token, created, expires_iso),
        )
    response.set_cookie(SESSION_COOKIE, sid, httponly=True, samesite="lax", secure=APP_SECURE_COOKIES)
    response.set_cookie("csrf_token", csrf_token, httponly=False, samesite="lax", secure=APP_SECURE_COOKIES)
    return {"ok": True, "expires_at": expires_iso}


@app.post("/api/logout")
async def logout(response: Response, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    if session_id:
        with db() as conn:
            conn.execute("DELETE FROM sessions WHERE session_id = ?", (session_id,))
    response.delete_cookie(SESSION_COOKIE)
    response.delete_cookie("csrf_token")
    return {"ok": True}


@app.get("/api/config")
def get_config(session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    return {
        "models": {
            "vision": runtime_openrouter_vision_model(),
            "text": runtime_openrouter_text_model(),
            "image": runtime_openrouter_image_model(),
        },
        "openrouter_key_configured": bool(runtime_openrouter_key()),
        "db_backend": DB_BACKEND,
        "telegram": {
            "bot_configured": bool(runtime_telegram_token()),
            "admin_user_id": tg_admin_user_id(),
            "preview_chat": runtime_telegram_preview_chat(),
            "publish_chat": runtime_telegram_publish_chat(),
            "mode": TELEGRAM_MODE,
        },
        "timezone": APP_TIMEZONE,
        "instagram_configured": bool(runtime_enable_instagram() and runtime_instagram_token() and runtime_instagram_user_id()),
        "pinterest_configured": bool(runtime_enable_pinterest() and runtime_pinterest_token() and runtime_pinterest_board_id()),
        "vk_configured": bool(runtime_enable_vk() and runtime_vk_token() and runtime_vk_group_id()),
    }


@app.get("/api/settings")
def get_settings(session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    return {
        "openrouter_api_key": "***" if runtime_openrouter_key() else "",
        "openrouter_text_model": runtime_openrouter_text_model(),
        "openrouter_vision_model": runtime_openrouter_vision_model(),
        "openrouter_image_model": runtime_openrouter_image_model(),
        "telegram_bot_token": "***" if runtime_telegram_token() else "",
        "telegram_admin_user_id": tg_admin_user_id(),
        "telegram_preview_chat": runtime_telegram_preview_chat(),
        "telegram_publish_chat": runtime_telegram_publish_chat(),
        "telegram_webhook_secret": "***" if runtime_telegram_webhook_secret() else "",
        "enable_vk": runtime_enable_vk(),
        "vk_access_token": "***" if runtime_vk_token() else "",
        "vk_group_id": runtime_vk_group_id(),
        "vk_api_version": runtime_vk_version(),
        "enable_instagram": runtime_enable_instagram(),
        "instagram_access_token": "***" if runtime_instagram_token() else "",
        "instagram_ig_user_id": runtime_instagram_user_id(),
        "instagram_graph_version": runtime_instagram_graph_version(),
        "enable_pinterest": runtime_enable_pinterest(),
        "pinterest_access_token": "***" if runtime_pinterest_token() else "",
        "pinterest_board_id": runtime_pinterest_board_id(),
    }


@app.put("/api/settings")
async def update_settings(request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    allowed = {
        "openrouter_api_key",
        "openrouter_text_model",
        "openrouter_vision_model",
        "openrouter_image_model",
        "telegram_bot_token",
        "telegram_admin_user_id",
        "telegram_preview_chat",
        "telegram_publish_chat",
        "telegram_webhook_secret",
        "enable_vk",
        "vk_access_token",
        "vk_group_id",
        "vk_api_version",
        "enable_instagram",
        "instagram_access_token",
        "instagram_ig_user_id",
        "instagram_graph_version",
        "enable_pinterest",
        "pinterest_access_token",
        "pinterest_board_id",
    }
    updated = []
    for key, value in payload.items():
        if key not in allowed:
            continue
        if key == "telegram_admin_user_id":
            if str(value).strip():
                kv_set("telegram_admin_user_id", str(int(value)))
            else:
                kv_set("telegram_admin_user_id", "")
            updated.append(key)
            continue
        if key.startswith("enable_"):
            setting_set(key, "1" if bool(value) else "0")
        else:
            setting_set(key, str(value or "").strip())
        updated.append(key)
    return {"ok": True, "updated": updated}


@app.post("/api/posts")
async def create_post(request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    mode = payload.get("mode")
    title = (payload.get("title") or "").strip()
    if not title:
        raise HTTPException(status_code=400, detail="title required")
    created_at = now_iso()
    if mode == "manual":
        text_body = (payload.get("text_body") or "").strip()
        if not text_body:
            raise HTTPException(status_code=400, detail="text_body required")
        source_kind = "manual"
        recognized_text = text_body
        source_url = None
    elif mode == "link":
        url = (payload.get("url") or "").strip()
        if not url:
            raise HTTPException(status_code=400, detail="url required")
        source_kind, recognized_text = extract_from_url(url)
        text_body = recognized_text
        source_url = url
    else:
        raise HTTPException(status_code=400, detail="mode must be manual or link")
    with db() as conn:
        if DB_BACKEND == "postgres":
            cur = conn.execute(
                """
                INSERT INTO posts (title, text_body, source_url, source_kind, status, recognized_text, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'draft', ?, ?, ?)
                RETURNING id
                """,
                (title, text_body, source_url, source_kind, recognized_text, created_at, created_at),
            )
            row = cur.fetchone()
            post_id = row["id"]
        else:
            cur = conn.execute(
                """
                INSERT INTO posts (title, text_body, source_url, source_kind, status, recognized_text, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'draft', ?, ?, ?)
                """,
                (title, text_body, source_url, source_kind, recognized_text, created_at, created_at),
            )
            post_id = cur.lastrowid
    return fetch_post(int(post_id))


@app.post("/api/parables")
async def create_parable(request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    mode = (payload.get("mode") or "manual").strip().lower()
    title = (payload.get("title") or "").strip()
    if not title:
        raise HTTPException(status_code=400, detail="title required")
    created_at = now_iso()
    if mode == "manual":
        text_body = (payload.get("text_body") or "").strip()
        if not text_body:
            raise HTTPException(status_code=400, detail="text_body required")
        source_kind = "parable_manual"
        source_url = None
        recognized_text = text_body
    elif mode == "link":
        url = (payload.get("url") or "").strip()
        if not url:
            raise HTTPException(status_code=400, detail="url required")
        _, recognized_text = extract_from_url(url)
        text_body = (payload.get("text_body") or recognized_text or "").strip()
        source_kind = "parable_link"
        source_url = url
    else:
        raise HTTPException(status_code=400, detail="mode must be manual|link")
    with db() as conn:
        if DB_BACKEND == "postgres":
            row = conn.execute(
                """
                INSERT INTO posts (title, text_body, source_url, source_kind, status, recognized_text, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'draft', ?, ?, ?)
                RETURNING id
                """,
                (title, text_body, source_url, source_kind, recognized_text, created_at, created_at),
            ).fetchone()
            post_id = row["id"]
        else:
            cur = conn.execute(
                """
                INSERT INTO posts (title, text_body, source_url, source_kind, status, recognized_text, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'draft', ?, ?, ?)
                """,
                (title, text_body, source_url, source_kind, recognized_text, created_at, created_at),
            )
            post_id = cur.lastrowid
    return fetch_post(int(post_id))


@app.get("/api/parables")
def list_parables(
    session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0, le=10000),
) -> list[dict[str, Any]]:
    ensure_auth(session_id)
    with db() as conn:
        rows = conn.execute(
            "SELECT * FROM posts WHERE source_kind IN ('parable_manual','parable_link') ORDER BY id DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
    return [row_to_post(r) for r in rows]


@app.get("/api/posts")
def list_posts(
    session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0, le=10000),
) -> list[dict[str, Any]]:
    ensure_auth(session_id)
    with db() as conn:
        rows = conn.execute("SELECT * FROM posts ORDER BY id DESC LIMIT ? OFFSET ?", (limit, offset)).fetchall()
    return [row_to_post(r) for r in rows]


@app.get("/api/posts/{post_id}")
def get_post(post_id: int, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    return fetch_post(post_id)


@app.put("/api/posts/{post_id}")
async def update_post_text(post_id: int, request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    updates = {}
    for key in ("title", "text_body"):
        if key in payload:
            updates[key] = (payload.get(key) or "").strip()
    if not updates:
        raise HTTPException(status_code=400, detail="nothing to update")
    update_post(post_id, **updates)
    return fetch_post(post_id)


@app.post("/api/posts/{post_id}/image-scenarios")
async def generate_scenarios(post_id: int, request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    post = fetch_post(post_id)
    payload = await request.json()
    force_default = bool(payload.get("force_default"))
    if force_default:
        scenarios = [
            "Спокойный морской берег на рассвете, мягкий свет, реалистично",
            "Лесная тропа в золотой час, лучи света через деревья, реалистично",
            "Закат над тихим озером, минималистичный пейзаж, реалистично",
        ]
    else:
        prompt = (
            "Сгенерируй 4 кратких сценария изображения для статьи/притчи. "
            "Нужны реалистичные квадратные фоны 1080x1080 без текста. "
            "Ответ строго JSON-массивом строк без пояснений.\n"
            f"Заголовок: {post['title']}\n"
            f"Текст: {post['text_body'][:2500]}"
        )
        raw = openrouter_generate_text(prompt)
        try:
            parsed = json.loads(raw)
            scenarios = [str(x).strip() for x in parsed if str(x).strip()]
        except Exception:
            # fallback if model responds with prose
            lines = [x.strip(" -•\t") for x in raw.splitlines() if x.strip()]
            scenarios = [x for x in lines[:4] if x]
        if not scenarios:
            scenarios = ["Стандартный реалистичный природный фон для духовной притчи, квадратный формат"]
    update_post(post_id, image_scenarios_json=scenarios)
    return {"ok": True, "scenarios": scenarios}


@app.post("/api/posts/{post_id}/preview")
async def create_preview(post_id: int, request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    post = fetch_post(post_id)
    payload = await request.json()
    scenario = (payload.get("scenario") or "").strip()
    if not scenario:
        scenario = "Стандартный реалистичный природный фон, спокойный, квадратный"
    regen_instruction = (payload.get("regen_instruction") or "").strip()
    caption = generate_caption(post["title"], post["text_body"])
    if post.get("source_kind") == "phrase":
        caption = generate_post_caption(post)
    image_prompt = default_image_prompt(post["title"], scenario, regen_instruction)

    image_url = None
    image_error = None
    try:
        result = openrouter_generate_image(post_id=post_id, prompt=image_prompt, size="1024x1024")
        image_url = result["image_url"]
    except HTTPException as e:
        image_error = e.detail
    except Exception as e:
        image_error = str(e)

    preview = {
        "chat": runtime_telegram_preview_chat(),
        "buttons": ["Опубликовать", "Перегенерировать", "Отмена"],
        "publish_options": ["Сейчас", "В указанное время и дату", "Заменить фразу"],
        "regenerate_options": ["Текст", "Картинку", "И то и то"],
        "note": "При перегенерации админ присылает текст-инструкцию, он учитывается в prompt изображения.",
        "image_error": image_error,
    }
    update_post(
        post_id,
        status="preview_ready",
        selected_scenario=scenario,
        image_prompt=image_prompt,
        final_image_url=image_url,
        telegram_caption=caption,
        preview_payload_json=preview,
        last_regen_instruction=regen_instruction or None,
    )
    post = fetch_post(post_id)
    tg_send = None
    tg_error = None
    try:
        tg_send = telegram_send_preview(post)
        result = (tg_send or {}).get("result") or {}
        if result.get("message_id") is not None:
            update_post(post_id, preview_message_id=str(result["message_id"]))
            post = fetch_post(post_id)
    except HTTPException as e:
        tg_error = e.detail
    except Exception as e:
        tg_error = str(e)
    if tg_error:
        preview = post.get("preview_payload") or {}
        preview["telegram_send_error"] = tg_error
        update_post(post_id, preview_payload_json=preview)
        post = fetch_post(post_id)
    return post


@app.post("/api/posts/{post_id}/regenerate")
async def regenerate_preview(post_id: int, request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    target = payload.get("target")
    instruction = (payload.get("instruction") or "").strip()
    post = fetch_post(post_id)
    if target not in ("text", "image", "both"):
        raise HTTPException(status_code=400, detail="target must be text|image|both")
    title = post["title"]
    text_body = post["text_body"]
    scenario = post.get("selected_scenario") or "Стандартный реалистичный природный фон, квадратный"

    if target in ("text", "both") and instruction:
        # MVP: keep structure strict, but allow short refinement by appending admin note to the body.
        text_body = f"{post['text_body']}\n\nP.S. Уточнение редактора: {instruction}"
        update_post(post_id, text_body=text_body)
    if target in ("image", "both"):
        image_prompt = default_image_prompt(title, scenario, instruction)
        image_url = None
        image_error = None
        try:
            result = openrouter_generate_image(post_id=post_id, prompt=image_prompt, size="1024x1024")
            image_url = result["image_url"]
        except HTTPException as e:
            image_error = e.detail
        except Exception as e:
            image_error = str(e)
        preview = post.get("preview_payload") or {}
        preview["image_error"] = image_error
        update_post(
            post_id,
            image_prompt=image_prompt,
            final_image_url=image_url,
            preview_payload_json=preview,
            last_regen_instruction=instruction or None,
        )

    latest = fetch_post(post_id)
    update_post(post_id, telegram_caption=generate_post_caption(latest))
    return fetch_post(post_id)


@app.post("/api/posts/{post_id}/publish")
async def publish(post_id: int, request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    mode = payload.get("mode")
    post = fetch_post(post_id)
    if mode == "now":
        return publish_now_internal(post_id)
    if mode == "schedule":
        scheduled_for = parse_to_utc_iso(payload.get("scheduled_for") or "")
        update_post(post_id, status="scheduled", scheduled_for=scheduled_for)
        return fetch_post(post_id)
    if mode == "replace_phrase":
        replacement = (payload.get("replacement_phrase") or "").strip()
        if not replacement:
            raise HTTPException(status_code=400, detail="replacement_phrase required")
        update_post(post_id, text_body=replacement)
        # Rebuild caption and image in one go as requested.
        req = JSONResponse({})
        _ = req  # keep linter quiet
        return await create_preview(
            post_id,
            _mock_request({"scenario": post.get("selected_scenario"), "regen_instruction": "Замена фразы"}),
            session_id=session_id,
        )
    raise HTTPException(status_code=400, detail="mode must be now|schedule|replace_phrase")


class _MockRequest:
    def __init__(self, payload: dict[str, Any]):
        self._payload = payload

    async def json(self) -> dict[str, Any]:
        return self._payload


def _mock_request(payload: dict[str, Any]) -> _MockRequest:
    return _MockRequest(payload)


def publish_now_internal(post_id: int) -> dict[str, Any]:
    post = fetch_post(post_id)
    tg_res = None
    tg_err = None
    try:
        tg_res = telegram_send_publish(post)
    except HTTPException as e:
        tg_err = e.detail
    except Exception as e:
        tg_err = str(e)
    preview = post.get("preview_payload") or {}
    preview["published"] = {"mode": "now", "at": now_iso(), "telegram": bool(tg_res), "telegram_error": tg_err}
    pub_message_id = None
    if tg_res and (tg_res.get("result") or {}).get("message_id") is not None:
        pub_message_id = str(tg_res["result"]["message_id"])
    update_post(post_id, status="published", preview_payload_json=preview, published_message_id=pub_message_id)
    post_after = fetch_post(post_id)
    mark_phrase_published_if_linked(post_after)
    return post_after


@app.get("/api/logs/image")
def image_logs(session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> list[dict[str, Any]]:
    ensure_auth(session_id)
    with db() as conn:
        rows = conn.execute(
            """
            SELECT id, post_id, model, size, input_tokens, output_tokens, reasoning_tokens, image_tokens,
                   estimated_cost_usd, status, error, created_at
            FROM image_generation_logs
            ORDER BY id DESC
            LIMIT 200
            """
        ).fetchall()
    return [dict(r) for r in rows]


@app.get("/api/integrations/readiness")
def integrations_readiness(session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    missing = []
    if not runtime_openrouter_key():
        missing.append("OPENROUTER_API_KEY")
    if not runtime_telegram_token():
        missing.append("TELEGRAM_BOT_TOKEN")
    if TELEGRAM_MODE == "webhook" and not runtime_telegram_webhook_secret():
        missing.append("TELEGRAM_WEBHOOK_SECRET")
    if runtime_enable_instagram() and (not runtime_instagram_token() or not runtime_instagram_user_id()):
        missing.extend([x for x in ["INSTAGRAM_ACCESS_TOKEN", "INSTAGRAM_IG_USER_ID"] if x not in missing])
    if runtime_enable_pinterest() and (not runtime_pinterest_token() or not runtime_pinterest_board_id()):
        missing.extend([x for x in ["PINTEREST_ACCESS_TOKEN", "PINTEREST_BOARD_ID"] if x not in missing])
    if runtime_enable_vk() and (not runtime_vk_token() or not runtime_vk_group_id()):
        missing.extend([x for x in ["VK_ACCESS_TOKEN", "VK_GROUP_ID"] if x not in missing])
    return {
        "telegram": {
            "bot_token": bool(runtime_telegram_token()),
            "admin_user_id": tg_admin_user_id(),
            "preview_chat": runtime_telegram_preview_chat(),
            "publish_chat": runtime_telegram_publish_chat(),
        },
        "instagram": {
            "enabled": runtime_enable_instagram(),
            "configured": bool(runtime_enable_instagram() and runtime_instagram_token() and runtime_instagram_user_id()),
            "needs": ["INSTAGRAM_ACCESS_TOKEN", "INSTAGRAM_IG_USER_ID"],
        },
        "pinterest": {
            "enabled": runtime_enable_pinterest(),
            "configured": bool(runtime_enable_pinterest() and runtime_pinterest_token() and runtime_pinterest_board_id()),
            "needs": ["PINTEREST_ACCESS_TOKEN", "PINTEREST_BOARD_ID"],
        },
        "vk": {
            "enabled": runtime_enable_vk(),
            "configured": bool(runtime_enable_vk() and runtime_vk_token() and runtime_vk_group_id()),
            "needs": ["VK_ACCESS_TOKEN", "VK_GROUP_ID"],
        },
        "database": {"backend": DB_BACKEND, "database_url_set": bool(DATABASE_URL)},
        "missing_required": missing,
    }


@app.post("/api/posts/{post_id}/publish/instagram")
def publish_instagram_endpoint(post_id: int, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    if not runtime_enable_instagram():
        raise HTTPException(status_code=503, detail="Instagram publishing temporarily disabled")
    post = fetch_post(post_id)
    res = instagram_publish_post(post)
    return {"ok": True, "post_id": post_id, "instagram": res}


@app.post("/api/posts/{post_id}/publish/pinterest")
def publish_pinterest_endpoint(post_id: int, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    if not runtime_enable_pinterest():
        raise HTTPException(status_code=503, detail="Pinterest publishing temporarily disabled")
    post = fetch_post(post_id)
    res = pinterest_publish_post(post)
    return {"ok": True, "post_id": post_id, "pinterest": res}


@app.post("/api/posts/{post_id}/publish/vk")
def publish_vk_endpoint(post_id: int, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    if not runtime_enable_vk():
        raise HTTPException(status_code=503, detail="VK publishing temporarily disabled")
    post = fetch_post(post_id)
    res = vk_publish_post(post)
    return {"ok": True, "post_id": post_id, "vk": res}


@app.post("/api/posts/{post_id}/publish/multi")
async def publish_multi(post_id: int, request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    targets = payload.get("targets") or []
    if not isinstance(targets, list) or not targets:
        raise HTTPException(status_code=400, detail="targets[] required")
    post = fetch_post(post_id)
    result: dict[str, Any] = {"post_id": post_id, "targets": {}, "ok": True}
    for target in targets:
        t = str(target).strip().lower()
        try:
            if t == "telegram":
                tg_res = telegram_send_publish(post)
                result["targets"]["telegram"] = {"ok": True, "result": tg_res}
            elif t == "vk":
                result["targets"]["vk"] = {"ok": True, "result": vk_publish_post(post)}
            elif t == "instagram":
                if not runtime_enable_instagram():
                    raise HTTPException(status_code=503, detail="Instagram disabled")
                result["targets"]["instagram"] = {"ok": True, "result": instagram_publish_post(post)}
            elif t == "pinterest":
                if not runtime_enable_pinterest():
                    raise HTTPException(status_code=503, detail="Pinterest disabled")
                result["targets"]["pinterest"] = {"ok": True, "result": pinterest_publish_post(post)}
            else:
                result["targets"][t] = {"ok": False, "error": "unsupported target"}
                result["ok"] = False
        except Exception as e:
            result["targets"][t] = {"ok": False, "error": str(e)}
            result["ok"] = False
    if result["targets"].get("telegram", {}).get("ok"):
        update_post(post_id, status="published")
        mark_phrase_published_if_linked(fetch_post(post_id))
    return result


@app.post("/api/phrases/{phrase_id}/create-post")
def create_post_from_phrase(phrase_id: int, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    with db() as conn:
        phrase = conn.execute("SELECT id, text_body FROM phrases WHERE id = ?", (phrase_id,)).fetchone()
        if not phrase:
            raise HTTPException(status_code=404, detail="phrase not found")
        text = (phrase["text_body"] or "").strip()
        if not text:
            raise HTTPException(status_code=400, detail="empty phrase")
        created_at = now_iso()
        title = text
        if DB_BACKEND == "postgres":
            cur = conn.execute(
                """
                INSERT INTO posts (title, text_body, source_url, source_kind, status, recognized_text, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'draft', ?, ?, ?)
                RETURNING id
                """,
                (title, text, f"phrase:{phrase_id}", "phrase", text, created_at, created_at),
            )
            row = cur.fetchone()
            post_id = row["id"]
        else:
            cur = conn.execute(
                """
                INSERT INTO posts (title, text_body, source_url, source_kind, status, recognized_text, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'draft', ?, ?, ?)
                """,
                (title, text, f"phrase:{phrase_id}", "phrase", text, created_at, created_at),
            )
            post_id = cur.lastrowid
    return fetch_post(int(post_id))


@app.get("/api/phrases")
def list_phrases(
    session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE),
    limit: int = Query(default=1000, ge=1, le=5000),
    offset: int = Query(default=0, ge=0, le=20000),
    status: str = Query(default="all"),
    search: str = Query(default=""),
    topic: str = Query(default=""),
) -> list[dict[str, Any]]:
    ensure_auth(session_id)
    where = []
    params: list[Any] = []
    status = (status or "all").strip().lower()
    if status in {"0", "new", "unpublished"}:
        where.append("coalesce(is_published,0)=0")
    elif status in {"1", "published"}:
        where.append("coalesce(is_published,0)=1")
    if search.strip():
        where.append("lower(text_body) LIKE ?")
        params.append(f"%{search.strip().lower()}%")
    if topic.strip():
        where.append("lower(coalesce(topic,'')) LIKE ?")
        params.append(f"%{topic.strip().lower()}%")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    with db() as conn:
        rows = conn.execute(
            f"SELECT id, text_body, topic, is_published, created_at, updated_at FROM phrases {where_sql} ORDER BY id ASC LIMIT ? OFFSET ?",
            (*params, limit, offset),
        ).fetchall()
    return [dict(r) for r in rows]


@app.post("/api/phrases/create-post-random")
def create_post_from_random_phrase(
    session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE),
    only_new: bool = Query(default=True),
) -> dict[str, Any]:
    ensure_auth(session_id)
    with db() as conn:
        if only_new:
            row = conn.execute(
                "SELECT id FROM phrases WHERE coalesce(is_published,0)=0 ORDER BY RANDOM() LIMIT 1"
            ).fetchone()
        else:
            row = conn.execute("SELECT id FROM phrases ORDER BY RANDOM() LIMIT 1").fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="phrases not found")
    return create_post_from_phrase(int(row["id"]), session_id=session_id)


@app.post("/api/phrases/import-tsv")
async def import_phrases_tsv(request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    raw_text = payload.get("raw_tsv") or ""
    if not raw_text.strip():
        raise HTTPException(status_code=400, detail="raw_tsv is required")

    parsed: list[tuple[int, str]] = []
    skipped = 0
    for line in raw_text.splitlines():
        s = line.strip()
        if not s:
            skipped += 1
            continue
        if "\t" not in s:
            skipped += 1
            continue
        flag, phrase = s.split("\t", 1)
        flag = flag.strip()
        phrase = phrase.strip()
        if flag not in {"0", "1"} or not phrase:
            skipped += 1
            continue
        parsed.append((int(flag), phrase))

    inserted = 0
    updated = 0
    now = now_iso()
    with db() as conn:
        for is_pub, phrase in parsed:
            existing = conn.execute("SELECT id, is_published FROM phrases WHERE text_body = ?", (phrase,)).fetchone()
            if existing:
                if int(existing["is_published"]) != is_pub:
                    conn.execute(
                        "UPDATE phrases SET is_published = ?, updated_at = ? WHERE id = ?",
                        (is_pub, now, existing["id"]),
                    )
                    updated += 1
                continue
            if DB_BACKEND == "postgres":
                conn.execute(
                    "INSERT INTO phrases(text_body, is_published, created_at, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT (text_body) DO NOTHING",
                    (phrase, is_pub, now, now),
                )
            else:
                conn.execute(
                    "INSERT OR IGNORE INTO phrases(text_body, is_published, created_at, updated_at) VALUES (?, ?, ?, ?)",
                    (phrase, is_pub, now, now),
                )
            inserted += 1

    return {"ok": True, "parsed": len(parsed), "inserted": inserted, "updated": updated, "skipped": skipped}


@app.post("/api/phrases/import-text")
async def import_phrases_text(request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    raw_text = (payload.get("raw_text") or "").strip()
    if not raw_text:
        raise HTTPException(status_code=400, detail="raw_text is required")
    default_status = int(payload.get("is_published", 0) or 0)
    phrases: list[tuple[int, str]] = []
    for line in raw_text.splitlines():
        s = line.strip()
        if not s:
            continue
        phrases.append((default_status, s))
    now = now_iso()
    inserted = 0
    updated = 0
    with db() as conn:
        for is_pub, phrase in phrases:
            existing = conn.execute("SELECT id, is_published FROM phrases WHERE text_body = ?", (phrase,)).fetchone()
            if existing:
                if int(existing["is_published"]) != is_pub:
                    conn.execute("UPDATE phrases SET is_published = ?, updated_at = ? WHERE id = ?", (is_pub, now, existing["id"]))
                    updated += 1
                continue
            if DB_BACKEND == "postgres":
                conn.execute(
                    "INSERT INTO phrases(text_body, is_published, created_at, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT (text_body) DO NOTHING",
                    (phrase, is_pub, now, now),
                )
            else:
                conn.execute(
                    "INSERT OR IGNORE INTO phrases(text_body, is_published, created_at, updated_at) VALUES (?, ?, ?, ?)",
                    (phrase, is_pub, now, now),
                )
            inserted += 1
    return {"ok": True, "parsed": len(phrases), "inserted": inserted, "updated": updated}


@app.post("/api/phrases/import-csv")
async def import_phrases_csv(request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    raw_csv = ""
    content_type = (request.headers.get("content-type") or "").lower()
    if "multipart/form-data" in content_type:
        form = await request.form()
        upload = form.get("file")
        if upload and hasattr(upload, "read"):
            file_bytes = await upload.read()
            raw_csv = file_bytes.decode("utf-8-sig", errors="ignore").strip()
    else:
        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail="invalid JSON payload") from exc
        raw_csv = (payload.get("raw_csv") or "").strip()
    if not raw_csv:
        raise HTTPException(status_code=400, detail="raw_csv is required")
    reader = csv.DictReader(io.StringIO(raw_csv))
    inserted = 0
    updated = 0
    skipped = 0
    parsed = 0
    now = now_iso()
    with db() as conn:
        for row in reader:
            phrase = (row.get("Текст ru") or row.get("text_ru") or row.get("text") or "").strip()
            if not phrase:
                skipped += 1
                continue
            status_raw = (row.get("0/1") or row.get("status") or "0").strip()
            is_pub = 1 if status_raw == "1" else 0
            topic = (row.get("Тема") or row.get("topic") or "").strip() or None
            parsed += 1
            existing = conn.execute("SELECT id, is_published, topic FROM phrases WHERE text_body = ?", (phrase,)).fetchone()
            if existing:
                if int(existing["is_published"]) != is_pub or (existing.get("topic") if isinstance(existing, dict) else existing["topic"]) != topic:
                    conn.execute(
                        "UPDATE phrases SET is_published = ?, topic = ?, updated_at = ? WHERE id = ?",
                        (is_pub, topic, now, existing["id"]),
                    )
                    updated += 1
                continue
            if DB_BACKEND == "postgres":
                conn.execute(
                    "INSERT INTO phrases(text_body, topic, is_published, created_at, updated_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT (text_body) DO NOTHING",
                    (phrase, topic, is_pub, now, now),
                )
            else:
                conn.execute(
                    "INSERT OR IGNORE INTO phrases(text_body, topic, is_published, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                    (phrase, topic, is_pub, now, now),
                )
            inserted += 1
    return {"ok": True, "parsed": parsed, "inserted": inserted, "updated": updated, "skipped": skipped}


@app.put("/api/phrases/{phrase_id:int}")
async def update_phrase(phrase_id: int, request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    text_body = (payload.get("text_body") or "").strip()
    topic = (payload.get("topic") or "").strip() or None
    is_published = payload.get("is_published")
    sets = []
    params: list[Any] = []
    if text_body:
        sets.append("text_body = ?")
        params.append(text_body)
    if "topic" in payload:
        sets.append("topic = ?")
        params.append(topic)
    if is_published is not None:
        sets.append("is_published = ?")
        params.append(int(is_published))
    if not sets:
        raise HTTPException(status_code=400, detail="nothing to update")
    sets.append("updated_at = ?")
    params.append(now_iso())
    params.append(phrase_id)
    with db() as conn:
        conn.execute(f"UPDATE phrases SET {', '.join(sets)} WHERE id = ?", params)
        row = conn.execute(
            "SELECT id, text_body, topic, is_published, created_at, updated_at FROM phrases WHERE id = ?",
            (phrase_id,),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="phrase not found")
    return dict(row)


@app.delete("/api/phrases/{phrase_id:int}")
def delete_phrase(phrase_id: int, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    with db() as conn:
        conn.execute("DELETE FROM phrases WHERE id = ?", (phrase_id,))
    return {"ok": True, "phrase_id": phrase_id}


@app.put("/api/phrases/bulk-status")
async def bulk_update_phrases_status(request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    ids = payload.get("ids") or []
    is_published = payload.get("is_published")
    if not isinstance(ids, list) or not ids:
        raise HTTPException(status_code=400, detail="ids[] required")
    if is_published not in {0, 1, "0", "1"}:
        raise HTTPException(status_code=400, detail="is_published must be 0 or 1")
    status_val = int(is_published)
    updated = 0
    with db() as conn:
        for phrase_id in ids:
            conn.execute(
                "UPDATE phrases SET is_published = ?, updated_at = ? WHERE id = ?",
                (status_val, now_iso(), int(phrase_id)),
            )
            updated += 1
    return {"ok": True, "updated": updated}


@app.delete("/api/phrases/bulk-delete")
async def bulk_delete_phrases(request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    ids = payload.get("ids") or []
    if not isinstance(ids, list) or not ids:
        raise HTTPException(status_code=400, detail="ids[] required")
    deleted = 0
    with db() as conn:
        for phrase_id in ids:
            conn.execute("DELETE FROM phrases WHERE id = ?", (int(phrase_id),))
            deleted += 1
    return {"ok": True, "deleted": deleted}


@app.post("/api/phrases/import-image-url")
async def import_phrases_image_url(request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    image_url = validate_public_http_url(payload.get("image_url") or "")
    ocr_text = openrouter_extract_text_from_image(image_url)
    phrases = extract_phrases_from_ocr_text(ocr_text)
    return {"ok": True, "image_url": image_url, "ocr_text": ocr_text, "phrases": phrases}


@app.post("/api/phrases/ocr-image-base64")
async def ocr_phrase_image_base64(request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    image_data_url = (payload.get("image_data_url") or "").strip()
    if not image_data_url.startswith("data:image/"):
        raise HTTPException(status_code=400, detail="image_data_url must be a data:image/* base64 URL")
    ocr_text = openrouter_extract_text_from_image(image_data_url)
    phrases = extract_phrases_from_ocr_text(ocr_text)
    return {"ok": True, "ocr_text": ocr_text, "phrases": phrases}


@app.post("/api/phrases/ocr-images-base64")
async def ocr_phrase_images_base64(request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    images = payload.get("images") or []
    if not isinstance(images, list) or not images:
        raise HTTPException(status_code=400, detail="images[] required")
    if len(images) > 20:
        raise HTTPException(status_code=400, detail="max 20 images per batch")

    items: list[dict[str, Any]] = []
    for idx, item in enumerate(images, start=1):
        data_url = str((item or {}).get("image_data_url") or "").strip()
        name = str((item or {}).get("name") or f"image_{idx}.png").strip()
        if not data_url.startswith("data:image/"):
            items.append({"name": name, "ok": False, "error": "invalid image_data_url"})
            continue
        try:
            ocr_text = openrouter_extract_text_from_image(data_url)
            phrases = extract_phrases_from_ocr_text(ocr_text)
            items.append(
                {
                    "name": name,
                    "ok": True,
                    "ocr_text": ocr_text,
                    "phrase": phrases[0] if phrases else "",
                    "phrases": phrases,
                }
            )
        except Exception as e:
            items.append({"name": name, "ok": False, "error": str(e)})
    recognized = sum(1 for x in items if x.get("ok") and x.get("phrase"))
    return {"ok": True, "items": items, "recognized": recognized, "total": len(items)}


@app.post("/api/phrases/import-image-base64")
async def import_phrase_image_base64(request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    """Backward compatible endpoint: OCR + auto-import."""
    ensure_auth(session_id)
    payload = await request.json()
    image_data_url = (payload.get("image_data_url") or "").strip()
    if not image_data_url.startswith("data:image/"):
        raise HTTPException(status_code=400, detail="image_data_url must be a data:image/* base64 URL")
    is_published = int(payload.get("is_published", 0) or 0)
    ocr_text = openrouter_extract_text_from_image(image_data_url)
    phrases = extract_phrases_from_ocr_text(ocr_text)
    raw_text = "\n".join(phrases)
    res = await import_phrases_text(_mock_request({"raw_text": raw_text, "is_published": is_published}), session_id=session_id)  # type: ignore[arg-type]
    return {"ok": True, "ocr_text": ocr_text, "phrases": phrases, **res}


@app.post("/api/phrases/daily-preview")
async def create_daily_phrase_preview(session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    with db() as conn:
        phrase = conn.execute(
            "SELECT id, text_body FROM phrases WHERE coalesce(is_published,0)=0 ORDER BY id ASC LIMIT 1"
        ).fetchone()
        if not phrase:
            raise HTTPException(status_code=404, detail="Нет неопубликованных фраз")
    post = create_post_from_phrase(int(phrase["id"]), session_id=session_id)
    # Auto-build preview and send to Telegram admin/preview chat.
    built = await create_preview(int(post["id"]), _mock_request({"scenario": "", "regen_instruction": ""}), session_id=session_id)
    return {"ok": True, "phrase_id": int(phrase["id"]), "post": built}


@app.get("/api/films")
def list_films(
    session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE),
    search: str = Query(default=""),
    limit: int = Query(default=200, ge=1, le=1000),
    offset: int = Query(default=0, ge=0, le=20000),
) -> list[dict[str, Any]]:
    ensure_auth(session_id)
    with db() as conn:
        if search.strip():
            rows = conn.execute(
                """
                SELECT id, title, year, country, description, tags, created_at, updated_at
                FROM films
                WHERE lower(title) LIKE ? OR lower(coalesce(description,'')) LIKE ? OR lower(coalesce(tags,'')) LIKE ?
                ORDER BY id DESC
                LIMIT ? OFFSET ?
                """,
                (
                    f"%{search.strip().lower()}%",
                    f"%{search.strip().lower()}%",
                    f"%{search.strip().lower()}%",
                    limit,
                    offset,
                ),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, title, year, country, description, tags, created_at, updated_at FROM films ORDER BY id DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
    return [dict(r) for r in rows]


@app.post("/api/films")
async def create_film(request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    title = (payload.get("title") or "").strip()
    if not title:
        raise HTTPException(status_code=400, detail="title required")
    year = payload.get("year")
    country = (payload.get("country") or "").strip() or None
    description = (payload.get("description") or "").strip() or None
    tags = (payload.get("tags") or "").strip() or None
    created = now_iso()
    with db() as conn:
        if DB_BACKEND == "postgres":
            row = conn.execute(
                """
                INSERT INTO films(title, year, country, description, tags, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                RETURNING id, title, year, country, description, tags, created_at, updated_at
                """,
                (title, year, country, description, tags, created, created),
            ).fetchone()
            return dict(row)
        cur = conn.execute(
            """
            INSERT INTO films(title, year, country, description, tags, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (title, year, country, description, tags, created, created),
        )
        row = conn.execute(
            "SELECT id, title, year, country, description, tags, created_at, updated_at FROM films WHERE id = ?",
            (cur.lastrowid,),
        ).fetchone()
        return dict(row)


@app.put("/api/films/{film_id}")
async def update_film(film_id: int, request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    fields: dict[str, Any] = {}
    for k in ("title", "year", "country", "description", "tags"):
        if k in payload:
            if k == "year":
                fields[k] = payload.get(k)
            else:
                fields[k] = (payload.get(k) or "").strip() or None
    if not fields:
        raise HTTPException(status_code=400, detail="nothing to update")
    fields["updated_at"] = now_iso()
    with db() as conn:
        sets = ", ".join(f"{k} = ?" for k in fields.keys())
        conn.execute(f"UPDATE films SET {sets} WHERE id = ?", (*fields.values(), film_id))
        row = conn.execute(
            "SELECT id, title, year, country, description, tags, created_at, updated_at FROM films WHERE id = ?",
            (film_id,),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="film not found")
    return dict(row)


@app.delete("/api/films/{film_id}")
def delete_film(film_id: int, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    with db() as conn:
        conn.execute("DELETE FROM films WHERE id = ?", (film_id,))
    return {"ok": True, "film_id": film_id}


@app.post("/api/films/{film_id}/create-post")
async def create_post_from_film(film_id: int, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    with db() as conn:
        film = conn.execute(
            "SELECT id, title, year, country, description, tags FROM films WHERE id = ?",
            (film_id,),
        ).fetchone()
    if not film:
        raise HTTPException(status_code=404, detail="film not found")
    title = str(film["title"]).strip()
    prompt = (
        "Сделай короткий вдохновляющий пост о фильме для Telegram-канала. "
        "Нужно 2-5 предложений, без спойлеров, на русском.\n"
        f"Название: {title}\n"
        f"Год: {film.get('year')}\n"
        f"Страна: {film.get('country')}\n"
        f"Описание: {film.get('description')}\n"
        f"Теги: {film.get('tags')}\n"
    )
    text_body = (openrouter_generate_text(prompt) or "").strip()
    if not text_body:
        text_body = f"Фильм: {title}. Рекомендуем к просмотру."
    created = now_iso()
    with db() as conn:
        if DB_BACKEND == "postgres":
            row = conn.execute(
                """
                INSERT INTO posts(title, text_body, source_url, source_kind, status, recognized_text, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'draft', ?, ?, ?)
                RETURNING id
                """,
                (title, text_body, f"film:{film_id}", "film", text_body, created, created),
            ).fetchone()
            post_id = row["id"]
        else:
            cur = conn.execute(
                """
                INSERT INTO posts(title, text_body, source_url, source_kind, status, recognized_text, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'draft', ?, ?, ?)
                """,
                (title, text_body, f"film:{film_id}", "film", text_body, created, created),
            )
            post_id = cur.lastrowid
    return fetch_post(int(post_id))


@app.post("/api/telegram/admin/bind")
async def bind_admin_from_web(request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    user_id = int(payload.get("telegram_user_id"))
    kv_set("telegram_admin_user_id", str(user_id))
    return {"ok": True, "telegram_admin_user_id": user_id}


def _parse_callback_data(data: str) -> tuple[str, Optional[int], Optional[str]]:
    parts = (data or "").split(":")
    if len(parts) < 3 or parts[0] != "ks":
        return "", None, None
    action = parts[1]
    try:
        post_id = int(parts[2])
    except Exception:
        post_id = None
    extra = parts[3] if len(parts) > 3 else None
    return action, post_id, extra


def _telegram_admin_check_or_learn(user_id: int) -> bool:
    admin = tg_admin_user_id()
    if admin is None:
        kv_set("telegram_admin_user_id", str(user_id))
        return True
    return admin == user_id


async def _telegram_handle_message(update: dict[str, Any]) -> dict[str, Any]:
    msg = update.get("message") or {}
    user = msg.get("from") or {}
    user_id = user.get("id")
    chat = msg.get("chat") or {}
    text = (msg.get("text") or "").strip()
    if not user_id:
        return {"ok": True}
    if not _telegram_admin_check_or_learn(int(user_id)):
        send_telegram_text(chat.get("id"), "Доступ запрещён.")
        return {"ok": True}
    if text == "/start":
        kv_set("telegram_admin_user_id", str(user_id))
        send_telegram_text(
            chat.get("id"),
            "Админ привязан. Теперь сюда будут приходить инструкции по перегенерации/публикации.",
            reply_markup=build_manual_create_keyboard(),
        )
        return {"ok": True}
    if text == "/new_post":
        tg_state_set(int(user_id), "await_manual_phrase_text", {})
        send_telegram_text(chat.get("id"), "Пришли текст фразы одним сообщением. Потом подтверждение Да/Нет.")
        return {"ok": True}
    if text == "/add_phrases":
        tg_state_set(int(user_id), "await_add_phrase_input", {})
        send_telegram_text(
            chat.get("id"),
            "Режим добавления фраз: отправь текст (по одной фразе в строке) или картинку с фразой. После распознавания подтверждение Да/Нет.",
        )
        return {"ok": True}

    state = tg_state_get(int(user_id))
    if not state:
        return {"ok": True, "message": "no pending state"}
    st = state["state"]
    payload = state["payload"] or {}
    post_id_raw = payload.get("post_id")
    post_id = int(post_id_raw) if post_id_raw is not None else None
    if st == "await_regen_instruction":
        if not post_id:
            tg_state_clear(int(user_id))
            send_telegram_text(chat.get("id"), "Не найден post_id. Начни заново с кнопок превью.")
            return {"ok": True}
        target = payload.get("target", "both")
        tg_state_clear(int(user_id))
        await regenerate_preview(post_id, _mock_request({"target": target, "instruction": text}), session_id="telegram-internal")  # type: ignore[arg-type]
        send_telegram_text(chat.get("id"), "Перегенерация выполнена. Новое превью отправлено.")
        return {"ok": True}
    if st == "await_schedule_datetime":
        if not post_id:
            tg_state_clear(int(user_id))
            send_telegram_text(chat.get("id"), "Не найден post_id. Начни заново с кнопок превью.")
            return {"ok": True}
        tg_state_clear(int(user_id))
        await publish(post_id, _mock_request({"mode": "schedule", "scheduled_for": text}), session_id="telegram-internal")  # type: ignore[arg-type]
        send_telegram_text(chat.get("id"), f"Публикация запланирована: {text}")
        return {"ok": True}
    if st == "await_replace_phrase":
        if not post_id:
            tg_state_clear(int(user_id))
            send_telegram_text(chat.get("id"), "Не найден post_id. Начни заново с кнопок превью.")
            return {"ok": True}
        tg_state_clear(int(user_id))
        await publish(post_id, _mock_request({"mode": "replace_phrase", "replacement_phrase": text}), session_id="telegram-internal")  # type: ignore[arg-type]
        send_telegram_text(chat.get("id"), "Фраза заменена, картинка и описание пересобраны, новое превью отправлено.")
        return {"ok": True}
    if st == "await_manual_phrase_text":
        phrase_text = text.strip()
        if not phrase_text:
            send_telegram_text(chat.get("id"), "Пустой текст. Пришли фразу ещё раз.")
            return {"ok": True}
        tg_state_set(int(user_id), "await_manual_phrase_confirm", {"phrase_text": phrase_text})
        send_telegram_text(
            chat.get("id"),
            f'Создать новую фразу?\n\n"{phrase_text}"',
            reply_markup=tg_keyboard([[("Да", "ks:manualsave:0:yes"), ("Нет", "ks:manualsave:0:no")]]),
        )
        return {"ok": True}
    if st == "await_manual_phrase_confirm":
        send_telegram_text(chat.get("id"), "Нажми кнопку Да/Нет под сообщением, чтобы продолжить.")
        return {"ok": True}
    if st == "await_add_phrase_input":
        photos = msg.get("photo") or []
        if photos:
            best = photos[-1]
            file_id = best.get("file_id")
            if not file_id:
                send_telegram_text(chat.get("id"), "Не нашёл file_id у картинки. Попробуй ещё раз.")
                return {"ok": True}
            try:
                data_url = telegram_file_data_url(file_id)
                ocr_text = openrouter_extract_text_from_image(data_url)
                phrases = extract_phrases_from_ocr_text(ocr_text)
                phrase_text = (phrases[0] if phrases else "").strip()
                if not phrase_text:
                    send_telegram_text(chat.get("id"), "Не удалось распознать фразу. Отправь другую картинку.")
                    return {"ok": True}
                tg_state_set(int(user_id), "await_add_phrase_confirm", {"phrase_text": phrase_text})
                send_telegram_text(
                    chat.get("id"),
                    f'Распознано: "{phrase_text}"\nСоздать новую фразу?',
                    reply_markup=tg_keyboard([[("Да", "ks:addphrase:0:yes"), ("Нет", "ks:addphrase:0:no")]]),
                )
                return {"ok": True}
            except Exception as e:
                send_telegram_text(chat.get("id"), f"Ошибка OCR: {str(e)[:300]}")
                return {"ok": True}
        raw = (text or "").strip()
        if not raw:
            send_telegram_text(chat.get("id"), "Пришли текст фразы (или список фраз) или картинку.")
            return {"ok": True}
        lines = [x.strip() for x in raw.splitlines() if x.strip()]
        if len(lines) == 1:
            tg_state_set(int(user_id), "await_add_phrase_confirm", {"phrase_text": lines[0]})
            send_telegram_text(
                chat.get("id"),
                f'Создать новую фразу?\n\n"{lines[0]}"',
                reply_markup=tg_keyboard([[("Да", "ks:addphrase:0:yes"), ("Нет", "ks:addphrase:0:no")]]),
            )
            return {"ok": True}
        tg_state_set(int(user_id), "await_add_bulk_confirm", {"phrases": lines})
        send_telegram_text(
            chat.get("id"),
            f"Получено {len(lines)} фраз. Добавить в общий список?",
            reply_markup=tg_keyboard([[("Да", "ks:addbulk:0:yes"), ("Нет", "ks:addbulk:0:no")]]),
        )
        return {"ok": True}
    if st == "await_add_phrase_confirm":
        send_telegram_text(chat.get("id"), "Нажми Да/Нет под сообщением для добавления фразы.")
        return {"ok": True}
    if st == "await_add_bulk_confirm":
        send_telegram_text(chat.get("id"), "Нажми Да/Нет под сообщением для пакетного добавления.")
        return {"ok": True}
    return {"ok": True}


async def _telegram_handle_callback(update: dict[str, Any]) -> dict[str, Any]:
    cq = update.get("callback_query") or {}
    cb_id = cq.get("id")
    user = cq.get("from") or {}
    user_id = user.get("id")
    msg = cq.get("message") or {}
    chat_id = (msg.get("chat") or {}).get("id")
    data = cq.get("data") or ""
    if not user_id:
        return {"ok": True}
    if not _telegram_admin_check_or_learn(int(user_id)):
        if cb_id:
            answer_callback(cb_id, "Нет доступа")
        return {"ok": True}
    action, post_id, extra = _parse_callback_data(data)
    if action in {"manual", "manualsave", "addphrases", "addphrase", "addbulk"}:
        pass
    elif not post_id:
        if cb_id:
            answer_callback(cb_id, "Некорректная кнопка")
        return {"ok": True}
    if action == "manual":
        tg_state_set(int(user_id), "await_manual_phrase_text", {})
        if cb_id:
            answer_callback(cb_id, "Пришли фразу текстом")
        if chat_id:
            send_telegram_text(chat_id, "Пришли текст фразы одним сообщением. После этого спрошу подтверждение Да/Нет.")
        return {"ok": True}
    if action == "manualsave":
        decision = (extra or "").strip().lower()
        state = tg_state_get(int(user_id))
        if not state or state.get("state") != "await_manual_phrase_confirm":
            if cb_id:
                answer_callback(cb_id, "Нет ожидающей фразы")
            if chat_id:
                send_telegram_text(chat_id, "Нет ожидающей фразы. Нажми «Сгенерить пост вручную» и отправь фразу заново.")
            return {"ok": True}
        phrase_text = str((state.get("payload") or {}).get("phrase_text") or "").strip()
        if decision != "yes":
            tg_state_clear(int(user_id))
            if cb_id:
                answer_callback(cb_id, "Отменено")
            if chat_id:
                send_telegram_text(chat_id, "Создание фразы отменено. Задача отброшена.")
            return {"ok": True}
        if not phrase_text:
            tg_state_clear(int(user_id))
            if cb_id:
                answer_callback(cb_id, "Пустая фраза")
            if chat_id:
                send_telegram_text(chat_id, "Не удалось взять текст фразы. Нажми «Сгенерить пост вручную» снова.")
            return {"ok": True}
        try:
            phrase = upsert_phrase_text(phrase_text, is_published=0)
            post = create_post_from_phrase(int(phrase["id"]), session_id="telegram-internal")
            await create_preview(int(post["id"]), _mock_request({"scenario": "", "regen_instruction": ""}), session_id="telegram-internal")  # type: ignore[arg-type]
            tg_state_clear(int(user_id))
            if cb_id:
                answer_callback(cb_id, "Создано")
            if chat_id:
                send_telegram_text(chat_id, f"Создано: фраза #{phrase['id']}, пост #{post['id']}. Превью отправлено.")
            return {"ok": True}
        except Exception as e:
            tg_state_clear(int(user_id))
            if cb_id:
                answer_callback(cb_id, "Ошибка")
            if chat_id:
                send_telegram_text(chat_id, f"Ошибка создания: {str(e)[:300]}")
            return {"ok": True}
    if action == "addphrases":
        tg_state_set(int(user_id), "await_add_phrase_input", {})
        if cb_id:
            answer_callback(cb_id, "Отправь текст или картинку")
        if chat_id:
            send_telegram_text(
                chat_id,
                "Режим добавления фраз: отправь текст (по строкам) или картинку. После этого подтверждение Да/Нет.",
            )
        return {"ok": True}
    if action == "addphrase":
        decision = (extra or "").strip().lower()
        state = tg_state_get(int(user_id))
        if not state or state.get("state") != "await_add_phrase_confirm":
            if cb_id:
                answer_callback(cb_id, "Нет ожидающей фразы")
            return {"ok": True}
        phrase_text = str((state.get("payload") or {}).get("phrase_text") or "").strip()
        tg_state_set(int(user_id), "await_add_phrase_input", {})
        if decision != "yes":
            if cb_id:
                answer_callback(cb_id, "Отменено")
            if chat_id:
                send_telegram_text(chat_id, "Добавление отменено.")
            return {"ok": True}
        if not phrase_text:
            if cb_id:
                answer_callback(cb_id, "Пустая фраза")
            return {"ok": True}
        phrase = upsert_phrase_text(phrase_text, is_published=0)
        if cb_id:
            answer_callback(cb_id, "Добавлено")
        if chat_id:
            send_telegram_text(chat_id, f"Фраза добавлена: #{phrase['id']}")
        return {"ok": True}
    if action == "addbulk":
        decision = (extra or "").strip().lower()
        state = tg_state_get(int(user_id))
        if not state or state.get("state") != "await_add_bulk_confirm":
            if cb_id:
                answer_callback(cb_id, "Нет ожидающего пакета")
            return {"ok": True}
        phrases = [x.strip() for x in ((state.get("payload") or {}).get("phrases") or []) if str(x).strip()]
        tg_state_set(int(user_id), "await_add_phrase_input", {})
        if decision != "yes":
            if cb_id:
                answer_callback(cb_id, "Отменено")
            if chat_id:
                send_telegram_text(chat_id, "Пакетное добавление отменено.")
            return {"ok": True}
        inserted = 0
        for phrase_text in phrases:
            upsert_phrase_text(phrase_text, is_published=0)
            inserted += 1
        if cb_id:
            answer_callback(cb_id, "Добавлено")
        if chat_id:
            send_telegram_text(chat_id, f"Добавлено фраз: {inserted}")
        return {"ok": True}
    if action == "cancel":
        update_post(post_id, status="cancelled")
        if cb_id:
            answer_callback(cb_id, "Отменено")
        if chat_id:
            send_telegram_text(chat_id, f"Пост #{post_id}: превью отменено.")
        return {"ok": True}
    if action == "regen":
        if cb_id:
            answer_callback(cb_id, "Выбери, что перегенерировать")
        if chat_id:
            send_telegram_text(
                chat_id,
                f"Пост #{post_id}. Что перегенерировать?",
                tg_keyboard(
                    [
                        [("Текст", f"ks:regenpick:{post_id}:text"), ("Картинку", f"ks:regenpick:{post_id}:image")],
                        [("И то и то", f"ks:regenpick:{post_id}:both")],
                    ]
                ),
            )
        return {"ok": True}
    if action == "regenpick":
        target = extra or "both"
        tg_state_set(int(user_id), "await_regen_instruction", {"post_id": post_id, "target": target})
        if cb_id:
            answer_callback(cb_id, "Пришли текст-инструкцию")
        if chat_id:
            send_telegram_text(chat_id, f"Пост #{post_id}. Пришли текст-инструкцию для перегенерации ({target}).")
        return {"ok": True}
    if action == "pub":
        if cb_id:
            answer_callback(cb_id, "Выбери вариант публикации")
        if chat_id:
            send_telegram_text(
                chat_id,
                f"Пост #{post_id}. Когда публиковать?",
                tg_keyboard(
                    [
                        [("Сейчас", f"ks:pubpick:{post_id}:now")],
                        [("В указанное время и дату", f"ks:pubpick:{post_id}:schedule")],
                        [("Заменить фразу", f"ks:pubpick:{post_id}:replace")],
                    ]
                ),
            )
        return {"ok": True}
    if action == "pubpick":
        choice = extra or ""
        if choice == "now":
            await publish(post_id, _mock_request({"mode": "now"}), session_id="telegram-internal")  # type: ignore[arg-type]
            if cb_id:
                answer_callback(cb_id, "Опубликовано")
            if chat_id:
                send_telegram_text(chat_id, f"Пост #{post_id} опубликован сейчас.")
            return {"ok": True}
        if choice == "schedule":
            tg_state_set(int(user_id), "await_schedule_datetime", {"post_id": post_id})
            if cb_id:
                answer_callback(cb_id, "Пришли ISO дату/время")
            if chat_id:
                send_telegram_text(chat_id, f"Пост #{post_id}. Пришли дату/время в ISO формате, например 2026-02-23T10:00:00+03:00")
            return {"ok": True}
        if choice == "replace":
            tg_state_set(int(user_id), "await_replace_phrase", {"post_id": post_id})
            if cb_id:
                answer_callback(cb_id, "Пришли новую фразу")
            if chat_id:
                send_telegram_text(chat_id, f"Пост #{post_id}. Пришли новый текст/фразу для замены. Система пересоберёт картинку и описание.")
            return {"ok": True}
    if cb_id:
        answer_callback(cb_id, "Неизвестное действие")
    return {"ok": True}


@app.post("/api/telegram/webhook")
async def telegram_webhook(request: Request) -> dict[str, Any]:
    if TELEGRAM_MODE != "webhook":
        raise HTTPException(status_code=409, detail="Telegram mode is not webhook")
    secret = request.headers.get("x-telegram-bot-api-secret-token")
    expected = runtime_telegram_webhook_secret()
    if expected and secret != expected:
        raise HTTPException(status_code=403, detail="Invalid telegram webhook secret")
    update = await request.json()
    if update.get("callback_query"):
        return await _telegram_handle_callback(update)
    if update.get("message"):
        return await _telegram_handle_message(update)
    return {"ok": True}


@app.post("/api/telegram/set-webhook")
async def telegram_set_webhook(request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    if not runtime_telegram_token():
        raise HTTPException(status_code=400, detail="TELEGRAM_BOT_TOKEN not set")
    payload = await request.json()
    public_url = (payload.get("public_url") or "").strip().rstrip("/")
    if not public_url:
        raise HTTPException(status_code=400, detail="public_url required, e.g. https://posting.example.com")
    hook_url = f"{public_url}/api/telegram/webhook"
    req: dict[str, Any] = {"url": hook_url}
    secret = runtime_telegram_webhook_secret()
    if secret:
        req["secret_token"] = secret
    res = telegram_api("setWebhook", req)
    return {"ok": True, "hook_url": hook_url, "telegram": res}


@app.get("/api/openrouter/pricing-note")
def pricing_note(session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    return {
        "message": "Some OpenRouter image models bill via a generic token bucket. If a model shows $0 input/$0 output and $X/M tokens, total cost depends on provider-reported token usage in response usage fields.",
        "example_sourceful_riverflow_v2_pro": {
            "input_tokens_rate": 0,
            "output_tokens_rate": 0,
            "generic_tokens_rate_per_1m": 35.93,
            "formula": "cost_usd = generic_tokens_used / 1_000_000 * 35.93",
        },
    }


@app.get("/{full_path:path}", response_class=HTMLResponse)
def spa_fallback(full_path: str) -> Any:
    if full_path.startswith("api/") or full_path in {"health"}:
        raise HTTPException(status_code=404, detail="not found")
    index_file = FRONTEND_DIST_DIR / "index.html"
    if index_file.exists():
        return FileResponse(str(index_file))
    raise HTTPException(status_code=503, detail="Frontend dist not found")
