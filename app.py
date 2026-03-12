import json
import asyncio
import logging
import os
import re
import difflib
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
import binascii
import textwrap
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from threading import Thread
from typing import Any, Callable, Optional
from uuid import uuid4
from pathlib import Path

from fastapi import Cookie, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.gzip import GZipMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from PIL import Image
from PIL import ImageChops
from PIL import ImageDraw
from PIL import ImageEnhance
from PIL import ImageFilter
from PIL import ImageFont
from PIL import ImageOps

try:
    import pytesseract
except Exception:
    pytesseract = None

try:
    import numpy as np
except Exception:
    np = None

try:
    import cv2
except Exception:
    cv2 = None

try:
    from paddleocr import PaddleOCR
except Exception:
    PaddleOCR = None

try:
    from minio import Minio
except Exception:
    Minio = None

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
_paddle_ocr_client: Optional[Any] = None


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


def media_public_url(object_key: str) -> str:
    safe = urllib.parse.quote((object_key or "").strip("/"), safe="/._-")
    return f"{APP_BASE_URL.rstrip('/')}/media/{safe}"


def media_key_from_url(url: str) -> Optional[str]:
    raw = (url or "").strip()
    if not raw:
        return None
    parsed = urllib.parse.urlparse(raw)
    path = parsed.path or ""
    if "/media/" not in path:
        return None
    key = path.split("/media/", 1)[1].strip("/")
    return urllib.parse.unquote(key) if key else None


def minio_client() -> Any:
    global _minio_client
    if _minio_client is not None:
        return _minio_client
    if Minio is None:
        raise RuntimeError("minio package not installed")
    _minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    return _minio_client


def storage_put_bytes(object_key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
    key = (object_key or "").strip("/")
    if not key:
        raise ValueError("object_key required")
    if STORAGE_MODE == "minio":
        client = minio_client()
        client.put_object(
            MINIO_BUCKET,
            key,
            io.BytesIO(data),
            length=len(data),
            content_type=content_type,
        )
        return media_public_url(key)
    out = (MEDIA_DIR / key).resolve()
    if not str(out).startswith(str(MEDIA_DIR)):
        raise RuntimeError("invalid media path")
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "wb") as f:
        f.write(data)
    return media_public_url(key)


def storage_get_bytes(object_key: str) -> tuple[bytes, str]:
    key = (object_key or "").strip("/")
    if not key:
        raise FileNotFoundError("empty media key")
    if STORAGE_MODE == "minio":
        client = minio_client()
        obj = client.get_object(MINIO_BUCKET, key)
        try:
            data = obj.read()
            ctype = obj.headers.get("Content-Type", "application/octet-stream")
            return data, ctype
        finally:
            obj.close()
            obj.release_conn()
    path = (MEDIA_DIR / key).resolve()
    if not str(path).startswith(str(MEDIA_DIR)) or not path.exists() or not path.is_file():
        raise FileNotFoundError(key)
    with open(path, "rb") as f:
        data = f.read()
    ext = path.suffix.lower()
    ctype = "application/octet-stream"
    if ext in {".jpg", ".jpeg"}:
        ctype = "image/jpeg"
    elif ext == ".png":
        ctype = "image/png"
    elif ext == ".webp":
        ctype = "image/webp"
    return data, ctype


def download_remote_image(url: str) -> tuple[bytes, str]:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=45) as resp:
        data = resp.read()
        ctype = (resp.headers.get("Content-Type") or "image/jpeg").split(";")[0].strip().lower()
    if not data:
        raise RuntimeError("empty image data")
    return data, ctype or "image/jpeg"


def normalize_image_to_square_1024(image_bytes: bytes, content_type: str = "image/jpeg") -> tuple[bytes, str]:
    if not image_bytes:
        raise RuntimeError("empty image bytes")
    with Image.open(io.BytesIO(image_bytes)) as img:
        rgb = img.convert("RGB")
        w, h = rgb.size
        side = min(w, h)
        left = max(0, (w - side) // 2)
        top = max(0, (h - side) // 2)
        cropped = rgb.crop((left, top, left + side, top + side))
        resized = cropped.resize((1024, 1024), Image.Resampling.LANCZOS)
        out = io.BytesIO()
        resized.save(out, format="JPEG", quality=92, optimize=True)
        return out.getvalue(), "image/jpeg"


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
OPENROUTER_TEXT_MODEL = os.getenv("OPENROUTER_TEXT_MODEL", "google/gemini-2.5-flash-lite").strip()
OPENROUTER_VISION_MODEL = os.getenv("OPENROUTER_VISION_MODEL", "meta-llama/llama-4-scout").strip()
OPENROUTER_IMAGE_MODEL = os.getenv("OPENROUTER_IMAGE_MODEL", "black-forest-labs/flux.2-pro").strip()
LOCK_PROVIDER_MODELS = os.getenv("LOCK_PROVIDER_MODELS", "1").strip().lower() in {"1", "true", "yes", "on"}
LOCKED_OPENROUTER_TEXT_MODEL = os.getenv("LOCKED_OPENROUTER_TEXT_MODEL", "google/gemini-2.5-flash-lite").strip()
LOCKED_OPENROUTER_VISION_MODEL = os.getenv("LOCKED_OPENROUTER_VISION_MODEL", "meta-llama/llama-4-scout").strip()
LOCKED_OPENROUTER_IMAGE_MODEL = os.getenv("LOCKED_OPENROUTER_IMAGE_MODEL", "black-forest-labs/flux.2-pro").strip()
OPENROUTER_SITE_URL = os.getenv("OPENROUTER_SITE_URL", "http://localhost:8000").strip()
OPENROUTER_APP_NAME = os.getenv("OPENROUTER_APP_NAME", "KindlySupport Posting").strip()
OCR_PRIMARY_ENGINE = os.getenv("OCR_PRIMARY_ENGINE", "local").strip().lower()
OCR_DISABLE_LLM_FALLBACK = os.getenv("OCR_DISABLE_LLM_FALLBACK", "0").strip() in {"1", "true", "yes"}
OCR_TESSERACT_TIMEOUT_SEC = int(os.getenv("OCR_TESSERACT_TIMEOUT_SEC", "18"))
OCR_BATCH_FAST_MODE = os.getenv("OCR_BATCH_FAST_MODE", "1").strip() in {"1", "true", "yes"}
OCR_BATCH_PARALLELISM = max(1, int(os.getenv("OCR_BATCH_PARALLELISM", "2")))
OCR_BATCH_FULL_FALLBACK_MAX_BATCH = max(1, int(os.getenv("OCR_BATCH_FULL_FALLBACK_MAX_BATCH", "8")))
PHRASE_DUPLICATE_THRESHOLD = float(os.getenv("PHRASE_DUPLICATE_THRESHOLD", "0.90"))
CARD_SIZE_PX = int(os.getenv("CARD_SIZE_PX", "1080"))
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
INSTAGRAM_PROXY_URL = os.getenv("INSTAGRAM_PROXY_URL", "").strip()
INSTAGRAM_DELIVERY_MODE = os.getenv("INSTAGRAM_DELIVERY_MODE", "external_queue").strip().lower()
INSTAGRAM_QUEUE_GITHUB_TOKEN = os.getenv("INSTAGRAM_QUEUE_GITHUB_TOKEN", "").strip()
INSTAGRAM_QUEUE_REPO = os.getenv("INSTAGRAM_QUEUE_REPO", "").strip()
INSTAGRAM_QUEUE_BRANCH = os.getenv("INSTAGRAM_QUEUE_BRANCH", "main").strip()
INSTAGRAM_QUEUE_PATH = os.getenv("INSTAGRAM_QUEUE_PATH", "queue/instagram").strip()
PINTEREST_ACCESS_TOKEN = os.getenv("PINTEREST_ACCESS_TOKEN", "").strip()
PINTEREST_BOARD_ID = os.getenv("PINTEREST_BOARD_ID", "").strip()
ENABLE_INSTAGRAM = os.getenv("ENABLE_INSTAGRAM", "0").strip() in {"1", "true", "yes"}
ENABLE_PINTEREST = os.getenv("ENABLE_PINTEREST", "0").strip() in {"1", "true", "yes"}
ENABLE_VK = os.getenv("ENABLE_VK", "0").strip() in {"1", "true", "yes"}
SESSION_TTL_DAYS = int(os.getenv("SESSION_TTL_DAYS", "30"))
APP_TIMEZONE = os.getenv("APP_TIMEZONE", "Europe/Moscow").strip()
APP_BASE_URL = os.getenv("APP_BASE_URL", "http://localhost:8000").strip()
APP_SECURE_COOKIES = os.getenv("APP_SECURE_COOKIES", "0").strip() in {"1", "true", "yes"}


def _build_trusted_hosts(app_base_url: str, env_hosts: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    def add(host: str) -> None:
        h = (host or "").strip()
        if not h or h in seen:
            return
        seen.add(h)
        out.append(h)

    # Baseline local/dev hosts.
    for h in ("localhost", "127.0.0.1"):
        add(h)

    # Explicit env hosts keep highest priority.
    for h in (env_hosts or "").split(","):
        add(h)

    # Auto-include APP_BASE_URL host to avoid accidental lockout.
    try:
        parsed = urllib.parse.urlparse(app_base_url or "")
        host = (parsed.hostname or "").strip().lower()
        if host:
            add(host)
            parts = host.split(".")
            if len(parts) >= 3:
                parent = ".".join(parts[1:])
                add(f"*.{parent}")
            if len(parts) >= 2:
                root = ".".join(parts[-2:])
                add(root)
                add(f"*.{root}")
    except Exception:
        pass

    return out or ["localhost", "127.0.0.1"]


TRUSTED_HOSTS = _build_trusted_hosts(APP_BASE_URL, os.getenv("TRUSTED_HOSTS", "localhost,127.0.0.1"))
CORS_ALLOW_ORIGINS = [x.strip() for x in os.getenv("CORS_ALLOW_ORIGINS", APP_BASE_URL).split(",") if x.strip()]
ENABLE_DAILY_AUTOPREVIEW = os.getenv("ENABLE_DAILY_AUTOPREVIEW", "1").strip() in {"1", "true", "yes"}
DAILY_AUTOPREVIEW_HOUR_MSK = int(os.getenv("DAILY_AUTOPREVIEW_HOUR_MSK", "9"))
DAILY_AUTOPREVIEW_MINUTE_MSK = int(os.getenv("DAILY_AUTOPREVIEW_MINUTE_MSK", "0"))
TELEGRAM_MODE = os.getenv("TELEGRAM_MODE", "webhook").strip().lower()
FRONTEND_DIST_DIR = Path(os.getenv("FRONTEND_DIST_DIR", "Content Platform Web App/dist")).resolve()
MEDIA_DIR = Path(os.getenv("MEDIA_DIR", "/tmp/generated_media")).resolve()
STORAGE_MODE = os.getenv("STORAGE_MODE", "local").strip().lower()
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000").strip()
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin").strip()
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin").strip()
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "kindlysupport-media").strip()
MINIO_SECURE = os.getenv("MINIO_SECURE", "0").strip() in {"1", "true", "yes"}

rate_bucket: dict[str, list[float]] = {}
_minio_client: Optional[Any] = None


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
                CREATE TABLE IF NOT EXISTS llm_text_logs (
                    id BIGSERIAL PRIMARY KEY,
                    trace_label TEXT,
                    model TEXT NOT NULL,
                    prompt TEXT NOT NULL,
                    response TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS phrases (
                    id BIGSERIAL PRIMARY KEY,
                    text_body TEXT NOT NULL UNIQUE,
                    author TEXT,
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
                CREATE TABLE IF NOT EXISTS llm_text_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trace_label TEXT,
                    model TEXT NOT NULL,
                    prompt TEXT NOT NULL,
                    response TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS phrases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    text_body TEXT NOT NULL UNIQUE,
                    author TEXT,
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
            conn.execute("ALTER TABLE phrases ADD COLUMN IF NOT EXISTS author TEXT")
            conn.execute("ALTER TABLE phrases ADD COLUMN IF NOT EXISTS topic TEXT")
        else:
            try:
                conn.execute("ALTER TABLE phrases ADD COLUMN author TEXT")
            except Exception:
                pass
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
    if LOCK_PROVIDER_MODELS:
        return LOCKED_OPENROUTER_TEXT_MODEL
    return setting_get("openrouter_text_model", OPENROUTER_TEXT_MODEL)


def runtime_openrouter_vision_model() -> str:
    if LOCK_PROVIDER_MODELS:
        return LOCKED_OPENROUTER_VISION_MODEL
    return setting_get("openrouter_vision_model", OPENROUTER_VISION_MODEL)


def runtime_openrouter_image_model() -> str:
    if LOCK_PROVIDER_MODELS:
        return LOCKED_OPENROUTER_IMAGE_MODEL
    return setting_get("openrouter_image_model", OPENROUTER_IMAGE_MODEL)


def runtime_openrouter_site_url() -> str:
    return setting_get("openrouter_site_url", OPENROUTER_SITE_URL)


def runtime_openrouter_app_name() -> str:
    return setting_get("openrouter_app_name", OPENROUTER_APP_NAME)


def runtime_ocr_primary_engine() -> str:
    val = setting_get("ocr_primary_engine", OCR_PRIMARY_ENGINE).strip().lower()
    if val in {"local", "llm", "paddle"}:
        return val
    return OCR_PRIMARY_ENGINE


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


def runtime_instagram_proxy_url() -> str:
    return setting_get("instagram_proxy_url", INSTAGRAM_PROXY_URL)


def runtime_instagram_delivery_mode() -> str:
    mode = setting_get("instagram_delivery_mode", INSTAGRAM_DELIVERY_MODE).strip().lower()
    if mode in {"direct", "external_queue"}:
        return mode
    return "external_queue"


def runtime_instagram_queue_repo() -> str:
    return setting_get("instagram_queue_repo", INSTAGRAM_QUEUE_REPO).strip()


def runtime_instagram_queue_branch() -> str:
    return setting_get("instagram_queue_branch", INSTAGRAM_QUEUE_BRANCH).strip() or "main"


def runtime_instagram_queue_path() -> str:
    path = setting_get("instagram_queue_path", INSTAGRAM_QUEUE_PATH).strip().strip("/")
    return path or "queue/instagram"


def runtime_instagram_queue_github_token() -> str:
    return setting_get("instagram_queue_github_token", INSTAGRAM_QUEUE_GITHUB_TOKEN).strip()


def runtime_instagram_needs() -> list[str]:
    if runtime_instagram_delivery_mode() == "direct":
        return ["INSTAGRAM_ACCESS_TOKEN", "INSTAGRAM_IG_USER_ID"]
    return ["INSTAGRAM_QUEUE_REPO", "INSTAGRAM_QUEUE_GITHUB_TOKEN"]


def runtime_instagram_configured() -> bool:
    if not runtime_enable_instagram():
        return False
    if runtime_instagram_delivery_mode() == "direct":
        return bool(runtime_instagram_token() and runtime_instagram_user_id())
    return bool(runtime_instagram_queue_repo() and runtime_instagram_queue_github_token())


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


def runtime_telegram_mode() -> str:
    mode = setting_get("telegram_mode", TELEGRAM_MODE).strip().lower()
    if mode in {"webhook", "polling"}:
        return mode
    return "webhook"


def tg_admin_user_id() -> Optional[int]:
    stored = setting_get("telegram_admin_user_id", "")
    if not stored:
        return None
    try:
        return int(stored)
    except Exception:
        return None


def bootstrap_runtime_settings() -> None:
    defaults = {
        "openrouter_api_key": OPENROUTER_API_KEY,
        "openrouter_text_model": OPENROUTER_TEXT_MODEL,
        "openrouter_vision_model": OPENROUTER_VISION_MODEL,
        "openrouter_image_model": OPENROUTER_IMAGE_MODEL,
        "openrouter_site_url": OPENROUTER_SITE_URL,
        "openrouter_app_name": OPENROUTER_APP_NAME,
        "telegram_bot_token": TELEGRAM_BOT_TOKEN,
        "telegram_preview_chat": TELEGRAM_PREVIEW_CHAT,
        "telegram_publish_chat": TELEGRAM_PUBLISH_CHAT,
        "telegram_webhook_secret": TELEGRAM_WEBHOOK_SECRET,
        "telegram_mode": TELEGRAM_MODE,
        "instagram_access_token": INSTAGRAM_ACCESS_TOKEN,
        "instagram_ig_user_id": INSTAGRAM_IG_USER_ID,
        "instagram_graph_version": INSTAGRAM_GRAPH_VERSION,
        "instagram_proxy_url": INSTAGRAM_PROXY_URL,
        "instagram_delivery_mode": INSTAGRAM_DELIVERY_MODE,
        "instagram_queue_github_token": INSTAGRAM_QUEUE_GITHUB_TOKEN,
        "instagram_queue_repo": INSTAGRAM_QUEUE_REPO,
        "instagram_queue_branch": INSTAGRAM_QUEUE_BRANCH,
        "instagram_queue_path": INSTAGRAM_QUEUE_PATH,
        "pinterest_access_token": PINTEREST_ACCESS_TOKEN,
        "pinterest_board_id": PINTEREST_BOARD_ID,
        "vk_access_token": VK_ACCESS_TOKEN,
        "vk_group_id": VK_GROUP_ID,
        "vk_api_version": VK_API_VERSION,
        "enable_instagram": "1" if ENABLE_INSTAGRAM else "0",
        "enable_pinterest": "1" if ENABLE_PINTEREST else "0",
        "enable_vk": "1" if ENABLE_VK else "0",
        "ocr_primary_engine": OCR_PRIMARY_ENGINE,
    }
    for key, default_value in defaults.items():
        if kv_get(settings_key(key)) is None:
            setting_set(key, default_value)

    # Backward compatibility for old key format.
    legacy_admin = kv_get("telegram_admin_user_id")
    if legacy_admin and kv_get(settings_key("telegram_admin_user_id")) is None:
        setting_set("telegram_admin_user_id", legacy_admin)

    if kv_get(settings_key("telegram_admin_user_id")) is None:
        env_admin = os.getenv("TELEGRAM_ADMIN_USER_ID", "").strip()
        if env_admin:
            try:
                setting_set("telegram_admin_user_id", str(int(env_admin)))
            except Exception:
                setting_set("telegram_admin_user_id", "")


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


def find_similar_phrase_in_db(
    text: str,
    min_score: Optional[float] = None,
    conn: Optional[Any] = None,
) -> tuple[Optional[dict[str, Any]], float]:
    phrase = (text or "").strip()
    if not phrase:
        return None, 0.0
    threshold = float(min_score if min_score is not None else PHRASE_DUPLICATE_THRESHOLD)
    norm = _norm_for_similarity(_clean_ocr_quote_text(phrase))
    if not norm:
        return None, 0.0
    # Small texts are noisy; keep exact-only behavior.
    if len(norm) < 18:
        if conn is not None:
            row = conn.execute(
                "SELECT id, text_body, author, topic, is_published, created_at, updated_at FROM phrases WHERE text_body = ? LIMIT 1",
                (phrase,),
            ).fetchone()
        else:
            with db() as db_conn:
                row = db_conn.execute(
                    "SELECT id, text_body, author, topic, is_published, created_at, updated_at FROM phrases WHERE text_body = ? LIMIT 1",
                    (phrase,),
                ).fetchone()
        return (dict(row), 1.0) if row else (None, 0.0)

    l = max(1, len(norm) - 28)
    r = len(norm) + 28
    best_row: Optional[dict[str, Any]] = None
    best_score = 0.0
    try:
        if conn is not None:
            rows = conn.execute(
                """
                SELECT id, text_body, author, topic, is_published, created_at, updated_at
                FROM phrases
                WHERE length(text_body) BETWEEN ? AND ?
                ORDER BY updated_at DESC
                LIMIT 4000
                """,
                (l, r),
            ).fetchall()
        else:
            with db() as db_conn:
                rows = db_conn.execute(
                    """
                    SELECT id, text_body, author, topic, is_published, created_at, updated_at
                    FROM phrases
                    WHERE length(text_body) BETWEEN ? AND ?
                    ORDER BY updated_at DESC
                    LIMIT 4000
                    """,
                    (l, r),
                ).fetchall()
        for row in rows:
            cand = str((row.get("text_body") if isinstance(row, dict) else row["text_body"]) or "").strip()
            if not cand:
                continue
            score = difflib.SequenceMatcher(None, norm, _norm_for_similarity(cand)).ratio()
            if score > best_score:
                best_score = score
                best_row = dict(row)
        if best_row and best_score >= threshold:
            return best_row, best_score
    except Exception:
        logger.exception("find_similar_phrase_failed")
    return None, best_score


def upsert_phrase_text(text: str, is_published: int = 0) -> dict[str, Any]:
    phrase = (text or "").strip()
    if not phrase:
        raise HTTPException(status_code=400, detail="empty phrase")
    quote_text, author = split_quote_and_author(phrase)
    quote_text = quote_text.strip()
    if not quote_text:
        raise HTTPException(status_code=400, detail="empty phrase")
    similar, score = find_similar_phrase_in_db(quote_text)
    if similar:
        # Treat close phrases as duplicates and keep existing canonical row.
        similar["duplicate_score"] = round(float(score), 4)
        return similar
    now = now_iso()
    with db() as conn:
        existing = conn.execute(
            "SELECT id, text_body, author, topic, is_published, created_at, updated_at FROM phrases WHERE text_body = ?",
            (quote_text,),
        ).fetchone()
        if existing:
            needs_update = False
            params_update: list[Any] = []
            set_parts: list[str] = []
            if int(existing["is_published"]) != int(is_published):
                set_parts.append("is_published = ?")
                params_update.append(int(is_published))
                needs_update = True
            existing_author = (existing.get("author") if isinstance(existing, dict) else existing["author"]) or ""
            if author and author != existing_author:
                set_parts.append("author = ?")
                params_update.append(author)
                needs_update = True
            if needs_update:
                set_parts.append("updated_at = ?")
                params_update.append(now)
                params_update.append(existing["id"])
                conn.execute(
                    f"UPDATE phrases SET {', '.join(set_parts)} WHERE id = ?",
                    params_update,
                )
            row = conn.execute(
                "SELECT id, text_body, author, topic, is_published, created_at, updated_at FROM phrases WHERE id = ?",
                (existing["id"],),
            ).fetchone()
            return dict(row)
        if DB_BACKEND == "postgres":
            row = conn.execute(
                """
                INSERT INTO phrases(text_body, author, topic, is_published, created_at, updated_at)
                VALUES (?, ?, NULL, ?, ?, ?)
                RETURNING id, text_body, author, topic, is_published, created_at, updated_at
                """,
                (quote_text, author, int(is_published), now, now),
            ).fetchone()
            return dict(row)
        cur = conn.execute(
            "INSERT OR IGNORE INTO phrases(text_body, author, topic, is_published, created_at, updated_at) VALUES (?, ?, NULL, ?, ?, ?)",
            (quote_text, author, int(is_published), now, now),
        )
        phrase_id = cur.lastrowid
        row = conn.execute(
            "SELECT id, text_body, author, topic, is_published, created_at, updated_at FROM phrases WHERE id = ?",
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
    proxy_url: str = "",
    timeout: int = 30,
    retries: int = 2,
) -> dict[str, Any]:
    body = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=body, method=method.upper())
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    if body is not None and "Content-Type" not in (headers or {}):
        req.add_header("Content-Type", "application/json")
    opener: Optional[urllib.request.OpenerDirector] = None
    if proxy_url:
        proxy = proxy_url.strip()
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({"http": proxy, "https": proxy})
        )
    for attempt in range(retries + 1):
        try:
            if opener:
                resp_ctx = opener.open(req, timeout=timeout)
            else:
                resp_ctx = urllib.request.urlopen(req, timeout=timeout)
            with resp_ctx as resp:
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


def telegram_send_photo_bytes(
    chat_id: str | int,
    image_bytes: bytes,
    filename: str = "image.jpg",
    caption: str = "",
    parse_mode: Optional[str] = None,
    reply_markup: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    tg_token = runtime_telegram_token()
    if not tg_token:
        raise HTTPException(status_code=400, detail="TELEGRAM_BOT_TOKEN not set")
    boundary = f"----kindly{secrets.token_hex(12)}"

    def _field(name: str, value: str) -> bytes:
        return (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
            f"{value}\r\n"
        ).encode("utf-8")

    body = bytearray()
    body.extend(_field("chat_id", str(chat_id)))
    if caption:
        body.extend(_field("caption", caption))
    if parse_mode:
        body.extend(_field("parse_mode", parse_mode))
    if reply_markup:
        body.extend(_field("reply_markup", json.dumps(reply_markup, ensure_ascii=False)))

    body.extend(
        (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="photo"; filename="{filename}"\r\n'
            "Content-Type: image/jpeg\r\n\r\n"
        ).encode("utf-8")
    )
    body.extend(image_bytes)
    body.extend(b"\r\n")
    body.extend(f"--{boundary}--\r\n".encode("utf-8"))

    req = urllib.request.Request(
        f"https://api.telegram.org/bot{tg_token}/sendPhoto",
        data=bytes(body),
        method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="ignore")
        raise HTTPException(status_code=502, detail=f"HTTP {e.code}: {detail[:1200]}")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"HTTP request failed: {str(e)[:800]}")


def answer_callback(callback_query_id: str, text: str = "") -> None:
    if not runtime_telegram_token():
        return
    try:
        telegram_api("answerCallbackQuery", {"callback_query_id": callback_query_id, "text": text[:200]})
    except Exception:
        pass


def _delete_callback_source_message(callback_query: dict[str, Any]) -> None:
    msg = callback_query.get("message") or {}
    chat_id = (msg.get("chat") or {}).get("id")
    message_id = msg.get("message_id")
    if chat_id is None or message_id is None:
        return
    telegram_delete_message(chat_id, message_id)


def telegram_delete_message(chat_id: str | int, message_id: str | int) -> bool:
    if not runtime_telegram_token():
        return False
    try:
        res = telegram_api(
            "deleteMessage",
            {"chat_id": chat_id, "message_id": int(message_id)},
        )
        return bool(res.get("ok", False))
    except Exception:
        return False


def _chat_id_key(chat_id: str | int) -> str:
    return str(chat_id).strip()


def get_preview_message_id_for_chat(post: dict[str, Any], chat_id: str | int) -> Optional[str]:
    preview = post.get("preview_payload") or {}
    ids = preview.get("preview_message_ids") if isinstance(preview, dict) else None
    if isinstance(ids, dict):
        v = ids.get(_chat_id_key(chat_id))
        if v is not None:
            return str(v)
    legacy = post.get("preview_message_id")
    if legacy:
        # Backward compatibility: legacy id assumed for configured preview chat.
        if _chat_id_key(chat_id) == _chat_id_key(runtime_telegram_preview_chat()):
            return str(legacy)
    return None


def set_preview_message_id_for_chat(post_id: int, post: dict[str, Any], chat_id: str | int, message_id: str | int) -> None:
    preview = post.get("preview_payload") or {}
    if not isinstance(preview, dict):
        preview = {}
    ids = preview.get("preview_message_ids")
    if not isinstance(ids, dict):
        ids = {}
    ids[_chat_id_key(chat_id)] = str(message_id)
    preview["preview_message_ids"] = ids
    update_post(post_id, preview_payload_json=preview)
    if _chat_id_key(chat_id) == _chat_id_key(runtime_telegram_preview_chat()):
        update_post(post_id, preview_message_id=str(message_id))


def _clear_preview_message_id_for_chat(post_id: int, post: dict[str, Any], chat_id: str | int) -> None:
    preview = post.get("preview_payload") or {}
    changed = False
    if isinstance(preview, dict):
        ids = preview.get("preview_message_ids")
        if isinstance(ids, dict):
            key = _chat_id_key(chat_id)
            if key in ids:
                ids.pop(key, None)
                preview["preview_message_ids"] = ids
                changed = True
    if changed:
        update_post(post_id, preview_payload_json=preview)
    if _chat_id_key(chat_id) == _chat_id_key(runtime_telegram_preview_chat()):
        update_post(post_id, preview_message_id=None)


def _get_service_message_ids_for_chat(post: dict[str, Any], chat_id: str | int) -> list[str]:
    preview = post.get("preview_payload") or {}
    ids = preview.get("service_message_ids") if isinstance(preview, dict) else None
    if not isinstance(ids, dict):
        return []
    arr = ids.get(_chat_id_key(chat_id))
    if not isinstance(arr, list):
        return []
    out: list[str] = []
    for x in arr:
        try:
            s = str(x).strip()
        except Exception:
            s = ""
        if s:
            out.append(s)
    return out


def _append_service_message_id_for_chat(post_id: int, post: dict[str, Any], chat_id: str | int, message_id: str | int) -> None:
    preview = post.get("preview_payload") or {}
    if not isinstance(preview, dict):
        preview = {}
    ids = preview.get("service_message_ids")
    if not isinstance(ids, dict):
        ids = {}
    key = _chat_id_key(chat_id)
    arr = ids.get(key)
    if not isinstance(arr, list):
        arr = []
    mid = str(message_id).strip()
    if mid and mid not in arr:
        arr.append(mid)
    # keep list bounded
    if len(arr) > 80:
        arr = arr[-80:]
    ids[key] = arr
    preview["service_message_ids"] = ids
    update_post(post_id, preview_payload_json=preview)


def _clear_service_message_ids_for_chat(post_id: int, post: dict[str, Any], chat_id: str | int) -> None:
    preview = post.get("preview_payload") or {}
    if not isinstance(preview, dict):
        return
    ids = preview.get("service_message_ids")
    if not isinstance(ids, dict):
        return
    key = _chat_id_key(chat_id)
    if key in ids:
        ids.pop(key, None)
        preview["service_message_ids"] = ids
        update_post(post_id, preview_payload_json=preview)


def cleanup_post_service_messages_for_chat(post_id: int, chat_id: str | int) -> None:
    post = fetch_post(post_id)
    mids = _get_service_message_ids_for_chat(post, chat_id)
    for mid in mids:
        telegram_delete_message(chat_id, mid)
    _clear_service_message_ids_for_chat(post_id, post, chat_id)


def cleanup_post_thread_messages_for_chat(post_id: int, chat_id: str | int) -> None:
    post = fetch_post(post_id)
    cleanup_post_service_messages_for_chat(post_id, chat_id)
    post = fetch_post(post_id)
    preview_mid = get_preview_message_id_for_chat(post, chat_id)
    if preview_mid:
        telegram_delete_message(chat_id, preview_mid)
        _clear_preview_message_id_for_chat(post_id, post, chat_id)


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


def build_daily_phrase_keyboard(phrase_id: int) -> dict[str, Any]:
    return tg_keyboard(
        [
            [("Сменить фразу", f"ks:dailyswap:{phrase_id}")],
            [("Сгенерировать пост", f"ks:dailygen:{phrase_id}")],
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
    caption = post.get("telegram_caption") or generate_caption(post["title"], post["text_body"])
    use_md = (post.get("source_kind") or "").strip() == "phrase"
    caption_md_limited = generate_post_caption_markdown_limited(post, max_len=1024) if use_md else ""
    caption_plain = generate_post_caption_plain(post)
    keyboard = build_preview_keyboard(int(post["id"]))

    def _send_text_only(chat_id: str | int) -> dict[str, Any]:
        payload_msg: dict[str, Any] = {
            "chat_id": chat_id,
            "text": caption,
            "reply_markup": keyboard,
            "disable_web_page_preview": True,
        }
        if use_md:
            payload_msg["parse_mode"] = "MarkdownV2"
        return telegram_api("sendMessage", payload_msg)

    def _send(chat_id: str | int) -> dict[str, Any]:
        if post.get("final_image_url"):
            media_key = media_key_from_url(str(post["final_image_url"]))
            if media_key:
                try:
                    image_bytes, _ = storage_get_bytes(media_key)
                    return telegram_send_photo_bytes(
                        chat_id=chat_id,
                        image_bytes=image_bytes,
                        filename=Path(media_key).name or "preview.jpg",
                        caption=caption_md_limited if use_md else ((caption_plain[:1000] + "...") if len(caption_plain) > 1024 else caption_plain),
                        parse_mode="MarkdownV2" if use_md else None,
                        reply_markup=keyboard,
                    )
                except Exception:
                    logger.exception("telegram_preview_send_file_failed media_key=%s", media_key)
                    # Self-heal for phrase cards: rebuild if object disappeared from storage.
                    try:
                        if str(post.get("source_kind") or "") == "phrase" and post.get("id"):
                            original = (
                                (post.get("preview_payload") or {}).get("base_image_url")
                                or (post.get("preview_payload") or {}).get("original_image_url")
                                or post.get("final_image_url")
                            )
                            rebuilt = render_phrase_card_image(str(post.get("title") or ""), str(original or ""))
                            update_post(int(post["id"]), final_image_url=rebuilt)
                            post["final_image_url"] = rebuilt
                            media_key2 = media_key_from_url(str(rebuilt))
                            if media_key2:
                                image_bytes2, _ = storage_get_bytes(media_key2)
                                return telegram_send_photo_bytes(
                                    chat_id=chat_id,
                                    image_bytes=image_bytes2,
                                    filename=Path(media_key2).name or "preview.jpg",
                                    caption=caption_md_limited if use_md else ((caption_plain[:1000] + "...") if len(caption_plain) > 1024 else caption_plain),
                                    parse_mode="MarkdownV2" if use_md else None,
                                    reply_markup=keyboard,
                                )
                    except Exception:
                        logger.exception("telegram_preview_self_heal_failed post_id=%s", post.get("id"))
            # Fallback 1: download remote image and upload as multipart file.
            try:
                raw, _ctype = download_remote_image(str(post["final_image_url"]))
                return telegram_send_photo_bytes(
                    chat_id=chat_id,
                    image_bytes=raw,
                    filename="preview.jpg",
                    caption=caption_md_limited if use_md else ((caption_plain[:1000] + "...") if len(caption_plain) > 1024 else caption_plain),
                    parse_mode="MarkdownV2" if use_md else None,
                    reply_markup=keyboard,
                )
            except Exception:
                logger.exception("telegram_preview_download_and_send_failed url=%s", post.get("final_image_url"))
            raise HTTPException(status_code=502, detail="preview image unavailable")
        return _send_text_only(chat_id)

    target_chat_id: str | int = runtime_telegram_preview_chat()
    try:
        return _send(target_chat_id)
    except HTTPException as e:
        detail = str(e.detail).lower()
        admin_id = tg_admin_user_id()
        if admin_id and "chat not found" in detail:
            logger.warning("preview_chat_not_found_fallback_to_admin preview_chat=%s admin_id=%s", target_chat_id, admin_id)
            return _send(admin_id)
        raise


def telegram_send_preview_to_chat(post: dict[str, Any], chat_id: str | int) -> Optional[dict[str, Any]]:
    if not runtime_telegram_token():
        return None
    caption = post.get("telegram_caption") or generate_caption(post["title"], post["text_body"])
    use_md = (post.get("source_kind") or "").strip() == "phrase"
    caption_md_limited = generate_post_caption_markdown_limited(post, max_len=1024) if use_md else ""
    caption_plain = generate_post_caption_plain(post)
    keyboard = build_preview_keyboard(int(post["id"]))
    def _send_text_only() -> dict[str, Any]:
        payload_msg: dict[str, Any] = {
            "chat_id": chat_id,
            "text": caption,
            "reply_markup": keyboard,
            "disable_web_page_preview": True,
        }
        if use_md:
            payload_msg["parse_mode"] = "MarkdownV2"
        return telegram_api("sendMessage", payload_msg)

    if post.get("final_image_url"):
        media_key = media_key_from_url(str(post["final_image_url"]))
        if media_key:
            try:
                image_bytes, _ = storage_get_bytes(media_key)
                return telegram_send_photo_bytes(
                    chat_id=chat_id,
                    image_bytes=image_bytes,
                    filename=Path(media_key).name or "preview.jpg",
                    caption=caption_md_limited if use_md else ((caption_plain[:1000] + "...") if len(caption_plain) > 1024 else caption_plain),
                    parse_mode="MarkdownV2" if use_md else None,
                    reply_markup=keyboard,
                )
            except Exception:
                logger.exception("telegram_preview_send_file_failed_to_chat media_key=%s chat_id=%s", media_key, chat_id)
                # Self-heal for phrase cards: rebuild if object disappeared from storage.
                try:
                    if str(post.get("source_kind") or "") == "phrase" and post.get("id"):
                        original = (
                            (post.get("preview_payload") or {}).get("base_image_url")
                            or (post.get("preview_payload") or {}).get("original_image_url")
                            or post.get("final_image_url")
                        )
                        rebuilt = render_phrase_card_image(str(post.get("title") or ""), str(original or ""))
                        update_post(int(post["id"]), final_image_url=rebuilt)
                        post["final_image_url"] = rebuilt
                        media_key2 = media_key_from_url(str(rebuilt))
                        if media_key2:
                            image_bytes2, _ = storage_get_bytes(media_key2)
                            return telegram_send_photo_bytes(
                                chat_id=chat_id,
                                image_bytes=image_bytes2,
                                filename=Path(media_key2).name or "preview.jpg",
                                caption=caption_md_limited if use_md else ((caption_plain[:1000] + "...") if len(caption_plain) > 1024 else caption_plain),
                                parse_mode="MarkdownV2" if use_md else None,
                                reply_markup=keyboard,
                            )
                except Exception:
                    logger.exception("telegram_preview_self_heal_failed_to_chat post_id=%s chat_id=%s", post.get("id"), chat_id)
        # Fallback 1: download remote image and upload as multipart file.
        try:
            raw, _ctype = download_remote_image(str(post["final_image_url"]))
            return telegram_send_photo_bytes(
                chat_id=chat_id,
                image_bytes=raw,
                filename="preview.jpg",
                caption=caption_md_limited if use_md else ((caption_plain[:1000] + "...") if len(caption_plain) > 1024 else caption_plain),
                parse_mode="MarkdownV2" if use_md else None,
                reply_markup=keyboard,
            )
        except Exception:
            logger.exception("telegram_preview_download_and_send_failed_to_chat url=%s chat_id=%s", post.get("final_image_url"), chat_id)
        raise HTTPException(status_code=502, detail="preview image unavailable")
    return _send_text_only()


def telegram_send_publish(post: dict[str, Any]) -> Optional[dict[str, Any]]:
    if not runtime_telegram_token():
        return None
    chat_id = runtime_telegram_publish_chat()
    caption = post.get("telegram_caption") or generate_caption(post["title"], post["text_body"])
    use_md = (post.get("source_kind") or "").strip() == "phrase"
    caption_md_limited = generate_post_caption_markdown_limited(post, max_len=1024) if use_md else ""
    caption_plain = generate_post_caption_plain(post)
    if post.get("final_image_url"):
        media_key = media_key_from_url(str(post["final_image_url"]))
        if media_key:
            try:
                image_bytes, _ = storage_get_bytes(media_key)
                return telegram_send_photo_bytes(
                    chat_id=chat_id,
                    image_bytes=image_bytes,
                    filename=Path(media_key).name or "publish.jpg",
                    caption=caption_md_limited if use_md else ((caption_plain[:1000] + "...") if len(caption_plain) > 1024 else caption_plain),
                    parse_mode="MarkdownV2" if use_md else None,
                )
            except Exception:
                logger.exception("telegram_publish_send_file_failed media_key=%s", media_key)
        raise HTTPException(status_code=502, detail="publish image unavailable")
    payload_text: dict[str, Any] = {"chat_id": chat_id, "text": caption, "disable_web_page_preview": True}
    if use_md:
        payload_text["parse_mode"] = "MarkdownV2"
    return telegram_api("sendMessage", payload_text)


def send_telegram_text(
    chat_id: str | int,
    text: str,
    reply_markup: Optional[dict[str, Any]] = None,
    track_post_id: Optional[int] = None,
) -> Optional[dict[str, Any]]:
    if not runtime_telegram_token():
        return None
    payload: dict[str, Any] = {"chat_id": chat_id, "text": text}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    res = telegram_api("sendMessage", payload)
    try:
        if track_post_id:
            message_id = ((res or {}).get("result") or {}).get("message_id")
            if message_id is not None:
                post = fetch_post(int(track_post_id))
                _append_service_message_id_for_chat(int(track_post_id), post, chat_id, str(message_id))
    except Exception:
        logger.exception("telegram_track_service_message_failed post_id=%s", track_post_id)
    return res


def instagram_publish_post(post: dict[str, Any]) -> dict[str, Any]:
    instagram_token = runtime_instagram_token()
    instagram_user_id = runtime_instagram_user_id()
    graph_version = runtime_instagram_graph_version()
    proxy_url = runtime_instagram_proxy_url()
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
            "caption": instagram_caption_text(post),
            "access_token": instagram_token,
        },
        proxy_url=proxy_url,
    )
    creation_id = create.get("id")
    if not creation_id:
        raise HTTPException(status_code=502, detail=f"Instagram create media failed: {create}")
    publish = http_json(
        "POST",
        f"{base}/media_publish",
        {"creation_id": creation_id, "access_token": instagram_token},
        proxy_url=proxy_url,
    )
    return {"create": create, "publish": publish}


def instagram_enqueue_post(post: dict[str, Any]) -> dict[str, Any]:
    github_token = runtime_instagram_queue_github_token()
    queue_repo = runtime_instagram_queue_repo()
    queue_branch = runtime_instagram_queue_branch()
    queue_path = runtime_instagram_queue_path()
    if not github_token or not queue_repo:
        raise HTTPException(status_code=400, detail="Instagram queue settings not configured")
    if not re.fullmatch(r"[^/\s]+/[^/\s]+", queue_repo):
        raise HTTPException(status_code=400, detail="instagram_queue_repo must look like owner/repo")
    image_url = (post.get("final_image_url") or "").strip()
    if not image_url:
        raise HTTPException(status_code=400, detail="Post has no final_image_url")
    caption = instagram_caption_text(post)
    now_tag = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    post_tag = f"post-{post.get('id') or 'x'}-{uuid4().hex[:8]}"

    def github_put_bytes(rel_path: str, content_bytes: bytes, commit_message: str) -> dict[str, Any]:
        api_url = f"https://api.github.com/repos/{queue_repo}/contents/{urllib.parse.quote(rel_path)}"
        content_b64 = base64.b64encode(content_bytes).decode("ascii")
        return http_json(
            "PUT",
            api_url,
            {"message": commit_message, "content": content_b64, "branch": queue_branch},
            headers={
                "Authorization": f"Bearer {github_token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            timeout=40,
            retries=1,
        )

    def github_put_text(rel_path: str, text_payload: str, commit_message: str) -> dict[str, Any]:
        return github_put_bytes(rel_path, text_payload.encode("utf-8"), commit_message)

    # Rehost image in GitHub to avoid Meta fetch issues with app-hosted media URLs.
    # Prefer object storage read by media key (no network/self-call dependency).
    rehosted_image_url = ""
    try:
        media_key = media_key_from_url(image_url)
        if media_key:
            image_bytes, _ = storage_get_bytes(media_key)
        else:
            image_bytes, _ = download_remote_image(image_url)
        media_rel_path = f"{queue_path}/media/{now_tag}-{post_tag}.jpg".strip("/")
        github_put_bytes(
            media_rel_path,
            image_bytes,
            f"chore: enqueue instagram media #{post.get('id') or 'unknown'}",
        )
        rehosted_image_url = f"https://raw.githubusercontent.com/{queue_repo}/{queue_branch}/{media_rel_path}"
    except Exception:
        logger.exception("instagram_enqueue_rehost_failed post_id=%s", post.get("id"))

    payload: dict[str, Any] = {
        "caption": caption,
        "image_url": rehosted_image_url or image_url,
        "publish_at": (post.get("scheduled_for") or ""),
    }
    file_name = f"{now_tag}-{post_tag}.json"
    rel_path = f"{queue_path}/{file_name}".strip("/")
    res = github_put_text(
        rel_path,
        json.dumps(payload, ensure_ascii=False, indent=2),
        f"chore: enqueue instagram post #{post.get('id') or 'unknown'}",
    )
    return {
        "queue_repo": queue_repo,
        "queue_branch": queue_branch,
        "queue_path": rel_path,
        "rehosted_image_url": rehosted_image_url or None,
        "github_response": {"content": res.get("content"), "commit": res.get("commit")},
    }


def instagram_caption_text(post: dict[str, Any]) -> str:
    # Instagram caption should come from canonical post content, not Telegram-specific cached caption.
    raw = generate_post_caption_plain(post).strip()
    if not raw:
        raw = (post.get("telegram_caption") or "").strip()
    # Telegram MarkdownV2 escapes are not valid/useful in Instagram captions.
    text = re.sub(r"\\([_*\[\]()~`>#+\-=|{}.!\\])", r"\1", raw)
    text = re.sub(r"\*([^*\n]+)\*", r"\1", text)
    return text[:2200]


def instagram_publish_or_enqueue(post: dict[str, Any]) -> dict[str, Any]:
    mode = runtime_instagram_delivery_mode()
    if mode == "direct":
        return {"mode": "direct", "result": instagram_publish_post(post)}
    return {"mode": "external_queue", "result": instagram_enqueue_post(post)}


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
        "link": post.get("source_url") or runtime_openrouter_site_url(),
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


def escape_markdown_v2(text: str) -> str:
    # Telegram MarkdownV2 escaping.
    return re.sub(r"([_*\[\]()~`>#+\-=|{}.!\\])", r"\\\1", text or "")


UNIVERSAL_CONTENT_SYSTEM_PROMPT = """
You create content for a Russian psychological support Telegram channel.

Style:
- warm
- human
- thoughtful
- simple and natural
- emotionally intelligent
- write as if speaking quietly to a tired but thoughtful adult
- no pathos
- no moralizing
- no motivational cliches
- no abstract jargon
- prefer direct meaning over rhetorical contrast
- do not use "not this, but that" sentence patterns
- avoid formulas like "Это не слабость, а свобода"
- avoid heavy negation
- normalize em dashes to short hyphens in Russian

Modes:

TEXT:
Write in Russian.
2 paragraphs, 3-5 sentences each.
Paragraph 1 - how the idea appears in life.
Paragraph 2 - deeper meaning or invitation.
Output must be plain text only.
Do not add headings, markdown, bullets, labels, or intro lines.
Never start with a title like "**...**".

SCENARIO:
Write in English.
Create one strong visual scenario.
Symbolic but clear, realistic or cinematic, no text in image.
Format:
1. composition
2. subject
3. environment
4. symbol
5. mood

FLUX:
Write in English.
Output one single clean Flux prompt.
Square image, no text, realistic cinematic style, clean composition, emotional depth.
""".strip()


def build_user_prompt_for_task(
    task: str,
    phrase: str = "",
    text: str = "",
    scenario: str = "",
    instruction: str = "",
    previous_text: str = "",
) -> str:
    mode = (task or "").strip().upper()
    if mode == "TEXT":
        parts = [
            "TASK: TEXT",
            f"PHRASE: {(phrase or '').strip()}",
        ]
        if instruction.strip():
            parts.append(f"EDITOR_INSTRUCTION: {instruction.strip()}")
        if previous_text.strip():
            parts.append(f"PREVIOUS_TEXT_AVOID_SIMILARITY: {previous_text.strip()[:1600]}")
        return "\n".join(parts)
    if mode == "SCENARIO":
        parts = [
            "TASK: SCENARIO",
            f"PHRASE: {(phrase or '').strip()}",
            f"TEXT: {(text or '').strip()[:2200]}",
        ]
        if instruction.strip():
            parts.append(f"EDITOR_INSTRUCTION: {instruction.strip()}")
        return "\n".join(parts)
    if mode == "FLUX":
        parts = [
            "TASK: FLUX",
            f"SCENARIO: {(scenario or '').strip()[:2600]}",
        ]
        if instruction.strip():
            parts.append(f"EDITOR_INSTRUCTION: {instruction.strip()}")
        return "\n".join(parts)
    return f"TASK: {mode or 'TEXT'}"


def _sentence_count(text: str) -> int:
    return len([x for x in re.split(r"(?<=[.!?])\s+", (text or "").strip()) if x.strip()])


def _normalize_generated_ru_text(text: str) -> str:
    out = (text or "").strip()
    # If model returned multi-mode payload, explicitly extract TEXT section first.
    m_text = re.search(
        r"(?is)\bTEXT\s*:\s*(.+?)(?=\n\s*(?:SCENARIO|FLUX)\s*:|$)",
        out,
    )
    if m_text:
        out = m_text.group(1).strip()
    # Strip leaked technical prompt blocks if model echoes input template.
    out = re.sub(
        r"(?is)\b(?:TASK|PHRASE|EDITOR_INSTRUCTION|PREVIOUS_TEXT_AVOID_SIMILARITY|SCENARIO|FLUX)\s*:\s*.*?(?=\b(?:TASK|PHRASE|EDITOR_INSTRUCTION|PREVIOUS_TEXT_AVOID_SIMILARITY|SCENARIO|FLUX)\s*:|$)",
        " ",
        out,
    )
    # Drop accidental markdown heading line like "**Когда путь ...**".
    out = re.sub(r"(?m)^\s*\*\*[^*\n]{2,120}\*\*\s*$", "", out)
    # Drop accidental plain heading line if it is a short standalone line before main text.
    lines = [ln.rstrip() for ln in out.splitlines()]
    if len(lines) >= 2:
        first = lines[0].strip()
        second = lines[1].strip()
        if (
            first
            and len(first) <= 80
            and not re.search(r"[.!?]\s*$", first)
            and second
            and len(second) > 60
        ):
            lines = lines[1:]
    out = "\n".join(lines)
    # Keep sentence dashes normalized, but don't break compound words.
    out = re.sub(r"\s+[—–-]\s+", " - ", out)
    out = re.sub(r"[—–]", "-", out)
    out = re.sub(r"\s+,", ",", out)
    out = re.sub(r"\s+,?\s*а не\s+", ", без ", out, flags=re.IGNORECASE)
    # Keep paragraph boundaries: collapse spaces inside lines, preserve blank-line separators.
    parts = [re.sub(r"[ \t]+", " ", p).strip() for p in re.split(r"\n\s*\n", out) if p.strip()]
    out = "\n\n".join(parts)
    # Restore paragraphs if model accidentally flattened.
    out = re.sub(r"([.!?])\s+([А-ЯЁA-Z])", r"\1 \2", out)
    return _cleanup_generated_ru_text(out.strip())


def _cleanup_generated_ru_text(text: str) -> str:
    out = (text or "").strip()
    if not out:
        return ""
    # Punctuation noise.
    out = re.sub(r",\s*,+", ", ", out)
    out = re.sub(r"\.{2,}", ".", out)
    out = re.sub(r"\s+([,.;:!?])", r"\1", out)
    out = re.sub(r"([,.;:!?])(?=[^\s\n])", r"\1 ", out)
    # Common RU hyphen compounds.
    out = re.sub(r"\b(по|кое)\s*-\s*([а-яё]+)\b", r"\1-\2", out, flags=re.IGNORECASE)
    out = re.sub(r"\b([а-яё]+)\s*-\s*(то|либо|нибудь|таки)\b", r"\1-\2", out, flags=re.IGNORECASE)
    # Known frequent model/OCR artifacts.
    out = re.sub(r"\bбез\s+ожиданиям\b", "без ожиданий", out, flags=re.IGNORECASE)
    out = re.sub(r"\bкогда\s*-\s*то\b", "когда-то", out, flags=re.IGNORECASE)
    out = re.sub(r"\bчто\s*-\s*то\b", "что-то", out, flags=re.IGNORECASE)
    out = re.sub(r"\bкто\s*-\s*то\b", "кто-то", out, flags=re.IGNORECASE)
    out = re.sub(r"\bгде\s*-\s*то\b", "где-то", out, flags=re.IGNORECASE)
    out = re.sub(r"\bзачем\s*-\s*то\b", "зачем-то", out, flags=re.IGNORECASE)
    # Space normalization preserving paragraphs.
    parts = [re.sub(r"[ \t]+", " ", p).strip() for p in re.split(r"\n\s*\n", out) if p.strip()]
    return "\n\n".join(parts).strip()


def _split_two_paragraphs(text: str) -> tuple[str, str]:
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", (text or "").strip()) if p.strip()]
    if len(paragraphs) >= 2:
        return paragraphs[0], paragraphs[1]
    flat = (text or "").strip()
    parts = [p.strip() for p in re.split(r"(?<=[.!?])\s+", flat) if p.strip()]
    if len(parts) >= 8:
        left = " ".join(parts[:4]).strip()
        right = " ".join(parts[4:8]).strip()
        return left, right
    if len(parts) >= 2:
        return parts[0], " ".join(parts[1:]).strip()
    return flat, ""


def _rich_text_quality_ok(text: str) -> bool:
    p1, p2 = _split_two_paragraphs(text)
    if not p1 or not p2:
        return False
    c1 = _sentence_count(p1)
    c2 = _sentence_count(p2)
    return c1 >= 3 and c2 >= 3 and len(p1) >= 180 and len(p2) >= 180


def _phrase_title_for_publish(text: str) -> str:
    # Phrase cards/captions should end cleanly without a trailing period.
    out = re.sub(r"\s+", " ", (text or "").strip())
    out = re.sub(r"[.]+$", "", out).strip()
    out = re.sub(r"[…]+$", "", out).strip()
    return out


def _texts_too_similar(a: str, b: str, threshold: float = 0.90) -> bool:
    aa = re.sub(r"\s+", " ", (a or "").strip().lower())
    bb = re.sub(r"\s+", " ", (b or "").strip().lower())
    if not aa or not bb:
        return False
    ratio = difflib.SequenceMatcher(None, aa, bb).ratio()
    if ratio >= threshold:
        return True
    # Catch near-copy with a large common block even if ratio is slightly lower.
    m = difflib.SequenceMatcher(None, aa, bb).find_longest_match(0, len(aa), 0, len(bb))
    return m.size >= 180


def _phrase_expansion_quality_ok(text: str, phrase: str) -> bool:
    normalized = _force_two_clean_paragraphs(text)
    p1, p2 = _split_two_paragraphs(normalized)
    if not p1 or not p2:
        return False
    if len(p1) < 100 or len(p2) < 100:
        return False
    if _texts_too_similar(p1, p2, threshold=0.80):
        return False
    phrase_clean = _phrase_title_for_publish(phrase)
    if phrase_clean and (
        _texts_too_similar(normalized, phrase_clean, threshold=0.80)
        or _texts_too_similar(p1, phrase_clean, threshold=0.82)
        or _texts_too_similar(p2, phrase_clean, threshold=0.82)
    ):
        return False
    return True


def _force_two_clean_paragraphs(text: str) -> str:
    p1, p2 = _split_two_paragraphs(text)
    p1 = re.sub(r"\s+", " ", (p1 or "").strip())
    p2 = re.sub(r"\s+", " ", (p2 or "").strip())
    # Remove heading residue in first paragraph start.
    p1 = re.sub(r"^\*+|\*+$", "", p1).strip()
    p2 = re.sub(r"^\*+|\*+$", "", p2).strip()
    if not p1 and p2:
        p1, p2 = p2, ""
    if not p2:
        # Best effort split by sentence count.
        sents = [s.strip() for s in re.split(r"(?<=[.!?])\s+", p1) if s.strip()]
        if len(sents) >= 6:
            pivot = len(sents) // 2
            p1 = " ".join(sents[:pivot]).strip()
            p2 = " ".join(sents[pivot:]).strip()
    return _cleanup_generated_ru_text(f"{p1}\n\n{p2}".strip())


def expand_phrase_text(phrase: str, instruction: str = "", previous_text: str = "") -> str:
    clean = (phrase or "").strip()
    if not clean:
        return ""
    prompt = build_user_prompt_for_task(
        "TEXT",
        phrase=clean,
        instruction=(instruction or "").strip(),
        previous_text=(previous_text or "").strip(),
    )
    try:
        raw = openrouter_generate_text(
            prompt,
            temperature=0.9,
            top_p=0.95,
            system_prompt=UNIVERSAL_CONTENT_SYSTEM_PROMPT,
            trace_label="TEXT:expand:v1",
        )
        text = _normalize_generated_ru_text(raw or "")
        if previous_text.strip():
            for _ in range(3):
                if not _texts_too_similar(text, previous_text, threshold=0.82):
                    break
                force_diff_instruction = (
                    "Перепиши текст заметно иначе по лексике и структуре. "
                    "Запрещено повторять формулировки и ритм предыдущего текста. "
                    "Сохрани только общий смысл фразы."
                )
                prompt_diff = build_user_prompt_for_task(
                    "TEXT",
                    phrase=clean,
                    instruction=f"{(instruction or '').strip()} {force_diff_instruction}".strip(),
                    previous_text=(previous_text or "").strip(),
                )
                raw_diff = openrouter_generate_text(
                    prompt_diff,
                    temperature=1.0,
                    top_p=0.95,
                    system_prompt=UNIVERSAL_CONTENT_SYSTEM_PROMPT,
                    trace_label="TEXT:expand:retry_diff",
                )
                text_diff = _normalize_generated_ru_text(raw_diff or "")
                if text_diff:
                    text = text_diff
        if not _rich_text_quality_ok(text):
            strict_instruction = (
                "Сделай текст длиннее и живее. Ровно 2 абзаца. В каждом абзаце 4 предложения. "
                "Без списков, без метакомментариев. Никаких шаблонов '..., а не ...'. "
                "Без заголовков и markdown. Верни только 2 абзаца чистого текста."
            )
            prompt2 = build_user_prompt_for_task(
                "TEXT",
                phrase=clean,
                instruction=f"{(instruction or '').strip()} {strict_instruction}".strip(),
                previous_text=(text or previous_text or "").strip(),
            )
            raw2 = openrouter_generate_text(
                prompt2,
                temperature=0.95,
                top_p=0.95,
                system_prompt=UNIVERSAL_CONTENT_SYSTEM_PROMPT,
                trace_label="TEXT:expand:retry_quality",
            )
            text2 = _normalize_generated_ru_text(raw2 or "")
            if _rich_text_quality_ok(text2):
                return _force_two_clean_paragraphs(text2)
            if text2.strip() and _phrase_expansion_quality_ok(text2, clean):
                # Keep non-empty regenerated text instead of dropping to deterministic template.
                return _force_two_clean_paragraphs(text2)
        if _rich_text_quality_ok(text) and _phrase_expansion_quality_ok(text, clean):
            return _force_two_clean_paragraphs(text)
        if text.strip() and _phrase_expansion_quality_ok(text, clean):
            return _force_two_clean_paragraphs(text)
        # Last attempt: hard reset from previous text to avoid template lock-in.
        prompt3 = build_user_prompt_for_task(
            "TEXT",
            phrase=clean,
            instruction=(
                f"{(instruction or '').strip()} "
                "Полный перезапуск текста с нуля. Ровно 2 абзаца по 4-5 предложений, "
                "без повторов прошлых формулировок."
            ).strip(),
            previous_text="",
        )
        raw3 = openrouter_generate_text(
            prompt3,
            temperature=1.0,
            top_p=0.95,
            system_prompt=UNIVERSAL_CONTENT_SYSTEM_PROMPT,
            trace_label="TEXT:expand:hard_reset",
        )
        text3 = _normalize_generated_ru_text(raw3 or "")
        if text3.strip() and _phrase_expansion_quality_ok(text3, clean):
            return _force_two_clean_paragraphs(text3)
        p1_dbg, p2_dbg = _split_two_paragraphs(text)
        logger.warning(
            "expand_phrase_text_low_quality phrase=%s p1_sent=%s p2_sent=%s p1_len=%s p2_len=%s",
            clean[:80],
            _sentence_count(p1_dbg),
            _sentence_count(p2_dbg),
            len(p1_dbg),
            len(p2_dbg),
        )
    except Exception:
        logger.exception("expand_phrase_text_failed phrase=%s", clean[:80])
    # Deterministic fallback, neutral and phrase-centered.
    return (
        f"Иногда одна короткая фраза помогает увидеть то, что мы долго не замечали. "
        f"«{clean}» звучит просто, но в ней есть точка опоры для повседневных решений и внутренних сомнений. "
        f"Когда мыслей слишком много, полезно вернуться к этой простой формулировке и проверить, где именно сейчас находится внимание. "
        f"Так появляется ясность и спокойный контакт с реальностью.\n\n"
        f"Эта мысль не требует резких шагов и больших обещаний, она работает через маленькие действия в текущем дне. "
        f"Достаточно одного честного выбора, одного точного слова или одного аккуратного шага, чтобы почувствовать движение вперёд. "
        f"Постепенно такие шаги собираются в устойчивое состояние и возвращают внутреннюю собранность. "
        f"Именно из этого рождается ощущение, что жизнь снова становится живой и цельной."
    )


def generate_image_scenario(title: str, text_body: str, extra_instruction: str = "") -> str:
    prompt = build_user_prompt_for_task(
        "SCENARIO",
        phrase=title,
        text=text_body,
        instruction=(extra_instruction or "").strip(),
    )
    raw = (
        openrouter_generate_text(
            prompt,
            temperature=0.8,
            top_p=0.95,
            system_prompt=UNIVERSAL_CONTENT_SYSTEM_PROMPT,
            trace_label="SCENARIO:v1",
        )
        or ""
    ).strip()
    return re.sub(r"\s+", " ", raw).strip() or "Спокойный фотореалистичный природный фон с глубиной и мягким светом."


def generate_detailed_image_prompt(title: str, text_body: str, scenario: str, extra_instruction: str = "") -> str:
    flux_context = scenario
    if title:
        flux_context = f"{scenario}\n\nQuote context: {title}\nPost context: {text_body[:1200]}".strip()
    prompt = build_user_prompt_for_task(
        "FLUX",
        scenario=flux_context,
        instruction=(extra_instruction or "").strip(),
    )
    raw = (
        openrouter_generate_text(
            prompt,
            temperature=0.8,
            top_p=0.95,
            system_prompt=UNIVERSAL_CONTENT_SYSTEM_PROMPT,
            trace_label="FLUX_PROMPT:v1",
        )
        or ""
    ).strip()
    ready = re.sub(r"\s+", " ", raw).strip()
    if ready:
        return ready
    return default_image_prompt(title, scenario, extra_instruction)


def split_quote_and_author(text: str) -> tuple[str, Optional[str]]:
    s = re.sub(r"\s+", " ", (text or "").strip())
    if not s:
        return "", None
    # "Цитата. Автор"
    m = re.match(r"^(.*?)([.!?])\s+([A-ZА-ЯЁ][A-Za-zА-Яа-яЁё' .-]{1,80})$", s)
    if m:
        quote = f"{m.group(1).strip()}{m.group(2)}".strip()
        author = m.group(3).strip()
        if _is_probable_author_line(author):
            return quote, author
    # "Цитата (Автор)"
    m2 = re.match(r"^(.*)\(([^()]{2,80})\)\s*$", s)
    if m2:
        quote = m2.group(1).strip().rstrip(" .")
        author = m2.group(2).strip()
        if quote and _is_probable_author_line(author):
            if not quote.endswith((".", "!", "?")):
                quote += "."
            return quote, author
    # "Цитата — Автор"
    m3 = re.match(r"^(.*)\s+—\s+([A-ZА-ЯЁ][A-Za-zА-Яа-яЁё' .-]{1,80})$", s)
    if m3:
        quote = m3.group(1).strip()
        author = m3.group(2).strip()
        if quote and _is_probable_author_line(author):
            if not quote.endswith((".", "!", "?")):
                quote += "."
            return quote, author
    return s, None


def phrase_struct(text: str) -> dict[str, str]:
    quote, author = split_quote_and_author(text)
    return {"text_body": (quote or "").strip(), "author": (author or "").strip()}


def backfill_phrase_authors() -> dict[str, int]:
    scanned = 0
    updated = 0
    with db() as conn:
        rows = conn.execute("SELECT id, text_body, author, is_published FROM phrases ORDER BY id ASC").fetchall()
        for row in rows:
            scanned += 1
            text_body = str(row["text_body"] or "").strip()
            current_author = (row.get("author") if isinstance(row, dict) else row["author"]) or None
            current_is_published = int((row.get("is_published") if isinstance(row, dict) else row["is_published"]) or 0)
            quote_text, parsed_author = split_quote_and_author(text_body)
            quote_text = quote_text.strip()
            parsed_author = (parsed_author or "").strip() or None
            if not quote_text:
                continue
            next_author = parsed_author if parsed_author else current_author
            if quote_text != text_body or next_author != current_author:
                duplicate = conn.execute(
                    "SELECT id, author, is_published FROM phrases WHERE text_body = ? AND id <> ? LIMIT 1",
                    (quote_text, row["id"]),
                ).fetchone()
                if duplicate:
                    dup_author = (duplicate.get("author") if isinstance(duplicate, dict) else duplicate["author"]) or None
                    dup_is_published = int((duplicate.get("is_published") if isinstance(duplicate, dict) else duplicate["is_published"]) or 0)
                    merged_author = dup_author or next_author
                    merged_is_published = 1 if (dup_is_published or current_is_published) else 0
                    conn.execute(
                        "UPDATE phrases SET author = ?, is_published = ?, updated_at = ? WHERE id = ?",
                        (merged_author, merged_is_published, now_iso(), duplicate["id"]),
                    )
                    conn.execute(
                        "UPDATE posts SET source_url = ? WHERE source_url = ?",
                        (f"phrase:{duplicate['id']}", f"phrase:{row['id']}"),
                    )
                    conn.execute("DELETE FROM phrases WHERE id = ?", (row["id"],))
                    updated += 1
                    continue
                conn.execute(
                    "UPDATE phrases SET text_body = ?, author = ?, is_published = ?, updated_at = ? WHERE id = ?",
                    (quote_text, next_author, current_is_published, now_iso(), row["id"]),
                )
                updated += 1
    return {"scanned": scanned, "updated": updated}


def _wrap_lines(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont, max_width: int) -> list[str]:
    words = (text or "").split()
    if not words:
        return []
    lines: list[str] = []
    current = words[0]
    for w in words[1:]:
        candidate = f"{current} {w}"
        bbox = draw.textbbox((0, 0), candidate, font=font)
        if (bbox[2] - bbox[0]) <= max_width:
            current = candidate
        else:
            lines.append(current)
            current = w
    lines.append(current)
    return lines


def _load_background_image(url: str) -> Optional[Image.Image]:
    safe = (url or "").strip()
    if not safe:
        return None
    try:
        media_key = media_key_from_url(safe)
        if media_key:
            raw, _ = storage_get_bytes(media_key)
        else:
            with urllib.request.urlopen(safe, timeout=25) as resp:
                raw = resp.read()
        return Image.open(io.BytesIO(raw)).convert("RGB")
    except Exception:
        return None


def render_phrase_card_image(
    phrase: str,
    image_url: Optional[str],
    progress_cb: Optional[Callable[[str], None]] = None,
) -> str:
    size = 1024
    base_size = 1386.0
    scale = size / base_size

    # Template specs (scaled from your Figma reference).
    quote_box_w = int(round(1093 * scale))
    quote_box_h = int(round(455 * scale))
    quote_font_px = int(round(75 * scale))
    quote_tracking_px = 0  # 0%
    quote_fill = (255, 255, 255, 255)  # 100%

    wm_box_w = int(round(350 * scale))
    wm_box_h = int(round(49 * scale))
    wm_font_px = int(round(40 * scale))
    wm_fill = (255, 255, 255, 32)  # very subtle watermark alpha

    bg = _load_background_image(image_url or "")
    if bg is None:
        raise RuntimeError("no generated background image")
    if progress_cb:
        progress_cb("Этап обрезки и подготовки картинки...")
    src_w, src_h = bg.size
    side = min(src_w, src_h)
    left = (src_w - side) // 2
    top = (src_h - side) // 2
    bg = bg.crop((left, top, left + side, top + side))
    bg = bg.resize((size, size), Image.Resampling.LANCZOS)
    bg = ImageEnhance.Contrast(bg).enhance(1.03)
    bg = ImageEnhance.Color(bg).enhance(0.93)
    bg = bg.filter(ImageFilter.GaussianBlur(radius=0.4))

    # Dark overlay.
    if progress_cb:
        progress_cb("Этап наложения затемнения...")
    # 40% dark overlay to keep text readable while preserving the image.
    overlay = Image.new("RGBA", (size, size), (0, 0, 0, 102))
    card = bg.convert("RGBA")
    card.alpha_composite(overlay)
    draw = ImageDraw.Draw(card)

    font_candidates = [
        "/app/fonts/Inter-Regular.ttf",
        "/usr/share/fonts/truetype/inter/Inter-Regular.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
    ]
    quote_font = None
    for f in font_candidates:
        try:
            quote_font = ImageFont.truetype(f, size=quote_font_px)
            break
        except Exception:
            continue
    if quote_font is None:
        quote_font = ImageFont.load_default()

    max_width = quote_box_w
    quote_text, _author = split_quote_and_author(phrase)
    text = _phrase_title_for_publish((quote_text or phrase or "").strip())
    lines = _wrap_lines(draw, text, quote_font, max_width)
    # Auto-fit font size into fixed quote box.
    while True:
        line_h = int((quote_font.size if hasattr(quote_font, "size") else 42) * 1.14)
        block_h = max(line_h * max(1, len(lines)), line_h)
        too_wide = any((draw.textbbox((0, 0), ln, font=quote_font)[2] - draw.textbbox((0, 0), ln, font=quote_font)[0]) > quote_box_w for ln in lines) if lines else False
        if block_h <= quote_box_h and not too_wide:
            break
        try:
            cur_size = quote_font.size if hasattr(quote_font, "size") else 42
            quote_font = ImageFont.truetype(font_candidates[0], size=max(34, cur_size - 2))
        except Exception:
            break
        lines = _wrap_lines(draw, text, quote_font, max_width)

    line_height = int((quote_font.size if hasattr(quote_font, "size") else 42) * 1.14)
    block_h = max(line_height * max(1, len(lines)), line_height)
    quote_box_x = (size - quote_box_w) // 2
    quote_box_y = (size - quote_box_h) // 2
    y = quote_box_y + max(0, (quote_box_h - block_h) // 2)
    if progress_cb:
        progress_cb("Этап наложения текстов...")
    for line in lines:
        bbox = draw.textbbox((0, 0), line, font=quote_font)
        w = bbox[2] - bbox[0]
        x = quote_box_x + max(0, (quote_box_w - w) // 2) + quote_tracking_px
        draw.text((x, y), line, font=quote_font, fill=quote_fill)
        y += line_height

    wm_font = quote_font
    for wf in font_candidates:
        try:
            wm_font = ImageFont.truetype(wf, size=wm_font_px)
            break
        except Exception:
            continue
    wm = "@kindlysupport"
    wb = draw.textbbox((0, 0), wm, font=wm_font)
    ww = wb[2] - wb[0]
    wh = wb[3] - wb[1]
    wm_x = (size - wm_box_w) // 2
    wm_y = size - wm_box_h - int(round(18 * scale))
    wx = wm_x + max(0, (wm_box_w - ww) // 2)
    wy = wm_y + max(0, (wm_box_h - wh) // 2)
    # Draw watermark on a dedicated transparent layer to guarantee alpha blending.
    wm_layer = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    wm_draw = ImageDraw.Draw(wm_layer, "RGBA")
    wm_draw.text((wx, wy), wm, font=wm_font, fill=wm_fill)
    card.alpha_composite(wm_layer)

    file_name = f"final/{datetime.now(MOSCOW_TZ).strftime('%Y/%m/%d')}/post_card_{int(time.time()*1000)}_{secrets.token_hex(4)}.jpg"
    out = io.BytesIO()
    card.convert("RGB").save(out, format="JPEG", quality=92, optimize=True)
    return storage_put_bytes(file_name, out.getvalue(), "image/jpeg")


def generate_post_caption(post: dict[str, Any]) -> str:
    source_kind = (post.get("source_kind") or "").strip()
    title = (post.get("title") or "").strip()
    text_body = (post.get("text_body") or "").strip()
    if source_kind == "phrase":
        quote, author = split_quote_and_author(title)
        phrase_title = _phrase_title_for_publish(quote or title)
        body = (text_body or "").strip()
        phrase_md = f"*{escape_markdown_v2(phrase_title)}*"
        if author:
            phrase_md = f"{phrase_md} — {escape_markdown_v2(author)}"
        body_md = escape_markdown_v2(body)
        return f"{phrase_md}\n\n{body_md}\n\n@kindlysupport" if body_md else f"{phrase_md}\n\n@kindlysupport"
    return generate_caption(title, text_body)


def generate_post_caption_plain(post: dict[str, Any]) -> str:
    source_kind = (post.get("source_kind") or "").strip()
    title = (post.get("title") or "").strip()
    text_body = (post.get("text_body") or "").strip()
    if source_kind == "phrase":
        quote, author = split_quote_and_author(title)
        phrase_title = _phrase_title_for_publish(quote or title)
        body = (text_body or "").strip()
        title_line = phrase_title
        if author:
            title_line = f"{title_line} - {author}"
        return f"{title_line}\n\n{body}\n\n@kindlysupport" if body else f"{title_line}\n\n@kindlysupport"
    return generate_caption(title, text_body)


def generate_post_caption_markdown_limited(post: dict[str, Any], max_len: int = 1024) -> str:
    source_kind = (post.get("source_kind") or "").strip()
    title = (post.get("title") or "").strip()
    text_body = (post.get("text_body") or "").strip()
    if source_kind != "phrase":
        raw = generate_caption(title, text_body)
        return raw if len(raw) <= max_len else raw[: max_len - 1]

    quote, author = split_quote_and_author(title)
    phrase_title = _phrase_title_for_publish(quote or title)
    body = (text_body or "").strip()

    title_md = f"*{escape_markdown_v2(phrase_title)}*"
    if author:
        title_md = f"{title_md} - {escape_markdown_v2(author)}"

    tail = "\n\n@kindlysupport"
    available = max(0, max_len - len(title_md) - len(tail) - 2)
    body_plain = body[:available].rstrip()
    # Keep paragraph break if truncation leaves enough content.
    if "\n\n" not in body_plain:
        p1, p2 = _split_two_paragraphs(body)
        if p1 and p2:
            merged = f"{p1}\n\n{p2}"
            body_plain = merged[:available].rstrip()
    body_md = escape_markdown_v2(body_plain)
    caption = f"{title_md}\n\n{body_md}{tail}"
    if len(caption) > max_len:
        # Conservative final trim to keep Markdown valid and avoid trailing escape.
        cap = caption[:max_len].rstrip("\\")
        while cap.endswith("\\"):
            cap = cap[:-1]
        return cap
    return caption


def should_regenerate_background_from_instruction(instruction: str) -> bool:
    text = (instruction or "").strip().lower()
    if not text:
        return False
    # Explicit user intent to change/generated new base image (not only text/overlay tweaks).
    image_intent_keywords = (
        "новый фон",
        "новое фото",
        "новая картинка",
        "замени фон",
        "смени фон",
        "сменить фон",
        "перегенерируй фон",
        "перегенерируй изображ",
        "перегенерируй картинк",
        "пересобери сценари",
        "новый сценари",
        "по сценарию",
        "другой сюжет",
        "другой ракурс",
        "image prompt",
        "regenerate image",
        "new background",
        "new scenario",
        "change background",
    )
    return any(k in text for k in image_intent_keywords)


def split_regen_instruction(instruction: str) -> dict[str, str]:
    raw = (instruction or "").strip()
    if not raw:
        return {"common": "", "text": "", "scenario": ""}
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    text_lines: list[str] = []
    scenario_lines: list[str] = []
    common_lines: list[str] = []
    for ln in lines:
        m_text = re.match(r"^(?:text|текст)\s*:\s*(.+)$", ln, flags=re.IGNORECASE)
        if m_text:
            text_lines.append(m_text.group(1).strip())
            continue
        m_scn = re.match(r"^(?:scenario|сценарий|картинка|изображение|фон)\s*:\s*(.+)$", ln, flags=re.IGNORECASE)
        if m_scn:
            scenario_lines.append(m_scn.group(1).strip())
            continue
        common_lines.append(ln)
    return {
        "common": " ".join(common_lines).strip(),
        "text": " ".join(text_lines).strip(),
        "scenario": " ".join(scenario_lines).strip(),
    }


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
    image_t = usage.get("image_tokens") or 0
    # Approximate. OpenRouter pricing can vary and may bill separate token classes.
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
            "HTTP-Referer": runtime_openrouter_site_url(),
            "X-Title": runtime_openrouter_app_name(),
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


def openrouter_generate_text(
    prompt: str,
    temperature: Optional[float] = None,
    top_p: Optional[float] = None,
    system_prompt: str = "",
    trace_label: str = "",
) -> str:
    extra: dict[str, Any] = {}
    if temperature is not None:
        extra["temperature"] = temperature
    if top_p is not None:
        extra["top_p"] = top_p
    messages: list[dict[str, Any]] = []
    if (system_prompt or "").strip():
        messages.append({"role": "system", "content": system_prompt.strip()})
    messages.append({"role": "user", "content": prompt})
    model = runtime_openrouter_text_model()
    try:
        res = openrouter_chat(
            model,
            messages,
            extra_body=extra or None,
        )
        out = (res.get("choices") or [{}])[0].get("message", {}).get("content", "")
        _log_llm_text_event(
            trace_label=trace_label,
            model=model,
            prompt=prompt,
            response=out,
            error="",
        )
        return out
    except Exception as e:
        _log_llm_text_event(
            trace_label=trace_label,
            model=model,
            prompt=prompt,
            response="",
            error=str(e),
        )
        raise


def _log_llm_text_event(
    trace_label: str,
    model: str,
    prompt: str,
    response: str,
    error: str,
) -> None:
    now = now_iso()
    try:
        with db() as conn:
            conn.execute(
                """
                INSERT INTO llm_text_logs
                (trace_label, model, prompt, response, error, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    (trace_label or "")[:120],
                    (model or "")[:120],
                    (prompt or "")[:24000],
                    (response or "")[:24000],
                    (error or "")[:2000],
                    now,
                ),
            )
    except Exception:
        logger.exception("llm_text_log_failed trace=%s", (trace_label or "")[:80])


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


def openrouter_extract_main_quote_from_image(image_url: str) -> str:
    res = openrouter_chat(
        runtime_openrouter_vision_model(),
        [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": (
                            "Найди на изображении ОСНОВНУЮ мотивирующую фразу/цитату.\n"
                            "Обычно это крупный текст по центру экрана.\n"
                            "Игнорируй: время, дату, шапку приложения, имя пользователя, кнопки, иконки, подписи типа 'Подробнее'.\n"
                            "Верни только саму цитату одной строкой без markdown и пояснений.\n"
                            "Если цитаты нет — верни пустую строку."
                        ),
                    },
                    {"type": "image_url", "image_url": {"url": image_url}},
                ],
            }
        ],
    )
    return (res.get("choices") or [{}])[0].get("message", {}).get("content", "")


def _load_image_for_local_ocr(image_url: str) -> Optional[Image.Image]:
    try:
        if image_url.startswith("data:image/"):
            _, b64 = image_url.split(",", 1)
            raw = base64.b64decode(b64, validate=False)
            img = Image.open(io.BytesIO(raw)).convert("RGB")
            return _trim_dark_borders(img)
        with urllib.request.urlopen(image_url, timeout=30) as resp:
            raw = resp.read()
        img = Image.open(io.BytesIO(raw)).convert("RGB")
        return _trim_dark_borders(img)
    except (ValueError, OSError, urllib.error.URLError, binascii.Error):
        return None


def _trim_dark_borders(img: Image.Image) -> Image.Image:
    """Trim large near-black bars around screenshots (letterboxing/phone captures)."""
    try:
        gray = img.convert("L")
        # Treat near-black as background.
        mask = gray.point(lambda p: 0 if p < 20 else 255, mode="1").convert("L")
        bbox = mask.getbbox()
        if not bbox:
            return img
        left, top, right, bottom = bbox
        w, h = img.size
        cw, ch = (right - left), (bottom - top)
        # Avoid tiny accidental crops.
        if cw < int(w * 0.55) or ch < int(h * 0.55):
            return img
        # Only apply if crop really removes substantial border area.
        if (left > int(w * 0.03)) or (top > int(h * 0.03)) or (right < int(w * 0.97)) or (bottom < int(h * 0.97)):
            return img.crop((left, top, right, bottom))
        return img
    except Exception:
        return img


def _tesseract_image_to_string_safe(img: Image.Image, *, lang: str, config: str) -> str:
    try:
        return (pytesseract.image_to_string(img, lang=lang, config=config, timeout=OCR_TESSERACT_TIMEOUT_SEC) or "").strip()
    except RuntimeError:
        logger.warning("tesseract_timeout image_to_string config=%s timeout_sec=%s", config, OCR_TESSERACT_TIMEOUT_SEC)
        return ""
    except Exception:
        logger.exception("tesseract_image_to_string_failed")
        return ""


def _tesseract_image_to_data_safe(img: Image.Image, *, lang: str, config: str) -> dict[str, Any]:
    try:
        return pytesseract.image_to_data(
            img,
            lang=lang,
            config=config,
            timeout=OCR_TESSERACT_TIMEOUT_SEC,
            output_type=pytesseract.Output.DICT,
        )
    except RuntimeError:
        logger.warning("tesseract_timeout image_to_data config=%s timeout_sec=%s", config, OCR_TESSERACT_TIMEOUT_SEC)
        return {}
    except Exception:
        logger.exception("tesseract_image_to_data_failed")
        return {}


def local_ocr_extract_text_from_image(image_url: str) -> str:
    if pytesseract is None:
        return ""
    img = _load_image_for_local_ocr(image_url)
    if img is None:
        return ""
    try:
        gray = img.convert("L")
        # First pass: soft preprocessing for textured backgrounds (books/screens).
        soft = ImageEnhance.Contrast(ImageOps.autocontrast(gray)).enhance(2.1)
        text = _tesseract_image_to_string_safe(
            soft,
            lang="rus+eng",
            config="--oem 1 --psm 6",
        )
        if text:
            return text
        # Fallback: hard threshold for high-contrast cards.
        bw = gray.point(lambda x: 255 if x > 165 else 0, mode="1")
        text = _tesseract_image_to_string_safe(
            bw,
            lang="rus+eng",
            config="--oem 1 --psm 11",
        )
        return text
    except Exception:
        logger.exception("local_ocr_failed")
        return ""


def paddle_ocr_client() -> Optional[Any]:
    global _paddle_ocr_client
    if PaddleOCR is None:
        return None
    if _paddle_ocr_client is not None:
        return _paddle_ocr_client
    try:
        _paddle_ocr_client = PaddleOCR(use_angle_cls=True, lang="ru")
        return _paddle_ocr_client
    except Exception:
        logger.exception("paddle_ocr_init_failed")
        return None


def _cv2_book_variants(gray_arr: Any) -> list[Any]:
    if cv2 is None or np is None:
        return []
    clahe = cv2.createCLAHE(clipLimit=2.5, tileGridSize=(8, 8))
    c1 = clahe.apply(gray_arr)
    c1 = cv2.fastNlMeansDenoising(c1, None, 10, 7, 21)
    up = cv2.resize(c1, (max(2, c1.shape[1] * 3), max(2, c1.shape[0] * 3)), interpolation=cv2.INTER_CUBIC)
    kernel = np.array([[0, -1, 0], [-1, 5, -1], [0, -1, 0]], dtype=np.float32)
    sharp = cv2.filter2D(up, -1, kernel)
    bw = cv2.adaptiveThreshold(sharp, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 31, 15)
    bw_inv = cv2.bitwise_not(bw)
    return [up, sharp, bw, bw_inv]


def paddle_ocr_extract_phrases_from_image(image_url: str) -> tuple[str, list[str]]:
    client = paddle_ocr_client()
    if client is None or cv2 is None or np is None:
        return "", []
    img = _load_image_for_local_ocr(image_url)
    if img is None:
        return "", []
    try:
        gray = img.convert("L")
        w, h = gray.size
        band = _find_book_quote_band_bounds(gray)
        if band:
            btop, bbot = band
            bh = max(1, bbot - btop)
            quote_crop = gray.crop((int(w * 0.10), int(btop + bh * 0.06), int(w * 0.90), int(btop + bh * 0.80)))
            author_crop = gray.crop((int(w * 0.24), int(btop + bh * 0.60), int(w * 0.76), int(btop + bh * 0.96)))
        else:
            quote_crop = gray.crop((int(w * 0.10), int(h * 0.26), int(w * 0.90), int(h * 0.74)))
            author_crop = gray.crop((int(w * 0.24), int(h * 0.56), int(w * 0.76), int(h * 0.82)))

        def _run_ocr_lines(gray_crop: Image.Image) -> list[tuple[str, float]]:
            arr = np.array(gray_crop)
            lines: list[tuple[str, float]] = []
            for v in _cv2_book_variants(arr):
                try:
                    out = client.ocr(v, cls=True) or []
                except Exception:
                    continue
                if not out:
                    continue
                block = out[0] if isinstance(out[0], list) else out
                for item in block:
                    if not item or len(item) < 2:
                        continue
                    txt = str((item[1][0] if isinstance(item[1], (list, tuple)) and len(item[1]) >= 1 else "") or "").strip()
                    conf = float(item[1][1]) if isinstance(item[1], (list, tuple)) and len(item[1]) >= 2 else 0.0
                    if not txt:
                        continue
                    txt = _normalize_ocr_spacing(txt)
                    if _is_ocr_noise_line(txt):
                        continue
                    lines.append((txt, conf))
            return lines

        quote_lines = _run_ocr_lines(quote_crop)
        author_lines = _run_ocr_lines(author_crop)

        # keep high-confidence textual rows and join in reading order approximation
        quote_candidates: list[str] = []
        for txt, conf in quote_lines:
            if conf < 0.42:
                continue
            if _is_probable_author_line(txt):
                continue
            quote_candidates.append(txt)
        quote_candidates = _merge_broken_quote_lines(quote_candidates)
        quote = _normalize_ocr_punctuation(_repair_common_ocr_omissions(" ".join(quote_candidates))).strip(" .,:;-\t")
        if quote and not quote.endswith((".", "!", "?")):
            quote += "."
        if not _is_plausible_quote_text(quote):
            quote = ""

        author = ""
        best_author_score = -1.0
        for txt, conf in author_lines:
            if conf < 0.35:
                continue
            if not _is_probable_author_line(txt):
                continue
            score = conf * 10.0 + len(re.findall(r"[А-Яа-яЁёA-Za-z]", txt))
            if score > best_author_score:
                best_author_score = score
                author = txt

        if quote and author and author.lower() not in quote.lower():
            quote = f"{quote.rstrip(' .')}. {author}"

        raw = "\n".join([x for x, _ in quote_lines]).strip()
        if quote:
            return raw, [quote]
        return raw, []
    except Exception:
        logger.exception("paddle_ocr_extract_failed")
        return "", []


def _median_value(nums: list[float]) -> float:
    if not nums:
        return 0.0
    arr = sorted(nums)
    n = len(arr)
    mid = n // 2
    if n % 2:
        return float(arr[mid])
    return float((arr[mid - 1] + arr[mid]) / 2.0)


def _normalize_ocr_spacing(text: str) -> str:
    s = re.sub(r"\s+", " ", (text or "").strip())
    if not s:
        return s
    # Remove spaces before punctuation and normalize dash spacing.
    s = re.sub(r"\s+([,.;:!?])", r"\1", s)
    s = re.sub(r"\s*[—-]\s*", " - ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _normalize_ocr_punctuation(text: str) -> str:
    s = (text or "").strip()
    if not s:
        return s
    # Heuristic: restore comma before common Russian gerund forms.
    s = re.sub(
        r"\b([А-Яа-яЁёA-Za-z]{2,})\s+((?:[А-Яа-яЁё]+ясь|[А-Яа-яЁё]+вшись|[А-Яа-яЁё]+ши))\b",
        r"\1, \2",
        s,
        flags=re.IGNORECASE,
    )
    return _normalize_ocr_spacing(s)


def _is_book_scaffold_line(text: str) -> bool:
    s = (text or "").strip().lower()
    if not s:
        return False
    if re.search(r"положител\w+\s+установ\w+", s):
        return True
    if re.search(r"что\s+сегодня", s):
        return True
    if re.search(r"что\s+я\s+смогу\s+сделать\s+завтра", s):
        return True
    if re.search(r"прекрасн\w+\s+событ\w+.*сегодня", s):
        return True
    if re.search(r"сделает\s+сегодняшн\w+\s+день\s+замечательн\w+", s):
        return True
    if "?" in s and re.search(r"(сегодня|завтра|друг|событ|день)", s):
        return True
    hits = 0
    for k in (
        "сегодня",
        "завтра",
        "друг",
        "установ",
        "прекрасн",
        "событ",
        "замечательн",
        "хорош",
        "сделано",
    ):
        if k in s:
            hits += 1
    return hits >= 3 and len(s) <= 130


def _contains_scaffold_markers(text: str) -> bool:
    s = (text or "").strip().lower()
    if not s:
        return False
    if _is_book_scaffold_line(s):
        return True
    if re.search(r"(положител\w+\s+установ\w+|сегодн\w+|завтр\w+|друг\w+|прекрасн\w+\s+событ\w+)", s):
        return True
    if re.search(r"(что\s+сдела\w+|что\s+я\s+смог\w+|что\s+сегодня\s+было)", s):
        return True
    return False


def _repair_common_ocr_omissions(text: str) -> str:
    s = (text or "").strip()
    if not s:
        return s
    low = s.lower()
    # Common lock-screen quote omission: lost first short token "Ты".
    if low.startswith("ничего не ") and "— ты " in low:
        return f"Ты {s}"
    return s


def _is_short_keep_fragment(text: str) -> bool:
    s = (text or "").strip()
    if not s:
        return False
    if re.search(r"\d", s):
        return False
    # Keep short pronoun/particle fragments that can be split off by OCR ("Ты", "И").
    return bool(re.fullmatch(r"[А-Яа-яЁёA-Za-z]{1,4}[.!?]?", s))


def _detect_ocr_profile(width: int, height: int, lines: list[dict[str, Any]]) -> str:
    if not lines or width <= 0 or height <= 0:
        return "generic"
    ratio = (height / max(1, width))
    texts = [str(x.get("text") or "") for x in lines]
    low_join = " ".join(texts).lower()
    if re.search(
        r"(что сделает сегодняшний день замечательным|положительная установка|что сегодня было сделано хорошего для других|что я смогу сделать завтра лучше|прекрасные события, которые произошли со мной сегодня)",
        low_join,
    ):
        return "book_page"
    line_count = len(texts)
    avg_len = sum(len(t) for t in texts) / max(1, line_count)
    ui_hits = sum(
        1
        for t in texts
        if re.search(r"(подробнее|поделиться|вопрос сессии|ежедневное вдохновение|привет|настройки|профиль)", t.lower())
    )
    if ratio > 1.55 and ui_hits >= 1:
        return "app_screenshot"
    if ratio > 1.55 and line_count <= 18:
        return "social_card"
    if ratio < 1.45 and line_count >= 20 and avg_len >= 18:
        return "book_page"
    return "generic"


def _find_book_quote_band_bounds(gray: Image.Image) -> Optional[tuple[int, int]]:
    w, h = gray.size
    if w <= 0 or h <= 0:
        return None
    x0 = int(w * 0.12)
    x1 = int(w * 0.88)
    # Search mostly in central belt to avoid lower textured background (blanket/table),
    # but keep it broad enough for slightly shifted camera framing.
    y0 = int(h * 0.16)
    y1 = int(h * 0.86)
    if x1 <= x0 or y1 <= y0:
        return None
    ys: list[int] = []
    means: list[float] = []
    dark_ratios: list[float] = []
    bright_ratios: list[float] = []
    for y in range(y0, y1, 2):
        row = gray.crop((x0, y, x1, y + 1))
        hist = row.histogram()
        total = max(1, sum(hist))
        mean = sum(i * int(v) for i, v in enumerate(hist)) / float(total)
        dark = sum(int(v) for v in hist[:130]) / float(total)
        bright = sum(int(v) for v in hist[180:]) / float(total)
        ys.append(y)
        means.append(mean)
        dark_ratios.append(dark)
        bright_ratios.append(bright)
    if not ys:
        return None
    # Adaptive thresholds per photo.
    mean_sorted = sorted(means)
    dark_sorted = sorted(dark_ratios)
    bright_sorted = sorted(bright_ratios)
    mid_idx = max(0, min(len(mean_sorted) - 1, len(mean_sorted) // 2))
    q35_idx = max(0, min(len(mean_sorted) - 1, int(len(mean_sorted) * 0.35)))
    q65_dark_idx = max(0, min(len(dark_sorted) - 1, int(len(dark_sorted) * 0.65)))
    q25_bright_idx = max(0, min(len(bright_sorted) - 1, int(len(bright_sorted) * 0.25)))
    mean_median = float(mean_sorted[mid_idx])
    mean_q35 = float(mean_sorted[q35_idx])
    dark_q65 = float(dark_sorted[q65_dark_idx])
    bright_q25 = float(bright_sorted[q25_bright_idx])

    is_band_row: list[bool] = []
    for i in range(len(ys)):
        # Quote strip tends to be darker than neighbors but not fully black.
        mean_ok = means[i] <= min(174.0, mean_median - 7.0, mean_q35 + 9.0)
        dark_ok = dark_ratios[i] >= max(0.24, dark_q65 - 0.03)
        bright_ok = bright_ratios[i] >= max(0.006, bright_q25 * 0.65)
        is_band_row.append(bool(mean_ok and dark_ok and bright_ok))
    best: Optional[tuple[float, int, int]] = None
    i = 0
    while i < len(ys):
        if not is_band_row[i]:
            i += 1
            continue
        j = i
        while j + 1 < len(ys) and is_band_row[j + 1]:
            j += 1
        top = ys[i]
        bottom = ys[j] + 1
        band_h = bottom - top
        if int(h * 0.06) <= band_h <= int(h * 0.36):
            seg_dark = sum(dark_ratios[k] for k in range(i, j + 1)) / max(1, (j - i + 1))
            seg_bright = sum(bright_ratios[k] for k in range(i, j + 1)) / max(1, (j - i + 1))
            mid = (top + bottom) / 2.0
            if mid < (h * 0.28) or mid > (h * 0.80):
                i = j + 1
                continue
            center_penalty = abs(mid - (h * 0.57)) / max(1.0, h * 0.57)
            score = (seg_dark * 2.5) + (seg_bright * 1.3) - center_penalty
            if best is None or score > best[0]:
                best = (score, top, bottom)
        i = j + 1
    if not best:
        return None
    _, top, bottom = best
    pad = max(4, int((bottom - top) * 0.10))
    return max(0, top - pad), min(h, bottom + pad)


def _score_ocr_block_candidate(
    phrase: str,
    block_top: int,
    block_bottom: int,
    block_mid_x: float,
    width: int,
    height: int,
    line_count: int,
    profile: str,
) -> float:
    text = (phrase or "").strip()
    if not text:
        return -999.0
    letters = len(re.findall(r"[A-Za-zА-Яа-яЁё]", text))
    words = re.findall(r"[A-Za-zА-Яа-яЁё'\\-]+", text)
    if letters < 10 or len(words) < 3:
        return -999.0

    # Base quality by length.
    score = 0.0
    if 28 <= len(text) <= 210:
        score += 3.5
    elif 16 <= len(text) <= 260:
        score += 1.8
    else:
        score -= 1.5

    # Reward punctuation typical for quote ending.
    if text.endswith((".", "!", "?")):
        score += 0.9

    digits = sum(ch.isdigit() for ch in text)
    if digits > 3:
        score -= 2.0

    low = text.lower()
    if re.search(r"(подробнее|поделиться|вопрос сессии|ежедневное вдохновение|привет[, ]|смена блокировки экран)", low):
        score -= 4.0
    if re.search(r"\b(нояб|дек|янв|фев|мар|апр|май|июн|июл|авг|сен|окт)\.?\b", low) and digits >= 2:
        score -= 2.0

    mid_y = (block_top + block_bottom) / 2.0
    center_bias = 1.0 - min(1.0, abs(mid_y - (height / 2.0)) / (height / 2.0))
    x_bias = 1.0 - min(1.0, abs(block_mid_x - (width / 2.0)) / (width / 2.0))
    score += center_bias * 2.6
    score += x_bias * 1.2

    # Profile-specific adjustments.
    if profile in {"app_screenshot", "social_card"}:
        if mid_y < height * 0.19:
            score -= 3.0
        if block_bottom > height * 0.92:
            score -= 2.2
        if 1 <= line_count <= 5:
            score += 1.0
    elif profile == "book_page":
        if 1 <= line_count <= 6:
            score += 1.2
        if line_count >= 10:
            score -= 1.7
        if len(text) > 230:
            score -= 1.2
    else:
        if 1 <= line_count <= 7:
            score += 0.8

    return score


def _extract_center_quote_book_style(gray: Image.Image) -> tuple[str, Optional[str], bool, float]:
    """Fallback OCR for book-style quote pages: central quote block + lower author line."""
    if pytesseract is None:
        return "", None, False, 0.0
    w, h = gray.size
    if w <= 0 or h <= 0:
        return "", None, False, 0.0

    band = _find_book_quote_band_bounds(gray)
    if band:
        btop, bbot = band
        bh = max(1, bbot - btop)
        quote_crop_primary = gray.crop((int(w * 0.10), int(btop + bh * 0.06), int(w * 0.90), int(btop + bh * 0.78)))
        quote_crop_alt = gray.crop((int(w * 0.14), int(btop + bh * 0.10), int(w * 0.86), int(btop + bh * 0.74)))
        author_crop_primary = gray.crop((int(w * 0.28), int(btop + bh * 0.62), int(w * 0.72), int(btop + bh * 0.94)))
        author_crop_alt = gray.crop((int(w * 0.24), int(btop + bh * 0.58), int(w * 0.76), int(btop + bh * 0.96)))
    else:
        # Geometric fallback tuned for quote cards on book pages.
        quote_crop_primary = gray.crop((int(w * 0.10), int(h * 0.26), int(w * 0.90), int(h * 0.74)))
        quote_crop_alt = gray.crop((int(w * 0.12), int(h * 0.18), int(w * 0.88), int(h * 0.60)))
        author_crop_primary = gray.crop((int(w * 0.24), int(h * 0.56), int(w * 0.76), int(h * 0.82)))
        author_crop_alt = gray.crop((int(w * 0.28), int(h * 0.53), int(w * 0.72), int(h * 0.72)))

    def _hist_quantile(hist: list[int], q: float) -> int:
        total = max(1, sum(hist))
        target = total * max(0.0, min(1.0, q))
        acc = 0
        for i, v in enumerate(hist):
            acc += int(v)
            if acc >= target:
                return i
        return 255

    def _build_contrast_variants(img: Image.Image) -> list[Image.Image]:
        auto = ImageOps.autocontrast(img)
        eq = ImageOps.equalize(auto)
        sharp = auto.filter(ImageFilter.UnsharpMask(radius=1.6, percent=220, threshold=2))
        # Flatten uneven background to make letters more separable.
        bg = auto.filter(ImageFilter.GaussianBlur(radius=7.0))
        flatten = ImageChops.subtract(auto, bg, scale=1.0, offset=128)
        flatten = ImageEnhance.Contrast(ImageOps.autocontrast(flatten)).enhance(2.8)

        # Adaptive stretching from histogram quantiles.
        hist = auto.histogram()
        p10 = _hist_quantile(hist, 0.10)
        p90 = _hist_quantile(hist, 0.90)
        if p90 <= p10:
            adaptive = auto
        else:
            lut: list[int] = []
            denom = float(p90 - p10)
            for x in range(256):
                if x <= p10:
                    lut.append(0)
                elif x >= p90:
                    lut.append(255)
                else:
                    lut.append(int(((x - p10) * 255.0) / denom))
            adaptive = auto.point(lut, mode="L")
        adaptive = ImageEnhance.Contrast(adaptive).enhance(2.5)

        # White text on dark background and inverse case.
        inv = ImageOps.invert(auto)
        thr_dark = auto.point(lambda x: 255 if x > 168 else 0, mode="L")
        thr_light = inv.point(lambda x: 255 if x > 148 else 0, mode="L")
        black_text_white_bg = ImageOps.invert(thr_dark).convert("L")
        white_text_white_bg = ImageOps.invert(thr_light).convert("L")

        up2 = auto.resize((max(2, auto.width * 2), max(2, auto.height * 2)), Image.Resampling.LANCZOS)
        up2_sharp = up2.filter(ImageFilter.UnsharpMask(radius=1.6, percent=240, threshold=1))
        return [auto, eq, sharp, flatten, adaptive, black_text_white_bg, white_text_white_bg, up2, up2_sharp]

    def _prep_variants(img: Image.Image) -> list[Image.Image]:
        return _build_contrast_variants(img)

    def _ocr_lines(img: Image.Image, cfg: str) -> list[str]:
        txt = _tesseract_image_to_string_safe(img, lang="rus+eng", config=cfg)
        lines = [re.sub(r"\s+", " ", x.strip()) for x in txt.splitlines()]
        return [x for x in lines if x]

    def _pick_best_quote(lines_batches: list[list[str]]) -> str:
        best = ""
        best_score = -1.0
        for lines in lines_batches:
            cleaned: list[str] = []
            for line in lines:
                if _is_ocr_noise_line(line):
                    continue
                if _is_book_scaffold_line(line):
                    continue
                low = line.lower()
                if re.search(
                    r"(что сделает сегодня.*день замечательн|положител.*установк|что сегодня было сделано|что я смогу сделать завтра|прекрасные событи.*произошл.*сегодня|для других\??$)",
                    low,
                ):
                    continue
                if _contains_scaffold_markers(low):
                    continue
                if len(re.findall(r"[A-Za-zА-Яа-яЁё]", line)) < 4:
                    continue
                if _is_probable_author_line(line):
                    continue
                cleaned.append(line)
            if not cleaned:
                continue
            candidate = _normalize_ocr_punctuation(_repair_common_ocr_omissions(" ".join(cleaned))).strip(" .,:;-\t")
            if not candidate:
                continue
            low_cand = candidate.lower()
            if _is_book_scaffold_line(candidate):
                continue
            if _contains_scaffold_markers(low_cand):
                continue
            if re.search(
                r"(что сделает сегодня|положител.*установк|что сегодня было сделано|что я смогу сделать завтра|прекрасные событи.*сегодня)",
                low_cand,
            ):
                continue
            if not _is_plausible_quote_text(candidate):
                continue
            letters = len(re.findall(r"[A-Za-zА-Яа-яЁё]", candidate))
            words = len(re.findall(r"[A-Za-zА-Яа-яЁё'-]+", candidate))
            score = (letters * 0.05) + (words * 0.4)
            if 40 <= len(candidate) <= 240:
                score += 3.0
            if score > best_score:
                best_score = score
                best = candidate
        if best_score < 5.4:
            return "", float(best_score)
        return best, float(best_score)

    def _pick_best_author(lines_batches: list[list[str]]) -> Optional[str]:
        best: Optional[str] = None
        best_score = -1.0
        for lines in lines_batches:
            for line in lines:
                s = re.sub(r"[^\wА-Яа-яЁё .-]", " ", line)
                s = re.sub(r"\s+", " ", s).strip()
                if not s or not _is_probable_author_line(s):
                    continue
                letters = len(re.findall(r"[A-Za-zА-Яа-яЁё]", s))
                score = float(letters)
                if 8 <= letters <= 26:
                    score += 5.0
                if score > best_score:
                    best_score = score
                    best = s
        return best

    quote_batches: list[list[str]] = []
    author_batches: list[list[str]] = []
    for crop in (quote_crop_primary, quote_crop_alt):
        for prepared in _prep_variants(crop):
            for cfg in ("--oem 1 --psm 6", "--oem 1 --psm 4"):
                quote_batches.append(_ocr_lines(prepared, cfg))
    for crop in (author_crop_primary, author_crop_alt):
        for prepared in _prep_variants(crop):
            for cfg in ("--oem 1 --psm 7", "--oem 1 --psm 6"):
                author_batches.append(_ocr_lines(prepared, cfg))

    quote, quote_score = _pick_best_quote(quote_batches)
    author = _pick_best_author(author_batches)
    if quote and author and author.lower() not in quote.lower():
        quote = f"{quote.rstrip(' .')}. {author}"
    if quote and not quote.endswith((".", "!", "?")):
        quote += "."
    return quote, author, bool(band), float(quote_score)


def _extract_center_quote_app_style(gray: Image.Image) -> tuple[str, Optional[str], float]:
    """Focused extractor for app-like screenshots with quote in lower-middle area."""
    if pytesseract is None:
        return "", None, 0.0
    w, h = gray.size
    if w <= 0 or h <= 0:
        return "", None, 0.0

    # Typical quote block on mobile cards is around center-lower, author below quote.
    quote_crop_main = gray.crop((int(w * 0.10), int(h * 0.40), int(w * 0.90), int(h * 0.79)))
    quote_crop_alt = gray.crop((int(w * 0.08), int(h * 0.34), int(w * 0.92), int(h * 0.82)))
    author_crop_main = gray.crop((int(w * 0.25), int(h * 0.66), int(w * 0.75), int(h * 0.82)))

    def _prep(img: Image.Image) -> list[Image.Image]:
        auto = ImageOps.autocontrast(img)
        eq = ImageOps.equalize(auto)
        sharp = auto.filter(ImageFilter.UnsharpMask(radius=1.5, percent=220, threshold=2))
        inv = ImageOps.invert(auto)
        thr_dark = auto.point(lambda x: 255 if x > 160 else 0, mode="L")
        thr_light = inv.point(lambda x: 255 if x > 142 else 0, mode="L")
        up2 = auto.resize((max(2, auto.width * 2), max(2, auto.height * 2)), Image.Resampling.LANCZOS)
        return [
            ImageEnhance.Contrast(auto).enhance(2.0),
            ImageEnhance.Contrast(eq).enhance(2.0),
            ImageEnhance.Contrast(sharp).enhance(2.0),
            ImageOps.invert(thr_dark).convert("L"),
            ImageOps.invert(thr_light).convert("L"),
            ImageEnhance.Contrast(up2).enhance(1.9),
        ]

    def _ocr_lines(img: Image.Image, cfg: str) -> list[str]:
        txt = _tesseract_image_to_string_safe(img, lang="rus+eng", config=cfg)
        lines = [re.sub(r"\s+", " ", x.strip()) for x in txt.splitlines()]
        return [x for x in lines if x]

    quote_batches: list[list[str]] = []
    for crop in (quote_crop_main, quote_crop_alt):
        for prepared in _prep(crop):
            for cfg in ("--oem 1 --psm 6", "--oem 1 --psm 4"):
                quote_batches.append(_ocr_lines(prepared, cfg))

    author_batches: list[list[str]] = []
    for prepared in _prep(author_crop_main):
        author_batches.append(_ocr_lines(prepared, "--oem 1 --psm 7"))

    best_quote = ""
    best_score = -1.0
    for lines in quote_batches:
        cleaned: list[str] = []
        for line in lines:
            low = line.lower()
            if _is_ocr_noise_line(line):
                continue
            if re.search(r"(подробнее|поделиться|ежедневное вдохновение|привет[, ]|\bмар[т]?\b\s+\d{4}|\d{1,2}\s*:\s*\d{2})", low):
                continue
            if _is_probable_author_line(line):
                continue
            cleaned.append(line)
        cleaned = _merge_broken_quote_lines(cleaned)
        candidate = _normalize_ocr_punctuation(_repair_common_ocr_omissions(" ".join(cleaned))).strip(" .,:;-\t")
        if not candidate:
            continue
        if not _is_plausible_quote_text(candidate):
            continue
        letters = len(re.findall(r"[A-Za-zА-Яа-яЁё]", candidate))
        score = letters * 0.06
        if 26 <= len(candidate) <= 210:
            score += 2.8
        if score > best_score:
            best_score = score
            best_quote = candidate

    best_author: Optional[str] = None
    best_author_score = -1.0
    for lines in author_batches:
        for line in lines:
            s = re.sub(r"[^\wА-Яа-яЁё .-]", " ", line)
            s = re.sub(r"\s+", " ", s).strip()
            if not s or not _is_probable_author_line(s):
                continue
            letters = len(re.findall(r"[A-Za-zА-Яа-яЁё]", s))
            score = float(letters)
            if 7 <= letters <= 24:
                score += 4.0
            if score > best_author_score:
                best_author_score = score
                best_author = s

    if best_quote and best_author and best_author.lower() not in best_quote.lower():
        best_quote = f"{best_quote.rstrip(' .')}. {best_author}"
    if best_quote and not best_quote.endswith((".", "!", "?")):
        best_quote += "."
    return best_quote, best_author, float(best_score)


def _attach_author_from_nearby_lines(
    phrase: str,
    block_bottom: int,
    block_mid_x: float,
    line_items: list[dict[str, Any]],
    width: int,
    height: int,
) -> str:
    base = (phrase or "").strip()
    if not base:
        return base
    if _is_probable_author_line(base):
        return base
    # Already looks like "quote. Author"
    last_words = base.split()[-4:]
    if last_words and _is_probable_author_line(" ".join(last_words)):
        return base

    max_gap = max(42, int(height * 0.14))
    best: Optional[tuple[int, str]] = None
    for line in line_items:
        text = str(line.get("text") or "").strip()
        if not text or not _is_probable_author_line(text):
            continue
        top = int(line.get("top") or 0)
        if top <= block_bottom:
            continue
        gap = top - block_bottom
        if gap > max_gap:
            continue
        center_x = float(line.get("center_x") or 0.0)
        if abs(center_x - block_mid_x) > width * 0.30:
            continue
        if best is None or gap < best[0]:
            best = (gap, text)
    if best:
        return f"{base.rstrip(' .')}. {best[1].strip()}"
    return base


def local_ocr_extract_phrases_from_image(image_url: str, fast_mode: bool = False) -> tuple[str, list[str]]:
    if pytesseract is None:
        return "", []
    img = _load_image_for_local_ocr(image_url)
    if img is None:
        return "", []
    try:
        gray = img.convert("L")
        width, height = gray.size
        # Fast-path for app-like vertical screenshots with centered quote cards.
        if fast_mode and height > width * 1.35:
            app_phrase, _app_author, app_score = _extract_center_quote_app_style(gray)
            if (
                app_phrase
                and app_score >= 4.8
                and _is_plausible_quote_text(app_phrase)
                and not looks_like_noise_phrase(app_phrase)
                and not _contains_scaffold_markers(app_phrase)
            ):
                return app_phrase, [app_phrase]

        def _collect_line_items(data: dict[str, Any]) -> list[dict[str, Any]]:
            line_buckets: dict[tuple[int, int, int], dict[str, Any]] = {}
            n = len(data.get("text", []))
            for i in range(n):
                raw = str(data["text"][i] or "").strip()
                if not raw:
                    continue
                conf_raw = str(data.get("conf", ["-1"])[i] or "-1").strip()
                try:
                    conf = float(conf_raw)
                except Exception:
                    conf = -1.0
                key = (int(data["block_num"][i]), int(data["par_num"][i]), int(data["line_num"][i]))
                left = int(data.get("left", [0])[i] or 0)
                top = int(data.get("top", [0])[i] or 0)
                w = int(data.get("width", [0])[i] or 0)
                h = int(data.get("height", [0])[i] or 0)
                row = line_buckets.setdefault(
                    key,
                    {
                        "words": [],
                        "conf_sum": 0.0,
                        "count": 0,
                        "conf_count": 0,
                        "left": left,
                        "top": top,
                        "right": left + w,
                        "bottom": top + h,
                        "heights": [],
                    },
                )
                row["words"].append(raw)
                if conf >= 0:
                    row["conf_sum"] += conf
                    row["conf_count"] += 1
                row["count"] += 1
                row["left"] = min(row["left"], left)
                row["top"] = min(row["top"], top)
                row["right"] = max(row["right"], left + w)
                row["bottom"] = max(row["bottom"], top + h)
                if h > 0:
                    row["heights"].append(h)

            out: list[dict[str, Any]] = []
            for key in sorted(line_buckets.keys()):
                row = line_buckets[key]
                if row["count"] <= 0:
                    continue
                avg_conf = row["conf_sum"] / max(1, row.get("conf_count") or row["count"])
                text = _normalize_ocr_spacing(" ".join(row["words"]))
                possible_author = _is_probable_author_line(text)
                if avg_conf < 47 and not (possible_author and avg_conf >= 28):
                    continue
                is_short_fragment = _is_short_keep_fragment(text)
                if len(re.findall(r"[A-Za-zА-Яа-яЁё]", text)) < 5 and not possible_author and not is_short_fragment:
                    continue
                if _is_ocr_noise_line(text) and not is_short_fragment:
                    continue
                top = int(row["top"])
                bottom = int(row["bottom"])
                h = max(1, bottom - top)
                if top < int(height * 0.07):
                    continue
                if bottom > int(height * 0.95):
                    continue
                out.append(
                    {
                        "text": text,
                        "left": int(row["left"]),
                        "top": top,
                        "right": int(row["right"]),
                        "bottom": bottom,
                        "height": h,
                        "center_x": (int(row["left"]) + int(row["right"])) / 2.0,
                    }
                )
            return out

        def _hist_quantile(hist: list[int], q: float) -> int:
            total = max(1, sum(hist))
            target = total * max(0.0, min(1.0, q))
            acc = 0
            for i, v in enumerate(hist):
                acc += int(v)
                if acc >= target:
                    return i
            return 255

        def _build_ocr_variants(src_gray: Image.Image) -> list[tuple[str, Image.Image, str]]:
            # OCR preprocessing: make background flatter and letters more contrasted.
            auto = ImageOps.autocontrast(src_gray)
            eq = ImageOps.equalize(auto)
            sharp = auto.filter(ImageFilter.UnsharpMask(radius=1.7, percent=230, threshold=2))
            denoise = auto.filter(ImageFilter.MedianFilter(size=3))

            # Background suppression (helps on textured cards and book photos).
            bg = auto.filter(ImageFilter.GaussianBlur(radius=8.0))
            flatten = ImageChops.subtract(auto, bg, scale=1.0, offset=128)
            flatten = ImageEnhance.Contrast(ImageOps.autocontrast(flatten)).enhance(2.9)
            flatten_sharp = flatten.filter(ImageFilter.UnsharpMask(radius=1.4, percent=240, threshold=1))

            # Adaptive contrast by quantiles.
            hist = auto.histogram()
            p12 = _hist_quantile(hist, 0.12)
            p88 = _hist_quantile(hist, 0.88)
            if p88 <= p12:
                adaptive = auto
            else:
                lut: list[int] = []
                denom = float(p88 - p12)
                for x in range(256):
                    if x <= p12:
                        lut.append(0)
                    elif x >= p88:
                        lut.append(255)
                    else:
                        lut.append(int(((x - p12) * 255.0) / denom))
                adaptive = auto.point(lut, mode="L")
            adaptive = ImageEnhance.Contrast(adaptive).enhance(2.4)

            # Threshold families for both text polarities.
            thr_dark_156 = auto.point(lambda x: 255 if x > 156 else 0, mode="L")
            thr_dark_172 = auto.point(lambda x: 255 if x > 172 else 0, mode="L")
            inv = ImageOps.invert(auto)
            thr_light_140 = inv.point(lambda x: 255 if x > 140 else 0, mode="L")
            thr_light_156 = inv.point(lambda x: 255 if x > 156 else 0, mode="L")

            up2 = auto.resize((width * 2, height * 2), Image.Resampling.LANCZOS)
            up2_sharp = up2.filter(ImageFilter.UnsharpMask(radius=1.8, percent=240, threshold=2))
            up2_flatten = flatten.resize((width * 2, height * 2), Image.Resampling.LANCZOS)

            return [
                ("soft_psm6", ImageEnhance.Contrast(auto).enhance(2.2), "--oem 1 --psm 6"),
                ("soft_psm4", ImageEnhance.Contrast(auto).enhance(2.1), "--oem 1 --psm 4"),
                ("soft_psm11", ImageEnhance.Contrast(auto).enhance(2.15), "--oem 1 --psm 11"),
                ("equalize_psm6", ImageEnhance.Contrast(eq).enhance(2.0), "--oem 1 --psm 6"),
                ("sharp_psm6", ImageEnhance.Contrast(sharp).enhance(2.1), "--oem 1 --psm 6"),
                ("denoise_psm6", ImageEnhance.Contrast(denoise).enhance(2.0), "--oem 1 --psm 6"),
                ("flatten_psm6", flatten, "--oem 1 --psm 6"),
                ("flatten_sharp_psm6", flatten_sharp, "--oem 1 --psm 6"),
                ("adaptive_psm6", adaptive, "--oem 1 --psm 6"),
                ("thr_dark156_psm6", thr_dark_156, "--oem 1 --psm 6"),
                ("thr_dark172_psm6", thr_dark_172, "--oem 1 --psm 6"),
                ("thr_light140_psm6", thr_light_140, "--oem 1 --psm 6"),
                ("thr_light156_psm6", thr_light_156, "--oem 1 --psm 6"),
                ("up2_soft_psm6", ImageEnhance.Contrast(up2).enhance(2.2), "--oem 1 --psm 6"),
                ("up2_sharp_psm6", ImageEnhance.Contrast(up2_sharp).enhance(2.0), "--oem 1 --psm 6"),
                ("up2_flatten_psm6", up2_flatten, "--oem 1 --psm 6"),
            ]

        variants: list[tuple[str, Image.Image, str]] = _build_ocr_variants(gray)
        if fast_mode:
            preferred_fast_order = [
                "soft_psm6",
                "flatten_psm6",
                "adaptive_psm6",
                "thr_dark156_psm6",
                "thr_light140_psm6",
                "up2_soft_psm6",
            ]
            by_name = {name: (name, img, cfg) for name, img, cfg in variants}
            variants = [by_name[name] for name in preferred_fast_order if name in by_name]

        def _extract_candidates(line_items_src: list[dict[str, Any]]) -> list[tuple[float, str]]:
            if not line_items_src:
                return []
            line_items_src.sort(key=lambda x: x["top"])
            heights = [float(x["height"]) for x in line_items_src if x["height"] > 0]
            median_h = max(14.0, _median_value(heights))
            gap_threshold = max(18.0, median_h * 1.25)

            blocks: list[list[dict[str, Any]]] = []
            cur: list[dict[str, Any]] = []
            prev_bottom: Optional[int] = None
            for line in line_items_src:
                if not cur:
                    cur = [line]
                    prev_bottom = line["bottom"]
                    continue
                gap = int(line["top"]) - int(prev_bottom or line["top"])
                if gap > gap_threshold:
                    blocks.append(cur)
                    cur = [line]
                else:
                    cur.append(line)
                prev_bottom = line["bottom"]
            if cur:
                blocks.append(cur)

            profile = _detect_ocr_profile(width, height, line_items_src)
            candidate_phrases_local: list[tuple[float, str]] = []
            for block in blocks:
                block_text_lines = [str(x["text"]).strip() for x in block if str(x["text"]).strip()]
                if not block_text_lines:
                    continue
                block_top = min(x["top"] for x in block)
                block_bottom = max(x["bottom"] for x in block)
                block_mid_x = sum(x["center_x"] for x in block) / max(1, len(block))
                line_count = len(block_text_lines)

                merged_lines = _merge_broken_quote_lines(block_text_lines)
                merged_lines = [re.sub(r"\s+", " ", x).strip() for x in merged_lines if x.strip()]
                if not merged_lines:
                    continue

                built: list[str] = []
                i = 0
                while i < len(merged_lines):
                    current = merged_lines[i]
                    if i + 1 < len(merged_lines):
                        nxt = merged_lines[i + 1]
                        if _is_probable_author_line(nxt) and len(current) >= 28:
                            built.append(f"{current.rstrip(' .')}. {nxt}")
                            i += 2
                            continue
                    built.append(current)
                    i += 1

                for phrase in built:
                    phrase = _normalize_ocr_phrase_case(_strip_ocr_date_prefix(phrase))
                    phrase = _repair_common_ocr_omissions(phrase)
                    phrase = _normalize_ocr_punctuation(phrase).strip(" .,:;-\t")
                    if not phrase:
                        continue
                    phrase = _attach_author_from_nearby_lines(
                        phrase=phrase,
                        block_bottom=block_bottom,
                        block_mid_x=block_mid_x,
                        line_items=line_items_src,
                        width=width,
                        height=height,
                    )
                    if looks_like_noise_phrase(phrase) or _is_ocr_noise_line(phrase):
                        continue
                    if len(re.findall(r"[A-Za-zА-Яа-яЁё]", phrase)) < 10:
                        continue
                    score = _score_ocr_block_candidate(
                        phrase=phrase,
                        block_top=block_top,
                        block_bottom=block_bottom,
                        block_mid_x=block_mid_x,
                        width=width,
                        height=height,
                        line_count=line_count,
                        profile=profile,
                    )
                    if score < 0.5:
                        continue
                    candidate_phrases_local.append((score, phrase))
            return candidate_phrases_local

        line_items: list[dict[str, Any]] = []
        candidate_phrases: list[tuple[float, str]] = []
        best_variant_score = -10_000.0
        for v_name, prepared, cfg in variants:
            data = _tesseract_image_to_data_safe(
                prepared,
                lang="rus+eng",
                config=cfg,
            )
            if not data or not data.get("text"):
                continue
            cand_lines = _collect_line_items(data)
            if not cand_lines:
                continue
            cand_phrases = _extract_candidates(list(cand_lines))
            top_scores = sorted((s for s, _ in cand_phrases), reverse=True)[:3]
            variant_score = sum(top_scores) + (0.2 * len(cand_phrases))
            # Fallback bias: if phrases empty, still keep strongest readable text pass.
            if not cand_phrases:
                quality = sum(len(re.findall(r"[A-Za-zА-Яа-яЁё]", str(x.get("text") or ""))) for x in cand_lines) / max(1, len(cand_lines))
                variant_score = quality * 0.05
            if variant_score > best_variant_score:
                best_variant_score = variant_score
                line_items = cand_lines
                candidate_phrases = cand_phrases
            if fast_mode and cand_phrases:
                top_score = max((s for s, _p in cand_phrases), default=0.0)
                if top_score >= 9.5:
                    logger.info("ocr_fast_early_stop variant=%s top_score=%.2f", v_name, top_score)
                    break

        if not line_items:
            raw_text = local_ocr_extract_text_from_image(image_url)
            return raw_text, []

        profile_final = _detect_ocr_profile(width, height, line_items)
        # For book pages prioritize the center quote extractor to avoid picking body text from top paragraphs.
        book_phrase, _book_author, book_band_found, book_quote_score = _extract_center_quote_book_style(gray)
        app_phrase, _app_author, app_quote_score = _extract_center_quote_app_style(gray)
        top_candidate_score = max((float(s) for s, _p in candidate_phrases), default=-999.0)
        top_candidate_phrase = ""
        if candidate_phrases:
            top_candidate_phrase = max(candidate_phrases, key=lambda x: x[0])[1]
        top_candidate_is_noisy = bool(
            top_candidate_phrase
            and (
                _is_ocr_noise_line(top_candidate_phrase)
                or len(re.findall(r"[A-Za-zА-Яа-яЁё]", top_candidate_phrase)) < 18
                or _contains_scaffold_markers(top_candidate_phrase)
            )
        )
        # If a quote strip is detected and quote extraction from this strip is confident,
        # prefer it over generic OCR blocks (generic often grabs upper body text).
        if (
            profile_final in {"app_screenshot", "social_card"}
            and app_phrase
            and app_quote_score >= 4.8
            and _is_plausible_quote_text(app_phrase)
            and not looks_like_noise_phrase(app_phrase)
            and len(re.findall(r"[A-Za-zА-Яа-яЁё]", app_phrase)) >= 18
            and (top_candidate_score < 9.0 or top_candidate_is_noisy)
        ):
            raw_text = "\n".join([x["text"] for x in line_items]).strip()
            return raw_text, [app_phrase]

        if (
            book_phrase
            and book_band_found
            and book_quote_score >= 4.8
            and _is_plausible_quote_text(book_phrase)
            and not looks_like_noise_phrase(book_phrase)
            and len(re.findall(r"[A-Za-zА-Яа-яЁё]", book_phrase)) >= 20
            and (top_candidate_score < 9.0 or top_candidate_is_noisy)
        ):
            raw_text = "\n".join([x["text"] for x in line_items]).strip()
            return raw_text, [book_phrase]
        if (
            profile_final == "book_page"
            and book_phrase
            and _is_plausible_quote_text(book_phrase)
            and not looks_like_noise_phrase(book_phrase)
            and not _is_book_scaffold_line(book_phrase)
            and len(re.findall(r"[A-Za-zА-Яа-яЁё]", book_phrase)) >= 22
        ):
            raw_text = "\n".join([x["text"] for x in line_items]).strip()
            return raw_text, [book_phrase]

        # Fallback for single quote pages in any profile.
        if (
            not candidate_phrases
            and book_phrase
            and _is_plausible_quote_text(book_phrase)
            and not looks_like_noise_phrase(book_phrase)
        ):
            raw_text = "\n".join([x["text"] for x in line_items]).strip()
            return raw_text, [book_phrase]

        # Fallback to plain parser if profile-scoring gave nothing.
        if not candidate_phrases:
            raw_text = "\n".join([x["text"] for x in line_items]).strip()
            fallback = [p for p in extract_phrases_from_ocr_text(raw_text) if not looks_like_noise_phrase(p)]
            return raw_text, fallback[:6]

        # Deduplicate by text, keep highest score and sort by confidence/centrality.
        best: dict[str, float] = {}
        for score, phrase in candidate_phrases:
            key = phrase.lower()
            if key not in best or score > best[key]:
                best[key] = score
        ranked = sorted(((s, k) for k, s in best.items()), key=lambda x: x[0], reverse=True)

        out: list[str] = []
        seen: set[str] = set()
        key_to_phrase = {p.lower(): p for _, p in candidate_phrases}
        for _, key in ranked:
            if key in seen:
                continue
            seen.add(key)
            phrase = key_to_phrase.get(key, key)
            out.append(phrase)
            if len(out) >= 8:
                break

        raw_text = "\n".join([x["text"] for x in line_items]).strip()
        return raw_text, out
    except Exception:
        logger.exception("local_ocr_phrase_extract_failed")
        return local_ocr_extract_text_from_image(image_url), []


def openrouter_generate_image(post_id: int, prompt: str, size: str = "1024x1024") -> dict[str, Any]:
    started = time.time()
    model = runtime_openrouter_image_model()
    status = "ok"
    error = None
    raw = {}
    usage = {}
    image_url = None
    image_bytes: Optional[bytes] = None
    image_mime = "image/png"
    try:
        # Flux-like models should request image-only modality.
        is_flux_like = "flux" in model.lower() or model.lower().startswith("black-forest-labs/")
        modalities = ["image"] if is_flux_like else ["image", "text"]
        raw = openrouter_chat(
            model,
            [{"role": "user", "content": prompt}],
            extra_body={
                "modalities": modalities,
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
                    if item.get("type") in {"output_image", "image"}:
                        b64 = item.get("b64_json") or item.get("base64") or item.get("data")
                        if isinstance(b64, str) and b64.strip():
                            try:
                                image_bytes = base64.b64decode(b64, validate=False)
                                image_mime = "image/png"
                                break
                            except Exception:
                                pass
        elif isinstance(content, str):
            # Some providers return a plain text URL in message content.
            m = re.search(r"https?://[^\s)>\]\"]+", content)
            if m:
                image_url = m.group(0)
            else:
                # Optional data URL payload.
                dm = re.search(r"data:image/(png|jpeg|jpg|webp);base64,([A-Za-z0-9+/=]+)", content)
                if dm:
                    try:
                        image_bytes = base64.b64decode(dm.group(2), validate=False)
                        mime_map = {"png": "image/png", "jpeg": "image/jpeg", "jpg": "image/jpeg", "webp": "image/webp"}
                        image_mime = mime_map.get(dm.group(1).lower(), "image/png")
                    except Exception:
                        pass
        if not image_url:
            image_url = raw.get("image_url") or raw.get("output", {}).get("image_url")
        if not image_url:
            # Alternate normalized shapes.
            msg_images = msg.get("images") if isinstance(msg, dict) else None
            if isinstance(msg_images, list) and msg_images:
                first = msg_images[0]
                if isinstance(first, dict):
                    image_url = first.get("url")
                    if not image_url and isinstance(first.get("image_url"), dict):
                        image_url = first["image_url"].get("url")
                    b64 = first.get("b64_json") or first.get("base64") or first.get("data")
                    if not image_url and isinstance(b64, str):
                        try:
                            image_bytes = base64.b64decode(b64, validate=False)
                            image_mime = "image/png"
                        except Exception:
                            pass
                elif isinstance(first, str) and first.startswith("http"):
                    image_url = first
        if not image_url and isinstance(raw.get("data"), list) and raw["data"]:
            first = raw["data"][0]
            if isinstance(first, dict):
                image_url = first.get("url")
                b64 = first.get("b64_json") or first.get("base64")
                if not image_url and isinstance(b64, str):
                    try:
                        image_bytes = base64.b64decode(b64, validate=False)
                        image_mime = "image/png"
                    except Exception:
                        pass
        if not image_url and image_bytes:
            normalized_bytes, normalized_ctype = normalize_image_to_square_1024(image_bytes, image_mime)
            key = f"original/{datetime.now(MOSCOW_TZ).strftime('%Y/%m/%d')}/or_img_{post_id}_{int(time.time()*1000)}_{secrets.token_hex(4)}.jpg"
            image_url = storage_put_bytes(key, normalized_bytes, normalized_ctype)
        if image_url and isinstance(image_url, str) and image_url.startswith("data:image/"):
            dm = re.match(r"^data:image/(png|jpeg|jpg|webp);base64,([A-Za-z0-9+/=]+)$", image_url.strip())
            if dm:
                try:
                    raw_bytes = base64.b64decode(dm.group(2), validate=False)
                    mime_map = {"png": "image/png", "jpeg": "image/jpeg", "jpg": "image/jpeg", "webp": "image/webp"}
                    detected_mime = mime_map.get(dm.group(1).lower(), "image/png")
                    normalized_bytes, normalized_ctype = normalize_image_to_square_1024(raw_bytes, detected_mime)
                    key = f"original/{datetime.now(MOSCOW_TZ).strftime('%Y/%m/%d')}/or_img_{post_id}_{int(time.time()*1000)}_{secrets.token_hex(4)}.jpg"
                    image_url = storage_put_bytes(key, normalized_bytes, normalized_ctype)
                except Exception:
                    pass
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
        ocr_text, _, _ = extract_phrases_from_image(url)
        return "url_image", ocr_text
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
        r"^подробнее$",
        r"^узнать больше$",
        r"^вопрос сессии$",
        r"^ежедневное вдохновение$",
        r"^привет[, ]",
        r"^\d{1,2}:\d{2}$",
        r"^\d{1,2}$",
        r"^\d{1,2}\s+[а-я]{3,}\.?\s*\d{4}\s*г?\.?$",
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
        if _is_book_scaffold_line(c):
            continue
        c = _normalize_ocr_phrase_case(c)
        c = _strip_ocr_date_prefix(c)
        if not c:
            continue
        low = c.lower()
        if low in seen:
            continue
        # Drop leftovers if response is still meta text.
        if "extracted text" in low or "visible text" in low:
            continue
        if low in {"поделиться", "подробнее", "узнать больше", "вопрос сессии", "ежедневное вдохновение"}:
            continue
        if _is_ocr_noise_line(c):
            continue
        if c.endswith(":"):
            continue
        seen.add(low)
        phrases.append(c)
    phrases = _merge_broken_quote_lines(phrases)
    # Merge "quote + author" into one phrase, e.g. "Текст цитаты (Пема Чёдрон)".
    merged: list[str] = []
    i = 0
    while i < len(phrases):
        current = phrases[i]
        if i + 1 < len(phrases):
            nxt = phrases[i + 1]
            if _is_probable_author_line(nxt) and len(current) >= 28:
                quote = current.rstrip(" .")
                merged.append(f"{quote}. {nxt}")
                i += 2
                continue
        merged.append(current)
        i += 1
    return merged


def _normalize_ocr_phrase_case(text: str) -> str:
    # Fix all-caps fragments in the middle of phrase (e.g. "ВНОВЬ").
    out: list[str] = []
    for token in text.split():
        m = re.match(r"^([\"'«(]*)(.*?)([\"'».),!?;:]*)$", token)
        if not m:
            out.append(token)
            continue
        prefix, core, suffix = m.groups()
        if core and any("А" <= ch <= "я" or ch in "Ёё" for ch in core):
            if core.isupper() and len(core) >= 3:
                core = core.lower()
        out.append(f"{prefix}{core}{suffix}")
    return " ".join(out).strip()


def _strip_ocr_date_prefix(text: str) -> str:
    s = (text or "").strip()
    if not s:
        return s
    # Remove leading OCR symbols before calendar fragments.
    s = re.sub(r"^[^\wА-Яа-яЁё]{1,4}\s*", "", s)
    # "° января 2026 ..." / "o января 2026 ..." -> remove
    s = re.sub(
        r"^\s*(?:[°ºoO]\s*)?(?:\d{1,2}\s+)?"
        r"(?:январ[ья]|феврал[ья]|март[а]?|апрел[ья]|ма[йя]|июн[ья]|июл[ья]|август[а]?|сентябр[ья]|октябр[ья]|ноябр[ья]|декабр[ья])"
        r"\s+(?:19|20)\d{2}\s*г?\.?\s+",
        "",
        s,
        flags=re.IGNORECASE,
    )
    # "нояб. 2024г Не торопитесь ..." -> "Не торопитесь ..."
    # "30 дек. 2024г. Не торопитесь ..." -> "Не торопитесь ..."
    s = re.sub(
        r"^\s*(?:\d{1,2}\s+)?(?:янв|фев|мар|апр|май|июн|июл|авг|сен|сент|окт|ноя|нояб|дек)\.?\s+\d{4}\s*г?\.?\s+",
        "",
        s,
        flags=re.IGNORECASE,
    )
    # "нояб, 2024г ...", "nov 2024 ..."
    s = re.sub(
        r"^\s*(?:\d{1,2}\s+)?[A-Za-zА-Яа-яЁё]{3,10}[.,]?\s+(?:19|20)\d{2}\s*[A-Za-zА-Яа-яЁё]*\.?\s+",
        "",
        s,
        flags=re.IGNORECASE,
    )
    # "2024г ...", "2024 ..."
    s = re.sub(r"^\s*(?:19|20)\d{2}\s*[A-Za-zА-Яа-яЁё]*\.?\s+", "", s, flags=re.IGNORECASE)
    # "30 Не торопитесь ..." -> "Не торопитесь ..."
    s = re.sub(r"^\s*\d{1,2}\s+(?=[А-ЯЁA-Z])", "", s)
    return s.strip()


def _is_ocr_noise_line(text: str) -> bool:
    s = (text or "").strip()
    if not s:
        return True
    if _is_book_scaffold_line(s):
        return True
    # Keep short author lines like "Руми", "Лао Цзы", "Ремарк".
    if _is_probable_author_line(s):
        return False
    low = s.lower()
    if "ежедневное вдохновение" in low:
        return True
    if "подробнее" in low:
        return True
    if re.search(
        r"(что сделает сегодняшний день замечательным|положительная установка|что сегодня было сделано хорошего для других|что я смогу сделать завтра лучше|прекрасные события, которые произошли со мной сегодня)",
        low,
    ):
        return True
    # Mostly numbers / UI counters / timestamps.
    digits = sum(ch.isdigit() for ch in s)
    letters = sum(ch.isalpha() for ch in s)
    if digits >= 3 and letters <= 4:
        return True
    if re.fullmatch(r"[\d\s:./-]+", s):
        return True
    # Very short fragments (often OCR tail like "чина", "в (п").
    words = re.findall(r"[A-Za-zА-Яа-яЁё]+", s)
    if len(words) == 1 and len(words[0]) <= 5 and not s.endswith((".", "!", "?")):
        return True
    # Keep short terminal lines like "ритме." from multi-line quotes.
    if len(words) <= 2 and len(s) <= 6 and not s.endswith((".", "!", "?")):
        return True
    # UI-strip with many isolated tiny tokens: "29 30 1 2 3 4 5"
    tokens = s.split()
    if tokens and all(re.fullmatch(r"\d{1,2}", t or "") for t in tokens) and len(tokens) >= 3:
        return True
    # Drop gibberish lines with too few meaningful words (typical OCR noise on textured background).
    words = re.findall(r"[A-Za-zА-Яа-яЁё]{2,}", s)
    if len(words) >= 4:
        meaningful = sum(1 for w in words if re.search(r"[аеёиоуыэюяaeiou]", w.lower()))
        if meaningful / max(1, len(words)) < 0.35:
            return True
    return False


def _is_plausible_quote_text(text: str) -> bool:
    s = (text or "").strip()
    if not s:
        return False
    letters = re.findall(r"[A-Za-zА-Яа-яЁё]", s)
    if len(letters) < 18:
        return False
    cyr = re.findall(r"[А-Яа-яЁё]", s)
    cyr_ratio = len(cyr) / max(1, len(letters))
    if cyr_ratio < 0.62:
        return False
    non_word = re.findall(r"[^A-Za-zА-Яа-яЁё0-9\s.,:;!?()\"'«»\-]", s)
    if len(non_word) > max(4, int(len(s) * 0.06)):
        return False
    words = re.findall(r"[A-Za-zА-Яа-яЁё-]+", s)
    if len(words) < 4:
        return False
    short_words = sum(1 for w in words if len(w) <= 1)
    if short_words >= max(4, int(len(words) * 0.4)):
        return False
    meaningful = 0
    for w in words:
        low = w.lower()
        if re.search(r"[аеёиоуыэюяaeiou]", low):
            meaningful += 1
    if meaningful / max(1, len(words)) < 0.52:
        return False
    return True


def _merge_broken_quote_lines(lines: list[str]) -> list[str]:
    if not lines:
        return lines
    merged: list[str] = []
    i = 0
    while i < len(lines):
        cur = (lines[i] or "").strip()
        if not cur:
            i += 1
            continue
        # If current line is not terminal punctuation, try to join with next line.
        while i + 1 < len(lines):
            nxt = (lines[i + 1] or "").strip()
            if not nxt or _is_probable_author_line(nxt) or _is_ocr_noise_line(nxt):
                break
            if cur.endswith((".", "!", "?")):
                break
            if len(cur) >= 180:
                break
            cur = f"{cur.rstrip(' ,;:')} {nxt.lstrip(' ,;:')}".strip()
            i += 1
            if cur.endswith((".", "!", "?")):
                break
        merged.append(cur)
        i += 1
    return merged


def _is_probable_author_line(text: str) -> bool:
    s = (text or "").strip()
    if not s:
        return False
    if len(s) > 42:
        return False
    if re.search(r"\d", s):
        return False
    if looks_like_noise_phrase(s):
        return False
    words = re.findall(r"[A-Za-zА-Яа-яЁё'\\-]+", s)
    if not (1 <= len(words) <= 4):
        return False
    # Typical author line looks like a short proper name.
    capitalized = sum(1 for w in words if w and w[0].isupper())
    return capitalized >= max(1, len(words) - 1)


def looks_like_noise_phrase(text: str) -> bool:
    s = (text or "").strip().lower()
    if not s:
        return True
    noise_patterns = [
        r"^ежедневное вдохновение$",
        r"^привет[, ]",
        r"^\d{1,2}:\d{2}$",
        r"^подробнее$",
        r"^узнать больше$",
        r"^\d{1,2}$",
        r"^\d{1,2}\s+[а-я]{3,}\.?\s*\d{4}\s*г?\.?$",
    ]
    return any(re.match(p, s) for p in noise_patterns)


def _extract_author_from_raw_ocr(ocr_text: str) -> Optional[str]:
    lines = [re.sub(r"\s+", " ", (x or "").strip()) for x in (ocr_text or "").splitlines()]
    lines = [x.strip(" .,:;-\t") for x in lines if x and x.strip()]
    for line in reversed(lines[-10:]):
        if _is_probable_author_line(line):
            return line
    return None


def _stitch_local_quote_fragments(phrases: list[str], ocr_text: str) -> list[str]:
    clean = [re.sub(r"\s+", " ", (p or "").strip()) for p in phrases if (p or "").strip()]
    if len(clean) < 2:
        if clean:
            author = _extract_author_from_raw_ocr(ocr_text)
            if author and not _is_probable_author_line(clean[0]) and author.lower() not in clean[0].lower():
                clean[0] = f"{clean[0].rstrip(' .')}. {author}"
        return clean

    # Merge common case: one quote accidentally split into 2-3 chunks.
    if 2 <= len(clean) <= 3:
        total_len = sum(len(x) for x in clean)
        first = clean[0]
        first_has_terminal = first.endswith((".", "!", "?"))
        second_is_author = _is_probable_author_line(clean[1]) if len(clean) > 1 else False
        if total_len <= 420 and (not first_has_terminal or second_is_author is False):
            merged = " ".join(x.strip(" .") for x in clean).strip()
            merged = _normalize_ocr_punctuation(_repair_common_ocr_omissions(merged))
            if merged and not merged.endswith((".", "!", "?")):
                merged = f"{merged}."
            author = _extract_author_from_raw_ocr(ocr_text)
            if author and author.lower() not in merged.lower():
                merged = f"{merged.rstrip(' .')}. {author}"
            return [merged]

    # Non-merge path: only attach author if it is clearly missing.
    author = _extract_author_from_raw_ocr(ocr_text)
    if author:
        out: list[str] = []
        for p in clean:
            if _is_probable_author_line(p):
                continue
            if author.lower() in p.lower():
                out.append(p)
            else:
                out.append(f"{p.rstrip(' .')}. {author}")
        return out
    return clean


def _clean_ocr_quote_text(text: str) -> str:
    s = (text or "").strip()
    if not s:
        return s
    # Remove obvious garbage prefix before first Cyrillic/Latin letter.
    m = re.search(r"[A-Za-zА-Яа-яЁё]", s)
    if m:
        s = s[m.start():]
    # Keep only meaningful punctuation/symbols for quote text.
    s = re.sub(r"[^0-9A-Za-zА-Яа-яЁё\s,.;:!?—\-()\"'«»]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    # Remove accidental repeated short tokens: "и и", "в в", etc.
    s = re.sub(r"\b([А-Яа-яЁёA-Za-z]{1,2})\s+\1\b", r"\1", s, flags=re.IGNORECASE)
    # Cut journal scaffold tails if OCR accidentally glued them.
    cut = re.search(
        r"\b(положител\w+\s+установ\w+|что\s+сегодня|сегодня\s+было|сдела\w+\s+хорош\w+\s+для\s+друг\w*|что\s+я\s+смог\w+\s+сделать\s+завтра|прекрасн\w+\s+событ\w+)",
        s,
        flags=re.IGNORECASE,
    )
    if cut and cut.start() > 20:
        s = s[: cut.start()].strip(" .,:;-\t")
    # Missing sentence boundary between chunks, e.g. "... момента Ждать ..."
    s = re.sub(r"([а-яё])\s+(Ждать|Мы|И|А|Но)\b", r"\1. \2", s)
    # Common punctuation misses.
    s = re.sub(r"\b(перемены)\s+(которые)\b", r"\1, \2", s, flags=re.IGNORECASE)
    # Common OCR drop in known phrase pattern.
    s = re.sub(r"\b(поступков)\s+и\s+(вначале)\b", r"\1 и идей \2", s, flags=re.IGNORECASE)
    s = _normalize_ocr_punctuation(_repair_common_ocr_omissions(s))
    if s and not s.endswith((".", "!", "?")):
        s += "."
    return s


def _norm_for_similarity(text: str) -> str:
    s = (text or "").lower()
    s = re.sub(r"[^a-zа-яё0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _enrich_phrase_from_bank(phrase: str) -> str:
    candidate = _clean_ocr_quote_text(phrase)
    quote_part, candidate_author = split_quote_and_author(candidate)
    candidate_quote = (quote_part or candidate).strip()
    norm = _norm_for_similarity(candidate_quote)
    if not norm or len(norm) < 24:
        return candidate
    try:
        with db() as conn:
            rows = conn.execute(
                "SELECT text_body, author FROM phrases WHERE length(text_body) >= 30 ORDER BY updated_at DESC LIMIT 1200"
            ).fetchall()
        best_text = candidate_quote
        best_author: Optional[str] = None
        best_score = 0.0
        for r in rows:
            txt = (r.get("text_body") if isinstance(r, dict) else r["text_body"]) or ""
            row_author = ((r.get("author") if isinstance(r, dict) else r["author"]) or "").strip() or None
            txt = str(txt).strip()
            if not txt:
                continue
            score = difflib.SequenceMatcher(None, norm, _norm_for_similarity(txt)).ratio()
            if score > best_score:
                best_score = score
                best_text = txt
                best_author = row_author
        if best_score >= 0.82:
            # If OCR failed to detect the author but the matched bank record has one,
            # append it so downstream split/save keeps author in a separate column.
            if not candidate_author and best_author and best_author.lower() not in best_text.lower():
                return f"{best_text.rstrip(' .')}. {best_author}"
            return best_text
    except Exception:
        logger.exception("phrase_bank_enrich_failed")
    return candidate


def extract_phrases_from_image(image_url: str, fast_mode: bool = False) -> tuple[str, list[str], str]:
    ocr_engine = runtime_ocr_primary_engine()
    if ocr_engine == "paddle":
        paddle_text, paddle_phrases = paddle_ocr_extract_phrases_from_image(image_url)
        paddle_phrases = [_enrich_phrase_from_bank(p) for p in paddle_phrases if _is_plausible_quote_text(_clean_ocr_quote_text(p))]
        if paddle_phrases:
            return paddle_text, paddle_phrases, "paddle"
        # fallback to local tesseract pipeline
        local_text, local_phrases = local_ocr_extract_phrases_from_image(image_url, fast_mode=fast_mode)
        if not local_phrases:
            local_text = local_text or local_ocr_extract_text_from_image(image_url)
            local_phrases = [p for p in extract_phrases_from_ocr_text(local_text) if not looks_like_noise_phrase(p)]
        local_phrases = _stitch_local_quote_fragments(local_phrases, local_text)
        local_phrases = [_enrich_phrase_from_bank(p) for p in local_phrases if _is_plausible_quote_text(_clean_ocr_quote_text(p))]
        if local_phrases:
            return local_text, local_phrases, "local_fallback"
        if OCR_DISABLE_LLM_FALLBACK:
            return paddle_text or local_text, [], "paddle"
        llm_quote_text = openrouter_extract_main_quote_from_image(image_url)
        llm_quote_phrases = [p for p in extract_phrases_from_ocr_text(llm_quote_text) if not looks_like_noise_phrase(p)]
        if llm_quote_phrases:
            return llm_quote_text, llm_quote_phrases, "llm_quote_fallback"
        llm_text = openrouter_extract_text_from_image(image_url)
        llm_phrases = [p for p in extract_phrases_from_ocr_text(llm_text) if not looks_like_noise_phrase(p)]
        if llm_phrases:
            return llm_text, llm_phrases, "llm_fallback"
        return paddle_text or local_text or llm_text, [], "paddle"

    if ocr_engine == "local":
        local_text, local_phrases = local_ocr_extract_phrases_from_image(image_url, fast_mode=fast_mode)
        if not local_phrases:
            local_text = local_text or local_ocr_extract_text_from_image(image_url)
            local_phrases = [p for p in extract_phrases_from_ocr_text(local_text) if not looks_like_noise_phrase(p)]
        local_phrases = _stitch_local_quote_fragments(local_phrases, local_text)
        local_phrases = [_enrich_phrase_from_bank(p) for p in local_phrases if _is_plausible_quote_text(_clean_ocr_quote_text(p))]
        if local_phrases:
            return local_text, local_phrases, "local"
        if OCR_DISABLE_LLM_FALLBACK:
            return local_text, [], "local"

        llm_text = openrouter_extract_text_from_image(image_url)
        llm_phrases = [p for p in extract_phrases_from_ocr_text(llm_text) if not looks_like_noise_phrase(p)]
        if llm_phrases:
            return llm_text, llm_phrases, "llm_fallback"

        llm_quote_text = openrouter_extract_main_quote_from_image(image_url)
        llm_quote_phrases = [p for p in extract_phrases_from_ocr_text(llm_quote_text) if not looks_like_noise_phrase(p)]
        if llm_quote_phrases:
            return llm_quote_text, llm_quote_phrases, "llm_quote_fallback"
        return local_text or llm_text, [], "local"

    # LLM-primary mode (can be enabled with OCR_PRIMARY_ENGINE=llm).
    llm_text = openrouter_extract_text_from_image(image_url)
    llm_phrases = [p for p in extract_phrases_from_ocr_text(llm_text) if not looks_like_noise_phrase(p)]
    if llm_phrases:
        return llm_text, llm_phrases, "llm"

    llm_quote_text = openrouter_extract_main_quote_from_image(image_url)
    llm_quote_phrases = [p for p in extract_phrases_from_ocr_text(llm_quote_text) if not looks_like_noise_phrase(p)]
    if llm_quote_phrases:
        return llm_quote_text, llm_quote_phrases, "llm_quote"
    if OCR_DISABLE_LLM_FALLBACK:
        return llm_text or llm_quote_text, [], "llm"

    local_text = local_ocr_extract_text_from_image(image_url)
    local_phrases = [p for p in extract_phrases_from_ocr_text(local_text) if not looks_like_noise_phrase(p)]
    local_phrases = _stitch_local_quote_fragments(local_phrases, local_text)
    local_phrases = [_enrich_phrase_from_bank(p) for p in local_phrases if p.strip()]
    if local_phrases:
        return local_text, local_phrases, "local_fallback"
    return llm_text or local_text, [], "llm"


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


def ensure_storage_ready() -> None:
    if STORAGE_MODE == "minio":
        client = minio_client()
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
    else:
        MEDIA_DIR.mkdir(parents=True, exist_ok=True)


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
    ensure_storage_ready()
    init_db()
    bootstrap_runtime_settings()
    try:
        stats = backfill_phrase_authors()
        logger.info("phrase_author_backfill scanned=%s updated=%s", stats.get("scanned"), stats.get("updated"))
    except Exception:
        logger.exception("phrase_author_backfill_failed")
    start_background_scheduler()


@app.api_route("/media/{object_path:path}", methods=["GET", "HEAD"])
def serve_media(object_path: str, request: Request) -> Response:
    key = (object_path or "").strip("/")
    if not key:
        raise HTTPException(status_code=404, detail="media key not found")
    try:
        data, ctype = storage_get_bytes(key)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="media not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"media read failed: {str(e)[:300]}")
    headers = {"Cache-Control": "public, max-age=31536000, immutable"}
    if request.method == "HEAD":
        head_headers = dict(headers)
        head_headers["Content-Length"] = str(len(data))
        return Response(content=b"", media_type=ctype, headers=head_headers)
    return Response(content=data, media_type=ctype, headers=headers)


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
    if kv_get("daily_phrase_offer_date") == today_key:
        return False

    with db() as conn:
        phrase = conn.execute(
            "SELECT id, text_body FROM phrases WHERE coalesce(is_published,0)=0 ORDER BY random() LIMIT 1"
        ).fetchone()
    if not phrase:
        try:
            send_telegram_text(
                runtime_telegram_preview_chat(),
                "09:00 — новых фраз не найдено. Добавьте фразы через /add_phrases, затем можно запустить вручную /new_post.",
            )
        except Exception:
            logger.exception("daily_phrase_offer_no_phrases_notify_failed")
        kv_set("daily_phrase_offer_date", today_key)
        return False

    phrase_id = int(phrase["id"])
    phrase_text = str(phrase["text_body"] or "").strip()
    try:
        send_telegram_text(
            runtime_telegram_preview_chat(),
            (
                "09:00 — фраза дня выбрана.\n\n"
                f"{phrase_text}\n\n"
                "Выберите действие:"
            ),
            reply_markup=build_daily_phrase_keyboard(phrase_id),
        )
        kv_set("daily_phrase_offer_date", today_key)
        kv_set("daily_phrase_offer_phrase_id", str(phrase_id))
        return True
    except Exception:
        logger.exception("daily_phrase_offer_send_failed phrase_id=%s", phrase_id)
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
        return FileResponse(
            str(index_file),
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )
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
        "telegram_mode": runtime_telegram_mode(),
    }


def _legal_page_html(title: str, content: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f5f7fb;
      --card: #ffffff;
      --text: #111827;
      --muted: #4b5563;
      --border: #e5e7eb;
      --accent: #2563eb;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      color: var(--text);
      background: linear-gradient(180deg, #eef2ff 0%, var(--bg) 24%);
    }}
    .wrap {{
      max-width: 860px;
      margin: 0 auto;
      padding: 28px 16px 48px;
    }}
    .card {{
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 24px;
      box-shadow: 0 8px 28px rgba(17, 24, 39, 0.05);
    }}
    h1 {{
      margin: 0 0 8px 0;
      font-size: 28px;
      line-height: 1.2;
    }}
    h2 {{
      margin: 22px 0 10px 0;
      font-size: 18px;
      line-height: 1.3;
    }}
    p, li {{
      color: var(--muted);
      font-size: 15px;
      line-height: 1.6;
    }}
    ul {{ margin: 8px 0 0 20px; padding: 0; }}
    .meta {{
      font-size: 13px;
      color: var(--muted);
      margin-bottom: 12px;
    }}
    a {{
      color: var(--accent);
      text-decoration: none;
    }}
    a:hover {{ text-decoration: underline; }}
  </style>
</head>
<body>
  <main class="wrap">
    <article class="card">
      {content}
    </article>
  </main>
</body>
</html>
"""


@app.get("/legal/about", response_class=HTMLResponse)
def legal_about() -> HTMLResponse:
    html = _legal_page_html(
        "Kindlysupport App Overview",
        """
        <h1>Kindlysupport Application Overview</h1>
        <div class="meta">Last updated: 2026-03-12</div>
        <p>
          Kindlysupport is a content publishing tool used to prepare and publish social media posts,
          including standard Pins to Pinterest boards selected by the account owner.
        </p>
        <h2>Application purpose</h2>
        <p>
          The application helps a creator or team automate repetitive publishing tasks:
          composing text, attaching an image, and sending the post to configured destinations.
        </p>
        <h2>Pinterest integration usage</h2>
        <ul>
          <li>Read boards to select destination board IDs.</li>
          <li>Create standard Pins with title, description, link, and image URL.</li>
          <li>No actions are performed without explicit user setup and publish request.</li>
        </ul>
        <h2>Support</h2>
        <p>
          For support requests, contact: <a href="mailto:forexel357@gmail.com">forexel357@gmail.com</a>
        </p>
        """,
    )
    return HTMLResponse(content=html)


@app.get("/about", response_class=HTMLResponse)
def legal_about_alias() -> HTMLResponse:
    return legal_about()


@app.get("/legal/privacy", response_class=HTMLResponse)
def legal_privacy() -> HTMLResponse:
    html = _legal_page_html(
        "Kindlysupport Privacy Policy",
        """
        <h1>Privacy Policy</h1>
        <div class="meta">Last updated: 2026-03-12</div>
        <p>
          This Privacy Policy describes how Kindlysupport ("we", "our", "us") handles data when you use
          the application and related integrations, including Pinterest API features.
        </p>
        <h2>What data we process</h2>
        <ul>
          <li>Account/session technical data needed to keep you signed in.</li>
          <li>Content data you provide for post creation (text, links, image URLs).</li>
          <li>Integration settings you enter (for example, access tokens and destination board IDs).</li>
          <li>Operational logs for reliability and troubleshooting.</li>
        </ul>
        <h2>How we use data</h2>
        <ul>
          <li>To create, schedule, and publish content requested by you.</li>
          <li>To connect to third-party APIs that you explicitly configure.</li>
          <li>To maintain security, prevent abuse, and diagnose failures.</li>
        </ul>
        <h2>Pinterest-specific processing</h2>
        <ul>
          <li>We use Pinterest access tokens only to perform actions you authorize.</li>
          <li>We may read boards and create Pins on boards you selected.</li>
          <li>We do not sell Pinterest account data or share tokens with advertisers.</li>
        </ul>
        <h2>Data sharing</h2>
        <p>
          We share data only with service providers and APIs required to deliver the requested functionality
          (for example, Pinterest and hosting infrastructure), or when required by law.
        </p>
        <h2>Data retention</h2>
        <p>
          We retain operational data only as long as needed for service operation, legal obligations,
          and security review. You can request deletion by contacting us.
        </p>
        <h2>Security</h2>
        <p>
          We apply reasonable technical and organizational safeguards, but no system can guarantee
          absolute security.
        </p>
        <h2>Your choices</h2>
        <ul>
          <li>You can revoke Pinterest access at any time from your Pinterest connected apps settings.</li>
          <li>You can request account or data removal by email.</li>
        </ul>
        <h2>Contact</h2>
        <p>
          Privacy requests: <a href="mailto:forexel357@gmail.com">forexel357@gmail.com</a>
        </p>
        """,
    )
    return HTMLResponse(content=html)


@app.get("/privacy-policy", response_class=HTMLResponse)
def legal_privacy_alias() -> HTMLResponse:
    return legal_privacy()


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
        "ocr": {
            "primary_engine": runtime_ocr_primary_engine(),
            "paddle_available": bool(PaddleOCR is not None and cv2 is not None and np is not None),
        },
        "openrouter_key_configured": bool(runtime_openrouter_key()),
        "db_backend": DB_BACKEND,
        "telegram": {
            "bot_configured": bool(runtime_telegram_token()),
            "admin_user_id": tg_admin_user_id(),
            "preview_chat": runtime_telegram_preview_chat(),
            "publish_chat": runtime_telegram_publish_chat(),
            "mode": runtime_telegram_mode(),
        },
        "timezone": APP_TIMEZONE,
        "instagram_configured": runtime_instagram_configured(),
        "pinterest_configured": bool(runtime_enable_pinterest() and runtime_pinterest_token() and runtime_pinterest_board_id()),
        "vk_configured": bool(runtime_enable_vk() and runtime_vk_token() and runtime_vk_group_id()),
    }


@app.get("/api/settings")
def get_settings(session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    return {
        "models_locked": LOCK_PROVIDER_MODELS,
        "ocr_primary_engine": runtime_ocr_primary_engine(),
        "openrouter_api_key": "***" if runtime_openrouter_key() else "",
        "openrouter_text_model": runtime_openrouter_text_model(),
        "openrouter_vision_model": runtime_openrouter_vision_model(),
        "openrouter_image_model": runtime_openrouter_image_model(),
        "openrouter_site_url": runtime_openrouter_site_url(),
        "openrouter_app_name": runtime_openrouter_app_name(),
        "telegram_bot_token": "***" if runtime_telegram_token() else "",
        "telegram_admin_user_id": tg_admin_user_id(),
        "telegram_preview_chat": runtime_telegram_preview_chat(),
        "telegram_publish_chat": runtime_telegram_publish_chat(),
        "telegram_webhook_secret": "***" if runtime_telegram_webhook_secret() else "",
        "telegram_mode": runtime_telegram_mode(),
        "enable_vk": runtime_enable_vk(),
        "vk_access_token": "***" if runtime_vk_token() else "",
        "vk_group_id": runtime_vk_group_id(),
        "vk_api_version": runtime_vk_version(),
        "enable_instagram": runtime_enable_instagram(),
        "instagram_delivery_mode": runtime_instagram_delivery_mode(),
        "instagram_access_token": "***" if runtime_instagram_token() else "",
        "instagram_ig_user_id": runtime_instagram_user_id(),
        "instagram_graph_version": runtime_instagram_graph_version(),
        "instagram_proxy_url": runtime_instagram_proxy_url(),
        "instagram_queue_github_token": "***" if runtime_instagram_queue_github_token() else "",
        "instagram_queue_repo": runtime_instagram_queue_repo(),
        "instagram_queue_branch": runtime_instagram_queue_branch(),
        "instagram_queue_path": runtime_instagram_queue_path(),
        "enable_pinterest": runtime_enable_pinterest(),
        "pinterest_access_token": "***" if runtime_pinterest_token() else "",
        "pinterest_board_id": runtime_pinterest_board_id(),
    }


@app.put("/api/settings")
async def update_settings(request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    allowed = {
        "ocr_primary_engine",
        "openrouter_api_key",
        "openrouter_text_model",
        "openrouter_vision_model",
        "openrouter_image_model",
        "openrouter_site_url",
        "openrouter_app_name",
        "telegram_bot_token",
        "telegram_admin_user_id",
        "telegram_preview_chat",
        "telegram_publish_chat",
        "telegram_webhook_secret",
        "telegram_mode",
        "enable_vk",
        "vk_access_token",
        "vk_group_id",
        "vk_api_version",
        "enable_instagram",
        "instagram_delivery_mode",
        "instagram_access_token",
        "instagram_ig_user_id",
        "instagram_graph_version",
        "instagram_proxy_url",
        "instagram_queue_github_token",
        "instagram_queue_repo",
        "instagram_queue_branch",
        "instagram_queue_path",
        "enable_pinterest",
        "pinterest_access_token",
        "pinterest_board_id",
    }
    updated = []
    blocked = []
    for key, value in payload.items():
        if key not in allowed:
            continue
        if LOCK_PROVIDER_MODELS and key in {"openrouter_text_model", "openrouter_vision_model", "openrouter_image_model"}:
            blocked.append(key)
            continue
        if key == "ocr_primary_engine":
            val = str(value or "").strip().lower()
            if val not in {"local", "llm", "paddle"}:
                raise HTTPException(status_code=400, detail="ocr_primary_engine must be local|llm|paddle")
            setting_set("ocr_primary_engine", val)
            updated.append(key)
            continue
        if key == "telegram_mode":
            val = str(value or "").strip().lower()
            if val not in {"webhook", "polling"}:
                raise HTTPException(status_code=400, detail="telegram_mode must be webhook|polling")
            setting_set("telegram_mode", val)
            updated.append(key)
            continue
        if key == "telegram_admin_user_id":
            if str(value).strip():
                setting_set("telegram_admin_user_id", str(int(value)))
            else:
                setting_set("telegram_admin_user_id", "")
            updated.append(key)
            continue
        if key == "instagram_delivery_mode":
            val = str(value or "").strip().lower()
            if val not in {"direct", "external_queue"}:
                raise HTTPException(status_code=400, detail="instagram_delivery_mode must be direct|external_queue")
            setting_set("instagram_delivery_mode", val)
            updated.append(key)
            continue
        if key.startswith("enable_"):
            setting_set(key, "1" if bool(value) else "0")
        else:
            setting_set(key, str(value or "").strip())
        updated.append(key)
    if LOCK_PROVIDER_MODELS:
        # Keep persistent settings aligned with the hard lock to avoid confusion in DB snapshots.
        setting_set("openrouter_text_model", LOCKED_OPENROUTER_TEXT_MODEL)
        setting_set("openrouter_vision_model", LOCKED_OPENROUTER_VISION_MODEL)
        setting_set("openrouter_image_model", LOCKED_OPENROUTER_IMAGE_MODEL)
    return {"ok": True, "updated": updated, "blocked": blocked, "models_locked": LOCK_PROVIDER_MODELS}


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
            "Проанализируй философскую фразу и предложи 5 разных визуальных метафор, "
            "которые можно превратить в изображение для психологического канала.\n\n"
            f"Фраза:\n«{post['title']}»\n\n"
            "Требования:\n"
            "- сцены должны быть простыми и символическими\n"
            "- изображение должно быть понятно без текста\n"
            "- стиль — реалистичный или кинематографичный\n"
            "- избегать банальных клише\n"
            "- без UI, логотипов и надписей\n\n"
            "Для каждого сценария опиши: композицию, символ, настроение сцены.\n"
            "Ответ строго JSON-массивом из 5 строк, без пояснений."
        )
        raw = openrouter_generate_text(prompt, temperature=0.9, top_p=0.95)
        try:
            parsed = json.loads(raw)
            scenarios = [str(x).strip() for x in parsed if str(x).strip()]
        except Exception:
            # fallback if model responds with prose
            lines = [x.strip(" -•\t") for x in raw.splitlines() if x.strip()]
            scenarios = [x for x in lines[:5] if x]
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
    text_idea = (payload.get("text_idea") or "").strip()
    scenario_idea = (payload.get("scenario_idea") or "").strip()
    if text_idea and post.get("source_kind") == "phrase":
        try:
            new_text = expand_phrase_text(post["title"], instruction=text_idea, previous_text=post.get("text_body") or "")
            if new_text:
                update_post(post_id, text_body=new_text)
                post = fetch_post(post_id)
        except Exception:
            logger.exception("preview_text_idea_failed post_id=%s", post_id)
    if not scenario:
        scenario = generate_image_scenario(post["title"], post["text_body"], scenario_idea)
    regen_instruction = (payload.get("regen_instruction") or "").strip()
    caption = generate_caption(post["title"], post["text_body"])
    if post.get("source_kind") == "phrase":
        caption = generate_post_caption_plain(post)
    image_prompt = generate_detailed_image_prompt(post["title"], post["text_body"], scenario, regen_instruction)

    image_url = None
    prev_image_url = post.get("final_image_url")
    original_image_url = None
    image_error = None
    try:
        result = openrouter_generate_image(post_id=post_id, prompt=image_prompt, size="1024x1024")
        image_url = result["image_url"]
        try:
            existing_media_key = media_key_from_url(str(image_url))
            if existing_media_key:
                # Already stored in our media bucket.
                original_image_url = str(image_url)
            else:
                original_bytes, original_ctype = download_remote_image(image_url)
                normalized_bytes, normalized_ctype = normalize_image_to_square_1024(original_bytes, original_ctype)
                original_key = f"original/{datetime.now(MOSCOW_TZ).strftime('%Y/%m/%d')}/post_{post_id}_{int(time.time()*1000)}_{secrets.token_hex(4)}.jpg"
                original_image_url = storage_put_bytes(original_key, normalized_bytes, normalized_ctype)
                image_url = original_image_url
        except Exception as e:
            image_error = f"{image_error or ''} | original_store_error: {str(e)}".strip(" |")
    except HTTPException as e:
        image_error = e.detail
    except Exception as e:
        image_error = str(e)

    # Phrase posts always get final local rendered card with dark overlay and title text.
    if post.get("source_kind") == "phrase":
        try:
            image_url = render_phrase_card_image(post["title"], image_url)
        except Exception as e:
            image_error = f"{image_error or ''} | local_render_error: {str(e)}".strip(" |")

    # For phrase content we do not allow text-only preview: image is mandatory.
    if post.get("source_kind") == "phrase" and not (image_url or prev_image_url):
        raise HTTPException(status_code=502, detail=f"image generation failed: {image_error or 'no generated background image'}")

    base_image_url = original_image_url or image_url

    preview = {
        "chat": runtime_telegram_preview_chat(),
        "buttons": ["Опубликовать", "Перегенерировать", "Отмена"],
        "publish_options": ["Сейчас", "В указанное время и дату", "Заменить фразу"],
        "regenerate_options": ["Текст", "Картинку", "И то и то"],
        "note": "При перегенерации админ присылает текст-инструкцию, он учитывается в prompt изображения.",
        "image_error": image_error,
        "original_image_url": original_image_url,
        "base_image_url": base_image_url,
    }
    update_post(
        post_id,
        status="preview_ready",
        selected_scenario=scenario,
        image_prompt=image_prompt,
        final_image_url=image_url or prev_image_url,
        telegram_caption=caption,
        preview_payload_json=preview,
        last_regen_instruction=regen_instruction or None,
    )
    post = fetch_post(post_id)
    tg_send = None
    tg_error = None
    try:
        old_mid = get_preview_message_id_for_chat(post, runtime_telegram_preview_chat())
        tg_send = telegram_send_preview(post)
        result = (tg_send or {}).get("result") or {}
        if result.get("message_id") is not None:
            new_mid = str(result["message_id"])
            set_preview_message_id_for_chat(post_id, post, runtime_telegram_preview_chat(), new_mid)
            if old_mid and old_mid != new_mid:
                telegram_delete_message(runtime_telegram_preview_chat(), old_mid)
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
    instr_parts = split_regen_instruction(instruction)
    text_instruction = instr_parts.get("text") or instr_parts.get("common") or instruction
    scenario_instruction = instr_parts.get("scenario") or instr_parts.get("common") or instruction
    progress_chat_id = payload.get("progress_chat_id")
    progress_enabled = bool(payload.get("progress"))

    def _progress(message: str) -> None:
        if progress_enabled and progress_chat_id:
            try:
                send_telegram_text(progress_chat_id, message, track_post_id=post_id)
            except Exception:
                logger.exception("telegram_progress_send_failed post_id=%s message=%s", post_id, message)

    post = fetch_post(post_id)
    if target not in ("text", "image", "both"):
        raise HTTPException(status_code=400, detail="target must be text|image|both")
    title = post["title"]
    text_body = post["text_body"]
    scenario = post.get("selected_scenario") or generate_image_scenario(title, text_body)
    prev_image_url = post.get("final_image_url")
    preview = post.get("preview_payload") or {}
    base_image_url = (preview.get("base_image_url") or preview.get("original_image_url") or prev_image_url)

    if target == "both":
        total_steps = 6
        step_ix = {
            "text": 1,
            "prompt": 2,
            "image": 3,
            "prep": 4,
            "darken": 5,
            "overlay": 6,
        }
    elif target == "image":
        total_steps = 5
        step_ix = {
            "prompt": 1,
            "image": 2,
            "prep": 3,
            "darken": 4,
            "overlay": 5,
        }
    else:  # text
        total_steps = 4
        step_ix = {
            "text": 1,
            "prep": 2,
            "darken": 3,
            "overlay": 4,
        }

    def _stage(key: str, text: str) -> None:
        idx = step_ix.get(key)
        if idx is None:
            _progress(text)
            return
        _progress(f"{idx}/{total_steps} {text}")

    def _render_progress(message: str) -> None:
        msg = (message or "").lower()
        if "обрезк" in msg or "подготов" in msg:
            _stage("prep", "Этап обрезки и подготовки картинки...")
            return
        if "затемнен" in msg:
            _stage("darken", "Этап наложения затемнения...")
            return
        if "наложени" in msg and "текст" in msg:
            _stage("overlay", "Этап наложения текста на картинку...")
            return
        _progress(message)

    if target in ("text", "both"):
        _stage("text", "Этап генерации текста...")
        try:
            generated = expand_phrase_text(
                title,
                instruction=text_instruction,
                previous_text=post.get("text_body") or "",
            )
            text_body = (generated or "").strip() or text_body
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"text generation failed: {str(e)}")
        # Rebuild scenario from fresh text for consistency in subsequent image regenerations.
        try:
            scenario = generate_image_scenario(title, text_body, scenario_instruction or "")
        except Exception:
            logger.exception("scenario_regen_failed post_id=%s", post_id)
        update_post(post_id, text_body=text_body)
        if scenario:
            update_post(post_id, selected_scenario=scenario)
        # Text-only regeneration must not touch the image pipeline.
        if target == "text":
            update_post(
                post_id,
                preview_payload_json=preview,
                last_regen_instruction=instruction or None,
            )
    if target in ("image", "both"):
        should_regen_bg = should_regenerate_background_from_instruction(scenario_instruction)
        if should_regen_bg:
            scenario = generate_image_scenario(title, text_body, scenario_instruction or "")
            _stage("prompt", "Этап генерации промпта для картинки...")
            image_prompt = generate_detailed_image_prompt(title, text_body, scenario, scenario_instruction)
            _stage("image", "Этап генерации картинки...")
        else:
            _progress("Инструкция без явного запроса на новый фон - использую текущее фото.")
            image_prompt = post.get("image_prompt") or ""
        image_url = None
        original_image_url = None
        image_error = None
        if should_regen_bg:
            try:
                result = openrouter_generate_image(post_id=post_id, prompt=image_prompt, size="1024x1024")
                image_url = result["image_url"]
                try:
                    existing_media_key = media_key_from_url(str(image_url))
                    if existing_media_key:
                        # Already stored in our media bucket.
                        original_image_url = str(image_url)
                    else:
                        original_bytes, original_ctype = download_remote_image(image_url)
                        normalized_bytes, normalized_ctype = normalize_image_to_square_1024(original_bytes, original_ctype)
                        original_key = f"original/{datetime.now(MOSCOW_TZ).strftime('%Y/%m/%d')}/post_{post_id}_{int(time.time()*1000)}_{secrets.token_hex(4)}.jpg"
                        original_image_url = storage_put_bytes(original_key, normalized_bytes, normalized_ctype)
                        image_url = original_image_url
                except Exception as e:
                    image_error = f"{image_error or ''} | original_store_error: {str(e)}".strip(" |")
            except HTTPException as e:
                image_error = e.detail
            except Exception as e:
                image_error = str(e)
        else:
            image_url = base_image_url or prev_image_url
            original_image_url = preview.get("original_image_url") or image_url
        if post.get("source_kind") == "phrase":
            try:
                image_url = render_phrase_card_image(
                    title,
                    image_url,
                    progress_cb=_render_progress if progress_enabled else None,
                )
            except Exception as e:
                image_error = f"{image_error or ''} | local_render_error: {str(e)}".strip(" |")
        if should_regen_bg and not image_url and post.get("source_kind") == "phrase":
            # Graceful fallback: if provider didn't return a fresh image, keep previous base background.
            fallback_bg = base_image_url or prev_image_url
            if fallback_bg:
                _progress("Новая картинка не получена, использую предыдущий фон для превью.")
                try:
                    image_url = render_phrase_card_image(
                        title,
                        str(fallback_bg),
                        progress_cb=_render_progress if progress_enabled else None,
                    )
                except Exception as e:
                    image_error = f"{image_error or ''} | fallback_render_error: {str(e)}".strip(" |")
        if not image_url:
            raise HTTPException(status_code=502, detail=f"image generation failed: {image_error or 'unknown error'}")
        preview["image_error"] = image_error
        preview["original_image_url"] = original_image_url
        preview["base_image_url"] = original_image_url or image_url or base_image_url
        update_post(
            post_id,
            image_prompt=image_prompt,
            final_image_url=image_url or prev_image_url,
            preview_payload_json=preview,
            last_regen_instruction=instruction or None,
        )

    latest = fetch_post(post_id)
    update_post(post_id, telegram_caption=generate_post_caption_plain(latest))
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
    ig_res = None
    ig_err = None
    try:
        tg_res = telegram_send_publish(post)
    except HTTPException as e:
        tg_err = e.detail
    except Exception as e:
        tg_err = str(e)
    if runtime_enable_instagram():
        try:
            ig_res = instagram_publish_or_enqueue(post)
        except HTTPException as e:
            ig_err = e.detail
        except Exception as e:
            ig_err = str(e)
    preview = post.get("preview_payload") or {}
    preview["published"] = {
        "mode": "now",
        "at": now_iso(),
        "telegram": bool(tg_res),
        "telegram_error": tg_err,
        "instagram": bool(ig_res),
        "instagram_error": ig_err,
        "instagram_result": ig_res,
    }
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
    if runtime_telegram_mode() == "webhook" and not runtime_telegram_webhook_secret():
        missing.append("TELEGRAM_WEBHOOK_SECRET")
    if runtime_enable_instagram() and not runtime_instagram_configured():
        missing.extend([x for x in runtime_instagram_needs() if x not in missing])
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
            "delivery_mode": runtime_instagram_delivery_mode(),
            "configured": runtime_instagram_configured(),
            "needs": runtime_instagram_needs(),
            "proxy_url": runtime_instagram_proxy_url(),
            "queue_repo": runtime_instagram_queue_repo(),
            "queue_branch": runtime_instagram_queue_branch(),
            "queue_path": runtime_instagram_queue_path(),
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
    res = instagram_publish_or_enqueue(post)
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
                result["targets"]["instagram"] = {"ok": True, "result": instagram_publish_or_enqueue(post)}
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
        where.append("(lower(text_body) LIKE ? OR lower(coalesce(author,'')) LIKE ?)")
        like_q = f"%{search.strip().lower()}%"
        params.append(like_q)
        params.append(like_q)
    if topic.strip():
        where.append("lower(coalesce(topic,'')) LIKE ?")
        params.append(f"%{topic.strip().lower()}%")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    with db() as conn:
        rows = conn.execute(
            f"SELECT id, text_body, author, topic, is_published, created_at, updated_at FROM phrases {where_sql} ORDER BY id ASC LIMIT ? OFFSET ?",
            (*params, limit, offset),
        ).fetchall()
    return [dict(r) for r in rows]


@app.post("/api/phrases/backfill-authors")
def backfill_phrases_authors_endpoint(
    session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE),
) -> dict[str, Any]:
    ensure_auth(session_id)
    stats = backfill_phrase_authors()
    return {"ok": True, **stats}


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
        quote_text, author = split_quote_and_author(phrase)
        if not quote_text.strip():
            skipped += 1
            continue
        parsed.append((int(flag), quote_text.strip(), (author or "").strip() or None))

    inserted = 0
    updated = 0
    duplicates = 0
    now = now_iso()
    with db() as conn:
        for is_pub, phrase, author in parsed:
            sim, _score = find_similar_phrase_in_db(phrase, conn=conn)
            if sim:
                duplicates += 1
                continue
            existing = conn.execute("SELECT id, is_published, author FROM phrases WHERE text_body = ?", (phrase,)).fetchone()
            if existing:
                current_author = (existing.get("author") if isinstance(existing, dict) else existing["author"]) or None
                if int(existing["is_published"]) != is_pub or current_author != author:
                    conn.execute(
                        "UPDATE phrases SET is_published = ?, author = ?, updated_at = ? WHERE id = ?",
                        (is_pub, author, now, existing["id"]),
                    )
                    updated += 1
                continue
            if DB_BACKEND == "postgres":
                conn.execute(
                    "INSERT INTO phrases(text_body, author, is_published, created_at, updated_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT (text_body) DO NOTHING",
                    (phrase, author, is_pub, now, now),
                )
            else:
                conn.execute(
                    "INSERT OR IGNORE INTO phrases(text_body, author, is_published, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                    (phrase, author, is_pub, now, now),
                )
            inserted += 1

    return {"ok": True, "parsed": len(parsed), "inserted": inserted, "updated": updated, "duplicates": duplicates, "skipped": skipped}


@app.post("/api/phrases/import-text")
async def import_phrases_text(request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    raw_text = (payload.get("raw_text") or "").strip()
    struct_items = payload.get("phrases_struct") or []
    if not raw_text:
        if not isinstance(struct_items, list) or not struct_items:
            raise HTTPException(status_code=400, detail="raw_text or phrases_struct is required")
    default_status = int(payload.get("is_published", 0) or 0)
    phrases: list[tuple[int, str, Optional[str]]] = []
    for line in raw_text.splitlines():
        s = line.strip()
        if not s:
            continue
        quote_text, author = split_quote_and_author(s)
        if not quote_text.strip():
            continue
        phrases.append((default_status, quote_text.strip(), (author or "").strip() or None))
    if isinstance(struct_items, list):
        for item in struct_items:
            if not isinstance(item, dict):
                continue
            quote_text = str(item.get("text_body") or "").strip()
            author = str(item.get("author") or "").strip() or None
            is_pub = int(item.get("is_published", default_status) or default_status)
            if not quote_text:
                continue
            phrases.append((is_pub, quote_text, author))
    now = now_iso()
    inserted = 0
    updated = 0
    duplicates = 0
    with db() as conn:
        for is_pub, phrase, author in phrases:
            sim, _score = find_similar_phrase_in_db(phrase, conn=conn)
            if sim:
                duplicates += 1
                continue
            existing = conn.execute("SELECT id, is_published, author FROM phrases WHERE text_body = ?", (phrase,)).fetchone()
            if existing:
                current_author = (existing.get("author") if isinstance(existing, dict) else existing["author"]) or None
                if int(existing["is_published"]) != is_pub or current_author != author:
                    conn.execute("UPDATE phrases SET is_published = ?, author = ?, updated_at = ? WHERE id = ?", (is_pub, author, now, existing["id"]))
                    updated += 1
                continue
            if DB_BACKEND == "postgres":
                conn.execute(
                    "INSERT INTO phrases(text_body, author, is_published, created_at, updated_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT (text_body) DO NOTHING",
                    (phrase, author, is_pub, now, now),
                )
            else:
                conn.execute(
                    "INSERT OR IGNORE INTO phrases(text_body, author, is_published, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                    (phrase, author, is_pub, now, now),
                )
            inserted += 1
    return {"ok": True, "parsed": len(phrases), "inserted": inserted, "updated": updated, "duplicates": duplicates}


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
    duplicates = 0
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
            quote_text, author = split_quote_and_author(phrase)
            phrase = quote_text.strip()
            author = (author or "").strip() or None
            if not phrase:
                skipped += 1
                continue
            parsed += 1
            sim, _score = find_similar_phrase_in_db(phrase, conn=conn)
            if sim:
                duplicates += 1
                continue
            existing = conn.execute("SELECT id, is_published, topic, author FROM phrases WHERE text_body = ?", (phrase,)).fetchone()
            if existing:
                existing_topic = existing.get("topic") if isinstance(existing, dict) else existing["topic"]
                existing_author = (existing.get("author") if isinstance(existing, dict) else existing["author"]) or None
                if int(existing["is_published"]) != is_pub or existing_topic != topic or existing_author != author:
                    conn.execute(
                        "UPDATE phrases SET is_published = ?, topic = ?, author = ?, updated_at = ? WHERE id = ?",
                        (is_pub, topic, author, now, existing["id"]),
                    )
                    updated += 1
                continue
            if DB_BACKEND == "postgres":
                conn.execute(
                    "INSERT INTO phrases(text_body, author, topic, is_published, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (text_body) DO NOTHING",
                    (phrase, author, topic, is_pub, now, now),
                )
            else:
                conn.execute(
                    "INSERT OR IGNORE INTO phrases(text_body, author, topic, is_published, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (phrase, author, topic, is_pub, now, now),
                )
            inserted += 1
    return {"ok": True, "parsed": parsed, "inserted": inserted, "updated": updated, "duplicates": duplicates, "skipped": skipped}


@app.put("/api/phrases/{phrase_id:int}")
async def update_phrase(phrase_id: int, request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    text_body = (payload.get("text_body") or "").strip()
    author = (payload.get("author") or "").strip() or None
    topic = (payload.get("topic") or "").strip() or None
    is_published = payload.get("is_published")
    sets = []
    params: list[Any] = []
    if text_body:
        quote_text, parsed_author = split_quote_and_author(text_body)
        text_body = quote_text.strip()
        if parsed_author and author is None:
            author = parsed_author.strip()
        sets.append("text_body = ?")
        params.append(text_body)
    if "author" in payload or (text_body and author is not None):
        sets.append("author = ?")
        params.append(author)
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
            "SELECT id, text_body, author, topic, is_published, created_at, updated_at FROM phrases WHERE id = ?",
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
    ocr_text, phrases, engine = extract_phrases_from_image(image_url)
    return {
        "ok": True,
        "image_url": image_url,
        "ocr_text": ocr_text,
        "phrases": phrases,
        "phrases_struct": [phrase_struct(p) for p in phrases],
        "ocr_engine": engine,
    }


@app.post("/api/phrases/ocr-image-base64")
async def ocr_phrase_image_base64(request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    image_data_url = (payload.get("image_data_url") or "").strip()
    if not image_data_url.startswith("data:image/"):
        raise HTTPException(status_code=400, detail="image_data_url must be a data:image/* base64 URL")
    ocr_text, phrases, engine = extract_phrases_from_image(image_data_url)
    return {
        "ok": True,
        "ocr_text": ocr_text,
        "phrases": phrases,
        "phrases_struct": [phrase_struct(p) for p in phrases],
        "ocr_engine": engine,
    }


@app.post("/api/phrases/ocr-images-base64")
async def ocr_phrase_images_base64(request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    images = payload.get("images") or []
    if not isinstance(images, list) or not images:
        raise HTTPException(status_code=400, detail="images[] required")
    if len(images) > 20:
        raise HTTPException(status_code=400, detail="max 20 images per batch")
    batch_size = len(images)

    def _quality_score(phrases: list[str]) -> float:
        if not phrases:
            return 0.0
        score = 0.0
        for p in phrases[:4]:
            c = _clean_ocr_quote_text(p)
            if not c or looks_like_noise_phrase(c) or _contains_scaffold_markers(c):
                continue
            if _is_plausible_quote_text(c):
                score += 10.0
            score += min(4.0, len(c) / 65.0)
            if _is_probable_author_line(c.split()[-1] if c.split() else ""):
                score += 0.5
        return score

    def _is_suspicious_result(phrases: list[str]) -> bool:
        if not phrases:
            return True
        first = _clean_ocr_quote_text(phrases[0] or "")
        if not first:
            return True
        if looks_like_noise_phrase(first) or _contains_scaffold_markers(first):
            return True
        if not _is_plausible_quote_text(first):
            return True
        return False

    async def _process_one(idx: int, item: dict[str, Any], sem: asyncio.Semaphore) -> dict[str, Any]:
        data_url = str((item or {}).get("image_data_url") or "").strip()
        name = str((item or {}).get("name") or f"image_{idx}.png").strip()
        if not data_url.startswith("data:image/"):
            return {"name": name, "ok": False, "error": "invalid image_data_url"}
        async with sem:
            try:
                ocr_text, phrases, engine = await asyncio.to_thread(
                    extract_phrases_from_image,
                    data_url,
                    OCR_BATCH_FAST_MODE,
                )
                if (
                    OCR_BATCH_FAST_MODE
                    and batch_size <= OCR_BATCH_FULL_FALLBACK_MAX_BATCH
                    and _is_suspicious_result(phrases)
                ):
                    full_text, full_phrases, full_engine = await asyncio.to_thread(
                        extract_phrases_from_image,
                        data_url,
                        False,
                    )
                    fast_score = _quality_score(phrases)
                    full_score = _quality_score(full_phrases)
                    if full_score > fast_score:
                        logger.info(
                            "ocr_batch_fallback_full used name=%s fast_score=%.2f full_score=%.2f",
                            name,
                            fast_score,
                            full_score,
                        )
                        ocr_text, phrases, engine = full_text, full_phrases, full_engine
                return {
                    "name": name,
                    "ok": True,
                    "ocr_text": ocr_text,
                    "phrase": phrases[0] if phrases else "",
                    "phrases": phrases,
                    "phrases_struct": [phrase_struct(p) for p in phrases],
                    "ocr_engine": engine,
                }
            except Exception as e:
                return {"name": name, "ok": False, "error": str(e)}

    sem = asyncio.Semaphore(OCR_BATCH_PARALLELISM)
    tasks = [_process_one(idx, item, sem) for idx, item in enumerate(images, start=1)]
    items: list[dict[str, Any]] = await asyncio.gather(*tasks)
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
    ocr_text, phrases, engine = extract_phrases_from_image(image_data_url)
    raw_text = "\n".join(phrases)
    res = await import_phrases_text(_mock_request({"raw_text": raw_text, "is_published": is_published}), session_id=session_id)  # type: ignore[arg-type]
    return {
        "ok": True,
        "ocr_text": ocr_text,
        "phrases": phrases,
        "phrases_struct": [phrase_struct(p) for p in phrases],
        "ocr_engine": engine,
        **res,
    }


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
    setting_set("telegram_admin_user_id", str(user_id))
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
        setting_set("telegram_admin_user_id", str(user_id))
        return True
    return admin == user_id


async def _telegram_handle_message(update: dict[str, Any]) -> dict[str, Any]:
    msg = update.get("message") or {}
    message_id = msg.get("message_id")
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
        setting_set("telegram_admin_user_id", str(user_id))
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
        if text and text.startswith("/"):
            send_telegram_text(
                chat.get("id"),
                "Команда не распознана. Доступно: /start, /new_post, /add_phrases",
                reply_markup=build_manual_create_keyboard(),
            )
        elif text:
            send_telegram_text(
                chat.get("id"),
                "Готов к работе. Выбери действие кнопкой ниже или используй /new_post и /add_phrases.",
                reply_markup=build_manual_create_keyboard(),
            )
        else:
            send_telegram_text(
                chat.get("id"),
                "Выбери действие:",
                reply_markup=build_manual_create_keyboard(),
            )
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
        send_telegram_text(
            chat.get("id"),
            "Генерация запущена, это может занять до нескольких минут.",
            track_post_id=post_id,
        )
        try:
            await regenerate_preview(
                post_id,
                _mock_request(
                    {
                        "target": target,
                        "instruction": text,
                        "progress": True,
                        "progress_chat_id": chat.get("id"),
                    }
                ),
                session_id="telegram-internal",
            )  # type: ignore[arg-type]
        except Exception as e:
            send_telegram_text(
                chat.get("id"),
                f"Перегенерация не выполнена: {str(e)[:220]}",
                track_post_id=post_id,
            )
            return {"ok": True}
        latest = fetch_post(post_id)
        preview_err = None
        try:
            chat_id = chat.get("id")
            # Remove previously tracked preview/progress messages to avoid duplicates.
            cleanup_post_thread_messages_for_chat(post_id, chat_id)
            latest = fetch_post(post_id)
            sent = telegram_send_preview_to_chat(latest, chat_id)
            result = (sent or {}).get("result") or {}
            if result.get("message_id") is not None:
                new_mid = str(result["message_id"])
                set_preview_message_id_for_chat(post_id, latest, chat_id, new_mid)
        except Exception as e:
            preview_err = str(e)
        if preview_err:
            send_telegram_text(
                chat.get("id"),
                f"Перегенерация выполнена, но превью не отправилось: {preview_err[:220]}",
                track_post_id=post_id,
            )
        else:
            send_telegram_text(
                chat.get("id"),
                "Перегенерация выполнена. Новое превью отправлено.",
                track_post_id=post_id,
            )
        return {"ok": True}
    if st == "await_schedule_datetime":
        if not post_id:
            tg_state_clear(int(user_id))
            send_telegram_text(chat.get("id"), "Не найден post_id. Начни заново с кнопок превью.")
            return {"ok": True}
        tg_state_clear(int(user_id))
        await publish(post_id, _mock_request({"mode": "schedule", "scheduled_for": text}), session_id="telegram-internal")  # type: ignore[arg-type]
        cleanup_post_service_messages_for_chat(post_id, chat.get("id"))
        if message_id is not None:
            telegram_delete_message(chat.get("id"), message_id)
        send_telegram_text(chat.get("id"), f"Публикация запланирована: {text}", track_post_id=post_id)
        return {"ok": True}
    if st == "await_replace_phrase":
        if not post_id:
            tg_state_clear(int(user_id))
            send_telegram_text(chat.get("id"), "Не найден post_id. Начни заново с кнопок превью.")
            return {"ok": True}
        tg_state_clear(int(user_id))
        await publish(post_id, _mock_request({"mode": "replace_phrase", "replacement_phrase": text}), session_id="telegram-internal")  # type: ignore[arg-type]
        latest = fetch_post(post_id)
        preview_err = None
        try:
            chat_id = chat.get("id")
            # Remove previously tracked preview/progress messages to avoid duplicates.
            cleanup_post_thread_messages_for_chat(post_id, chat_id)
            latest = fetch_post(post_id)
            sent = telegram_send_preview_to_chat(latest, chat_id)
            result = (sent or {}).get("result") or {}
            if result.get("message_id") is not None:
                new_mid = str(result["message_id"])
                set_preview_message_id_for_chat(post_id, latest, chat_id, new_mid)
        except Exception as e:
            preview_err = str(e)
        if preview_err:
            send_telegram_text(
                chat.get("id"),
                f"Фраза заменена, но превью не отправилось: {preview_err[:220]}",
                track_post_id=post_id,
            )
        else:
            send_telegram_text(
                chat.get("id"),
                "Фраза заменена, картинка и описание пересобраны, новое превью отправлено.",
                track_post_id=post_id,
            )
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
    if st == "await_manual_post_idea_decision":
        send_telegram_text(chat.get("id"), "Нажми Да/Нет: есть идеи для текста/картинки или генерируем автоматически.")
        return {"ok": True}
    if st == "await_manual_post_idea_input":
        phrase_text = str((payload.get("phrase_text") or "")).strip()
        idea_raw = (text or "").strip()
        if not phrase_text:
            tg_state_clear(int(user_id))
            send_telegram_text(chat.get("id"), "Потерялся текст фразы. Нажми «Сгенерить пост вручную» и отправь фразу снова.")
            return {"ok": True}
        parts = split_regen_instruction(idea_raw)
        text_idea = parts.get("text") or parts.get("common") or ""
        scenario_idea = parts.get("scenario") or parts.get("common") or ""
        tg_state_clear(int(user_id))
        try:
            phrase = upsert_phrase_text(phrase_text, is_published=0)
            post = create_post_from_phrase(int(phrase["id"]), session_id="telegram-internal")
            await create_preview(
                int(post["id"]),
                _mock_request(
                    {
                        "scenario": "",
                        "regen_instruction": "",
                        "text_idea": text_idea,
                        "scenario_idea": scenario_idea,
                    }
                ),
                session_id="telegram-internal",
            )  # type: ignore[arg-type]
            dup_note = ""
            if "duplicate_score" in phrase:
                dup_note = f" (дубль, score={phrase.get('duplicate_score')})"
            send_telegram_text(
                chat.get("id"),
                f"Создано: фраза #{phrase['id']}{dup_note}, пост #{post['id']}. Идеи учтены, превью отправлено.",
            )
            return {"ok": True}
        except Exception as e:
            send_telegram_text(chat.get("id"), f"Ошибка создания: {str(e)[:300]}")
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
                ocr_text, phrases, engine = extract_phrases_from_image(data_url)
                phrase_text = (phrases[0] if phrases else "").strip()
                if not phrase_text:
                    send_telegram_text(chat.get("id"), "Не удалось распознать фразу. Отправь другую картинку.")
                    return {"ok": True}
                tg_state_set(int(user_id), "await_add_phrase_confirm", {"phrase_text": phrase_text})
                send_telegram_text(
                    chat.get("id"),
                    f'Распознано ({engine}): "{phrase_text}"\nСоздать новую фразу?',
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
    if action in {"manual", "manualsave", "manualidea", "addphrases", "addphrase", "addbulk", "dailygen", "dailyswap"}:
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
        tg_state_set(int(user_id), "await_manual_post_idea_decision", {"phrase_text": phrase_text})
        if cb_id:
            answer_callback(cb_id, "Фраза принята")
        if chat_id:
            send_telegram_text(
                chat_id,
                "Есть идеи для текста или картинки?",
                reply_markup=tg_keyboard([[("Да", "ks:manualidea:0:yes"), ("Нет", "ks:manualidea:0:no")]]),
            )
        return {"ok": True}
    if action == "manualidea":
        decision = (extra or "").strip().lower()
        state = tg_state_get(int(user_id))
        if not state or state.get("state") != "await_manual_post_idea_decision":
            if cb_id:
                answer_callback(cb_id, "Нет ожидающего шага")
            if chat_id:
                send_telegram_text(chat_id, "Нет ожидающего шага. Нажми «Сгенерить пост вручную» и начни снова.")
            return {"ok": True}
        phrase_text = str((state.get("payload") or {}).get("phrase_text") or "").strip()
        if not phrase_text:
            tg_state_clear(int(user_id))
            if cb_id:
                answer_callback(cb_id, "Пустая фраза")
            if chat_id:
                send_telegram_text(chat_id, "Не удалось взять текст фразы. Нажми «Сгенерить пост вручную» снова.")
            return {"ok": True}
        if decision == "yes":
            tg_state_set(int(user_id), "await_manual_post_idea_input", {"phrase_text": phrase_text})
            if cb_id:
                answer_callback(cb_id, "Жду идеи")
            if chat_id:
                send_telegram_text(
                    chat_id,
                    (
                        "Пришли идею одним сообщением.\n"
                        "Можно так:\n"
                        "Текст: ...\n"
                        "Сценарий: ...\n"
                        "Если пришлёшь просто один абзац, он будет учтён и для текста, и для картинки."
                    ),
                )
            return {"ok": True}
        tg_state_clear(int(user_id))
        try:
            phrase = upsert_phrase_text(phrase_text, is_published=0)
            post = create_post_from_phrase(int(phrase["id"]), session_id="telegram-internal")
            await create_preview(
                int(post["id"]),
                _mock_request({"scenario": "", "regen_instruction": "", "text_idea": "", "scenario_idea": ""}),
                session_id="telegram-internal",
            )  # type: ignore[arg-type]
            if cb_id:
                answer_callback(cb_id, "Создано")
            if chat_id:
                dup_note = ""
                if "duplicate_score" in phrase:
                    dup_note = f" (дубль, score={phrase.get('duplicate_score')})"
                send_telegram_text(chat_id, f"Создано: фраза #{phrase['id']}{dup_note}, пост #{post['id']}. Превью отправлено.")
            return {"ok": True}
        except Exception as e:
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
        is_dup = "duplicate_score" in phrase
        if cb_id:
            answer_callback(cb_id, "Дубль" if is_dup else "Добавлено")
        if chat_id:
            if is_dup:
                send_telegram_text(
                    chat_id,
                    f"Похоже на дубль (score={phrase.get('duplicate_score')}). Новая запись не создана, используется #{phrase['id']}.",
                )
            else:
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
        duplicates = 0
        for phrase_text in phrases:
            row = upsert_phrase_text(phrase_text, is_published=0)
            if "duplicate_score" in row:
                duplicates += 1
            else:
                inserted += 1
        if cb_id:
            answer_callback(cb_id, "Добавлено")
        if chat_id:
            send_telegram_text(chat_id, f"Добавлено фраз: {inserted}. Срезано как дубль: {duplicates}.")
        return {"ok": True}
    if action == "dailyswap":
        _delete_callback_source_message(cq)
        current_phrase_id = int(post_id or 0)
        with db() as conn:
            phrase = conn.execute(
                "SELECT id, text_body FROM phrases WHERE coalesce(is_published,0)=0 AND id <> ? ORDER BY random() LIMIT 1",
                (current_phrase_id,),
            ).fetchone()
        if not phrase:
            if cb_id:
                answer_callback(cb_id, "Других новых фраз пока нет")
            return {"ok": True}
        new_phrase_id = int(phrase["id"])
        new_phrase_text = str(phrase["text_body"] or "").strip()
        today_key = now_msk().strftime("%Y-%m-%d")
        kv_set("daily_phrase_offer_date", today_key)
        kv_set("daily_phrase_offer_phrase_id", str(new_phrase_id))
        if cb_id:
            answer_callback(cb_id, "Фраза заменена")
        if chat_id:
            old_mid = msg.get("message_id")
            send_telegram_text(
                chat_id,
                (
                    "Фраза дня обновлена.\n\n"
                    f"{new_phrase_text}\n\n"
                    "Выберите действие:"
                ),
                reply_markup=build_daily_phrase_keyboard(new_phrase_id),
            )
            if old_mid is not None:
                telegram_delete_message(chat_id, old_mid)
        return {"ok": True}
    if action == "dailygen":
        _delete_callback_source_message(cq)
        phrase_id = int(post_id or 0)
        if not phrase_id:
            if cb_id:
                answer_callback(cb_id, "Не найдена фраза")
            return {"ok": True}
        if cb_id:
            answer_callback(cb_id, "Генерация запущена")
        if chat_id:
            send_telegram_text(chat_id, "Запускаю генерацию поста по выбранной фразе...")
        try:
            post = create_post_from_phrase(phrase_id, session_id="telegram-internal")
            await create_preview(
                int(post["id"]),
                _mock_request({"scenario": "", "regen_instruction": "", "text_idea": "", "scenario_idea": ""}),
                session_id="telegram-internal",
            )  # type: ignore[arg-type]
            if chat_id:
                send_telegram_text(chat_id, f"Пост #{post['id']} сгенерирован и отправлен на согласование.")
                old_mid = msg.get("message_id")
                if old_mid is not None:
                    telegram_delete_message(chat_id, old_mid)
            kv_set("daily_phrase_offer_generated_date", now_msk().strftime("%Y-%m-%d"))
            kv_set("daily_phrase_offer_generated_post_id", str(int(post["id"])))
        except Exception as e:
            logger.exception("daily_phrase_generate_failed phrase_id=%s", phrase_id)
            if chat_id:
                send_telegram_text(chat_id, f"Не удалось сгенерировать пост по фразе: {str(e)[:220]}")
        return {"ok": True}
    if action == "cancel":
        update_post(post_id, status="cancelled")
        if cb_id:
            answer_callback(cb_id, "Отменено")
        if chat_id:
            send_telegram_text(chat_id, "Превью отменено.")
        return {"ok": True}
    if action == "regen":
        _delete_callback_source_message(cq)
        if cb_id:
            answer_callback(cb_id, "Выбери, что перегенерировать")
        if chat_id:
            send_telegram_text(
                chat_id,
                "Что перегенерировать?",
                tg_keyboard(
                    [
                        [("Текст", f"ks:regenpick:{post_id}:text"), ("Картинку", f"ks:regenpick:{post_id}:image")],
                        [("И то и то", f"ks:regenpick:{post_id}:both")],
                    ]
                ),
                track_post_id=post_id,
            )
        return {"ok": True}
    if action == "regenpick":
        _delete_callback_source_message(cq)
        target = extra or "both"
        tg_state_set(int(user_id), "await_regen_instruction", {"post_id": post_id, "target": target})
        if cb_id:
            if target == "text":
                answer_callback(cb_id, "Пришли инструкцию для текста")
            elif target == "image":
                answer_callback(cb_id, "Пришли инструкцию для картинки")
            else:
                answer_callback(cb_id, "Пришли инструкцию для текста/картинки")
        if chat_id:
            if target == "text":
                prompt = (
                    "Пришли текст-инструкцию для перегенерации.\n"
                    "Можно отдельно указать идеи строкой:\n"
                    "Текст: ..."
                )
            elif target == "image":
                prompt = (
                    "Пришли инструкцию для перегенерации картинки.\n"
                    "Можно отдельно указать идею строкой:\n"
                    "Сценарий: ..."
                )
            else:
                prompt = (
                    "Пришли инструкцию для перегенерации текста и картинки.\n"
                    "Можно отдельно указать идеи строками:\n"
                    "Текст: ...\n"
                    "Сценарий: ..."
                )
            send_telegram_text(
                chat_id,
                prompt,
                track_post_id=post_id,
            )
        return {"ok": True}
    if action == "pub":
        if cb_id:
            answer_callback(cb_id, "Выбери вариант публикации")
        if chat_id:
            send_telegram_text(
                chat_id,
                "Когда публиковать?",
                tg_keyboard(
                    [
                        [("Сейчас", f"ks:pubpick:{post_id}:now")],
                        [("В указанное время и дату", f"ks:pubpick:{post_id}:schedule")],
                        [("Заменить фразу", f"ks:pubpick:{post_id}:replace")],
                    ]
                ),
                track_post_id=post_id,
            )
        return {"ok": True}
    if action == "pubpick":
        _delete_callback_source_message(cq)
        choice = extra or ""
        if choice == "now":
            await publish(post_id, _mock_request({"mode": "now"}), session_id="telegram-internal")  # type: ignore[arg-type]
            if cb_id:
                answer_callback(cb_id, "Опубликовано")
            if chat_id:
                # Cleanup previous service/preview messages for this generation thread.
                cleanup_post_thread_messages_for_chat(post_id, chat_id)
                send_telegram_text(chat_id, "Пост опубликован сейчас.")
            return {"ok": True}
        if choice == "schedule":
            tg_state_set(int(user_id), "await_schedule_datetime", {"post_id": post_id})
            if cb_id:
                answer_callback(cb_id, "Пришли ISO дату/время")
            if chat_id:
                send_telegram_text(
                    chat_id,
                    "Пришли дату/время в ISO формате, например 2026-02-23T10:00:00+03:00",
                    track_post_id=post_id,
                )
            return {"ok": True}
        if choice == "replace":
            tg_state_set(int(user_id), "await_replace_phrase", {"post_id": post_id})
            if cb_id:
                answer_callback(cb_id, "Пришли новую фразу")
            if chat_id:
                send_telegram_text(
                    chat_id,
                    "Пришли новый текст/фразу для замены. Система пересоберёт картинку и описание.",
                    track_post_id=post_id,
                )
            return {"ok": True}
    if cb_id:
        answer_callback(cb_id, "Неизвестное действие")
    return {"ok": True}


@app.post("/api/telegram/webhook")
async def telegram_webhook(request: Request) -> dict[str, Any]:
    if runtime_telegram_mode() != "webhook":
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
        return FileResponse(
            str(index_file),
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )
    raise HTTPException(status_code=503, detail="Frontend dist not found")
