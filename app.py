import json
import os
import secrets
import sqlite3
import time
import urllib.parse
import urllib.error
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import Cookie, FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse

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


def now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


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
INSTAGRAM_GRAPH_VERSION = os.getenv("INSTAGRAM_GRAPH_VERSION", "v22.0").strip()
INSTAGRAM_ACCESS_TOKEN = os.getenv("INSTAGRAM_ACCESS_TOKEN", "").strip()
INSTAGRAM_IG_USER_ID = os.getenv("INSTAGRAM_IG_USER_ID", "").strip()
PINTEREST_ACCESS_TOKEN = os.getenv("PINTEREST_ACCESS_TOKEN", "").strip()
PINTEREST_BOARD_ID = os.getenv("PINTEREST_BOARD_ID", "").strip()
ENABLE_INSTAGRAM = os.getenv("ENABLE_INSTAGRAM", "0").strip() in {"1", "true", "yes"}
ENABLE_PINTEREST = os.getenv("ENABLE_PINTEREST", "0").strip() in {"1", "true", "yes"}


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
                    is_published INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
        else:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
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
                    is_published INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            # lightweight migration for older sqlite db
            for col_def in (
                "preview_message_id TEXT",
                "published_message_id TEXT",
            ):
                try:
                    conn.execute(f"ALTER TABLE posts ADD COLUMN {col_def}")
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
    timeout: int = 60,
) -> dict[str, Any]:
    body = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=body, method=method.upper())
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    if body is not None and "Content-Type" not in (headers or {}):
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="ignore")
        raise HTTPException(status_code=502, detail=f"HTTP {e.code}: {detail[:1200]}")


def telegram_api(method: str, payload: dict[str, Any]) -> dict[str, Any]:
    if not TELEGRAM_BOT_TOKEN:
        raise HTTPException(status_code=400, detail="TELEGRAM_BOT_TOKEN not set")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    return http_json("POST", url, payload)


def answer_callback(callback_query_id: str, text: str = "") -> None:
    if not TELEGRAM_BOT_TOKEN:
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


def telegram_send_preview(post: dict[str, Any]) -> Optional[dict[str, Any]]:
    if not TELEGRAM_BOT_TOKEN:
        return None
    chat_id = TELEGRAM_PREVIEW_CHAT
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
    if not TELEGRAM_BOT_TOKEN:
        return None
    chat_id = TELEGRAM_PUBLISH_CHAT
    caption = post.get("telegram_caption") or generate_caption(post["title"], post["text_body"])
    if post.get("final_image_url"):
        return telegram_api(
            "sendPhoto",
            {"chat_id": chat_id, "photo": post["final_image_url"], "caption": caption[:1024]},
        )
    return telegram_api("sendMessage", {"chat_id": chat_id, "text": caption, "disable_web_page_preview": True})


def send_telegram_text(chat_id: str | int, text: str, reply_markup: Optional[dict[str, Any]] = None) -> Optional[dict[str, Any]]:
    if not TELEGRAM_BOT_TOKEN:
        return None
    payload: dict[str, Any] = {"chat_id": chat_id, "text": text}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return telegram_api("sendMessage", payload)


def instagram_publish_post(post: dict[str, Any]) -> dict[str, Any]:
    if not INSTAGRAM_ACCESS_TOKEN or not INSTAGRAM_IG_USER_ID:
        raise HTTPException(status_code=400, detail="Instagram credentials not configured")
    if not post.get("final_image_url"):
        raise HTTPException(status_code=400, detail="Post has no final_image_url")
    base = f"https://graph.facebook.com/{INSTAGRAM_GRAPH_VERSION}/{INSTAGRAM_IG_USER_ID}"
    create = http_json(
        "POST",
        f"{base}/media",
        {
            "image_url": post["final_image_url"],
            "caption": (post.get("telegram_caption") or "")[:2200],
            "access_token": INSTAGRAM_ACCESS_TOKEN,
        },
    )
    creation_id = create.get("id")
    if not creation_id:
        raise HTTPException(status_code=502, detail=f"Instagram create media failed: {create}")
    publish = http_json(
        "POST",
        f"{base}/media_publish",
        {"creation_id": creation_id, "access_token": INSTAGRAM_ACCESS_TOKEN},
    )
    return {"create": create, "publish": publish}


def pinterest_publish_post(post: dict[str, Any]) -> dict[str, Any]:
    if not ENABLE_PINTEREST:
        raise HTTPException(status_code=503, detail="Pinterest publishing temporarily disabled")
    if not PINTEREST_ACCESS_TOKEN or not PINTEREST_BOARD_ID:
        raise HTTPException(status_code=400, detail="Pinterest credentials not configured")
    if not post.get("final_image_url"):
        raise HTTPException(status_code=400, detail="Post has no final_image_url")
    payload = {
        "board_id": PINTEREST_BOARD_ID,
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
        headers={"Authorization": f"Bearer {PINTEREST_ACCESS_TOKEN}"},
    )
    return res


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
    if not OPENROUTER_API_KEY:
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
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json",
            "HTTP-Referer": OPENROUTER_SITE_URL,
            "X-Title": OPENROUTER_APP_NAME,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="ignore")
        raise HTTPException(status_code=502, detail=f"OpenRouter HTTPError: {detail[:1000]}")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OpenRouter error: {e}")


def openrouter_generate_text(prompt: str) -> str:
    res = openrouter_chat(
        OPENROUTER_TEXT_MODEL,
        [{"role": "user", "content": prompt}],
    )
    return (res.get("choices") or [{}])[0].get("message", {}).get("content", "")


def openrouter_extract_text_from_image(image_url: str) -> str:
    res = openrouter_chat(
        OPENROUTER_VISION_MODEL,
        [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": "Extract the visible text from this image in Russian if present. Return plain text only."},
                    {"type": "image_url", "image_url": {"url": image_url}},
                ],
            }
        ],
    )
    return (res.get("choices") or [{}])[0].get("message", {}).get("content", "")


def openrouter_generate_image(post_id: int, prompt: str, size: str = "1024x1024") -> dict[str, Any]:
    started = time.time()
    model = OPENROUTER_IMAGE_MODEL
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
    lower = url.lower()
    if lower.endswith(".pdf"):
        raise HTTPException(status_code=400, detail="PDF не поддерживается. Только ссылка на HTML или картинку.")
    if lower.endswith((".png", ".jpg", ".jpeg", ".webp")):
        return "url_image", openrouter_extract_text_from_image(url)
    # MVP stub for HTML extraction. Replace with readability parser later.
    return "url_html", f"Текст, извлечённый из HTML-ссылки (заглушка)\nИсточник: {url}"


app = FastAPI(title="Kindlysupport Posting MVP")


@app.on_event("startup")
def startup() -> None:
    init_db()


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return INDEX_HTML


@app.get("/health")
def health() -> dict[str, Any]:
    return {"ok": True, "time": now_iso(), "db": DB_PATH, "db_backend": DB_BACKEND}


@app.post("/api/login")
async def login(request: Request, response: Response) -> dict[str, Any]:
    payload = await request.json()
    if payload.get("email") != ADMIN_EMAIL or payload.get("password") != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="invalid credentials")
    sid = secrets.token_urlsafe(32)
    created = now_iso()
    expires = datetime.now(tz=UTC).timestamp() + 30 * 24 * 3600
    expires_iso = datetime.fromtimestamp(expires, tz=UTC).isoformat()
    with db() as conn:
        conn.execute("INSERT INTO sessions(session_id, created_at, expires_at) VALUES (?, ?, ?)", (sid, created, expires_iso))
    response.set_cookie(SESSION_COOKIE, sid, httponly=True, samesite="lax", secure=False)
    return {"ok": True}


@app.post("/api/logout")
async def logout(response: Response, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    if session_id:
        with db() as conn:
            conn.execute("DELETE FROM sessions WHERE session_id = ?", (session_id,))
    response.delete_cookie(SESSION_COOKIE)
    return {"ok": True}


@app.get("/api/config")
def get_config(session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    return {
        "models": {
            "vision": OPENROUTER_VISION_MODEL,
            "text": OPENROUTER_TEXT_MODEL,
            "image": OPENROUTER_IMAGE_MODEL,
        },
        "openrouter_key_configured": bool(OPENROUTER_API_KEY),
        "db_backend": DB_BACKEND,
        "telegram": {
            "bot_configured": bool(TELEGRAM_BOT_TOKEN),
            "admin_user_id": tg_admin_user_id(),
            "preview_chat": TELEGRAM_PREVIEW_CHAT,
            "publish_chat": TELEGRAM_PUBLISH_CHAT,
        },
        "instagram_configured": bool(ENABLE_INSTAGRAM and INSTAGRAM_ACCESS_TOKEN and INSTAGRAM_IG_USER_ID),
        "pinterest_configured": bool(ENABLE_PINTEREST and PINTEREST_ACCESS_TOKEN and PINTEREST_BOARD_ID),
    }


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


@app.get("/api/posts")
def list_posts(session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> list[dict[str, Any]]:
    ensure_auth(session_id)
    with db() as conn:
        rows = conn.execute("SELECT * FROM posts ORDER BY id DESC LIMIT 100").fetchall()
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
        "chat": TELEGRAM_PREVIEW_CHAT,
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
        return fetch_post(post_id)
    if mode == "schedule":
        scheduled_for = (payload.get("scheduled_for") or "").strip()
        if not scheduled_for:
            raise HTTPException(status_code=400, detail="scheduled_for required")
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
    return {
        "telegram": {
            "bot_token": bool(TELEGRAM_BOT_TOKEN),
            "admin_user_id": tg_admin_user_id(),
            "preview_chat": TELEGRAM_PREVIEW_CHAT,
            "publish_chat": TELEGRAM_PUBLISH_CHAT,
        },
        "instagram": {
            "enabled": ENABLE_INSTAGRAM,
            "configured": bool(ENABLE_INSTAGRAM and INSTAGRAM_ACCESS_TOKEN and INSTAGRAM_IG_USER_ID),
            "needs": ["INSTAGRAM_ACCESS_TOKEN", "INSTAGRAM_IG_USER_ID"],
        },
        "pinterest": {
            "enabled": ENABLE_PINTEREST,
            "configured": bool(ENABLE_PINTEREST and PINTEREST_ACCESS_TOKEN and PINTEREST_BOARD_ID),
            "needs": ["PINTEREST_ACCESS_TOKEN", "PINTEREST_BOARD_ID"],
        },
        "database": {"backend": DB_BACKEND, "database_url_set": bool(DATABASE_URL)},
    }


@app.post("/api/posts/{post_id}/publish/instagram")
def publish_instagram_endpoint(post_id: int, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    if not ENABLE_INSTAGRAM:
        raise HTTPException(status_code=503, detail="Instagram publishing temporarily disabled")
    post = fetch_post(post_id)
    res = instagram_publish_post(post)
    return {"ok": True, "post_id": post_id, "instagram": res}


@app.post("/api/posts/{post_id}/publish/pinterest")
def publish_pinterest_endpoint(post_id: int, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    if not ENABLE_PINTEREST:
        raise HTTPException(status_code=503, detail="Pinterest publishing temporarily disabled")
    post = fetch_post(post_id)
    res = pinterest_publish_post(post)
    return {"ok": True, "post_id": post_id, "pinterest": res}


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
def list_phrases(session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> list[dict[str, Any]]:
    ensure_auth(session_id)
    with db() as conn:
        rows = conn.execute(
            "SELECT id, text_body, is_published, created_at, updated_at FROM phrases ORDER BY id ASC LIMIT 5000"
        ).fetchall()
    return [dict(r) for r in rows]


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


@app.post("/api/phrases/import-image-url")
async def import_phrases_image_url(request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    image_url = (payload.get("image_url") or "").strip()
    if not image_url:
        raise HTTPException(status_code=400, detail="image_url is required")
    ocr_text = openrouter_extract_text_from_image(image_url)
    # First version: one line = one phrase. User can edit OCR result in UI and re-import as text if needed.
    return {"ok": True, "image_url": image_url, "ocr_text": ocr_text}


@app.post("/api/phrases/import-image-base64")
async def import_phrases_image_base64(request: Request, session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    payload = await request.json()
    image_data_url = (payload.get("image_data_url") or "").strip()
    if not image_data_url.startswith("data:image/"):
        raise HTTPException(status_code=400, detail="image_data_url must be a data:image/* base64 URL")
    is_published = int(payload.get("is_published", 0) or 0)
    ocr_text = openrouter_extract_text_from_image(image_data_url)

    phrases: list[str] = []
    for line in ocr_text.splitlines():
        s = line.strip()
        if not s:
            continue
        # Remove common list prefixes from OCR dumps
        s = s.lstrip("•-—* \t").strip()
        if s:
            phrases.append(s)

    now = now_iso()
    inserted = 0
    updated = 0
    with db() as conn:
        for phrase in phrases:
            existing = conn.execute("SELECT id, is_published FROM phrases WHERE text_body = ?", (phrase,)).fetchone()
            if existing:
                if int(existing["is_published"]) != is_published:
                    conn.execute("UPDATE phrases SET is_published = ?, updated_at = ? WHERE id = ?", (is_published, now, existing["id"]))
                    updated += 1
                continue
            if DB_BACKEND == "postgres":
                conn.execute(
                    "INSERT INTO phrases(text_body, is_published, created_at, updated_at) VALUES (?, ?, ?, ?) ON CONFLICT (text_body) DO NOTHING",
                    (phrase, is_published, now, now),
                )
            else:
                conn.execute(
                    "INSERT OR IGNORE INTO phrases(text_body, is_published, created_at, updated_at) VALUES (?, ?, ?, ?)",
                    (phrase, is_published, now, now),
                )
            inserted += 1

    return {
        "ok": True,
        "ocr_text": ocr_text,
        "parsed": len(phrases),
        "inserted": inserted,
        "updated": updated,
        "is_published": is_published,
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
        send_telegram_text(chat.get("id"), "Админ привязан. Теперь сюда будут приходить инструкции по перегенерации/публикации.")
        return {"ok": True}

    state = tg_state_get(int(user_id))
    if not state:
        return {"ok": True, "message": "no pending state"}
    st = state["state"]
    payload = state["payload"] or {}
    post_id = int(payload.get("post_id"))
    if st == "await_regen_instruction":
        target = payload.get("target", "both")
        tg_state_clear(int(user_id))
        await regenerate_preview(post_id, _mock_request({"target": target, "instruction": text}), session_id="telegram-internal")  # type: ignore[arg-type]
        send_telegram_text(chat.get("id"), "Перегенерация выполнена. Новое превью отправлено.")
        return {"ok": True}
    if st == "await_schedule_datetime":
        tg_state_clear(int(user_id))
        await publish(post_id, _mock_request({"mode": "schedule", "scheduled_for": text}), session_id="telegram-internal")  # type: ignore[arg-type]
        send_telegram_text(chat.get("id"), f"Публикация запланирована: {text}")
        return {"ok": True}
    if st == "await_replace_phrase":
        tg_state_clear(int(user_id))
        await publish(post_id, _mock_request({"mode": "replace_phrase", "replacement_phrase": text}), session_id="telegram-internal")  # type: ignore[arg-type]
        send_telegram_text(chat.get("id"), "Фраза заменена, картинка и описание пересобраны, новое превью отправлено.")
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
    if not post_id:
        if cb_id:
            answer_callback(cb_id, "Некорректная кнопка")
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
    secret = request.headers.get("x-telegram-bot-api-secret-token")
    if TELEGRAM_WEBHOOK_SECRET and secret != TELEGRAM_WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Invalid telegram webhook secret")
    update = await request.json()
    if update.get("callback_query"):
        return await _telegram_handle_callback(update)
    if update.get("message"):
        return await _telegram_handle_message(update)
    return {"ok": True}


@app.post("/api/telegram/set-webhook")
def telegram_set_webhook(session_id: Optional[str] = Cookie(default=None, alias=SESSION_COOKIE)) -> dict[str, Any]:
    ensure_auth(session_id)
    if not TELEGRAM_BOT_TOKEN:
        raise HTTPException(status_code=400, detail="TELEGRAM_BOT_TOKEN not set")
    # The actual public URL must be passed by client app via query param or body in a future iteration.
    raise HTTPException(status_code=400, detail="Use Telegram setWebhook manually with your public URL + /api/telegram/webhook")


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


INDEX_HTML = """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>KindlySupport Admin</title>
  <style>
    :root {
      --bg: #eee7da;
      --panel: #f8f4ec;
      --panel2: #f4efe6;
      --ink: #1f1a17;
      --muted: #71685f;
      --line: #d8cdbd;
      --accent: #2f6b57;
      --danger: #9f4c45;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      font-family: Georgia, "Iowan Old Style", serif;
      background:
        radial-gradient(circle at 0% 0%, #fff7e5, transparent 35%),
        radial-gradient(circle at 100% 0%, #efe2c8, transparent 40%),
        linear-gradient(180deg, #ece4d5, #f4efe7);
    }
    .shell { max-width: 1360px; margin: 0 auto; padding: 20px; }
    .card {
      background: color-mix(in srgb, var(--panel) 92%, white);
      border: 1px solid var(--line);
      border-radius: 16px;
      box-shadow: 0 8px 30px rgba(50, 35, 18, .05);
    }
    .auth-wrap { max-width: 520px; margin: 36px auto; padding: 18px; }
    h1 { margin: 0 0 8px; font-size: 30px; }
    h2 { margin: 0 0 8px; font-size: 20px; }
    h3 { margin: 0 0 8px; font-size: 16px; }
    .small { font-size: 12px; color: var(--muted); }
    .status { margin: 8px 0 0; font-size: 13px; color: var(--muted); }
    label { display:block; margin-top: 10px; font-size: 12px; color: var(--muted); }
    input, textarea, select, button {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px;
      font: inherit;
      background: #fffdfa;
      color: var(--ink);
    }
    textarea { min-height: 88px; resize: vertical; }
    button { cursor: pointer; background: var(--ink); color: #fff; border-color: var(--ink); }
    button.secondary { background: transparent; color: var(--ink); }
    button.accent { background: var(--accent); border-color: var(--accent); }
    button.danger { background: var(--danger); border-color: var(--danger); }
    .row2 { display:grid; grid-template-columns: 1fr 1fr; gap: 8px; }
    .layout { display:grid; grid-template-columns: 1fr; gap: 16px; }
    .side { padding: 14px; display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
    .side h2 { margin: 0 6px 0 0; font-size: 18px; }
    .side hr { display: none; }
    .main { padding: 14px; min-height: 70vh; }
    .nav-btn { text-align: center; margin: 0; width: auto; }
    .nav-btn.active { background: var(--accent); border-color: var(--accent); color: #fff; }
    .module { display: none; }
    .module.active { display: block; }
    .grid { display:grid; grid-template-columns: 380px 1fr; gap: 12px; }
    .panel { border: 1px solid var(--line); border-radius: 12px; background: var(--panel2); padding: 12px; }
    .posts { max-height: 260px; overflow:auto; border:1px dashed var(--line); border-radius:12px; padding: 8px; background: #fffdf9; }
    .post-item { padding: 8px; border-radius: 8px; border:1px solid transparent; cursor:pointer; }
    .post-item:hover { background:#fff; border-color: var(--line); }
    .post-item.selected { border-color: var(--accent); background: #fff; }
    .toolbar { display:flex; gap:8px; flex-wrap:wrap; }
    .toolbar button { width:auto; }
    .chips { display:flex; gap:6px; flex-wrap:wrap; margin-top: 8px; }
    .chips button { width:auto; }
    .clickable-row { cursor: pointer; }
    .clickable-row:hover { background: #f6efdf; }
    .preview-card {
      position: relative;
      width: 100%;
      aspect-ratio: 1 / 1;
      border-radius: 14px;
      border: 1px solid var(--line);
      overflow: hidden;
      background: linear-gradient(180deg, #d7d0c0, #c7bba4);
      margin-top: 10px;
    }
    .preview-card img {
      position: absolute; inset: 0;
      width: 100%; height: 100%;
      object-fit: cover;
    }
    .preview-card .shade {
      position: absolute; inset: 0;
      background: rgba(20, 16, 12, .45);
    }
    .preview-card .phrase {
      position: absolute;
      inset: 10% 8% 16% 8%;
      display: grid;
      place-items: center;
      text-align: center;
      color: #fffdf8;
      font-weight: 700;
      line-height: 1.22;
      font-size: clamp(18px, 2vw, 30px);
      text-shadow: 0 2px 16px rgba(0,0,0,.45);
      white-space: pre-wrap;
    }
    .preview-card .wm {
      position: absolute;
      bottom: 12px;
      width: 100%;
      text-align: center;
      color: rgba(255,255,255,.7);
      font-size: 14px;
      letter-spacing: .4px;
    }
    .dropzone {
      border: 2px dashed var(--line);
      border-radius: 12px;
      padding: 14px;
      background: #fffdf8;
      color: var(--muted);
      text-align: center;
      transition: .15s ease;
      margin-top: 8px;
    }
    .dropzone.dragover {
      border-color: var(--accent);
      background: #f3fbf7;
      color: var(--ink);
    }
    pre { white-space: pre-wrap; word-break: break-word; background:#fffcf6; border:1px solid var(--line); padding:10px; border-radius:10px; max-height: 220px; overflow:auto; }
    table { width: 100%; border-collapse: collapse; font-size: 13px; background: #fffdf9; border-radius: 12px; overflow:hidden; }
    th, td { border-bottom: 1px solid var(--line); padding: 8px; vertical-align: top; text-align: left; }
    .muted-box { border: 1px dashed var(--line); border-radius: 12px; padding: 12px; color: var(--muted); background: #fbf7ef; }
    .hidden { display:none !important; }
    @media (max-width: 980px) {
      .grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <section id="authView" class="card auth-wrap">
      <h1>KindlySupport</h1>
      <div class="small">Авторизация закрывает все модули. После входа откроются разделы.</div>
      <div id="authStatus" class="status">Не авторизован.</div>
      <label>Email</label>
      <input id="loginEmail" placeholder="admin@example.com" />
      <label>Password</label>
      <input id="loginPassword" type="password" placeholder="Пароль" />
      <div style="margin-top:10px">
        <button onclick="login()">Войти</button>
      </div>
    </section>

    <section id="appView" class="layout hidden">
      <aside class="card side">
        <h2 style="margin-bottom:4px">Модули</h2>
        <div id="configInfo" class="small" style="margin-bottom:10px">Загрузка...</div>
        <button id="tab-phrases-content" class="nav-btn active" onclick="switchTab('phrases-content')">Фразы</button>
        <button id="tab-phrases" class="nav-btn secondary" onclick="switchTab('phrases')">База фраз</button>
        <button id="tab-parables" class="nav-btn secondary" onclick="switchTab('parables')">Притчи</button>
        <button id="tab-films" class="nav-btn secondary" onclick="switchTab('films')">Фильмы</button>
        <hr style="border:none;border-top:1px solid var(--line);margin:12px 0" />
        <button class="secondary" onclick="logout()">Выйти</button>
      </aside>

      <main class="card main">
        <section id="module-phrases-content" class="module active">
          <h2>Фразы</h2>
          <div class="small">Постинг идёт от контента: фраза → текст/картинка → превью → публикация.</div>
          <div class="grid" style="margin-top:12px">
            <div class="panel">
              <h3>Работа с фразами</h3>
              <div class="small">Основной сценарий: кликнуть по фразе в модуле <b>База фраз</b>. Ниже ручной ввод оставлен как резервный путь.</div>
              <div class="toolbar" style="margin:8px 0 4px">
                <button onclick="dailyPhrasePreview()">Утренний пост из неопубликованной</button>
              </div>
              <div id="dailyPhraseResult" class="small">Создаёт пост из первой фразы со статусом `0`, собирает превью и отправляет в Telegram.</div>
              <label>Заголовок</label>
              <input id="phrasePostTitle" placeholder="Например: Фраза дня" />
              <label>Текст фразы</label>
              <textarea id="phrasePostText" placeholder="Вставь фразу для публикации"></textarea>
              <label>Режим источника</label>
              <select id="modeSel" onchange="toggleMode()">
                <option value="manual">Вручную</option>
                <option value="link">По ссылке (для притч/текста)</option>
              </select>
              <div id="linkFields" class="hidden">
                <label>Ссылка</label>
                <input id="urlInput" placeholder="https://..." />
              </div>
              <button class="accent" style="margin-top:10px" onclick="createPhrasePost()">Создать пост</button>

              <h3 style="margin-top:14px">Посты (фразы)</h3>
              <div class="posts" id="postsList"></div>
              <button class="secondary" style="margin-top:8px" onclick="loadPosts()">Обновить список</button>
            </div>

            <div class="panel">
              <h3>Редактор, превью и публикация</h3>
              <div id="emptyState" class="small">Выбери пост слева.</div>
              <div id="editorArea" class="hidden">
                <label>ID</label>
                <input id="postId" disabled />
                <label>Заголовок</label>
                <input id="editTitle" />
                <label>Текст</label>
                <textarea id="editText" style="min-height:150px"></textarea>
                <button class="secondary" onclick="savePost()">Сохранить текст</button>

                <label>Сценарий картинки</label>
                <textarea id="scenarioInput" style="min-height:80px"></textarea>
                <div class="toolbar" style="margin-top:8px">
                  <button onclick="generateScenarios(false)">Сгенерить сценарии</button>
                  <button class="secondary" onclick="generateScenarios(true)">Стандартные</button>
                </div>
                <div class="chips" id="scenarioChips"></div>

                <label>Инструкция для перегенерации</label>
                <textarea id="regenInstruction" placeholder="Что учесть при перегенерации текста/картинки"></textarea>
                <div class="toolbar" style="margin-top:8px">
                  <button class="accent" onclick="buildPreview()">Собрать превью</button>
                  <button class="secondary" onclick="regen('text')">Переген текст</button>
                  <button class="secondary" onclick="regen('image')">Переген картинку</button>
                  <button class="secondary" onclick="regen('both')">Переген оба</button>
                </div>

                <label>Статус</label>
                <input id="statusInput" disabled />
                <label>Дата/время публикации (ISO, MSK UTC+3)</label>
                <input id="scheduledAt" placeholder="2026-02-23T10:00:00+03:00" />
                <label>Текст для замены фразы</label>
                <textarea id="replacementText"></textarea>
                <div class="toolbar" style="margin-top:8px">
                  <button onclick="publishNow()">Опубликовать сейчас</button>
                  <button class="secondary" onclick="publishSchedule()">В указанное время</button>
                  <button class="secondary" onclick="replacePhrase()">Заменить фразу</button>
                </div>

                <label>Подпись Telegram</label>
                <pre id="captionPre"></pre>
                <label>Prompt изображения</label>
                <pre id="promptPre"></pre>
                <label>Preview payload</label>
                <pre id="previewPre"></pre>
                <label>Визуальный пост (авто-сборка)</label>
                <div id="visualPostCard" class="preview-card">
                  <div class="shade"></div>
                  <div class="phrase" id="visualPostPhrase">Выбери фразу и собери превью</div>
                  <div class="wm" id="visualPostWm">@kindlysupport</div>
                </div>
                <label>Логи генерации изображений</label>
                <button class="secondary" onclick="loadImageLogs()">Обновить логи</button>
                <pre id="imgLogsPre"></pre>
              </div>
            </div>
          </div>
        </section>

        <section id="module-phrases" class="module">
          <h2>База фраз</h2>
          <div class="small">Здесь хранятся все фразы: опубликованные (`1`) и новые (`0`). Клик по строке создаёт пост по этой фразе.</div>
          <div id="phrasesListView" style="margin-top:12px">
            <div class="panel">
              <div class="toolbar">
                <button class="accent" onclick="openPhrasesImport()">Импорт фраз</button>
                <button onclick="loadPhrases()">Обновить базу фраз</button>
              </div>
              <div class="small" id="phrasesStats" style="margin:8px 0">Нет данных</div>
              <div style="max-height:620px; overflow:auto;">
                <table>
                  <thead><tr><th>ID</th><th>Статус</th><th>Фраза</th></tr></thead>
                  <tbody id="phrasesTableBody"></tbody>
                </table>
              </div>
            </div>
          </div>

          <div id="phrasesImportView" class="hidden" style="margin-top:12px">
            <div class="panel">
              <div class="toolbar">
                <button class="secondary" onclick="closePhrasesImport()">Назад к списку</button>
              </div>
              <h3 style="margin-top:8px">Импорт фраз</h3>

              <h3 style="margin-top:14px">Загрузка фраз (текст)</h3>
              <div class="small">Одна строка = одна фраза. Можно загрузить сразу как новые (`0`) или как опубликованные (`1`).</div>
              <textarea id="phrasesTextInput" style="min-height:120px" placeholder="Каждая строка = отдельная фраза"></textarea>
              <div class="toolbar" style="margin-top:8px">
                <button class="accent" onclick="importPhrasesText(0)">Загрузить как новые (0)</button>
                <button class="secondary" onclick="importPhrasesText(1)">Загрузить как опубликованные (1)</button>
              </div>

              <h3 style="margin-top:14px">Загрузка фраз (картинка)</h3>
              <div class="small">Вставь ссылку на картинку с фразами или перетащи файл. OCR распознает текст и запишет фразы в БД.</div>
              <div id="phrasesDropzone" class="dropzone"
                   ondragover="onPhraseImageDragOver(event)"
                   ondragleave="onPhraseImageDragLeave(event)"
                   ondrop="onPhraseImageDrop(event)">
                Перетащи картинку с рабочего стола сюда
                <div class="small" style="margin-top:6px">PNG / JPG / WEBP. Картинка уйдёт на распознавание и фразы запишутся в БД автоматически.</div>
              </div>
              <div id="phrasesDropResult" class="status"></div>
              <label>Ссылка на картинку</label>
              <input id="phrasesImageUrl" placeholder="https://...png/jpg/webp" />
              <div class="toolbar" style="margin-top:8px">
                <button onclick="ocrPhrasesImage()">Распознать</button>
                <button class="secondary" onclick="useOcrAsTextImport()">Перенести OCR в текстовый импорт</button>
              </div>
              <label>OCR результат</label>
              <textarea id="phrasesOcrText" style="min-height:110px" placeholder="Сюда попадет распознанный текст"></textarea>

              <h3 style="margin-top:14px">Импорт TSV</h3>
              <div class="small">Формат строки: `1<TAB>текст` или `0<TAB>текст`.</div>
              <textarea id="phrasesTsvInput" style="min-height:260px" placeholder="Вставь список фраз сюда"></textarea>
              <button class="accent" style="margin-top:8px" onclick="importPhrasesTsv()">Импортировать в БД</button>
              <div id="phrasesImportResult" class="status"></div>
            </div>
          </div>
        </section>

        <section id="module-parables" class="module">
          <h2>Притчи</h2>
          <div class="small">Отдельный модуль притч. Сейчас использует тот же backend-пайплайн (создание/превью/публикация), но UI будет выделен отдельно.</div>
          <div class="muted-box" style="margin-top:12px">
            В следующем шаге сюда вынесем отдельный интерфейс: ручной ввод притчи, вставка ссылки, распознавание, редактор и превью.
            Пока для теста используй модуль <b>Фразы</b>.
          </div>
        </section>

        <section id="module-films" class="module">
          <h2>Фильмы</h2>
          <div class="small">Отдельный модуль для будущего сценария "фильмы".</div>
          <div class="muted-box" style="margin-top:12px">
            Заглушка. Добавим отдельные сущности, импорты и шаблоны публикаций после завершения Telegram-потока для фраз/притч.
          </div>
        </section>
      </main>
    </section>
  </div>

  <script>
    let selectedPostId = null;
    let isAuthorized = false;

    async function api(url, method='GET', body) {
      const res = await fetch(url, {
        method,
        headers: {'Content-Type': 'application/json'},
        credentials: 'include',
        body: body ? JSON.stringify(body) : undefined
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.detail || JSON.stringify(data));
      return data;
    }

    function toggleMode() {
      const mode = document.getElementById('modeSel').value;
      document.getElementById('linkFields').classList.toggle('hidden', mode !== 'link');
    }

    function setAuthUI(auth) {
      isAuthorized = auth;
      document.getElementById('authView').classList.toggle('hidden', auth);
      document.getElementById('appView').classList.toggle('hidden', !auth);
      document.getElementById('authStatus').textContent = auth ? 'Авторизован.' : 'Не авторизован.';
    }

    function openPhrasesImport() {
      document.getElementById('phrasesListView').classList.add('hidden');
      document.getElementById('phrasesImportView').classList.remove('hidden');
    }

    function closePhrasesImport() {
      document.getElementById('phrasesImportView').classList.add('hidden');
      document.getElementById('phrasesListView').classList.remove('hidden');
    }

    function switchTab(tab) {
      ['phrases-content','phrases','parables','films'].forEach(t => {
        document.getElementById(`module-${t}`).classList.toggle('active', t === tab);
        const btn = document.getElementById(`tab-${t}`);
        btn.classList.toggle('active', t === tab);
        btn.classList.toggle('secondary', t !== tab);
      });
      if (tab === 'phrases') {
        closePhrasesImport();
      }
    }

    async function loadConfig() {
      try {
        const cfg = await api('/api/config');
        setAuthUI(true);
        document.getElementById('configInfo').textContent =
          `DB=${cfg.db_backend} | Telegram=${cfg.telegram?.bot_configured ? 'ok' : 'off'} | image=${cfg.models.image}`;
      } catch (e) {
        setAuthUI(false);
      }
    }

    async function login() {
      await api('/api/login', 'POST', {
        email: document.getElementById('loginEmail').value.trim(),
        password: document.getElementById('loginPassword').value
      });
      await loadConfig();
      await Promise.all([loadPosts(), loadPhrases()]);
    }

    async function logout() {
      try { await api('/api/logout', 'POST', {}); } catch (e) {}
      selectedPostId = null;
      setAuthUI(false);
      document.getElementById('postsList').innerHTML = '';
      document.getElementById('phrasesTableBody').innerHTML = '';
      document.getElementById('emptyState').classList.remove('hidden');
      document.getElementById('editorArea').classList.add('hidden');
    }

    async function createPhrasePost() {
      const mode = document.getElementById('modeSel').value;
      const body = {
        mode,
        title: (document.getElementById('phrasePostTitle').value || 'Фраза дня').trim()
      };
      if (mode === 'manual') body.text_body = document.getElementById('phrasePostText').value;
      if (mode === 'link') body.url = document.getElementById('urlInput').value;
      const post = await api('/api/posts', 'POST', body);
      await loadPosts();
      await selectPost(post.id);
      switchTab('phrases-content');
    }

    async function createPostFromPhraseId(phraseId) {
      const post = await api(`/api/phrases/${phraseId}/create-post`, 'POST', {});
      switchTab('phrases-content');
      await loadPosts();
      await selectPost(post.id);
    }

    async function dailyPhrasePreview() {
      try {
        const res = await api('/api/phrases/daily-preview', 'POST', {});
        document.getElementById('dailyPhraseResult').textContent =
          `Создан пост #${res.post.id} из фразы #${res.phrase_id}. Превью отправлено.`;
        switchTab('phrases-content');
        await loadPosts();
        await selectPost(res.post.id);
        await loadPhrases();
      } catch (e) {
        document.getElementById('dailyPhraseResult').textContent = String(e.message || e);
      }
    }

    async function loadPosts() {
      if (!isAuthorized) return;
      const posts = await api('/api/posts');
      const el = document.getElementById('postsList');
      el.innerHTML = '';
      posts.forEach(p => {
        const item = document.createElement('div');
        item.className = 'post-item' + (p.id === selectedPostId ? ' selected' : '');
        item.innerHTML = `<div><b>#${p.id}</b> ${escapeHtml(p.title)}</div><div class="small">${escapeHtml(p.status)} · ${escapeHtml(p.source_kind)}</div>`;
        item.onclick = () => selectPost(p.id);
        el.appendChild(item);
      });
    }

    async function selectPost(id) {
      selectedPostId = id;
      const p = await api(`/api/posts/${id}`);
      document.getElementById('emptyState').classList.add('hidden');
      document.getElementById('editorArea').classList.remove('hidden');
      document.getElementById('postId').value = p.id;
      document.getElementById('editTitle').value = p.title || '';
      document.getElementById('editText').value = p.text_body || '';
      document.getElementById('scenarioInput').value = p.selected_scenario || (p.image_scenarios?.[0] || '');
      document.getElementById('statusInput').value = p.status || '';
      document.getElementById('captionPre').textContent = p.telegram_caption || '';
      document.getElementById('promptPre').textContent = p.image_prompt || '';
      document.getElementById('previewPre').textContent = p.preview_payload ? JSON.stringify(p.preview_payload, null, 2) : '';
      renderScenarioChips(p.image_scenarios || []);
      renderVisualPostCard(p);
      document.getElementById('replacementText').value = '';
      await loadImageLogs();
      await loadPosts();
    }

    function renderVisualPostCard(post) {
      const box = document.getElementById('visualPostCard');
      const phraseEl = document.getElementById('visualPostPhrase');
      const wmEl = document.getElementById('visualPostWm');
      const phrase = (post.title || post.text_body || '').trim() || 'Фраза';
      phraseEl.textContent = phrase;
      wmEl.textContent = '@kindlysupport';
      const existingImg = box.querySelector('img');
      if (existingImg) existingImg.remove();
      if (post.final_image_url) {
        const img = document.createElement('img');
        img.src = post.final_image_url;
        img.alt = 'generated';
        box.prepend(img);
      }
    }

    function renderScenarioChips(scenarios) {
      const wrap = document.getElementById('scenarioChips');
      wrap.innerHTML = '';
      scenarios.forEach(s => {
        const b = document.createElement('button');
        b.className = 'secondary';
        b.textContent = s;
        b.onclick = () => { document.getElementById('scenarioInput').value = s; };
        wrap.appendChild(b);
      });
    }

    async function savePost() {
      if (!selectedPostId) return;
      await api(`/api/posts/${selectedPostId}`, 'PUT', {
        title: document.getElementById('editTitle').value,
        text_body: document.getElementById('editText').value
      });
      await selectPost(selectedPostId);
    }

    async function generateScenarios(forceDefault) {
      if (!selectedPostId) return;
      await api(`/api/posts/${selectedPostId}/image-scenarios`, 'POST', {force_default: forceDefault});
      await selectPost(selectedPostId);
    }

    async function buildPreview() {
      if (!selectedPostId) return;
      await api(`/api/posts/${selectedPostId}/preview`, 'POST', {
        scenario: document.getElementById('scenarioInput').value,
        regen_instruction: document.getElementById('regenInstruction').value
      });
      await selectPost(selectedPostId);
    }

    async function regen(target) {
      if (!selectedPostId) return;
      await api(`/api/posts/${selectedPostId}/regenerate`, 'POST', {
        target,
        instruction: document.getElementById('regenInstruction').value
      });
      await selectPost(selectedPostId);
    }

    async function publishNow() {
      if (!selectedPostId) return;
      await api(`/api/posts/${selectedPostId}/publish`, 'POST', { mode: 'now' });
      await selectPost(selectedPostId);
    }

    async function publishSchedule() {
      if (!selectedPostId) return;
      await api(`/api/posts/${selectedPostId}/publish`, 'POST', {
        mode: 'schedule',
        scheduled_for: document.getElementById('scheduledAt').value
      });
      await selectPost(selectedPostId);
    }

    async function replacePhrase() {
      if (!selectedPostId) return;
      await api(`/api/posts/${selectedPostId}/publish`, 'POST', {
        mode: 'replace_phrase',
        replacement_phrase: document.getElementById('replacementText').value
      });
      await selectPost(selectedPostId);
    }

    async function loadImageLogs() {
      try {
        const logs = await api('/api/logs/image');
        document.getElementById('imgLogsPre').textContent = JSON.stringify(logs.slice(0, 20), null, 2);
      } catch (e) {
        document.getElementById('imgLogsPre').textContent = String(e.message || e);
      }
    }

    async function loadPhrases() {
      if (!isAuthorized) return;
      try {
        const rows = await api('/api/phrases');
        const body = document.getElementById('phrasesTableBody');
        body.innerHTML = '';
        let pub = 0;
        let unpub = 0;
        rows.forEach(r => {
          if (Number(r.is_published) === 1) pub += 1; else unpub += 1;
          const tr = document.createElement('tr');
          tr.className = 'clickable-row';
          tr.innerHTML = `<td>${r.id}</td><td>${Number(r.is_published) ? '1 (опубл.)' : '0 (не опубл.)'}</td><td>${escapeHtml(r.text_body || '')}</td>`;
          tr.onclick = () => createPostFromPhraseId(r.id);
          body.appendChild(tr);
        });
        document.getElementById('phrasesStats').textContent = `Всего: ${rows.length} | Опубликовано: ${pub} | Не опубликовано: ${unpub}`;
        if (rows.length === 0) {
          document.getElementById('phrasesImportResult').textContent = 'База фраз пустая. Загрузи фразы текстом, по картинке (OCR) или TSV.';
        }
      } catch (e) {
        document.getElementById('phrasesStats').textContent = String(e.message || e);
      }
    }

    async function importPhrasesTsv() {
      const raw = document.getElementById('phrasesTsvInput').value;
      const res = await api('/api/phrases/import-tsv', 'POST', { raw_tsv: raw });
      document.getElementById('phrasesImportResult').textContent = `Импорт завершен: parsed=${res.parsed}, inserted=${res.inserted}, updated=${res.updated}, skipped=${res.skipped}`;
      await loadPhrases();
      closePhrasesImport();
    }

    async function importPhrasesText(isPublished) {
      const raw = document.getElementById('phrasesTextInput').value;
      const res = await api('/api/phrases/import-text', 'POST', { raw_text: raw, is_published: isPublished });
      document.getElementById('phrasesImportResult').textContent = `Текстовый импорт: parsed=${res.parsed}, inserted=${res.inserted}, updated=${res.updated}`;
      await loadPhrases();
      closePhrasesImport();
    }

    async function ocrPhrasesImage() {
      const imageUrl = (document.getElementById('phrasesImageUrl').value || '').trim();
      const res = await api('/api/phrases/import-image-url', 'POST', { image_url: imageUrl });
      document.getElementById('phrasesOcrText').value = res.ocr_text || '';
    }

    function useOcrAsTextImport() {
      document.getElementById('phrasesTextInput').value = document.getElementById('phrasesOcrText').value || '';
    }

    function onPhraseImageDragOver(ev) {
      ev.preventDefault();
      document.getElementById('phrasesDropzone').classList.add('dragover');
    }

    function onPhraseImageDragLeave(ev) {
      ev.preventDefault();
      document.getElementById('phrasesDropzone').classList.remove('dragover');
    }

    async function onPhraseImageDrop(ev) {
      ev.preventDefault();
      const dz = document.getElementById('phrasesDropzone');
      dz.classList.remove('dragover');
      const files = ev.dataTransfer?.files;
      if (!files || !files.length) return;
      const file = files[0];
      if (!file.type.startsWith('image/')) {
        document.getElementById('phrasesDropResult').textContent = 'Нужен файл-картинка (image/*).';
        return;
      }
      document.getElementById('phrasesDropResult').textContent = `Загрузка и распознавание: ${file.name}...`;
      try {
        const dataUrl = await fileToDataUrl(file);
        const res = await api('/api/phrases/import-image-base64', 'POST', {
          image_data_url: dataUrl,
          is_published: 0
        });
        document.getElementById('phrasesOcrText').value = res.ocr_text || '';
        document.getElementById('phrasesDropResult').textContent =
          `Готово: parsed=${res.parsed}, inserted=${res.inserted}, updated=${res.updated}`;
        await loadPhrases();
        closePhrasesImport();
      } catch (e) {
        document.getElementById('phrasesDropResult').textContent = String(e.message || e);
      }
    }

    function fileToDataUrl(file) {
      return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => resolve(reader.result);
        reader.onerror = reject;
        reader.readAsDataURL(file);
      });
    }

    function escapeHtml(s='') {
      return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    }

    toggleMode();
    switchTab('phrases-content');
    loadConfig();
  </script>
</body>
</html>
"""
