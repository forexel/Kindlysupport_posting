#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib import parse, request, error

GRAPH_VERSION = os.getenv("IG_GRAPH_VERSION", "v22.0").strip() or "v22.0"
GRAPH_HOST = os.getenv("IG_GRAPH_HOST", "graph.facebook.com").strip() or "graph.facebook.com"
IG_USER_ID = os.getenv("IG_USER_ID", "").strip()
IG_ACCESS_TOKEN = os.getenv("IG_ACCESS_TOKEN", "").strip()
QUEUE_DIR = Path(os.getenv("IG_QUEUE_DIR", "queue/instagram"))
DONE_DIR = Path(os.getenv("IG_DONE_DIR", "queue/instagram_done"))
FAILED_DIR = Path(os.getenv("IG_FAILED_DIR", "queue/instagram_failed"))
AUTO_COMMIT = os.getenv("IG_AUTO_COMMIT", "1").strip().lower() in {"1", "true", "yes", "on"}
FAIL_ON_ITEM_ERROR = os.getenv("IG_FAIL_ON_ITEM_ERROR", "0").strip().lower() in {"1", "true", "yes", "on"}
CONTAINER_WAIT_SECONDS = int(os.getenv("IG_CONTAINER_WAIT_SECONDS", "90"))
CONTAINER_POLL_INTERVAL_SECONDS = int(os.getenv("IG_CONTAINER_POLL_INTERVAL_SECONDS", "5"))


@dataclass
class QueueItem:
    path: Path
    data: dict[str, Any]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso_utc(ts: str) -> Optional[datetime]:
    value = (ts or "").strip()
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


def load_queue_items() -> list[QueueItem]:
    items: list[QueueItem] = []
    if not QUEUE_DIR.exists():
        return items
    for p in sorted(QUEUE_DIR.glob("*.json")):
        try:
            payload = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                items.append(QueueItem(path=p, data=payload))
            else:
                raise ValueError("queue item root must be JSON object")
        except Exception as exc:
            move_with_meta(p, FAILED_DIR, {"error": f"invalid_json: {exc}"})
    return items


def build_url(path: str, params: dict[str, str]) -> str:
    return f"https://{GRAPH_HOST}/{GRAPH_VERSION}/{path}?{parse.urlencode(params)}"


def fetch_json(url: str, method: str = "POST", timeout: int = 30) -> dict[str, Any]:
    req = request.Request(url=url, method=method.upper())
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"HTTP {exc.code}: {detail[:1200]}") from exc
    except Exception as exc:
        raise RuntimeError(f"request_failed: {exc}") from exc


def publish_instagram(image_url: str, caption: str) -> dict[str, Any]:
    if not IG_USER_ID or not IG_ACCESS_TOKEN:
        raise RuntimeError("IG_USER_ID or IG_ACCESS_TOKEN is empty")

    create_url = build_url(
        f"{IG_USER_ID}/media",
        {
            "image_url": image_url,
            "caption": caption[:2200],
            "access_token": IG_ACCESS_TOKEN,
        },
    )
    create_res = fetch_json(create_url, "POST")
    creation_id = str(create_res.get("id") or "").strip()
    if not creation_id:
        raise RuntimeError(f"create_failed: {create_res}")

    wait_for_container_ready(creation_id)

    publish_url = build_url(
        f"{IG_USER_ID}/media_publish",
        {
            "creation_id": creation_id,
            "access_token": IG_ACCESS_TOKEN,
        },
    )
    publish_res = fetch_json(publish_url, "POST")
    return {"create": create_res, "publish": publish_res}


def wait_for_container_ready(creation_id: str) -> None:
    deadline = time.time() + max(0, CONTAINER_WAIT_SECONDS)
    last_status = ""
    while time.time() <= deadline:
        status_url = build_url(
            creation_id,
            {
                "fields": "status_code",
                "access_token": IG_ACCESS_TOKEN,
            },
        )
        status_res = fetch_json(status_url, "GET")
        status = str(status_res.get("status_code") or "").strip().upper()
        if status == "FINISHED":
            return
        if status in {"ERROR", "EXPIRED"}:
            raise RuntimeError(f"container_status={status}: {status_res}")
        if status:
            last_status = status
        time.sleep(max(1, CONTAINER_POLL_INTERVAL_SECONDS))
    raise RuntimeError(f"container_not_ready_timeout last_status={last_status or 'UNKNOWN'}")


def move_with_meta(src: Path, dst_dir: Path, meta: dict[str, Any]) -> Path:
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / src.name
    shutil.move(str(src), str(dst))
    meta_path = dst.with_suffix(dst.suffix + ".meta.json")
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return dst


def has_git_changes() -> bool:
    proc = subprocess.run(["git", "status", "--porcelain"], check=False, capture_output=True, text=True)
    return bool(proc.stdout.strip())


def git_commit_and_push(message: str) -> None:
    subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=True)
    subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], check=True)
    subprocess.run(["git", "add", "queue/instagram", "queue/instagram_done", "queue/instagram_failed"], check=True)
    if has_git_changes():
        subprocess.run(["git", "commit", "-m", message], check=True)
        subprocess.run(["git", "push"], check=True)


def main() -> int:
    items = load_queue_items()
    if not items:
        print("queue empty")
        return 0

    processed = 0
    failed = 0
    skipped = 0

    for item in items:
        payload = item.data
        image_url = str(payload.get("image_url") or "").strip()
        caption = str(payload.get("caption") or "").strip()
        publish_at = parse_iso_utc(str(payload.get("publish_at") or "").strip())

        if publish_at and publish_at > now_utc():
            skipped += 1
            continue

        if not image_url:
            failed += 1
            move_with_meta(item.path, FAILED_DIR, {"error": "image_url is required", "payload": payload})
            continue

        try:
            result = publish_instagram(image_url=image_url, caption=caption)
            processed += 1
            move_with_meta(item.path, DONE_DIR, {"ok": True, "result": result, "payload": payload})
        except Exception as exc:
            failed += 1
            move_with_meta(item.path, FAILED_DIR, {"ok": False, "error": str(exc), "payload": payload})

    print(f"processed={processed} failed={failed} skipped={skipped}")

    if AUTO_COMMIT and has_git_changes():
        try:
            git_commit_and_push("chore: process instagram publishing queue")
        except Exception as exc:
            print(f"warning: git push failed: {exc}")
            return 1

    if failed > 0:
        print(
            "completed with failed items; check queue/instagram_failed/*.meta.json "
            "for exact API errors"
        )
    return 1 if (failed > 0 and FAIL_ON_ITEM_ERROR) else 0


if __name__ == "__main__":
    sys.exit(main())
