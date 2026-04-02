#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import random
import re
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RECOVERED_DIR = ROOT / "recovered_pritchi_local_run"
CATALOG_TSV = ROOT / "pritchi_from_185" / "pritchi_catalog.tsv"
PARABLES_JSONL = RECOVERED_DIR / "parables.jsonl"
FILL_JSONL = RECOVERED_DIR / "parables_fill_archive.jsonl"
STATE_JSON = RECOVERED_DIR / "archive_fill_state.json"
LOG_FILE = RECOVERED_DIR / "archive_fill.log"
FAILED_URLS = RECOVERED_DIR / "archive_fill_failed.tsv"
ARCHIVE_MODULE_PATH = ROOT / "publisher" / "pritchi_archive_scraper.py"


def log(message: str) -> None:
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {message}"
    print(line, flush=True)
    with LOG_FILE.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def load_archive_module():
    spec = importlib.util.spec_from_file_location("pritchi_archive_fill_mod", ARCHIVE_MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load archive module from {ARCHIVE_MODULE_PATH}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def load_saved_ids() -> set[str]:
    saved: set[str] = set()
    for path in [PARABLES_JSONL, FILL_JSONL]:
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                ext_id = str(row.get("source_external_id") or "").strip()
                if ext_id:
                    saved.add(ext_id)
    return saved


def load_failed_urls() -> set[str]:
    failed: set[str] = set()
    if not FAILED_URLS.exists():
        return failed
    with FAILED_URLS.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            failed.add(line.split("\t", 1)[0])
    return failed


def iter_missing_rows(skip_failed: bool) -> list[tuple[int, str, str]]:
    saved_ids = load_saved_ids()
    failed_urls = load_failed_urls() if skip_failed else set()
    rows: list[tuple[int, str, str]] = []
    with CATALOG_TSV.open("r", encoding="utf-8") as fh:
        for index, line in enumerate(fh, start=1):
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split("\t")
            if len(parts) < 2:
                continue
            ext_id, url = parts[0].strip(), parts[1].strip().rstrip("/")
            if not ext_id or not url:
                continue
            if ext_id in saved_ids:
                continue
            if url in failed_urls:
                continue
            rows.append((index, ext_id, url))
    return rows


def append_jsonl(path: Path, row: dict) -> None:
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")


def append_failed(url: str, reason: str) -> None:
    with FAILED_URLS.open("a", encoding="utf-8") as fh:
        fh.write(f"{url}\t{reason[:500]}\n")


def load_state() -> dict:
    if not STATE_JSON.exists():
        return {"processed": 0, "saved": 0, "failed": 0, "last_index": 0, "last_url": ""}
    try:
        return json.loads(STATE_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {"processed": 0, "saved": 0, "failed": 0, "last_index": 0, "last_url": ""}


def save_state(state: dict) -> None:
    STATE_JSON.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def parable_to_row(parable) -> dict:
    return {
        "source_url": parable.source_url,
        "snapshot_url": parable.snapshot_url,
        "source_external_id": parable.source_external_id,
        "title": parable.title,
        "text_body": parable.text_body,
        "category": parable.category,
        "section_title": parable.section_title,
        "source_ref": parable.source_ref,
        "source_published_at": parable.source_published_at,
        "tags_json": json.loads(parable.tags_json) if getattr(parable, "tags_json", "") else [],
        "recovered_via": "archive",
        "imported_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def import_one(mod, url: str, snapshot_timeout: int, snapshot_retries: int):
    snapshots = mod.list_snapshot_urls(url, limit=8)
    if not snapshots:
        return None, "no_snapshot"
    last_error = "no_parseable_snapshot"
    for snapshot_url in snapshots:
        try:
            html_text = mod.http_get_text(snapshot_url, timeout=snapshot_timeout, retries=snapshot_retries)
            parable = mod.parse_pritchi_html(url, snapshot_url, html_text)
            return parable, ""
        except Exception as exc:
            last_error = f"{type(exc).__name__}:{str(exc)[:220]}"
    return None, last_error


def run(limit: int, delay_min: float, delay_max: float, skip_failed: bool, snapshot_timeout: int, snapshot_retries: int) -> int:
    mod = load_archive_module()
    state = load_state()
    missing = iter_missing_rows(skip_failed=skip_failed)
    if limit > 0:
        missing = missing[:limit]
    log(
        f"start missing_total={len(iter_missing_rows(skip_failed=skip_failed))} "
        f"run_batch={len(missing)} skip_failed={'yes' if skip_failed else 'no'}"
    )
    for pos, (index, ext_id, url) in enumerate(missing, start=1):
        state["processed"] = int(state.get("processed") or 0) + 1
        state["last_index"] = index
        state["last_url"] = url
        save_state(state)
        log(f"item_start idx={index} id={ext_id} pos={pos}/{len(missing)} url={url}")
        parable, error = import_one(mod, url, snapshot_timeout=snapshot_timeout, snapshot_retries=snapshot_retries)
        if parable is not None:
            row = parable_to_row(parable)
            append_jsonl(FILL_JSONL, row)
            append_jsonl(PARABLES_JSONL, row)
            state["saved"] = int(state.get("saved") or 0) + 1
            log(f"item_saved idx={index} id={ext_id} title={parable.title[:80]}")
        else:
            append_failed(url, error)
            state["failed"] = int(state.get("failed") or 0) + 1
            log(f"item_failed idx={index} id={ext_id} reason={error}")
        save_state(state)
        if pos < len(missing):
            time.sleep(max(0.0, random.uniform(delay_min, max(delay_min, delay_max))))
    log(
        f"done processed={state.get('processed',0)} saved={state.get('saved',0)} "
        f"failed={state.get('failed',0)} last_index={state.get('last_index',0)}"
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fill missing recovered pritchi entries via archive snapshots")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--delay-min", type=float, default=2.0)
    parser.add_argument("--delay-max", type=float, default=5.0)
    parser.add_argument("--snapshot-timeout", type=int, default=12)
    parser.add_argument("--snapshot-retries", type=int, default=1)
    parser.add_argument("--retry-failed", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    return run(
        limit=max(0, args.limit),
        delay_min=max(0.0, args.delay_min),
        delay_max=max(args.delay_min, args.delay_max),
        skip_failed=not args.retry_failed,
        snapshot_timeout=max(3, args.snapshot_timeout),
        snapshot_retries=max(1, args.snapshot_retries),
    )


if __name__ == "__main__":
    raise SystemExit(main())
