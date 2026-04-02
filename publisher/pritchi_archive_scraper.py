#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import json
import random
import re
import sqlite3
import subprocess
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from html import unescape
from pathlib import Path
from typing import Iterable, Optional


ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "tmp" / "pritchi_archive"
STATE_DIR.mkdir(parents=True, exist_ok=True)

DISCOVERED_URLS = STATE_DIR / "discovered_urls.txt"
IMPORTED_URLS = STATE_DIR / "imported_urls.txt"
FAILED_URLS = STATE_DIR / "failed_urls.txt"
SCRAPER_LOG = STATE_DIR / "scraper.log"
STATE_JSON = STATE_DIR / "state.json"
DB_PATH = ROOT / "content.db"
PROD_SSH_TARGET = "app@77.222.55.88"
PROD_DB_CONTAINER = "apps-infra-db-1"
PROD_DB_NAME = "kindlysupport"
PROD_DB_USER = "postgres"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)
PRITCHI_SITEMAP_URL = "https://pritchi.ru/sitemap_parable.xml.gz"
DEFAULT_DELAY_MIN = 5.0
DEFAULT_DELAY_MAX = 15.0


@dataclass
class ParsedParable:
    source_url: str
    snapshot_url: str
    source_external_id: str
    title: str
    text_body: str
    category: str
    section_title: str
    source_ref: str
    source_published_at: str
    tags_json: str


def log(message: str) -> None:
    line = f"{time.strftime('%Y-%m-%d %H:%M:%S')} {message}"
    print(line, flush=True)
    with SCRAPER_LOG.open("a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def load_state() -> dict:
    if not STATE_JSON.exists():
        return {
            "cdx_resume_key": "",
            "discovered": 0,
            "imported": 0,
            "failed": 0,
            "last_mode": "",
        }
    try:
        return json.loads(STATE_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {
            "cdx_resume_key": "",
            "discovered": 0,
            "imported": 0,
            "failed": 0,
            "last_mode": "",
        }


def save_state(state: dict) -> None:
    STATE_JSON.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def jitter_sleep(min_seconds: float, max_seconds: float) -> None:
    delay = max(0.0, random.uniform(min_seconds, max(max_seconds, min_seconds)))
    time.sleep(delay)


def http_get_text(url: str, timeout: int = 40, retries: int = 4, encoding: str = "utf-8") -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        },
    )
    last_error: Optional[Exception] = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
            return raw.decode(encoding, errors="replace")
        except Exception as exc:
            last_error = exc
            if attempt + 1 < retries:
                time.sleep(min(30, 3 * (attempt + 1)))
                continue
    raise RuntimeError(f"http_get_text failed for {url}: {last_error}")


def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS parables_archive (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_url TEXT NOT NULL UNIQUE,
                snapshot_url TEXT,
                source_external_id TEXT,
                title TEXT NOT NULL,
                text_body TEXT NOT NULL,
                category TEXT,
                section_title TEXT,
                source_ref TEXT,
                source_published_at TEXT,
                tags_json TEXT NOT NULL DEFAULT '[]',
                imported_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_parables_archive_external_id
            ON parables_archive(source_external_id)
            """
        )


def sql_literal(value: Optional[str]) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def run_prod_sql(sql: str) -> str:
    result = subprocess.run(
        [
            "ssh",
            PROD_SSH_TARGET,
            "docker",
            "exec",
            "-i",
            PROD_DB_CONTAINER,
            "psql",
            "-U",
            PROD_DB_USER,
            "-d",
            PROD_DB_NAME,
            "-At",
            "-v",
            "ON_ERROR_STOP=1",
        ],
        input=sql,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"psql failed rc={result.returncode}")
    return result.stdout


def read_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [ln.strip() for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]


def write_lines(path: Path, lines: Iterable[str]) -> None:
    payload = "\n".join(lines)
    if payload:
        payload += "\n"
    path.write_text(payload, encoding="utf-8")


def append_unique(path: Path, values: Iterable[str]) -> int:
    existing = set(read_lines(path))
    added = 0
    with path.open("a", encoding="utf-8") as fh:
        for value in values:
            if not value or value in existing:
                continue
            fh.write(value + "\n")
            existing.add(value)
            added += 1
    return added


def strip_html_tags(html_fragment: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", html_fragment, flags=re.IGNORECASE)
    text = re.sub(r"</p\s*>", "\n\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\r", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def cdx_url(limit: int, resume_key: str = "") -> str:
    base = "https://web.archive.org/cdx/search/cdx"
    params = {
        "url": "https://pritchi.ru/id_*",
        "output": "json",
        "fl": "timestamp,original,statuscode",
        "filter": "statuscode:200",
        "collapse": "original",
        "from": "2020",
        "limit": str(limit),
        "showResumeKey": "true",
    }
    if resume_key:
        params["resumeKey"] = resume_key
    return base + "?" + urllib.parse.urlencode(params)


def fetch_cdx_batch(limit: int, resume_key: str = "") -> tuple[list[str], str]:
    payload = json.loads(http_get_text(cdx_url(limit, resume_key), timeout=45, retries=4))
    if not isinstance(payload, list) or not payload:
        return [], ""
    rows = payload[1:]
    next_resume = ""
    if rows and isinstance(rows[-1], str):
        next_resume = rows[-1].strip()
        rows = rows[:-1]
    urls: list[str] = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 2:
            continue
        original = str(row[1]).strip().rstrip("/")
        if re.match(r"^https?://pritchi\.ru/id_\d+$", original):
            urls.append(original)
    deduped = list(dict.fromkeys(urls))
    return deduped, next_resume


def fetch_sitemap_urls(limit: int = 0) -> list[str]:
    req = urllib.request.Request(PRITCHI_SITEMAP_URL, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
    try:
        xml_text = gzip.decompress(raw).decode("utf-8", errors="replace")
    except Exception:
        xml_text = raw.decode("utf-8", errors="replace")
    urls = re.findall(r"<loc>(https://pritchi\.ru/id_\d+)</loc>", xml_text)
    urls = list(dict.fromkeys([u.rstrip("/") for u in urls]))
    if limit > 0:
        urls = urls[:limit]
    return urls


def list_snapshot_urls(original_url: str, limit: int = 8) -> list[str]:
    base = "https://web.archive.org/cdx/search/cdx"
    params = {
        "url": original_url,
        "output": "json",
        "fl": "timestamp,original,statuscode,mimetype",
        "filter": "statuscode:200",
        "limit": str(limit),
        "from": "2016",
    }
    raw = http_get_text(base + "?" + urllib.parse.urlencode(params), timeout=45, retries=4)
    payload = json.loads(raw)
    if not isinstance(payload, list) or len(payload) < 2:
        return []
    snaps: list[str] = []
    for row in payload[1:]:
        if not isinstance(row, list) or len(row) < 2:
            continue
        timestamp = str(row[0]).strip()
        original = str(row[1]).strip()
        if not timestamp or not original:
            continue
        snap = f"https://web.archive.org/web/{timestamp}/{original}"
        snaps.append(snap)
    return list(dict.fromkeys(snaps))


def parse_pritchi_html(source_url: str, snapshot_url: str, html_text: str) -> ParsedParable:
    lower_html = html_text.lower()
    if "введите код" in lower_html or "капча" in lower_html or "kcaptcha" in lower_html:
        raise RuntimeError(f"captcha page for {source_url}")

    title_match = re.search(r'<h1[^>]*class="[^"]*post-title[^"]*"[^>]*>(.*?)</h1>', html_text, flags=re.IGNORECASE | re.DOTALL)
    text_match = re.search(r'<div[^>]*class="[^"]*textblock[^"]*"[^>]*>(.*?)</div>', html_text, flags=re.IGNORECASE | re.DOTALL)
    category_match = re.search(r'<span[^>]*class="[^"]*post-cat[^"]*"[^>]*>(.*?)</span>', html_text, flags=re.IGNORECASE | re.DOTALL)
    source_ref_match = re.search(r"Источник:\s*(.*?)(?:</span>|<br)", html_text, flags=re.IGNORECASE | re.DOTALL)
    published_match = re.search(r"Дата публикации:\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})", html_text, flags=re.IGNORECASE)
    tags_block_match = re.search(r'<div[^>]*class="[^"]*tags[^"]*"[^>]*>(.*?)</div>', html_text, flags=re.IGNORECASE | re.DOTALL)
    ext_id_match = re.search(r"/id_(\d+)", source_url)
    if not title_match or not text_match:
        raise RuntimeError(f"parse failed for {source_url}")

    tags: list[str] = []
    if tags_block_match:
        tag_matches = re.findall(r'<a[^>]*rel="tag"[^>]*>(.*?)</a>', tags_block_match.group(1), flags=re.IGNORECASE | re.DOTALL)
        tags = [strip_html_tags(tag) for tag in tag_matches if strip_html_tags(tag)]

    breadcrumb_parts = [
        strip_html_tags(x)
        for x in re.findall(r'<a[^>]*class="[^"]*text-nowrap[^"]*"[^>]*>(.*?)</a>', html_text, flags=re.IGNORECASE | re.DOTALL)
    ]
    section_title = breadcrumb_parts[-1] if breadcrumb_parts else ""

    source_published_at = ""
    if published_match:
        source_published_at = published_match.group(1)

    title = strip_html_tags(title_match.group(1))
    text_body = strip_html_tags(text_match.group(1))
    if not title or not text_body:
        raise RuntimeError(f"empty parsed content for {source_url}")

    return ParsedParable(
        source_url=source_url,
        snapshot_url=snapshot_url,
        source_external_id=ext_id_match.group(1) if ext_id_match else "",
        title=title,
        text_body=text_body,
        category=strip_html_tags(category_match.group(1)) if category_match else "",
        section_title=section_title,
        source_ref=strip_html_tags(source_ref_match.group(1)) if source_ref_match else "",
        source_published_at=source_published_at,
        tags_json=json.dumps(tags, ensure_ascii=False),
    )


def upsert_parable(parable: ParsedParable) -> None:
    init_db()
    now = now_iso()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO parables_archive (
                source_url, snapshot_url, source_external_id, title, text_body, category,
                section_title, source_ref, source_published_at, tags_json, imported_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_url) DO UPDATE SET
                snapshot_url = excluded.snapshot_url,
                source_external_id = excluded.source_external_id,
                title = excluded.title,
                text_body = excluded.text_body,
                category = excluded.category,
                section_title = excluded.section_title,
                source_ref = excluded.source_ref,
                source_published_at = excluded.source_published_at,
                tags_json = excluded.tags_json,
                updated_at = excluded.updated_at
            """,
            (
                parable.source_url,
                parable.snapshot_url,
                parable.source_external_id,
                parable.title,
                parable.text_body,
                parable.category,
                parable.section_title,
                parable.source_ref,
                parable.source_published_at,
                parable.tags_json,
                now,
                now,
            ),
        )


def fetch_prod_stub_rows(limit: int) -> list[dict[str, str]]:
    sql = f"""
    SELECT json_build_object(
      'id', id,
      'source_url', source_url,
      'source_external_id', coalesce(source_external_id, '')
    )::text
    FROM parables
    WHERE coalesce(text_body, '') = ''
      AND coalesce(source_url, '') LIKE 'https://pritchi.ru/id_%'
    ORDER BY id DESC
    LIMIT {int(limit)};
    """
    raw = run_prod_sql(sql)
    rows: list[dict[str, str]] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def update_prod_parable_success(row_id: str, parable: ParsedParable) -> None:
    sql = f"""
    UPDATE parables
    SET
      title = {sql_literal(parable.title)},
      text_body = {sql_literal(parable.text_body)},
      source_url = {sql_literal(parable.source_url)},
      source_site = 'pritchi.ru',
      source_external_id = {sql_literal(parable.source_external_id)},
      category = {sql_literal(parable.category)},
      section_title = {sql_literal(parable.section_title)},
      source_ref = {sql_literal(parable.source_ref)},
      source_published_at = {sql_literal(parable.source_published_at)},
      tags_json = {sql_literal(parable.tags_json)},
      is_stub = 0,
      last_import_error = NULL,
      updated_at = {sql_literal(now_iso())}
    WHERE id = {int(row_id)};
    """
    run_prod_sql(sql)


def update_prod_parable_error(row_id: str, error_message: str) -> None:
    sql = f"""
    UPDATE parables
    SET
      last_import_error = {sql_literal(error_message[:1000])},
      updated_at = {sql_literal(now_iso())}
    WHERE id = {int(row_id)};
    """
    run_prod_sql(sql)


def discover_from_archive(limit: int) -> tuple[int, str]:
    state = load_state()
    resume_key = str(state.get("cdx_resume_key") or "")
    urls, next_resume = fetch_cdx_batch(limit=limit, resume_key=resume_key)
    added = append_unique(DISCOVERED_URLS, urls)
    state["cdx_resume_key"] = next_resume
    state["discovered"] = int(state.get("discovered") or 0) + added
    state["last_mode"] = "discover_archive"
    save_state(state)
    log(f"discover_archive fetched={len(urls)} added={added} next_resume={'yes' if next_resume else 'no'}")
    return added, next_resume


def discover_from_sitemap(limit: int) -> int:
    urls = fetch_sitemap_urls(limit)
    added = append_unique(DISCOVERED_URLS, urls)
    state = load_state()
    state["discovered"] = int(state.get("discovered") or 0) + added
    state["last_mode"] = "discover_sitemap"
    save_state(state)
    log(f"discover_sitemap fetched={len(urls)} added={added}")
    return added


def import_one(original_url: str) -> bool:
    snapshot_urls = list_snapshot_urls(original_url)
    if not snapshot_urls:
        append_unique(FAILED_URLS, [f"{original_url}\tno_snapshot"])
        return False
    last_error = "no_parseable_snapshot"
    for snapshot_url in snapshot_urls:
        try:
            html_text = http_get_text(snapshot_url, timeout=45, retries=3)
            parable = parse_pritchi_html(original_url, snapshot_url, html_text)
            upsert_parable(parable)
            append_unique(IMPORTED_URLS, [original_url])
            return True
        except Exception as exc:
            last_error = f"{type(exc).__name__}:{str(exc)[:180]}"
            continue
    append_unique(FAILED_URLS, [f"{original_url}\t{last_error}"])
    return False


def import_discovered(limit: int, delay_min: float, delay_max: float, retry_failed: bool) -> tuple[int, int]:
    init_db()
    discovered = read_lines(DISCOVERED_URLS)
    imported = set(read_lines(IMPORTED_URLS))
    failed_urls = set()
    if not retry_failed:
        failed_urls = {ln.split("\t", 1)[0] for ln in read_lines(FAILED_URLS)}
    queue = [url for url in discovered if url not in imported and url not in failed_urls]
    if limit > 0:
        queue = queue[:limit]

    imported_count = 0
    failed_count = 0
    for index, url in enumerate(queue, start=1):
        try:
            ok = import_one(url)
            if ok:
                imported_count += 1
                log(f"import ok idx={index}/{len(queue)} url={url}")
            else:
                failed_count += 1
                log(f"import fail idx={index}/{len(queue)} url={url} reason=no_snapshot")
        except Exception as exc:
            failed_count += 1
            append_unique(FAILED_URLS, [f"{url}\t{type(exc).__name__}:{str(exc)[:300]}"])
            log(f"import fail idx={index}/{len(queue)} url={url} reason={type(exc).__name__}:{str(exc)[:200]}")
        if index < len(queue):
            jitter_sleep(delay_min, delay_max)

    state = load_state()
    state["imported"] = int(state.get("imported") or 0) + imported_count
    state["failed"] = int(state.get("failed") or 0) + failed_count
    state["last_mode"] = "import"
    save_state(state)
    return imported_count, failed_count


def show_status() -> int:
    init_db()
    discovered = len(read_lines(DISCOVERED_URLS))
    imported_urls = len(read_lines(IMPORTED_URLS))
    failed_urls = len(read_lines(FAILED_URLS))
    with sqlite3.connect(DB_PATH) as conn:
        db_count = conn.execute("SELECT COUNT(*) FROM parables_archive").fetchone()[0]
    state = load_state()
    log(
        "status "
        f"discovered={discovered} imported_urls={imported_urls} failed_urls={failed_urls} "
        f"db_rows={db_count} cdx_resume={'yes' if state.get('cdx_resume_key') else 'no'} "
        f"last_mode={state.get('last_mode') or '-'}"
    )
    return 0


def cmd_discover(args: argparse.Namespace) -> int:
    if args.source in {"archive", "both"}:
        try:
            discover_from_archive(args.limit)
        except Exception as exc:
            log(f"discover_archive_failed reason={type(exc).__name__}:{str(exc)[:200]}")
        if args.source == "archive":
            return 0
    if args.source in {"sitemap", "both"}:
        try:
            discover_from_sitemap(args.limit)
        except Exception as exc:
            log(f"discover_sitemap_failed reason={type(exc).__name__}:{str(exc)[:200]}")
    return 0


def cmd_import(args: argparse.Namespace) -> int:
    imported_count, failed_count = import_discovered(
        limit=args.limit,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        retry_failed=args.retry_failed,
    )
    log(f"import_done imported={imported_count} failed={failed_count}")
    return 0


def cmd_status(_args: argparse.Namespace) -> int:
    return show_status()


def cmd_import_url(args: argparse.Namespace) -> int:
    init_db()
    ok = import_one(args.url.rstrip("/"))
    log(f"import_url url={args.url.rstrip('/')} ok={ok}")
    return 0 if ok else 1


def cmd_fill_prod_stubs(args: argparse.Namespace) -> int:
    rows = fetch_prod_stub_rows(args.limit)
    if not rows:
        log("fill_prod_stubs none")
        return 0
    filled = 0
    failed = 0
    for index, row in enumerate(rows, start=1):
        url = row["source_url"].rstrip("/")
        try:
            snapshot_urls = list_snapshot_urls(url)
            if not snapshot_urls:
                failed += 1
                update_prod_parable_error(row["id"], "no_snapshot")
                log(f"fill_prod_stubs fail idx={index}/{len(rows)} id={row['id']} url={url} reason=no_snapshot")
            else:
                last_error = "no_parseable_snapshot"
                done = False
                for snapshot_url in snapshot_urls:
                    try:
                        html_text = http_get_text(snapshot_url, timeout=45, retries=3)
                        parable = parse_pritchi_html(url, snapshot_url, html_text)
                        update_prod_parable_success(row["id"], parable)
                        filled += 1
                        done = True
                        log(f"fill_prod_stubs ok idx={index}/{len(rows)} id={row['id']} url={url}")
                        break
                    except Exception as exc:
                        last_error = f"{type(exc).__name__}:{str(exc)[:300]}"
                if not done:
                    failed += 1
                    update_prod_parable_error(row["id"], last_error)
                    log(f"fill_prod_stubs fail idx={index}/{len(rows)} id={row['id']} url={url} reason={last_error[:180]}")
        except Exception as exc:
            failed += 1
            update_prod_parable_error(row["id"], f"{type(exc).__name__}:{str(exc)[:300]}")
            log(f"fill_prod_stubs fail idx={index}/{len(rows)} id={row['id']} url={url} reason={type(exc).__name__}:{str(exc)[:180]}")
        if index < len(rows):
            jitter_sleep(args.delay_min, args.delay_max)
    log(f"fill_prod_stubs_done filled={filled} failed={failed}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Archive-based scraper for pritchi.ru")
    sub = parser.add_subparsers(dest="command", required=True)

    discover = sub.add_parser("discover", help="Discover real pritchi URLs from archive or sitemap")
    discover.add_argument("--source", choices=["archive", "sitemap", "both"], default="both")
    discover.add_argument("--limit", type=int, default=200)
    discover.set_defaults(func=cmd_discover)

    import_cmd = sub.add_parser("import", help="Import discovered URLs via archive snapshots")
    import_cmd.add_argument("--limit", type=int, default=100)
    import_cmd.add_argument("--delay-min", type=float, default=DEFAULT_DELAY_MIN)
    import_cmd.add_argument("--delay-max", type=float, default=DEFAULT_DELAY_MAX)
    import_cmd.add_argument("--retry-failed", action="store_true")
    import_cmd.set_defaults(func=cmd_import)

    status_cmd = sub.add_parser("status", help="Print current scraper status")
    status_cmd.set_defaults(func=cmd_status)

    import_url_cmd = sub.add_parser("import-url", help="Import one explicit pritchi URL through archive")
    import_url_cmd.add_argument("url")
    import_url_cmd.set_defaults(func=cmd_import_url)

    fill_prod_cmd = sub.add_parser("fill-prod-stubs", help="Fill existing prod parables stubs through archive")
    fill_prod_cmd.add_argument("--limit", type=int, default=20)
    fill_prod_cmd.add_argument("--delay-min", type=float, default=DEFAULT_DELAY_MIN)
    fill_prod_cmd.add_argument("--delay-max", type=float, default=DEFAULT_DELAY_MAX)
    fill_prod_cmd.set_defaults(func=cmd_fill_prod_stubs)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
