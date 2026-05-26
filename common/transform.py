from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def _safe_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y"}:
            return True
        if lowered in {"false", "0", "no", "n"}:
            return False
    return None


def _parse_event_time(event: Dict[str, Any]) -> datetime:
    timestamp = event.get("timestamp")
    if timestamp is not None:
        try:
            return datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
        except (TypeError, ValueError, OSError):
            pass

    meta = event.get("meta") or {}
    meta_dt = meta.get("dt")
    if meta_dt:
        try:
            # Wikimedia often uses ISO-8601 with Z suffix.
            return datetime.fromisoformat(str(meta_dt).replace("Z", "+00:00"))
        except ValueError:
            pass

    return datetime.now(timezone.utc)


def _make_event_id(event: Dict[str, Any]) -> str:
    wiki = event.get("wiki") or event.get("server_name") or "unknown_wiki"
    raw_id = event.get("id")

    if raw_id not in (None, ""):
        return f"{wiki}:{raw_id}"

    meta = event.get("meta") or {}
    meta_id = meta.get("id")
    if meta_id not in (None, ""):
        return str(meta_id)

    stable_json = json.dumps(event, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(stable_json.encode("utf-8")).hexdigest()


def normalize_wikimedia_recentchange(event: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a Wikimedia recentchange event into a relational-friendly row."""
    length = event.get("length") or {}
    revision = event.get("revision") or {}

    length_old = _safe_int(length.get("old"))
    length_new = _safe_int(length.get("new"))

    if length_old is not None and length_new is not None:
        bytes_delta = length_new - length_old
    else:
        bytes_delta = None

    server_url = event.get("server_url")
    title_url = event.get("title_url")
    page_url = None
    if title_url:
        page_url = str(title_url)
    elif server_url and event.get("title"):
        safe_title = str(event["title"]).replace(" ", "_")
        page_url = f"{server_url}/wiki/{safe_title}"

    return {
        "event_id": _make_event_id(event),
        "event_time": _parse_event_time(event),
        "ingest_time": datetime.now(timezone.utc),
        "wiki": event.get("wiki"),
        "domain": event.get("meta", {}).get("domain") or event.get("server_name"),
        "server_name": event.get("server_name"),
        "user_name": event.get("user"),
        "title": event.get("title"),
        "change_type": event.get("type"),
        "namespace_id": _safe_int(event.get("namespace")),
        "is_bot": _safe_bool(event.get("bot")),
        "is_minor": _safe_bool(event.get("minor")),
        "length_old": length_old,
        "length_new": length_new,
        "bytes_delta": bytes_delta,
        "rev_old": _safe_int(revision.get("old")),
        "rev_new": _safe_int(revision.get("new")),
        "page_url": page_url,
        "comment": event.get("comment"),
        "raw_event": event,
    }
