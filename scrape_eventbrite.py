#!/usr/bin/env python3
"""
Scrape Eventbrite for veteran events in Montana and Wyoming over the next 60 days.

This script queries the Eventbrite API for events matching veteran‑related terms in the
states of Montana and Wyoming. It validates the API token, paginates through search
results, filters events to those occurring within the next LOOKAHEAD_DAYS, deduplicates
them by (name, start), and writes both a machine‑readable JSON file and a human‑
friendly Markdown file.

The script always produces diagnostic output. On success, events.json will contain
{"generated": true, "events": [...], "warnings": [...]}, and events.md will list
each event with its date, location, and signup link. On failure, events.json will
contain {"generated": false, "error": ...}, and events.md will note that no events
were found.
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import requests

# API configuration
API_BASE = "https://www.eventbriteapi.com/v3"
OUT_JSON = "events.json"
OUT_MD = "events.md"

DEFAULT_STATES = ["Montana", "Wyoming"]
DEFAULT_QUERY = os.environ.get(
    "EVENTBRITE_QUERY",
    "veteran OR veterans OR military OR service member",
)
DEFAULT_WITHIN = os.environ.get("EVENTBRITE_WITHIN", "500mi")
LOOKAHEAD_DAYS = int(os.environ.get("EVENTBRITE_DAYS", "60"))
PAGE_DELAY_SEC = float(os.environ.get("EVENTBRITE_PAGE_DELAY_SEC", "0.5"))


def save_json(payload: Dict, path: str = OUT_JSON) -> None:
    """Write a dict to a JSON file with UTF‑8 encoding."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def save_markdown(events: List[Dict], path: str = OUT_MD) -> None:
    """Render a list of events to a Markdown file for easy reading."""
    lines: List[str] = []
    lines.append("# Upcoming Veteran Events in Montana and Wyoming\n")
    if not events:
        lines.append(f"No events found within the next {LOOKAHEAD_DAYS} days.\n")
    else:
        for e in events:
            name = e.get("name") or "Unnamed Event"
            lines.append(f"## {name}\n")
            start = e.get("start") or ""
            start_fmt = start.replace("T", " ").replace("Z", "") if start else ""
            if start_fmt:
                lines.append(f"- **Date:** {start_fmt}\n")
            loc_parts: List[str] = []
            if e.get("venue_name"):
                loc_parts.append(e["venue_name"])
            if e.get("address"):
                loc_parts.append(e["address"])
            if e.get("city"):
                loc_parts.append(e["city"])
            if e.get("state"):
                loc_parts.append(e["state"])
            if loc_parts:
                lines.append(f"- **Location:** {', '.join(loc_parts)}\n")
            url = e.get("url")
            if url:
                lines.append(f"- **Sign up:** [{url}]({url})\n")
            lines.append("")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def get_token() -> str:
    """Retrieve the Eventbrite API token from environment variables."""
    token = os.environ.get("EVENTBRITE_TOKEN")
    if not token:
        save_json({"generated": False, "error": "EVENTBRITE_TOKEN is not set"}, OUT_JSON)
        save_markdown([], OUT_MD)
        sys.exit(2)
    return token


def validate_token(session: requests.Session, headers: Dict[str, str]) -> None:
    """Validate the token by calling the /users/me endpoint. Raises RuntimeError on failure."""
    url = f"{API_BASE}/users/me/"
    try:
        resp = session.get(url, headers=headers, timeout=15)
    except requests.RequestException as exc:
        raise RuntimeError(f"token_validation_request_error:{exc}")
    if resp.status_code == 200:
        return
    if resp.status_code in (401, 403):
        raise RuntimeError(f"token_invalid_or_forbidden:http_{resp.status_code}:{resp.text[:256]}")
    if resp.status_code == 429:
        raise RuntimeError(f"rate_limited_on_token_validation:http_429:{resp.text[:256]}")
    raise RuntimeError(f"token_validation_http_error:http_{resp.status_code}:{resp.text[:256]}")


def search_region(
    session: requests.Session,
    headers: Dict[str, str],
    query: str,
    location_address: str,
    within: str,
) -> Tuple[List[Dict], List[str]]:
    """Perform a paginated search on Eventbrite for a single region."""
    params = {
        "q": query,
        "location.address": location_address,
        "location.within": within,
        "expand": "venue",
        "sort_by": "date",
        "page": 1,
    }
    results: List[Dict] = []
    warnings: List[str] = []
    while True:
        url = f"{API_BASE}/events/search"
        try:
            resp = session.get(url, headers=headers, params=params, timeout=30)
        except requests.RequestException as exc:
            warnings.append(f"request_error:{location_address}:{exc}")
            break
        if resp.status_code == 404:
            warnings.append(f"404:{location_address}:{resp.text[:256]}")
            break
        if resp.status_code in (401, 403):
            warnings.append(f"auth_error_http_{resp.status_code}:{location_address}:{resp.text[:256]}")
            break
        if resp.status_code == 429:
            warnings.append(f"rate_limited_http_429:{location_address}:{resp.text[:256]}")
            break
        if resp.status_code != 200:
            warnings.append(f"http_{resp.status_code}:{location_address}:{resp.text[:256]}")
            break
        try:
            data = resp.json()
        except ValueError:
            warnings.append(f"invalid_json_response:{location_address}:{resp.text[:256]}")
            break
        results.extend(data.get("events") or [])
        if not data.get("pagination", {}).get("has_more_items"):
            break
        params["page"] = int(params.get("page", 1)) + 1
        time.sleep(PAGE_DELAY_SEC)
    return results, warnings


def normalize_events(events: List[Dict]) -> List[Dict]:
    """Normalize raw Eventbrite events into a simplified structure."""
    normalized: List[Dict] = []
    for e in events:
        venue = e.get("venue") or {}
        address = venue.get("address") or {}
        normalized.append({
            "id": e.get("id"),
            "name": (e.get("name") or {}).get("text"),
            "url": e.get("url"),
            "start": (e.get("start") or {}).get("local"),
            "end": (e.get("end") or {}).get("local"),
            "is_free": e.get("is_free"),
            "status": e.get("status"),
            "city": address.get("city"),
            "state": address.get("region"),
            "venue_name": venue.get("name"),
            "address": address.get("localized_address_display"),
        })
    return normalized


def filter_upcoming(events: List[Dict], days: int = LOOKAHEAD_DAYS) -> List[Dict]:
    """Return events starting within the next `days` days."""
    now = datetime.utcnow()
    cutoff = now + timedelta(days=days)
    filtered: List[Dict] = []
    for e in events:
        start = e.get("start")
        if not start:
            continue
        try:
            dt = datetime.fromisoformat(start)
        except ValueError:
            continue
        if now <= dt <= cutoff:
            filtered.append(e)
    return filtered


def fetch_events(token: str, query: str = DEFAULT_QUERY, states: List[str] = None, within: str = DEFAULT_WITHIN) -> Dict:
    """Fetch events for all requested states and return a structured payload."""
    if states is None:
        states = DEFAULT_STATES
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": os.environ.get("VNN_USER_AGENT", "mt-wy-veteran-events-scraper/1.0"),
    }
    session = requests.Session()
    # Validate token early
    validate_token(session, headers)
    all_raw: List[Dict] = []
    all_warnings: List[str] = []
    for state in states:
        events, warns = search_region(session, headers, query, state, within)
        all_raw.extend(events)
        all_warnings.extend(warns)
    normalized = normalize_events(all_raw)
    upcoming = filter_upcoming(normalized, LOOKAHEAD_DAYS)
    # Deduplicate events by (name, start)
    seen = set()
    unique: List[Dict] = []
    for e in upcoming:
        key = (e.get("name"), e.get("start"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(e)
    return {
        "generated": True,
        "source": "eventbrite",
        "query": query,
        "regions": states,
        "within": within,
        "count": len(unique),
        "events": unique,
        "warnings": all_warnings,
    }


def main() -> int:
    token = get_token()
    try:
        print("Token acquired; starting fetch...")
        payload = fetch_events(token)
        print(f"Fetched {payload['count']} events.")
        save_json(payload, OUT_JSON)
        save_markdown(payload.get("events", []), OUT_MD)
        return 0
    except Exception as exc:
        save_json({"generated": False, "error": str(exc)}, OUT_JSON)
        save_markdown([], OUT_MD)
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
