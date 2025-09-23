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
            # Fix: Handle timezone-aware datetime formatting
            start_fmt = ""
            if start:
                try:
                    # Parse and format datetime properly
                    dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
                    start_fmt = dt.strftime('%Y-%m-%d %H:%M')
                except (ValueError, AttributeError):
                    start_fmt = start.replace("T", " ").replace("Z", "")
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
    
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def get_token() -> str:
    """Retrieve the Eventbrite API token from environment variables."""
    token = os.environ.get("EVENTBRITE_TOKEN")
    if not token:
        print("ERROR: EVENTBRITE_TOKEN environment variable is not set", file=sys.stderr)
        save_json({"generated": False, "error": "EVENTBRITE_TOKEN is not set"}, OUT_JSON)
        save_markdown([], OUT_MD)
        sys.exit(2)
    return token.strip()  # Fix: Strip whitespace from token


def validate_token(session: requests.Session, headers: Dict[str, str]) -> None:
    """Validate the token by calling the /users/me endpoint. Raises RuntimeError on failure."""
    url = f"{API_BASE}/users/me/"
    print(f"Validating token with URL: {url}")
    try:
        resp = session.get(url, headers=headers, timeout=15)
        print(f"Token validation response: {resp.status_code}")
    except requests.RequestException as exc:
        print(f"Token validation request error: {exc}", file=sys.stderr)
        raise RuntimeError(f"token_validation_request_error:{exc}")
    
    if resp.status_code == 200:
        print("Token validation successful")
        return
    elif resp.status_code in (401, 403):
        print(f"Token invalid or forbidden: {resp.status_code} - {resp.text[:256]}", file=sys.stderr)
        raise RuntimeError(f"token_invalid_or_forbidden:http_{resp.status_code}:{resp.text[:256]}")
    elif resp.status_code == 429:
        print(f"Rate limited on token validation: {resp.text[:256]}", file=sys.stderr)
        raise RuntimeError(f"rate_limited_on_token_validation:http_429:{resp.text[:256]}")
    else:
        print(f"Token validation HTTP error: {resp.status_code} - {resp.text[:256]}", file=sys.stderr)
        raise RuntimeError(f"token_validation_http_error:http_{resp.status_code}:{resp.text[:256]}")


def search_region(
    session: requests.Session,
    headers: Dict[str, str],
    query: str,
    location_address: str,
    within: str,
) -> Tuple[List[Dict], List[str]]:
    """Perform a paginated search on Eventbrite for a single region."""
    # Fix: Add start_date parameter to ensure we get upcoming events
    start_date = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    end_date = (datetime.utcnow() + timedelta(days=LOOKAHEAD_DAYS)).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    params = {
        "q": query,
        "location.address": location_address,
        "location.within": within,
        "start_date.range_start": start_date,
        "start_date.range_end": end_date,
        "expand": "venue",
        "sort_by": "date",
        "page": 1,
    }
    print(f"Searching region: {location_address} with query: {query}")
    print(f"Date range: {start_date} to {end_date}")
    
    results: List[Dict] = []
    warnings: List[str] = []
    page_count = 0
    max_pages = 50  # Fix: Prevent infinite loops
    
    while page_count < max_pages:
        url = f"{API_BASE}/events/search/"  # Fix: Ensure trailing slash consistency
        print(f"Fetching page {params['page']} for {location_address}")
        
        try:
            resp = session.get(url, headers=headers, params=params, timeout=30)
            print(f"Response status: {resp.status_code}")
        except requests.RequestException as exc:
            error_msg = f"request_error:{location_address}:{exc}"
            print(f"Request error: {error_msg}", file=sys.stderr)
            warnings.append(error_msg)
            break
        
        if resp.status_code == 404:
            warning_msg = f"404:{location_address}:{resp.text[:256]}"
            print(f"404 error: {warning_msg}", file=sys.stderr)
            warnings.append(warning_msg)
            break
        elif resp.status_code in (401, 403):
            warning_msg = f"auth_error_http_{resp.status_code}:{location_address}:{resp.text[:256]}"
            print(f"Auth error: {warning_msg}", file=sys.stderr)
            warnings.append(warning_msg)
            break
        elif resp.status_code == 429:
            warning_msg = f"rate_limited_http_429:{location_address}:{resp.text[:256]}"
            print(f"Rate limited: {warning_msg}", file=sys.stderr)
            warnings.append(warning_msg)
            # Fix: Add longer delay for rate limiting
            time.sleep(5)
            break
        elif resp.status_code != 200:
            warning_msg = f"http_{resp.status_code}:{location_address}:{resp.text[:256]}"
            print(f"HTTP error: {warning_msg}", file=sys.stderr)
            warnings.append(warning_msg)
            break
        
        try:
            data = resp.json()
        except ValueError as e:
            warning_msg = f"invalid_json_response:{location_address}:{resp.text[:256]}"
            print(f"JSON parsing error: {warning_msg}", file=sys.stderr)
            warnings.append(warning_msg)
            break
        
        page_events = data.get("events", [])
        results.extend(page_events)
        print(f"Found {len(page_events)} events on page {params['page']}")
        
        pagination = data.get("pagination", {})
        if not pagination.get("has_more_items", False):
            print(f"No more pages for {location_address}")
            break
        
        params["page"] = int(params.get("page", 1)) + 1
        page_count += 1
        time.sleep(PAGE_DELAY_SEC)
    
    print(f"Total events found for {location_address}: {len(results)}")
    return results, warnings


def normalize_events(events: List[Dict]) -> List[Dict]:
    """Normalize raw Eventbrite events into a simplified structure."""
    normalized: List[Dict] = []
    for e in events:
        try:
            venue = e.get("venue") or {}
            address = venue.get("address") or {}
            
            # Fix: Handle nested name structure properly
            name_obj = e.get("name", {})
            name = name_obj.get("text") if isinstance(name_obj, dict) else str(name_obj) if name_obj else None
            
            # Fix: Handle nested start/end structure properly
            start_obj = e.get("start", {})
            start = start_obj.get("local") if isinstance(start_obj, dict) else str(start_obj) if start_obj else None
            
            end_obj = e.get("end", {})
            end = end_obj.get("local") if isinstance(end_obj, dict) else str(end_obj) if end_obj else None
            
            normalized_event = {
                "id": e.get("id"),
                "name": name,
                "url": e.get("url"),
                "start": start,
                "end": end,
                "is_free": e.get("is_free"),
                "status": e.get("status"),
                "city": address.get("city"),
                "state": address.get("region"),
                "venue_name": venue.get("name"),
                "address": address.get("localized_address_display"),
            }
            normalized.append(normalized_event)
        except Exception as ex:
            print(f"Error normalizing event {e.get('id', 'unknown')}: {ex}", file=sys.stderr)
            continue
    
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
            # Fix: Handle different datetime formats more robustly
            if isinstance(start, str):
                # Handle various ISO formats
                if start.endswith('Z'):
                    dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
                elif '+' in start or start.endswith(('00', '30')):  # Has timezone
                    dt = datetime.fromisoformat(start)
                else:
                    # Assume local time, parse as naive datetime
                    dt = datetime.fromisoformat(start)
            else:
                continue
                
            # Convert to UTC for comparison if timezone-aware
            if dt.tzinfo is not None:
                dt = dt.utctimezone() if hasattr(dt, 'utctimezone') else dt.replace(tzinfo=None)
                
        except (ValueError, AttributeError) as ex:
            print(f"Error parsing datetime '{start}' for event {e.get('id', 'unknown')}: {ex}", file=sys.stderr)
            continue
        
        if now <= dt <= cutoff:
            filtered.append(e)
    
    return filtered


def fetch_events(token: str, query: str = DEFAULT_QUERY, states: List[str] = None, within: str = DEFAULT_WITHIN) -> Dict:
    """Fetch events for all requested states and return a structured payload."""
    if states is None:
        states = DEFAULT_STATES
    
    # Fix: Ensure proper User-Agent header
    user_agent = os.environ.get("VNN_USER_AGENT", "mt-wy-veteran-events-scraper/1.0")
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": user_agent,
        "Content-Type": "application/json",  # Fix: Add content type
    }
    
    print(f"Using query: '{query}' for states: {states} within {within}")
    
    session = requests.Session()
    # Set session defaults
    session.headers.update(headers)
    
    try:
        # Validate token early
        validate_token(session, headers)
        
        all_raw: List[Dict] = []
        all_warnings: List[str] = []
        
        for state in states:
            print(f"\n--- Searching {state} ---")
            events, warns = search_region(session, headers, query, state, within)
            all_raw.extend(events)
            all_warnings.extend(warns)
            print(f"Accumulated {len(all_raw)} total raw events so far")
        
        print(f"\n--- Processing {len(all_raw)} raw events ---")
        normalized = normalize_events(all_raw)
        print(f"Normalized to {len(normalized)} events")
        
        upcoming = filter_upcoming(normalized, LOOKAHEAD_DAYS)
        print(f"Filtered to {len(upcoming)} upcoming events")
        
        # Deduplicate events by (name, start)
        seen = set()
        unique: List[Dict] = []
        for e in upcoming:
            key = (e.get("name"), e.get("start"))
            if key in seen:
                print(f"Skipping duplicate event: {e.get('name')}")
                continue
            seen.add(key)
            unique.append(e)
        
        print(f"Final unique events: {len(unique)}")
        
        return {
            "generated": True,
            "source": "eventbrite",
            "query": query,
            "regions": states,
            "within": within,
            "count": len(unique),
            "events": unique,
            "warnings": all_warnings,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
        
    except Exception as exc:
        print(f"Error in fetch_events: {exc}", file=sys.stderr)
        raise


def main() -> int:
    """Main entry point with enhanced error handling and logging."""
    print("=== Eventbrite Veteran Events Scraper ===")
    print(f"Looking for events in the next {LOOKAHEAD_DAYS} days")
    
    try:
        token = get_token()
        print("Token acquired successfully")
        
        payload = fetch_events(token)
        print(f"\n=== SUCCESS: Fetched {payload['count']} events ===")
        
        save_json(payload, OUT_JSON)
        save_markdown(payload.get("events", []), OUT_MD)
        
        print(f"Results saved to {OUT_JSON} and {OUT_MD}")
        
        if payload.get("warnings"):
            print(f"Warnings encountered: {len(payload['warnings'])}")
            for warning in payload["warnings"]:
                print(f"  - {warning}")
        
        return 0
        
    except Exception as exc:
        error_payload = {
            "generated": False, 
            "error": str(exc),
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        save_json(error_payload, OUT_JSON)
        save_markdown([], OUT_MD)
        print(f"=== FATAL ERROR: {exc} ===", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
