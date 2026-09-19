#!/usr/bin/env python3
"""
MNQ TopstepX/ProjectX market-data publisher.

Security design:
- Reads TOPSTEP_USERNAME and TOPSTEP_API_KEY only from environment variables.
- Calls authentication + market-data endpoints only.
- Does NOT call account, position, order, cancel, or trade endpoints.
- Writes only market data / derived analytics to public/mnq_latest.json.

Intended execution:
- GitHub Actions at 05:20 America/Los_Angeles, weekdays.
- Two UTC cron entries are used to survive DST changes; this script guards
  scheduled runs so only the invocation occurring during the 05:00 PT hour runs.
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone, time
from pathlib import Path
from urllib import request, error
from zoneinfo import ZoneInfo

API_BASE = os.getenv("TOPSTEP_API_BASE", "https://api.topstepx.com").rstrip("/")
USERNAME = os.getenv("TOPSTEP_USERNAME")
API_KEY = os.getenv("TOPSTEP_API_KEY")
LIVE = os.getenv("TOPSTEP_LIVE_DATA", "true").lower() in {"1", "true", "yes", "y"}

LA = ZoneInfo("America/Los_Angeles")
UTC = timezone.utc

OUT = Path("public/mnq_latest.json")
OUT.parent.mkdir(parents=True, exist_ok=True)


def now_utc():
    return datetime.now(UTC)


def iso_z(dt):
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def should_run():
    # workflow_dispatch can force a run regardless of clock.
    if os.getenv("GITHUB_EVENT_NAME") == "workflow_dispatch":
        return True
    local = datetime.now(LA)
    # Scheduled workflow runs twice in UTC to remain DST-safe.
    return local.weekday() < 5 and local.hour == 5


def post_json(path, payload, token=None):
    data = json.dumps(payload).encode("utf-8")
    headers = {
        "Accept": "text/plain",
        "Content-Type": "application/json",
        "User-Agent": "MNQ-Premarket-Collector/1.0",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    req = request.Request(API_BASE + path, data=data, headers=headers, method="POST")
    try:
        with request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw)
    except error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{path}: HTTP {e.code}: {body}") from e
    except error.URLError as e:
        raise RuntimeError(f"{path}: network error: {e}") from e


def authenticate():
    if not USERNAME or not API_KEY:
        raise RuntimeError("Missing TOPSTEP_USERNAME or TOPSTEP_API_KEY GitHub secret.")
    r = post_json("/api/Auth/loginKey", {"userName": USERNAME, "apiKey": API_KEY})
    token = r.get("token")
    if not r.get("success") or not token:
        raise RuntimeError(
            f"Topstep authentication failed. "
            f"errorCode={r.get('errorCode')} errorMessage={r.get('errorMessage')}"
        )
    return token


def looks_like_mnq(c):
    fields = " ".join(
        str(c.get(k, "")) for k in ("name", "description", "symbolId")
    ).upper()
    return (
        "MNQ" in fields
        or "MICRO E-MINI NASDAQ-100" in fields
        or "MICRO E-MINI NASDAQ 100" in fields
    )


def get_active_mnq(token):
    r = post_json("/api/Contract/search", {"searchText": "MNQ", "live": LIVE}, token)
    contracts = [c for c in (r.get("contracts") or []) if looks_like_mnq(c)]

    active = [c for c in contracts if c.get("activeContract") is True]
    if active:
        return active[0]
    if contracts:
        # Search results are generally ordered by relevance; keep a fallback.
        return contracts[0]

    r = post_json("/api/Contract/available", {"live": LIVE}, token)
    contracts = [c for c in (r.get("contracts") or []) if looks_like_mnq(c)]
    active = [c for c in contracts if c.get("activeContract") is True]
    if active:
        return active[0]
    if contracts:
        return contracts[0]

    raise RuntimeError("No MNQ contract returned by TopstepX/ProjectX.")


def get_bars(token, contract_id):
    end = now_utc()
    # Enough history for previous week + current week and session analysis.
    start = end - timedelta(days=15)
    payload = {
        "contractId": contract_id,
        "live": LIVE,
        "startTime": iso_z(start),
        "endTime": iso_z(end),
        "unit": 2,              # Minute
        "unitNumber": 5,        # 5-minute bars
        "limit": 10000,
        "includePartialBar": True,
    }
    r = post_json("/api/History/retrieveBars", payload, token)
    bars = r.get("bars") or []
    if not bars:
        raise RuntimeError(
            f"Topstep returned zero bars. errorCode={r.get('errorCode')} "
            f"errorMessage={r.get('errorMessage')}"
        )
    return bars


def parse_bar_time(bar):
    # ProjectX bar timestamp is normally "t".
    raw = bar.get("t") or bar.get("time") or bar.get("timestamp")
    if not raw:
        raise ValueError("Bar contains no timestamp field.")
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def normalize_bar(bar):
    # Preserve original API fields while adding explicit aliases for easy consumers.
    return {
        "t": bar.get("t") or bar.get("time") or bar.get("timestamp"),
        "o": bar.get("o", bar.get("open")),
        "h": bar.get("h", bar.get("high")),
        "l": bar.get("l", bar.get("low")),
        "c": bar.get("c", bar.get("close")),
        "v": bar.get("v", bar.get("volume")),
    }


def high_low(rows):
    if not rows:
        return None
    highs = [float(b["h"]) for b in rows if b.get("h") is not None]
    lows = [float(b["l"]) for b in rows if b.get("l") is not None]
    if not highs or not lows:
        return None
    return {"high": max(highs), "low": min(lows)}


def prior_calendar_day_levels(bars):
    """
    Simple PT calendar-day reference. The downstream memo can additionally
    calculate exchange/session definitions from the raw bars.
    """
    local_now = datetime.now(LA)
    today = local_now.date()
    prior_dates = sorted({
        parse_bar_time(b).astimezone(LA).date()
        for b in bars
        if parse_bar_time(b).astimezone(LA).date() < today
    }, reverse=True)
    if not prior_dates:
        return None
    d = prior_dates[0]
    rows = [
        normalize_bar(b) for b in bars
        if parse_bar_time(b).astimezone(LA).date() == d
    ]
    x = high_low(rows)
    if x:
        x["date"] = d.isoformat()
    return x


def prior_week_levels(bars):
    local_now = datetime.now(LA)
    current_monday = local_now.date() - timedelta(days=local_now.weekday())
    previous_monday = current_monday - timedelta(days=7)
    previous_sunday = current_monday - timedelta(days=1)

    rows = []
    for b in bars:
        d = parse_bar_time(b).astimezone(LA).date()
        if previous_monday <= d <= previous_sunday:
            rows.append(normalize_bar(b))
    x = high_low(rows)
    if x:
        x["startDate"] = previous_monday.isoformat()
        x["endDate"] = previous_sunday.isoformat()
    return x


def session_levels(bars, start_local, end_local, label_date):
    """
    PT-local window helper. Window may cross midnight.
    label_date is the local date on which the session starts.
    """
    start_dt = datetime.combine(label_date, start_local, tzinfo=LA)
    end_date = label_date if end_local > start_local else label_date + timedelta(days=1)
    end_dt = datetime.combine(end_date, end_local, tzinfo=LA)

    rows = []
    for b in bars:
        dt = parse_bar_time(b).astimezone(LA)
        if start_dt <= dt < end_dt:
            rows.append(normalize_bar(b))
    x = high_low(rows)
    if x:
        x["start"] = start_dt.isoformat()
        x["end"] = end_dt.isoformat()
    return x


def simple_volume_stats(bars):
    vals = [float(b.get("v") or 0) for b in bars[-288:] if b.get("v") is not None]
    if not vals:
        return None
    vals_sorted = sorted(vals)
    n = len(vals_sorted)
    median = vals_sorted[n // 2] if n % 2 else (vals_sorted[n//2-1] + vals_sorted[n//2]) / 2
    return {
        "barsSampled": len(vals),
        "median5mVolume": median,
        "max5mVolume": max(vals),
    }


def build_output(contract, bars):
    normalized = [normalize_bar(b) for b in bars]
    local_now = datetime.now(LA)
    today = local_now.date()
    yesterday = today - timedelta(days=1)

    # These are defaults only. Raw bars are included so the memo can use
    # a preferred session definition without recollecting data.
    # Asia default: 17:00-00:00 PT from prior calendar date.
    # London default: 00:00-05:20 PT on current date.
    asia = session_levels(bars, time(17, 0), time(0, 0), yesterday)
    london = session_levels(bars, time(0, 0), time(5, 20), today)

    latest = normalized[-1]

    return {
        "schemaVersion": 1,
        "generatedUtc": iso_z(now_utc()),
        "generatedPacific": datetime.now(LA).isoformat(),
        "source": "TopstepX / ProjectX CME market data",
        "liveRequested": LIVE,
        "security": {
            "containsCredentials": False,
            "containsAccountData": False,
            "containsOrdersOrPositions": False,
        },
        "contract": {
            "id": contract.get("id"),
            "name": contract.get("name"),
            "description": contract.get("description"),
            "symbolId": contract.get("symbolId"),
            "activeContract": contract.get("activeContract"),
        },
        "latestBar": latest,
        "derived": {
            "previousCalendarDayPT": prior_calendar_day_levels(bars),
            "previousCalendarWeekPT": prior_week_levels(bars),
            "asiaDefaultPT_1700_0000": asia,
            "londonDefaultPT_0000_0520": london,
            "recentVolume": simple_volume_stats(normalized),
        },
        "notes": [
            "Raw 5-minute OHLCV bars are included for independent calculation.",
            "Session definitions are configurable downstream; included Asia/London values are defaults.",
            "No footprint/bid-ask attribution is inferred from OHLCV alone."
        ],
        "bars5m": normalized,
    }


def main():
    if not should_run():
        local = datetime.now(LA)
        print(f"Skipping duplicate DST cron invocation. Pacific time is {local.isoformat()}")
        return

    token = authenticate()
    contract = get_active_mnq(token)
    bars = get_bars(token, contract["id"])
    payload = build_output(contract, bars)

    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"Wrote {OUT}")
    print(f"Contract: {payload['contract']['name']} ({payload['contract']['id']})")
    print(f"Bars: {len(payload['bars5m'])}")
    print(f"Latest: {payload['latestBar']}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
