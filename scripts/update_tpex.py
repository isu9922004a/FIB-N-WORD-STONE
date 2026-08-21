#!/usr/bin/env python3
"""Build a static TPEx official monthly OHLC relay from TPEx dailyQuotes.

The script intentionally derives monthly OHLC from the TPEx official all-stock
Daily Quotes endpoint. It writes one compact JSON file per stock, with one
synthetic OHLC row per month. The Cloudflare Worker can then fetch one file and
select the requested month without hitting TPEx directly.
"""
from __future__ import annotations

import argparse
import calendar
import datetime as dt
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

SCHEMA = "shitou-tpex-official-relay-v1"
SOURCE = "TPEx Official dailyQuotes"
BASE = "https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes"
REFERER = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/pricing.html"
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/151 Safari/537.36 ShitouTPExRelay/1.0"

CODE_KEYS = ["代號", "證券代號", "股票代號", "SecuritiesCompanyCode", "Code", "code"]
NAME_KEYS = ["名稱", "證券名稱", "股票名稱", "SecuritiesCompanyName", "Name", "name"]
OPEN_KEYS = ["開盤", "開盤價", "Open", "open"]
HIGH_KEYS = ["最高", "最高價", "High", "high"]
LOW_KEYS = ["最低", "最低價", "Low", "low"]
CLOSE_KEYS = ["收盤", "收盤價", "Close", "close"]
VOLUME_KEYS = ["成交股數", "成交量", "TradeVolume", "Volume", "volume"]


def log(msg: str) -> None:
    print(msg, flush=True)


def parse_number(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().replace(",", "").replace("＋", "+").replace("－", "-")
    text = text.replace("−", "-")
    if not text or text in {"--", "---", "----", "N/A", "nan", "None"}:
        return None
    # Some official cells may contain annotations; keep the first ordinary number.
    m = re.search(r"[-+]?\d+(?:\.\d+)?", text)
    if not m:
        return None
    try:
        return float(m.group(0))
    except ValueError:
        return None


def parse_volume(value: Any) -> int:
    n = parse_number(value)
    return int(n) if n is not None and n >= 0 else 0


def normalize_field(x: Any) -> str:
    if isinstance(x, dict):
        for k in ("title", "name", "label", "field"):
            if x.get(k):
                return str(x[k]).strip()
    return str(x or "").strip()


def find_index(fields: List[str], candidates: Iterable[str]) -> Optional[int]:
    normalized = {re.sub(r"\s+", "", f): i for i, f in enumerate(fields)}
    for c in candidates:
        key = re.sub(r"\s+", "", c)
        if key in normalized:
            return normalized[key]
    # fuzzy fallback
    for i, f in enumerate(fields):
        compact = re.sub(r"\s+", "", f)
        for c in candidates:
            if re.sub(r"\s+", "", c) in compact:
                return i
    return None


def roc_to_iso(text: str) -> Optional[str]:
    t = str(text or "").strip()
    if re.fullmatch(r"\d{8}", t):
        # Gregorian yyyymmdd
        try:
            return dt.datetime.strptime(t, "%Y%m%d").date().isoformat()
        except ValueError:
            return None
    if re.fullmatch(r"\d{7}", t):
        # ROC yyyMMdd, e.g. 1150820
        try:
            year = int(t[:3]) + 1911
            return dt.date(year, int(t[3:5]), int(t[5:7])).isoformat()
        except ValueError:
            return None
    m = re.fullmatch(r"(\d{2,4})[/-](\d{1,2})[/-](\d{1,2})", t)
    if m:
        y, mo, d = map(int, m.groups())
        if y < 1911:
            y += 1911
        try:
            return dt.date(y, mo, d).isoformat()
        except ValueError:
            return None
    return None


def payload_date(payload: Dict[str, Any]) -> Optional[str]:
    for key in ("date", "Date", "queryDate", "tradeDate"):
        if key in payload:
            iso = roc_to_iso(str(payload[key]))
            if iso:
                return iso
    return None


def request_json(day: dt.date, retries: int = 3, timeout: int = 30) -> Optional[Dict[str, Any]]:
    datestr = day.strftime("%Y/%m/%d")
    query_variants = [
        {"l": "zh-tw", "s": "0,asc,0", "o": "json", "date": datestr},
        {"response": "json", "date": datestr},
    ]
    last_error = None
    for query in query_variants:
        url = BASE + "?" + urllib.parse.urlencode(query)
        for attempt in range(1, retries + 1):
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "application/json,text/plain,*/*",
                    "Referer": REFERER,
                    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.7",
                },
            )
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    final_url = resp.geturl()
                    raw = resp.read()
                    ctype = resp.headers.get("Content-Type", "")
                    if "/errors" in final_url:
                        raise RuntimeError(f"TPEx redirected to /errors: {final_url}")
                    text = raw.decode("utf-8", "replace").lstrip("\ufeff").strip()
                    if not text or text.startswith("<"):
                        raise RuntimeError(f"TPEx returned non-JSON ({ctype}): {text[:120]}")
                    payload = json.loads(text)
                    pdate = payload_date(payload)
                    if pdate and pdate != day.isoformat():
                        # Do not accidentally label a previous trading day's payload as a holiday.
                        return None
                    return payload
            except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, RuntimeError, json.JSONDecodeError) as e:
                last_error = e
                if attempt < retries:
                    time.sleep(min(8, attempt * 2))
        # Try the second official query form before giving up.
    if last_error:
        raise RuntimeError(f"TPEx fetch failed for {day}: {last_error}")
    return None


def row_from_mapping(mapping: Dict[str, Any], day: dt.date) -> Optional[Dict[str, Any]]:
    def pick(keys: List[str]) -> Any:
        for k in keys:
            if k in mapping and mapping[k] not in (None, ""):
                return mapping[k]
        return None

    code = str(pick(CODE_KEYS) or "").strip()
    if not re.fullmatch(r"\d{4,6}", code):
        return None
    name = str(pick(NAME_KEYS) or "").strip()
    o, h, l, c = map(parse_number, [pick(OPEN_KEYS), pick(HIGH_KEYS), pick(LOW_KEYS), pick(CLOSE_KEYS)])
    if None in (o, h, l, c):
        return None
    return {
        "date": day.isoformat(),
        "stock": code,
        "name": name,
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "volume": parse_volume(pick(VOLUME_KEYS)),
    }


def extract_daily_rows(payload: Dict[str, Any], day: dt.date) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    # New TPEx JSON: tables[].fields + tables[].data
    for table in payload.get("tables") or []:
        fields_raw = table.get("fields") or table.get("columns") or []
        data = table.get("data") or table.get("rows") or []
        fields = [normalize_field(x) for x in fields_raw]
        if not fields or not isinstance(data, list):
            continue
        idx_code = find_index(fields, CODE_KEYS)
        idx_close = find_index(fields, CLOSE_KEYS)
        idx_open = find_index(fields, OPEN_KEYS)
        idx_high = find_index(fields, HIGH_KEYS)
        idx_low = find_index(fields, LOW_KEYS)
        if None in (idx_code, idx_close, idx_open, idx_high, idx_low):
            continue
        for raw in data:
            if isinstance(raw, dict):
                row = row_from_mapping(raw, day)
            elif isinstance(raw, list):
                mapping = {fields[i]: raw[i] if i < len(raw) else None for i in range(len(fields))}
                row = row_from_mapping(mapping, day)
            else:
                row = None
            if row:
                out.append(row)

    # Object-array fallbacks used by some OpenAPI-like variants.
    if not out:
        for key in ("data", "rows", "aaData"):
            arr = payload.get(key)
            if not isinstance(arr, list):
                continue
            for raw in arr:
                if isinstance(raw, dict):
                    row = row_from_mapping(raw, day)
                    if row:
                        out.append(row)

    # Conservative positional fallback for the common TPEx dailyQuotes layout:
    # 代號, 名稱, 收盤, 漲跌, 開盤, 最高, 最低, 均價, 成交股數, ...
    if not out:
        for table in payload.get("tables") or []:
            data = table.get("data") or []
            for raw in data:
                if not isinstance(raw, list) or len(raw) < 9:
                    continue
                code = str(raw[0]).strip()
                if not re.fullmatch(r"\d{4,6}", code):
                    continue
                c = parse_number(raw[2])
                o = parse_number(raw[4])
                h = parse_number(raw[5])
                l = parse_number(raw[6])
                if None in (o, h, l, c):
                    continue
                out.append({
                    "date": day.isoformat(),
                    "stock": code,
                    "name": str(raw[1]).strip(),
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "volume": parse_volume(raw[8]),
                })

    # Deduplicate by code.
    dedup: Dict[str, Dict[str, Any]] = {}
    for row in out:
        dedup[row["stock"]] = row
    return list(dedup.values())


def month_key(day: dt.date) -> str:
    return day.strftime("%Y%m")


def month_ago_first(today: dt.date, months: int) -> dt.date:
    y = today.year
    m = today.month - months + 1
    while m <= 0:
        y -= 1
        m += 12
    return dt.date(y, m, 1)


def daterange(start: dt.date, end: dt.date) -> Iterable[dt.date]:
    cur = start
    one = dt.timedelta(days=1)
    while cur <= end:
        yield cur
        cur += one


def update_month(existing: Optional[Dict[str, Any]], daily: Dict[str, Any]) -> Dict[str, Any]:
    if existing is None:
        return {
            "month": month_key(dt.date.fromisoformat(daily["date"])),
            "date": daily["date"],
            "firstDate": daily["date"],
            "open": daily["open"],
            "high": daily["high"],
            "low": daily["low"],
            "close": daily["close"],
            "volume": daily["volume"],
        }
    if daily["date"] <= str(existing.get("date") or ""):
        return existing
    existing["high"] = max(float(existing["high"]), float(daily["high"]))
    existing["low"] = min(float(existing["low"]), float(daily["low"]))
    existing["close"] = daily["close"]
    existing["date"] = daily["date"]
    existing["volume"] = int(existing.get("volume") or 0) + int(daily.get("volume") or 0)
    return existing


def load_stock_file(path: Path) -> Tuple[str, Dict[str, Dict[str, Any]]]:
    if not path.exists():
        return "", {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        rows = payload.get("rows") or []
        months = {str(r.get("month") or str(r.get("date", ""))[:7].replace("-", "")): dict(r) for r in rows if r}
        return str(payload.get("name") or ""), months
    except Exception:
        return "", {}


def write_stock_file(data_dir: Path, stock: str, name: str, months: Dict[str, Dict[str, Any]], keep_months: int) -> None:
    ordered_keys = sorted(k for k in months if re.fullmatch(r"\d{6}", k))[-keep_months:]
    rows = [months[k] for k in ordered_keys]
    if not rows:
        return
    payload = {
        "schema": SCHEMA,
        "source": SOURCE,
        "provider": "TPEx Official mirror via GitHub Actions",
        "market": "TPEx",
        "stock": stock,
        "name": name,
        "generatedAt": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "coverage": {
            "fromMonth": ordered_keys[0],
            "toMonth": ordered_keys[-1],
            "months": len(ordered_keys),
            "latestDate": rows[-1].get("date"),
        },
        "rows": rows,
    }
    path = data_dir / f"{stock}.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")
    tmp.replace(path)


def process_days(days: List[dt.date], data_dir: Path, keep_months: int, sleep_seconds: float, fail_fast: bool) -> Dict[str, Any]:
    cache: Dict[str, Tuple[str, Dict[str, Dict[str, Any]]]] = {}
    stats = {"daysRequested": 0, "daysWithData": 0, "stockRows": 0, "errors": []}

    for i, day in enumerate(days, 1):
        if day.weekday() >= 5:
            continue
        stats["daysRequested"] += 1
        try:
            payload = request_json(day)
            rows = extract_daily_rows(payload or {}, day) if payload else []
            if rows:
                stats["daysWithData"] += 1
                stats["stockRows"] += len(rows)
                for daily in rows:
                    stock = daily["stock"]
                    if stock not in cache:
                        cache[stock] = load_stock_file(data_dir / f"{stock}.json")
                    old_name, months = cache[stock]
                    name = daily.get("name") or old_name
                    mk = month_key(day)
                    months[mk] = update_month(months.get(mk), daily)
                    cache[stock] = (name, months)
            if i % 25 == 0 or rows:
                log(f"[{i}/{len(days)}] {day} rows={len(rows)}")
        except Exception as e:
            msg = f"{day}: {e}"
            stats["errors"].append(msg)
            log("ERROR " + msg)
            if fail_fast:
                raise
        time.sleep(max(0.0, sleep_seconds))

    for stock, (name, months) in sorted(cache.items()):
        write_stock_file(data_dir, stock, name, months, keep_months)

    # Manifest is useful for human diagnostics; Worker does not depend on it.
    manifest = {
        "schema": SCHEMA,
        "source": SOURCE,
        "generatedAt": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "stocksUpdated": len(cache),
        **stats,
    }
    (data_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return manifest


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["bootstrap", "update"], default="update")
    ap.add_argument("--months", type=int, default=72, help="Bootstrap history and retained months")
    ap.add_argument("--update-days", type=int, default=14, help="Update mode lookback window to catch missed runs")
    ap.add_argument("--sleep", type=float, default=0.35, help="Delay between official requests")
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--fail-fast", action="store_true")
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    today = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).date()

    if args.mode == "bootstrap":
        start = month_ago_first(today, max(24, args.months))
    else:
        start = today - dt.timedelta(days=max(2, args.update_days) - 1)

    days = list(daterange(start, today))
    log(f"mode={args.mode} range={start}..{today} calendarDays={len(days)} keepMonths={args.months}")
    manifest = process_days(days, data_dir, args.months, args.sleep, args.fail_fast)
    log(json.dumps(manifest, ensure_ascii=False))

    # A bootstrap that got zero official data should fail loudly rather than publish an empty relay.
    if args.mode == "bootstrap" and manifest["daysWithData"] == 0:
        log("FATAL: bootstrap received zero TPEx trading days")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
