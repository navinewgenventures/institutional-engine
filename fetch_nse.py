# fetch_nse.py
from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

import requests

BASE_URL = "https://www.nseindia.com"

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}

# Tuning
MAX_RETRIES = 4
INITIAL_DELAY_SEC = 1.5
TIMEOUT_SEC = 15


@dataclass(frozen=True)
class NSEClient:
    session: requests.Session

    @staticmethod
    def create() -> "NSEClient":
        """
        Creates a session and warms it up to establish cookies.
        """
        s = requests.Session()
        s.headers.update(DEFAULT_HEADERS)

        # Warm-up request to set cookies. NSE sometimes blocks direct API calls without this.
        try:
            s.get(BASE_URL, timeout=TIMEOUT_SEC)
        except Exception as e:
            # Not fatal; we still proceed.
            logging.warning(f"NSE warm-up request failed (non-fatal): {e}")

        return NSEClient(session=s)

    def get_json(self, path: str) -> Any:
        """
        Safe GET JSON with retries, backoff and jitter.
        Raises the last exception on final failure.
        """
        url = BASE_URL + path
        delay = INITIAL_DELAY_SEC

        last_err: Optional[Exception] = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.get(url, timeout=TIMEOUT_SEC)

                # Typical NSE blocks / throttling
                if resp.status_code in (403, 429):
                    raise RuntimeError(f"{resp.status_code} blocked/throttled by NSE")

                resp.raise_for_status()

                # NSE occasionally returns HTML (cloudflare / block page).
                ctype = (resp.headers.get("Content-Type") or "").lower()
                if "application/json" not in ctype:
                    # Still try json parsing, but if it fails, raise a clear error
                    try:
                        return resp.json()
                    except Exception:
                        snippet = resp.text[:200].replace("\n", " ")
                        raise RuntimeError(f"Non-JSON response from NSE: {ctype} | {snippet}")

                return resp.json()

            except Exception as e:
                last_err = e
                logging.warning(f"NSE request failed ({attempt}/{MAX_RETRIES}) {url}: {e}")

                if attempt == MAX_RETRIES:
                    break

                # exponential backoff + jitter
                jitter = random.uniform(0.0, 0.4)
                time.sleep(delay + jitter)
                delay *= 2

        # If we got here, all retries failed
        raise last_err if last_err else RuntimeError("Unknown NSE request failure")


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def _to_float(x: Any) -> float:
    if x is None:
        return 0.0
    try:
        return float(x)
    except Exception:
        # Sometimes values can be strings with commas. Rare, but safe.
        try:
            return float(str(x).replace(",", "").strip())
        except Exception:
            return 0.0


def _normalize_category(cat: Any) -> str:
    return str(cat or "").strip().upper()


# -------------------------------------------------------------------
# 1) CASH: FII + DII (single source of truth)
# -------------------------------------------------------------------

def fetch_institutional_cash(client: Optional[NSEClient] = None) -> Tuple[float, float, float, float, float, float]:
    """
    Returns:
      (fii_buy, fii_sell, fii_net, dii_buy, dii_sell, dii_net)

    Source: /api/fiidiiTradeReact (same as your NSE excel view)
    """
    c = client or NSEClient.create()
    data = c.get_json("/api/fiidiiTradeReact")

    if not isinstance(data, list):
        raise RuntimeError("Unexpected NSE cash payload shape (expected list)")

    fii_row: Optional[Dict[str, Any]] = None
    dii_row: Optional[Dict[str, Any]] = None

    for row in data:
        cat = _normalize_category(row.get("category"))
        if cat == "FII/FPI":
            fii_row = row
        elif cat == "DII":
            dii_row = row

    if not fii_row:
        raise KeyError("FII/FPI row not found in NSE cash payload")
    if not dii_row:
        raise KeyError("DII row not found in NSE cash payload")

    fii_buy = _to_float(fii_row.get("buyValue"))
    fii_sell = _to_float(fii_row.get("sellValue"))
    fii_net = _to_float(fii_row.get("netValue"))

    dii_buy = _to_float(dii_row.get("buyValue"))
    dii_sell = _to_float(dii_row.get("sellValue"))
    dii_net = _to_float(dii_row.get("netValue"))

    return fii_buy, fii_sell, fii_net, dii_buy, dii_sell, dii_net


def fetch_fii_cash(client: Optional[NSEClient] = None) -> Tuple[float, float, float]:
    """
    Backward-compatible wrapper if your main.py still calls fetch_fii_cash().
    """
    fii_buy, fii_sell, fii_net, _, _, _ = fetch_institutional_cash(client=client)
    return fii_buy, fii_sell, fii_net


# -------------------------------------------------------------------
# 2) PARTICIPANT-WISE INDEX FUTURES (FII)
# -------------------------------------------------------------------

def fetch_fii_futures(client: Optional[NSEClient] = None) -> Tuple[int, int]:
    """
    Returns:
      (net_position, total_oi)

    Source: /api/participant-wise-oi-data
    Note: NSE endpoint availability can change; your main.py already has fallback.
    """
    c = client or NSEClient.create()
    payload = c.get_json("/api/participant-wise-oi-data")

    if not isinstance(payload, dict) or "data" not in payload:
        raise RuntimeError("Unexpected futures payload shape (expected dict with 'data')")

    rows = payload.get("data") or []
    fii_row = None
    for item in rows:
        if str(item.get("clientType") or "").strip().upper() == "FII":
            fii_row = item
            break

    if not fii_row:
        raise KeyError("FII row not found in participant-wise futures payload")

    long_pos = int(_to_float(fii_row.get("futureIndexLong")))
    short_pos = int(_to_float(fii_row.get("futureIndexShort")))

    net_position = long_pos - short_pos
    total_oi = long_pos + short_pos

    return net_position, total_oi


# -------------------------------------------------------------------
# 3) INDEX OPTIONS PCR (NIFTY)
# -------------------------------------------------------------------

def fetch_index_pcr(client: Optional[NSEClient] = None, symbol: str = "NIFTY") -> Tuple[int, int, float]:
    """
    Returns:
      (total_call_oi, total_put_oi, pcr)

    Source: /api/option-chain-indices?symbol=NIFTY
    """
    c = client or NSEClient.create()
    payload = c.get_json(f"/api/option-chain-indices?symbol={symbol}")

    records = payload.get("records") if isinstance(payload, dict) else None
    data = records.get("data") if isinstance(records, dict) else None
    if not isinstance(data, list):
        raise RuntimeError("Unexpected option chain payload shape")

    total_call_oi = 0
    total_put_oi = 0

    for strike in data:
        ce = strike.get("CE")
        pe = strike.get("PE")

        if isinstance(ce, dict):
            total_call_oi += int(_to_float(ce.get("openInterest")))
        if isinstance(pe, dict):
            total_put_oi += int(_to_float(pe.get("openInterest")))

    pcr = (total_put_oi / total_call_oi) if total_call_oi else 1.0
    return total_call_oi, total_put_oi, round(pcr, 3)