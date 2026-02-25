import requests
import time
import logging

BASE_URL = "https://www.nseindia.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive"
}

MAX_RETRIES = 4
INITIAL_DELAY = 2  # seconds


# ==========================================================
# SESSION CREATION
# ==========================================================

def create_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    session.get(BASE_URL, timeout=10)  # establish cookies
    return session


# ==========================================================
# SAFE REQUEST WITH RETRY + BACKOFF
# ==========================================================

def safe_get_json(session, url):
    delay = INITIAL_DELAY

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, timeout=15)

            # If NSE blocks
            if response.status_code == 403:
                raise Exception("403 Forbidden from NSE")

            response.raise_for_status()

            return response.json()

        except Exception as e:
            logging.warning(
                f"NSE request failed (Attempt {attempt}/{MAX_RETRIES}): {str(e)}"
            )

            if attempt == MAX_RETRIES:
                logging.error("Max retries reached. Failing request.")
                raise

            time.sleep(delay)
            delay *= 2  # exponential backoff


# ==========================================================
# 1️⃣ FII / DII CASH ACTIVITY
# ==========================================================

def fetch_fii_cash():
    session = create_session()
    url = BASE_URL + "/api/fiidiiTradeReact"

    data = safe_get_json(session, url)

    # Filter only FII/FPI row
    fii_row = next(
        item for item in data
        if item.get("category") == "FII/FPI"
    )

    fii_buy = float(fii_row["buyValue"])
    fii_sell = float(fii_row["sellValue"])
    fii_net = float(fii_row["netValue"])

    return fii_buy, fii_sell, fii_net


# ==========================================================
# 2️⃣ PARTICIPANT-WISE INDEX FUTURES
# ==========================================================

def fetch_fii_futures():
    session = create_session()

    url = BASE_URL + "/api/participant-wise-oi-data"

    data = safe_get_json(session, url)

    fii_row = next(
        item for item in data["data"]
        if item.get("clientType") == "FII"
    )

    long_pos = int(fii_row["futureIndexLong"])
    short_pos = int(fii_row["futureIndexShort"])

    net_position = long_pos - short_pos
    total_oi = long_pos + short_pos

    return net_position, total_oi


# ==========================================================
# 3️⃣ INDEX OPTIONS PCR
# ==========================================================

def fetch_index_pcr():
    session = create_session()

    url = BASE_URL + "/api/option-chain-indices?symbol=NIFTY"
    data = safe_get_json(session, url)

    total_call_oi = 0
    total_put_oi = 0

    for strike in data["records"]["data"]:
        if "CE" in strike:
            total_call_oi += strike["CE"]["openInterest"]
        if "PE" in strike:
            total_put_oi += strike["PE"]["openInterest"]

    pcr = total_put_oi / total_call_oi if total_call_oi != 0 else 1

    return total_call_oi, total_put_oi, round(pcr, 3)

    # ==========================================================
    # 4 Institutional Cash
    # ==========================================================

def fetch_institutional_cash():
    """
    Returns: (fii_buy, fii_sell, fii_net, dii_buy, dii_sell, dii_net)
    All values are floats (₹ Crore), pulled from NSE participant-wise cash data.
    """
    session = create_session()
    url = BASE_URL + "/api/fiidiiTradeReact"
    data = safe_get_json(session, url)

    # NSE returns a list of rows, typically containing DII and FII/FPI entries for a date
    fii = None
    dii = None

    for row in data:
        cat = (row.get("category") or "").strip().upper()
        if cat == "FII/FPI":
            fii = row
        elif cat == "DII":
            dii = row

    if not fii:
        raise KeyError("FII/FPI row not found in NSE payload")
    if not dii:
        raise KeyError("DII row not found in NSE payload")

    fii_buy = float(fii["buyValue"])
    fii_sell = float(fii["sellValue"])
    fii_net = float(fii["netValue"])

    dii_buy = float(dii["buyValue"])
    dii_sell = float(dii["sellValue"])
    dii_net = float(dii["netValue"])

    return fii_buy, fii_sell, fii_net, dii_buy, dii_sell, dii_net