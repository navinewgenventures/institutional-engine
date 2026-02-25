import datetime
import logging
import sys

from db import upsert, fetch_last_n
from calculations import (
    calculate_z_score,
    calculate_futures_z,
    calculate_sts,
    calculate_ema
)
from classification import classify_bias, classify_phase
from telegram import send_message
from fetch_nse import (
    fetch_fii_cash,
    fetch_fii_futures,
    fetch_index_pcr
)

# --------------------------------------------------
# Logging Setup (Render Friendly)
# --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout
)


def run():

    today = datetime.date.today()
    logging.info(f"Starting Institutional Engine for {today}")

    # ==========================================================
    # 1️⃣ FETCH LIVE NSE DATA
    # ==========================================================

    fii_buy, fii_sell, fii_net = fetch_fii_cash()

    # ---- Futures Fetch (Safe Mode) ----
    try:
        net_position, total_oi = fetch_fii_futures()
    except Exception:
        logging.warning("Futures fetch failed. Using neutral fallback.")
        net_position = 0
        total_oi = 0

    # ---- PCR Fetch (Safe Mode) ----
    try:
        total_call_oi, total_put_oi, pcr_today = fetch_index_pcr()
    except Exception:
        logging.warning("PCR fetch failed. Using neutral fallback.")
        total_call_oi = 0
        total_put_oi = 0
        pcr_today = 1

    logging.info("NSE data fetched successfully")

    # ==========================================================
    # 2️⃣ STORE RAW DATA
    # ==========================================================

    upsert("fii_cash_raw", {
        "trade_date": str(today),
        "fii_buy": fii_buy,
        "fii_sell": fii_sell,
        "fii_net": fii_net
    })

    upsert("index_futures_raw", {
        "trade_date": str(today),
        "net_position": net_position,
        "oi": total_oi,
        "oi_change": total_oi
    })

    upsert("options_summary_raw", {
        "trade_date": str(today),
        "total_call_oi": total_call_oi,
        "total_put_oi": total_put_oi,
        "pcr": pcr_today
    })

    logging.info("Raw data stored successfully")

    # ==========================================================
    # 3️⃣ FETCH 30-DAY HISTORICAL DATA
    # ==========================================================

    cash_hist = fetch_last_n("fii_cash_raw", "fii_net", 30)
    pos_hist = fetch_last_n("index_futures_raw", "net_position", 30)
    oi_hist = fetch_last_n("index_futures_raw", "oi_change", 30)
    pcr_hist = fetch_last_n("options_summary_raw", "pcr", 30)

    if min(len(cash_hist), len(pos_hist), len(oi_hist), len(pcr_hist)) < 20:
        logging.warning("Insufficient historical data (need 30 days).")
        send_message("⚠ Institutional Engine: Not enough historical data yet.")
        return

    # ==========================================================
    # 4️⃣ CALCULATE Z-SCORES
    # ==========================================================

    cash_z = calculate_z_score(fii_net, cash_hist)
    position_z = calculate_z_score(net_position, pos_hist)
    oi_z = calculate_z_score(total_oi, oi_hist)
    pcr_z = calculate_z_score(pcr_today, pcr_hist)

    futures_z = calculate_futures_z(position_z, oi_z)
    sts = calculate_sts(futures_z, cash_z, pcr_z)

    logging.info("Z-scores and STS calculated")

    # ==========================================================
    # 5️⃣ CALCULATE IRS (EMA)
    # ==========================================================

    previous_irs_list = fetch_last_n("institutional_regime", "irs", 1)
    previous_irs = previous_irs_list[0] if previous_irs_list else sts

    irs = calculate_ema(sts, previous_irs, period=10)

    bias = classify_bias(sts)
    phase = classify_phase(irs)

    logging.info("IRS calculated and classified")

    # ==========================================================
    # 6️⃣ STORE DERIVED DATA
    # ==========================================================

    upsert("institutional_zscores", {
        "trade_date": str(today),
        "cash_z": round(cash_z, 3),
        "futures_z": round(futures_z, 3),
        "pcr_z": round(pcr_z, 3),
        "sts": round(sts, 3)
    })

    upsert("institutional_regime", {
        "trade_date": str(today),
        "sts": round(sts, 3),
        "irs": round(irs, 3),
        "market_phase": phase,
        "tomorrow_bias": bias
    })

    logging.info("Derived data stored successfully")

    # ==========================================================
    # 7️⃣ TELEGRAM MESSAGE
    # ==========================================================

    message = f"""
📊 Institutional Intelligence – {today}

FII Net: ₹{fii_net:,.0f}

Cash Z: {cash_z:.2f}
Futures Z: {futures_z:.2f}
PCR Z: {pcr_z:.2f}

—————————————
Short-Term Signal (STS): {sts:.2f}
Tomorrow Bias: {bias}

Regime Score (IRS): {irs:.2f}
Market Phase: {phase}
—————————————
"""

    send_message(message)

    logging.info("Telegram report sent successfully")


# --------------------------------------------------
# EXECUTION WRAPPER (CRITICAL FOR RENDER CRON)
# --------------------------------------------------

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logging.exception("Institutional Engine Failed")
        try:
            
        except Exception:
            pass