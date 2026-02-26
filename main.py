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
from classification import classify_phase
from telegram import send_message
from fetch_nse import (
    NSEClient,
    fetch_institutional_cash,
    fetch_fii_futures,
    fetch_index_pcr
)

# ==========================================================
# Logging Setup
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout
)


def run():

    today = datetime.date.today()
    logging.info(f"Starting Institutional Engine for {today}")

    # ==========================================================
    # 1️⃣ FETCH LIVE NSE DATA (Single Session)
    # ==========================================================

    client = NSEClient.create()

    fii_buy, fii_sell, fii_net, dii_buy, dii_sell, dii_net = fetch_institutional_cash(client)
    combined_net = fii_net + dii_net

    # ==========================================================
    # PREVENT DUPLICATE SEND (Send only when data changes)
    # ==========================================================

    existing_today = fetch_last_n("fii_cash_raw", "fii_net", 1)

    if existing_today:
        last_stored_net = existing_today[0]
        if round(last_stored_net, 2) == round(fii_net, 2):
            logging.info("FII data unchanged. Skipping Telegram send.")
            return

    # ---- Futures Fetch ----
    try:
        net_position, total_oi = fetch_fii_futures(client)
    except Exception:
        logging.warning("Futures fetch failed. Using neutral fallback.")
        net_position = 0
        total_oi = 0

    # ---- PCR Fetch ----
    try:
        total_call_oi, total_put_oi, pcr_today = fetch_index_pcr(client)
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

    upsert("dii_cash_raw", {
        "trade_date": str(today),
        "dii_buy": dii_buy,
        "dii_sell": dii_sell,
        "dii_net": dii_net
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
    # 3️⃣ FETCH HISTORICAL DATA
    # ==========================================================

    cash_hist = fetch_last_n("fii_cash_raw", "fii_net", 30)
    pos_hist = fetch_last_n("index_futures_raw", "net_position", 30)
    oi_hist = fetch_last_n("index_futures_raw", "oi_change", 30)
    pcr_hist = fetch_last_n("options_summary_raw", "pcr", 30)

    if min(len(cash_hist), len(pos_hist), len(oi_hist), len(pcr_hist)) < 20:
        logging.warning("Insufficient historical data.")
        return

    # ==========================================================
    # 4️⃣ CALCULATE SIGNALS
    # ==========================================================

    cash_z = calculate_z_score(fii_net, cash_hist)
    position_z = calculate_z_score(net_position, pos_hist)
    oi_z = calculate_z_score(total_oi, oi_hist)
    pcr_z = calculate_z_score(pcr_today, pcr_hist)

    futures_z = calculate_futures_z(position_z, oi_z)
    sts = calculate_sts(futures_z, cash_z, pcr_z)

    previous_irs_list = fetch_last_n("institutional_regime", "irs", 1)
    previous_irs = previous_irs_list[0] if previous_irs_list else sts

    irs = calculate_ema(sts, previous_irs, period=10)
    phase = classify_phase(irs)

    logging.info("Signals calculated")

    # ==========================================================
    # 5️⃣ STORE DERIVED DATA
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
        "market_phase": phase
    })

    logging.info("Derived data stored successfully")

    # ==========================================================
    # 6️⃣ FLOW STRUCTURE CLASSIFICATION
    # ==========================================================

    if fii_net > 0 and dii_net > 0:
        flow_structure = "Institutional Accumulation"
    elif fii_net > 0 and dii_net < 0:
        flow_structure = "Foreign Accumulation | Domestic Distribution"
    elif fii_net < 0 and dii_net > 0:
        flow_structure = "Domestic Absorption"
    elif fii_net < 0 and dii_net < 0:
        flow_structure = "Institutional Distribution"
    else:
        flow_structure = "Neutral Flow"

    # ==========================================================
    # 7️⃣ TELEGRAM MESSAGE
    # ==========================================================

    message = f"""
🏛 Institutional Flow Dashboard
Date: {today.strftime("%d %b %Y")}

💰 FII Net: ₹{fii_net:,.0f} Cr
🏦 DII Net: ₹{dii_net:,.0f} Cr
🔁 Combined Net: ₹{combined_net:,.0f} Cr

───────────────
📊 Cash Z: {cash_z:.2f}
📈 Futures Z: {futures_z:.2f}
📊 PCR Z: {pcr_z:.2f}
───────────────
⚡ STS: {sts:.2f}
🏛 IRS: {irs:.2f}
🌡 Phase: {phase}
───────────────
🧠 Flow Structure: {flow_structure}

Quantitative institutional positioning analysis.
For informational purposes only.
"""

    send_message(message)
    logging.info("Telegram report sent successfully")


# ==========================================================
# EXECUTION WRAPPER
# ==========================================================

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logging.exception("Institutional Engine Failed")
        try:
            send_message(f"⚠ Institutional Engine Failed:\n{str(e)}")
        except Exception:
            pass