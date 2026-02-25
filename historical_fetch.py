import datetime
import logging

from db import upsert, fetch_last_n
from calculations import calculate_z_score, calculate_ema
from classification import classify_bias, classify_phase
from fetch_nse import create_session, safe_get_json

BASE_URL = "https://www.nseindia.com"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def fetch_full_fii_history():
    session = create_session()
    url = BASE_URL + "/api/fiidiiTradeReact"
    data = safe_get_json(session, url)
    return list(reversed(data))  # oldest first


def run_backfill():

    fii_history = fetch_full_fii_history()
    previous_irs = None

    for entry in fii_history:

        if entry.get("category") != "FII/FPI":
            continue

        trade_date = datetime.datetime.strptime(
            entry["date"], "%d-%b-%Y"
        ).date()

        fii_net = float(entry["netValue"])

        logging.info(f"Processing {trade_date}")

        # Insert raw cash
        upsert("fii_cash_raw", {
            "trade_date": str(trade_date),
            "fii_buy": float(entry["buyValue"]),
            "fii_sell": float(entry["sellValue"]),
            "fii_net": fii_net
        })

        # Fetch rolling 30-day cash history
        cash_hist = fetch_last_n("fii_cash_raw", "fii_net", 30)

        if len(cash_hist) < 30:
            logging.info(f"Skipping {trade_date} (insufficient history)")
            continue

        cash_z = calculate_z_score(fii_net, cash_hist)

        # For seeding, STS = cash_z only
        sts = cash_z

        if previous_irs is None:
            irs = sts
        else:
            irs = calculate_ema(sts, previous_irs)

        previous_irs = irs

        bias = classify_bias(sts)
        phase = classify_phase(irs)

        upsert("institutional_zscores", {
            "trade_date": str(trade_date),
            "cash_z": round(cash_z, 3),
            "futures_z": 0,
            "pcr_z": 0,
            "sts": round(sts, 3)
        })

        upsert("institutional_regime", {
            "trade_date": str(trade_date),
            "sts": round(sts, 3),
            "irs": round(irs, 3),
            "market_phase": phase,
            "tomorrow_bias": bias
        })

        logging.info(f"Completed {trade_date}")

    logging.info("Cash-only backfill completed successfully.")


if __name__ == "__main__":
    run_backfill()