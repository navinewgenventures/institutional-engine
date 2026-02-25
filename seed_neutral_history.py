import datetime
import logging

from db import upsert
from calculations import calculate_ema
from classification import classify_bias, classify_phase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def seed_neutral_history(days=30):

    today = datetime.date.today()
    start_date = today - datetime.timedelta(days=days)

    previous_irs = 0  # Start neutral

    logging.info(f"Seeding {days} neutral days...")

    for i in range(days):
        trade_date = start_date + datetime.timedelta(days=i)

        # Skip weekends
        if trade_date.weekday() >= 5:
            continue

        fii_net = 0
        sts = 0

        irs = calculate_ema(sts, previous_irs)
        previous_irs = irs

        bias = classify_bias(sts)
        phase = classify_phase(irs)

        # Insert raw
        upsert("fii_cash_raw", {
            "trade_date": str(trade_date),
            "fii_buy": 0,
            "fii_sell": 0,
            "fii_net": 0
        })

        upsert("index_futures_raw", {
            "trade_date": str(trade_date),
            "net_position": 0,
            "oi": 0,
            "oi_change": 0
        })

        upsert("options_summary_raw", {
            "trade_date": str(trade_date),
            "total_call_oi": 0,
            "total_put_oi": 0,
            "pcr": 1
        })

        upsert("institutional_zscores", {
            "trade_date": str(trade_date),
            "cash_z": 0,
            "futures_z": 0,
            "pcr_z": 0,
            "sts": 0
        })

        upsert("institutional_regime", {
            "trade_date": str(trade_date),
            "sts": 0,
            "irs": round(irs, 3),
            "market_phase": phase,
            "tomorrow_bias": bias
        })

        logging.info(f"Seeded {trade_date}")

    logging.info("Neutral seeding completed successfully.")


if __name__ == "__main__":
    seed_neutral_history(30)