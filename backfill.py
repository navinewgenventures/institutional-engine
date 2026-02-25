import datetime
import logging

from db import upsert, fetch_last_n
from calculations import (
    calculate_z_score,
    calculate_futures_z,
    calculate_sts,
    calculate_ema
)
from classification import classify_bias, classify_phase

import pandas as pd


logging.basicConfig(level=logging.INFO)


def backfill_from_csv(csv_path):

    df = pd.read_csv(csv_path)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.sort_values("trade_date")

    previous_irs = None

    for _, row in df.iterrows():

        trade_date = row["trade_date"].date()

        fii_net = row["fii_net"]
        net_position = row["net_position"]
        oi_change = row["oi_change"]
        pcr_today = row["pcr"]

        # -------------------------
        # Insert raw
        # -------------------------
        upsert("fii_cash_raw", {
            "trade_date": str(trade_date),
            "fii_buy": 0,
            "fii_sell": 0,
            "fii_net": fii_net
        })

        upsert("index_futures_raw", {
            "trade_date": str(trade_date),
            "net_position": net_position,
            "oi": oi_change,
            "oi_change": oi_change
        })

        upsert("options_summary_raw", {
            "trade_date": str(trade_date),
            "total_call_oi": 0,
            "total_put_oi": 0,
            "pcr": pcr_today
        })

        # -------------------------
        # Fetch history up to that date
        # -------------------------
        cash_hist = fetch_last_n("fii_cash_raw", "fii_net", 30)
        pos_hist = fetch_last_n("index_futures_raw", "net_position", 30)
        oi_hist = fetch_last_n("index_futures_raw", "oi_change", 30)
        pcr_hist = fetch_last_n("options_summary_raw", "pcr", 30)

        if min(len(cash_hist), len(pos_hist), len(oi_hist), len(pcr_hist)) < 30:
            logging.info(f"Skipping {trade_date} (insufficient history)")
            continue

        # -------------------------
        # Calculate signals
        # -------------------------
        cash_z = calculate_z_score(fii_net, cash_hist)
        position_z = calculate_z_score(net_position, pos_hist)
        oi_z = calculate_z_score(oi_change, oi_hist)
        pcr_z = calculate_z_score(pcr_today, pcr_hist)

        futures_z = calculate_futures_z(position_z, oi_z)
        sts = calculate_sts(futures_z, cash_z, pcr_z)

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
            "futures_z": round(futures_z, 3),
            "pcr_z": round(pcr_z, 3),
            "sts": round(sts, 3)
        })

        upsert("institutional_regime", {
            "trade_date": str(trade_date),
            "sts": round(sts, 3),
            "irs": round(irs, 3),
            "market_phase": phase,
            "tomorrow_bias": bias
        })

        logging.info(f"Processed {trade_date}")


if __name__ == "__main__":
    backfill_from_csv("historical_data.csv")