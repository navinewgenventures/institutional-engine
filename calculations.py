import numpy as np

def calculate_z_score(today_value, historical_values):
    mean = np.mean(historical_values)
    std = np.std(historical_values)

    if std == 0:
        return 0

    z = (today_value - mean) / std
    return max(min(z, 3), -3)


def calculate_futures_z(position_z, oi_z):
    return (position_z * 0.6) + (oi_z * 0.4)


def calculate_sts(futures_z, cash_z, pcr_z):
    sts = (futures_z * 0.50) + (cash_z * 0.30) + (pcr_z * 0.20)
    return max(min(sts, 3), -3)


def calculate_ema(current_value, previous_ema, period=10):
    alpha = 2 / (period + 1)
    return (current_value * alpha) + (previous_ema * (1 - alpha))