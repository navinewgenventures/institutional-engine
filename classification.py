def classify_bias(sts):
    if sts > 2:
        return "Strong Bullish"
    elif sts > 1:
        return "Bullish"
    elif sts < -2:
        return "Strong Bearish"
    elif sts < -1:
        return "Bearish"
    return "Neutral"


def classify_phase(irs):
    if irs > 1.5:
        return "Accumulation"
    elif irs > 0.5:
        return "Bullish Bias"
    elif irs < -1.5:
        return "Distribution"
    elif irs < -0.5:
        return "Bearish Bias"
    return "Transition"