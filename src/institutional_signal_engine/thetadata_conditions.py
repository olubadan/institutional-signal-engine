"""Versioned numeric ThetaData trade-condition mapping.

Names are retained for audit only. Runtime eligibility uses numeric codes and
the explicit exclusion sets below, never free-text descriptions.
"""

from enum import StrEnum

CONDITION_MAPPING_VERSION = "thetadata-trade-conditions-v3-20260805"


class ConditionName(StrEnum):
    REGULAR = "REGULAR"
    OUT_OF_SEQ = "OUT_OF_SEQ"
    SPREAD = "SPREAD"
    STRADDLE = "STRADDLE"
    BUY_WRITE = "BUY_WRITE"
    COMBO = "COMBO"
    CANCELLATION = "CANCELLATION"
    LATE_REPORT = "LATE_REPORT"
    CORRECTION = "CORRECTION"
    OTHER = "OTHER"


# Exact numeric code/name pairs published in ThetaData's Trade Conditions table.
THETADATA_CONDITION_CODES: dict[int, str] = {
    0: "REGULAR",
    1: "FORM_T",
    2: "OUT_OF_SEQ",
    3: "AVG_PRC",
    4: "AVG_PRC_NASDAQ",
    5: "OPEN_REPORT_LATE",
    6: "OPEN_REPORT_OUT_OF_SEQ",
    7: "OPEN_REPORT_IN_SEQ",
    8: "PRIOR_REFERENCE_PRICE",
    9: "NEXT_DAY_SALE",
    10: "BUNCHED",
    11: "CASH_SALE",
    12: "SELLER",
    13: "SOLD_LAST",
    14: "RULE_127",
    15: "BUNCHED_SOLD",
    16: "NON_BOARD_LOT",
    17: "POSIT",
    18: "AUTO_EXECUTION",
    19: "HALT",
    20: "DELAYED",
    21: "REOPEN",
    22: "ACQUISITION",
    23: "CASH_MARKET",
    24: "NEXT_DAY_MARKET",
    25: "BURST_BASKET",
    26: "OPEN_DETAIL",
    27: "INTRA_DETAIL",
    28: "BASKET_ON_CLOSE",
    29: "RULE_155",
    30: "DISTRIBUTION",
    31: "SPLIT",
    32: "REGULAR_SETTLE",
    33: "CUSTOM_BASKET_CROSS",
    34: "ADJ_TERMS",
    35: "SPREAD",
    36: "STRADDLE",
    37: "BUY_WRITE",
    38: "COMBO",
    39: "STPD",
    40: "CANC",
    41: "CANC_LAST",
    42: "CANC_OPEN",
    43: "CANC_ONLY",
    44: "CANC_STPD",
    45: "MATCH_CROSS",
    46: "FAST_MARKET",
    47: "NOMINAL",
    48: "CABINET",
    49: "BLANK_PRICE",
    50: "NOT_SPECIFIED",
    51: "MC_OFFICIAL_CLOSE",
    52: "SPECIAL_TERMS",
    53: "CONTINGENT_ORDER",
    54: "INTERNAL_CROSS",
    55: "STOPPED_REGULAR",
    56: "STOPPED_SOLD_LAST",
    57: "STOPPED_OUT_OF_SEQ",
    58: "BASIS",
    59: "VWAP",
    60: "SPECIAL_SESSION",
    61: "NANEX_ADMIN",
    62: "OPEN_REPORT",
    63: "MARKET_ON_CLOSE",
    64: "SETTLE_PRICE",
    65: "OUT_OF_SEQ_PRE_MKT",
    66: "MC_OFFICIAL_OPEN",
    67: "FUTURES_SPREAD",
    68: "OPEN_RANGE",
    69: "CLOSE_RANGE",
    70: "NOMINAL_CABINET",
    71: "CHANGING_TRANS",
    72: "CHANGING_TRANS_CAB",
    73: "NOMINAL_UPDATE",
    74: "PIT_SETTLEMENT",
    75: "BLOCK_TRADE",
    76: "EXG_FOR_PHYSICAL",
    77: "VOLUME_ADJUSTMENT",
    78: "VOLATILITY_TRADE",
    79: "YELLOW_FLAG",
    80: "FLOOR_PRICE",
    81: "OFFICIAL_PRICE",
    82: "UNOFFICIAL_PRICE",
    83: "MID_BID_ASK_PRICE",
    84: "END_SESSION_HIGH",
    85: "END_SESSION_LOW",
    86: "BACKWARDATION",
    87: "CONTANGO",
    88: "HOLIDAY",
    89: "PRE_OPENING",
    90: "POST_FULL",
    91: "POST_RESTRICTED",
    92: "CLOSING_AUCTION",
    93: "BATCH",
    94: "TRADING",
    95: "INTERMARKET_SWEEP",
    96: "DERIVATIVE",
    97: "REOPENING",
}

MULTI_LEG_CODES = frozenset({35, 36, 37, 38, 67})
CANCELLATION_CODES = frozenset({40, 41, 42, 43, 44})
LATE_REPORT_CODES = frozenset({2, 5, 6, 13, 15, 26, 27, 28, 56, 57, 65})
CORRECTION_CODES = frozenset({61, 77})
EXCLUDED_CODES = frozenset().union(
    MULTI_LEG_CODES, CANCELLATION_CODES, LATE_REPORT_CODES, CORRECTION_CODES
)


def condition_name(code: int) -> str | None:
    return THETADATA_CONDITION_CODES.get(code)


def eligible_condition(code: int) -> bool:
    return code in THETADATA_CONDITION_CODES and code not in EXCLUDED_CODES
