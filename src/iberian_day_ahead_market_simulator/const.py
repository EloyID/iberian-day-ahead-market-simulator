import re

import iberian_day_ahead_market_simulator.columns as cols

SPAIN_ZONE = "ES"
PORTUGAL_ZONE = "PT"

BIDDING_ZONES_OPTIONS = [SPAIN_ZONE, PORTUGAL_ZONE]

UNIDADES_SPLITTING_ZONE_COLUMN = "ZONA"
UNIDADES_ZONE_COLUMN = "ZONA/FRONTERA"

FRONTIER_MAPPING = {
    2: "PT",
    3: "FR",
    4: "AD",  # ANDORRA
    5: "MA",  # MOROCCO
}

FRONTIER_MAPPING_REVERSE = {v: k for k, v in FRONTIER_MAPPING.items()}
CAT_FRONTIER_OPTIONS = list(FRONTIER_MAPPING.keys())


##### INTERNATIONAL UOFS #####

FRANCE_ID_ORDER = "12345678901234"
FRANCE_ID_UNIDAD = "MIEU"

SPAIN_UOF = "MIE"
PORTUGAL_UOF = "MIP"
INTERCONEXION_UOFS = [FRANCE_ID_UNIDAD, PORTUGAL_UOF, SPAIN_UOF]


### CAT_BIDDING_ZONE_VALUES ###
CAT_BIDDING_ZONE_FRANCE = "FR"
CAT_BIDDING_ZONE_SPAIN = "ES"
CAT_BIDDING_ZONE_PORTUGAL = "PT"
CAT_BIDDING_ZONE_MIBEL = "MI"

CAT_BUY = "C"
CAT_SELL = "V"

BUY_SELL_OPTIONS = [CAT_BUY, CAT_SELL]

ORDER_TYPE_SIMPLE = "S"
ORDER_TYPE_SIMPLE_BLOCK = "C01"
ORDER_TYPE_SCO = "C02"
ORDER_TYPE_EXCLUSIVE_GROUP = "C04"
ORDER_TYPE_EXPORT_FRANCE = "Exp FR"
ORDER_TYPE_IMPORT_FRANCE = "Imp FR"
ORDER_TYPE_EXPORT_PORTUGAL = "Exp PT"
ORDER_TYPE_IMPORT_PORTUGAL = "Imp PT"
ORDER_TYPE_IMPORT_SPAIN = "Imp ES"
ORDER_TYPE_EXPORT_SPAIN = "Exp ES"

ORDER_TYPE_OPTIONS = [
    ORDER_TYPE_SIMPLE,
    ORDER_TYPE_SIMPLE_BLOCK,
    ORDER_TYPE_SCO,
    ORDER_TYPE_EXCLUSIVE_GROUP,
    ORDER_TYPE_EXPORT_FRANCE,
    ORDER_TYPE_IMPORT_FRANCE,
    ORDER_TYPE_EXPORT_PORTUGAL,
    ORDER_TYPE_IMPORT_PORTUGAL,
    ORDER_TYPE_IMPORT_SPAIN,
    ORDER_TYPE_EXPORT_SPAIN,
]

##### OTHER CONSTANTS #####

BLOCK_UNIQUE_IDENTIFIERS = [
    cols.ID_ORDER,
    cols.INT_NUM_BLOCK,
]

DET_CAB_UNIQUE_IDENTIFIERS = [
    cols.INT_PERIOD,
    cols.CAT_BUY_SELL,
    cols.ID_ORDER,
    cols.INT_NUM_SUBORDER,
    cols.INT_NUM_BLOCK,
    cols.INT_NUM_EXCL_GROUP,
]

ITERATIONS_DF_COLUMNS = [
    cols.PARADOXICAL_ORDERS_COLUMN,
    cols.IDS_MIC_SCOS,
    cols.IDS_BID_BLOCKS,
    cols.FLOAT_OBJECTIVE_VALUE,
    cols.BOOL_IS_EXPECTED_INCOME_RESPECTED,
    cols.SOLVER_RESULTS_COLUMN,
    cols.INT_MIC_SCOS_COUNT,
    cols.INT_BID_BLOCKS_COUNT,
    cols.INT_PARADOXICAL_ORDERS_COUNT,
    cols.DF_CLEARED_POWER_COLUMN,
    cols.DF_CLEARING_PRICES_COLUMN,
    cols.DF_SPAIN_PORTUGAL_TRANSMISSIONS_COLUMN,
]


COD_OFERTA_RESIDUAL_DEMAND_V = "RESIDUAL_DEMAND_V"
COD_OFERTA_RESIDUAL_DEMAND_C = "RESIDUAL_DEMAND_C"

CODIGO_UNIDAD_RESIDUAL_DEMAND_V = "RDC_V"
CODIGO_UNIDAD_RESIDUAL_DEMAND_C = "RDC_C"

RDC_CAB_V_BASE = {
    cols.ID_ORDER: COD_OFERTA_RESIDUAL_DEMAND_V,
    cols.ID_UNIDAD: CODIGO_UNIDAD_RESIDUAL_DEMAND_V,
    cols.CAT_BUY_SELL: "V",
    cols.FLOAT_MIC: 0.0,
    cols.FLOAT_MAX_POWER: 9999999999,
}
RDC_CAB_C_BASE = {
    cols.ID_ORDER: COD_OFERTA_RESIDUAL_DEMAND_C,
    cols.ID_UNIDAD: CODIGO_UNIDAD_RESIDUAL_DEMAND_C,
    cols.CAT_BUY_SELL: "C",
    cols.FLOAT_MIC: 0.0,
    cols.FLOAT_MAX_POWER: 9999999999,
}

get_rdc_price_columns = lambda market_periods_count: [
    f"price_{i}" for i in range(1, market_periods_count + 1)
]
get_rdc_power_columns = lambda market_periods_count: [
    f"power_{i}" for i in range(1, market_periods_count + 1)
]


def get_rdc_period_numbers(columns, prefix: str) -> list[int]:
    """
    Read the market period numbers out of the power_<i> or price_<i> column names.

    Args:
        columns: Column names to scan.
        prefix (str): Either "power" or "price".

    Returns:
        list[int]: The period numbers found, in ascending order.
    """
    return sorted(
        int(match.group(1))
        for column in columns
        if (match := re.fullmatch(rf"{prefix}_(\d+)", str(column)))
    )


def spans_whole_market_session(period_numbers: list[int]) -> bool:
    """
    Tell whether period numbers run from 1 to the end of an allowed session.

    A session is one of TOTAL_PERIODS_OPTIONS periods long and its periods are
    numbered without a gap, so this rejects both an unknown session length and a
    set of columns that lost a period in the middle.

    Args:
        period_numbers (list[int]): Period numbers in ascending order.

    Returns:
        bool: True when the numbers are exactly 1..n for an allowed n.
    """
    return (
        len(period_numbers) in TOTAL_PERIODS_OPTIONS
        and period_numbers == list(range(1, len(period_numbers) + 1))
    )

TOTAL_PERIODS_H_OPTIONS = [23, 24, 25]
TOTAL_PERIODS_QH_OPTIONS = [92, 96, 100]
TOTAL_PERIODS_OPTIONS = TOTAL_PERIODS_H_OPTIONS + TOTAL_PERIODS_QH_OPTIONS

##### MARKET PRICE LIMITS #####

# price range a bid can span, used both to make a residual demand order a price
# taker and as the span of the default price grid a residual demand curve is
# sampled on. A grid that stopped short of MAX_BID_PRICE would silently
# truncate the curve where expensive orders still sit.
MIN_BID_PRICE = -500.0
MAX_BID_PRICE = 3500.0
