import numpy as np
import pandas as pd

from iberian_day_ahead_market_simulator.clear_mibel_with_price_curve import (
    get_cleared_power_with_price_curve,
)
from iberian_day_ahead_market_simulator.const import (
    CAT_BUY,
    CAT_SELL,
    ORDER_TYPE_EXCLUSIVE_GROUP,
    ORDER_TYPE_SIMPLE,
    ORDER_TYPE_SIMPLE_BLOCK,
    ORDER_TYPE_SCO,
    get_rdc_power_columns,
    get_rdc_price_columns,
)
from iberian_day_ahead_market_simulator.data_preprocessor import (
    get_det_cab_for_simulation,
)
from iberian_day_ahead_market_simulator.tools import (
    substract_reference_curve_from_all_curves,
)
from iberian_day_ahead_market_simulator.validate_inputs import (
    format_price_curves,
    get_participants_bidding_zones,
    get_reference_price_curve_index,
    get_validated_det_and_cab,
)
import iberian_day_ahead_market_simulator.columns as cols

# the bid types calculate_cleared_power_curves_by_order_type_from_bids reports apart, as
# (cat_buy_sell, cat_order_type): every buy together, since the clearing treats every buy
# as a simple bid, and the sells by order type. None matches any order type.
CLEARED_POWER_BID_TYPES = {
    "cleared_power_buy": (CAT_BUY, None),
    "cleared_power_sell_simple": (CAT_SELL, ORDER_TYPE_SIMPLE),
    "cleared_power_sell_C01": (CAT_SELL, ORDER_TYPE_SIMPLE_BLOCK),
    "cleared_power_sell_C02": (CAT_SELL, ORDER_TYPE_SCO),
    "cleared_power_sell_C04": (CAT_SELL, ORDER_TYPE_EXCLUSIVE_GROUP),
}


def _sum_cleared_power_by_period_and_bid_type(
    det_cab: pd.DataFrame, market_periods_count: int
) -> dict[str, pd.Series]:
    """
    Sum the cleared power of each bid type of CLEARED_POWER_BID_TYPES, per market period.

    The orders of both bidding zones are summed together.

    Args:
        det_cab (pd.DataFrame): The orders, with cols.INT_PERIOD, cols.CAT_BUY_SELL,
            cols.CAT_ORDER_TYPE and cols.FLOAT_CLEARED_POWER (MW).
        market_periods_count (int): The session's number of market periods.

    Returns:
        dict[str, pd.Series]: One entry per bid type, keyed as CLEARED_POWER_BID_TYPES: its
            cleared power (MW) indexed by market period, 1 to market_periods_count. A period
            without any order of the type is 0 MW.
    """
    market_periods = range(1, market_periods_count + 1)

    cleared_power_by_bid_type = {}
    for bid_type, (buy_sell, order_type) in CLEARED_POWER_BID_TYPES.items():
        # the rows of the bid type: its side of the market, and its order type if it has one
        is_bid_type = det_cab[cols.CAT_BUY_SELL] == buy_sell
        if order_type is not None:
            is_bid_type &= det_cab[cols.CAT_ORDER_TYPE] == order_type

        # a period without any order of the type is absent from the groupby, so it is
        # added back as 0 MW rather than shifting the later periods
        cleared_power_by_bid_type[bid_type] = (
            det_cab.loc[is_bid_type]
            .groupby(cols.INT_PERIOD)[cols.FLOAT_CLEARED_POWER]
            .sum()
            .reindex(market_periods, fill_value=0.0)
        )

    return cleared_power_by_bid_type


def calculate_cleared_power_curves_by_order_type_from_bids(
    price_curves: np.ndarray,
    det: pd.DataFrame | str,
    cab: pd.DataFrame | str,
    participants_bidding_zones: pd.DataFrame | None = None,
    spain_as_default_bidding_zone: bool = False,
    reference_price_curve: np.ndarray | None = None,
) -> dict[str, pd.DataFrame]:
    """Cleared power of each bid type, built from the individual orders, one per price curve given.

    Every order is cleared at each price curve with `get_cleared_power_with_price_curve`, as
    in `calculate_residual_demand_curves_from_bids`, and the cleared power is summed by bid
    type over both bidding zones together. Only the orders of DET/CAB are cleared: the
    France interconnection is left out.

    Parameters
    ----------
    price_curves : np.ndarray
        One price per market period, or a 2D array of such curves (one row each).
    det, cab : pd.DataFrame | str
        The day's order detail and headers, or paths to the raw OMIE files.
    participants_bidding_zones : pd.DataFrame | None
        Unit to bidding zone mapping; None reads the package's bundled list. The zones are
        summed together, but every unit needs one for its orders to be built.
    spain_as_default_bidding_zone : bool
        Put units missing from that mapping in Spain instead of raising.
    reference_price_curve : np.ndarray | None
        When given, every curve is reported relative to this one.

    Returns
    -------
    dict[str, pd.DataFrame]
        One dataframe per bid type, keyed as CLEARED_POWER_BID_TYPES:
        "cleared_power_buy" (every buy), "cleared_power_sell_simple",
        "cleared_power_sell_C01" (block orders), "cleared_power_sell_C02" (scalable complex
        orders) and "cleared_power_sell_C04" (block orders in an exclusive group). Each has
        one row per price curve and, per market period `i`, a `price_<i>` (EUR/MWh) and a
        `power_<i>` (MW, the bid type's cleared power) column.

    Raises
    ------
    ValueError
        If `price_curves` does not cover exactly the DET file's market periods, or if
        `reference_price_curve` is not one of them.
    """

    price_curves = format_price_curves(price_curves)
    if reference_price_curve is not None:
        reference_price_curve_index = get_reference_price_curve_index(
            price_curves, reference_price_curve
        )

    det, cab, market_periods_count = get_validated_det_and_cab(det, cab, price_curves)

    rdc_price_columns = get_rdc_price_columns(market_periods_count)
    rdc_power_columns = get_rdc_power_columns(market_periods_count)

    # the orders of DET/CAB only, without the France interconnection's
    det_cab = get_det_cab_for_simulation(
        det=det,
        cab=cab,
        participants_bidding_zones=get_participants_bidding_zones(
            participants_bidding_zones
        ),
        spain_as_default_bidding_zone=spain_as_default_bidding_zone,
    )

    cleared_power_rows_by_bid_type = {
        bid_type: [] for bid_type in CLEARED_POWER_BID_TYPES
    }

    for price_curve in price_curves:
        det_cab_aux = det_cab.copy()
        price_series = pd.Series(price_curve, index=rdc_price_columns)

        # Calculate cleared power values
        det_cab_aux[cols.FLOAT_CLEARED_POWER] = get_cleared_power_with_price_curve(
            price_curve, det_cab_aux, market_periods_count
        )

        # Sum them by bid type, one row of each bid type's curves
        cleared_power_by_bid_type = _sum_cleared_power_by_period_and_bid_type(
            det_cab_aux, market_periods_count
        )
        for bid_type, cleared_power in cleared_power_by_bid_type.items():
            cleared_power.index = rdc_power_columns
            cleared_power_rows_by_bid_type[bid_type].append(
                pd.concat([price_series, cleared_power])
            )

    cleared_power_curves_by_bid_type = {
        bid_type: pd.DataFrame(cleared_power_rows)
        for bid_type, cleared_power_rows in cleared_power_rows_by_bid_type.items()
    }

    if reference_price_curve is not None:
        # substract that curve from all curves
        cleared_power_curves_by_bid_type = {
            bid_type: substract_reference_curve_from_all_curves(
                cleared_power_curves_df, reference_price_curve_index, rdc_power_columns
            )
            for bid_type, cleared_power_curves_df in cleared_power_curves_by_bid_type.items()
        }

    return cleared_power_curves_by_bid_type
