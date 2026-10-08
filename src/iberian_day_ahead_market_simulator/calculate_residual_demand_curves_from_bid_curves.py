import logging

import numpy as np
import pandas as pd

from iberian_day_ahead_market_simulator.const import (
    MAX_BID_PRICE,
    MIN_BID_PRICE,
    get_rdc_power_columns,
    get_rdc_price_columns,
)
from iberian_day_ahead_market_simulator.parse_omie_files import parse_curva_pbc_file

import iberian_day_ahead_market_simulator.columns as cols
from iberian_day_ahead_market_simulator.tools import get_float_bid_power_cumsum

logger = logging.getLogger(__name__)


def format_curva_pbc_rdc(
    curva_pbc_C_extended_rdc: pd.DataFrame,
    price_points: list[float],
    market_periods_count,
) -> pd.DataFrame:
    """
    Format the residual demand curve DataFrame.

    Parameters
    ----------
    curva_pbc_rdc : pd.DataFrame
        The residual demand curve DataFrame.
    price_points : list[float]
        The price points for interpolation.

    Returns
    -------
    pd.DataFrame
        The formatted residual demand curve DataFrame.
    """
    rdc_power_columns = get_rdc_power_columns(market_periods_count)
    rdc_price_columns = get_rdc_price_columns(market_periods_count)

    cleared_bids_continued_with_submitted_residual_demand = pd.DataFrame(
        {col: price_points for col in rdc_price_columns}
    )

    for power_col, period in zip(rdc_power_columns, range(1, market_periods_count + 1)):
        curva_pbc_period = curva_pbc_C_extended_rdc.query(
            f"{cols.INT_PERIOD} == {period}"
        )

        # a period holding no bid at all has no curve to read a residual demand off,
        # which is reported as a missing column rather than as an empty sample array
        # error from inside np.interp
        if curva_pbc_period.empty:
            logger.warning(
                "No bid curve for period %s of %s, its residual demand is set to NaN.",
                period,
                market_periods_count,
            )
            cleared_bids_continued_with_submitted_residual_demand[power_col] = np.nan
            continue

        residual_demand_period = np.interp(
            price_points,
            curva_pbc_period[cols.FLOAT_BID_PRICE],
            curva_pbc_period["residual_demand"],
        )
        cleared_bids_continued_with_submitted_residual_demand[power_col] = (
            residual_demand_period
        )

    return cleared_bids_continued_with_submitted_residual_demand


def calculate_residual_demand(curva_pbc_C_extended: pd.DataFrame) -> pd.DataFrame:
    """Calculate the residual demand curve from the bid curves.

    Args:
        curva_pbc_C_extended (pd.DataFrame):

    Returns:
        pd.DataFrame:
    """
    curva_pbc_C_extended_rdc = curva_pbc_C_extended.copy()

    # filter repeated rows for each period and price
    # we want to keep the maximum power sold and bought for each case
    curva_pbc_C_extended_rdc_C = (
        curva_pbc_C_extended_rdc.query(f"{cols.CAT_BUY_SELL} == 'C'")
        .sort_values(by=[cols.FLOAT_BID_POWER_CUMSUM])
        .drop_duplicates(subset=[cols.INT_PERIOD, cols.FLOAT_BID_PRICE], keep="last")
    )
    curva_pbc_C_extended_rdc_V = (
        curva_pbc_C_extended_rdc.query(f"{cols.CAT_BUY_SELL} == 'V'")
        .sort_values(by=[cols.FLOAT_BID_POWER_CUMSUM])
        .drop_duplicates(subset=[cols.INT_PERIOD, cols.FLOAT_BID_PRICE], keep="last")
    )

    # concatenate the two dataframes
    curva_pbc_C_extended_rdc = pd.concat(
        [curva_pbc_C_extended_rdc_C, curva_pbc_C_extended_rdc_V], ignore_index=True
    )

    # at each price point, we want to know the cumulative power sold and bought
    curva_pbc_C_extended_rdc["sell_cumsum"] = np.where(
        curva_pbc_C_extended_rdc[cols.CAT_BUY_SELL] == "V",
        curva_pbc_C_extended_rdc[cols.FLOAT_BID_POWER_CUMSUM],
        np.nan,
    )
    curva_pbc_C_extended_rdc["buy_cumsum"] = np.where(
        curva_pbc_C_extended_rdc[cols.CAT_BUY_SELL] == "C",
        curva_pbc_C_extended_rdc[cols.FLOAT_BID_POWER_CUMSUM],
        np.nan,
    )

    # sort by price, and at an identical price put the sell row before the buy row:
    # the fills below each need to see the opposite side at that very same price,
    # and a buy and a sell bid at the same price is the normal case at the clearing
    # price. The order is set with an explicit flag rather than by relying on the
    # NaN placement of a cumsum sort key or on the category order of cat_buy_sell.
    curva_pbc_C_extended_rdc["aux_is_buy"] = (
        curva_pbc_C_extended_rdc[cols.CAT_BUY_SELL] == "C"
    ).astype(int)
    curva_pbc_C_extended_rdc = curva_pbc_C_extended_rdc.sort_values(
        by=[cols.FLOAT_BID_PRICE, "aux_is_buy"]
    )

    # note that we use ffill for sell_cumsum and bfill for buy_cumsum
    # beaause the sell_cumsum is compared to the buy_cumsum from higher prices,
    # and the buy_cumsum is compared to the sell_cumsum from lower prices
    curva_pbc_C_extended_rdc["sell_cumsum"] = curva_pbc_C_extended_rdc.groupby(
        cols.INT_PERIOD
    )["sell_cumsum"].ffill()
    curva_pbc_C_extended_rdc["buy_cumsum"] = curva_pbc_C_extended_rdc.groupby(
        cols.INT_PERIOD
    )["buy_cumsum"].bfill()

    # the fills leave the extremes of each period empty: below the cheapest sell bid
    # no power is sold and above the priciest buy bid no power is bought, and both of
    # those are a volume of 0 rather than an unknown value
    curva_pbc_C_extended_rdc[["sell_cumsum", "buy_cumsum"]] = (
        curva_pbc_C_extended_rdc[["sell_cumsum", "buy_cumsum"]].fillna(0)
    )

    # calculate the residual demand as the difference between the cumulative buy and sell power
    curva_pbc_C_extended_rdc["residual_demand"] = (
        curva_pbc_C_extended_rdc["buy_cumsum"] - curva_pbc_C_extended_rdc["sell_cumsum"]
    )

    # the buy and the sell row of a tied price now carry the same residual demand, so
    # collapsing them keeps one point per price and spares np.interp a duplicated
    # sample point, for which its behaviour is not defined
    curva_pbc_C_extended_rdc = curva_pbc_C_extended_rdc.drop_duplicates(
        subset=[cols.INT_PERIOD, cols.FLOAT_BID_PRICE], keep="last"
    ).drop(columns=["aux_is_buy"])

    return curva_pbc_C_extended_rdc


def calculated_curva_pbc_cleared_extended(curva_pbc: pd.DataFrame) -> pd.DataFrame:
    """Calculate the extended curves concatenating the cleared curve with the offered bids that
    are beyond the clearing price

    Args:
        curva_pbc (pd.DataFrame):

    Returns:
        pd.DataFrame:
    """
    # operate on a copy: the cumsum column below must not appear in the caller's
    # dataframe, which is typically the whole parsed curva_pbc file
    curva_pbc = curva_pbc.copy()

    curva_pbc[cols.FLOAT_BID_POWER_CUMSUM] = get_float_bid_power_cumsum(curva_pbc)

    # filter the bid curves to get the offered/cleared bids for selling and buying
    curva_pbc_O_V = curva_pbc.query(
        f'{cols.CAT_OFERTADA_CASADA} == "O" and {cols.CAT_BUY_SELL} == "V"'
    ).sort_values(by=[cols.FLOAT_BID_POWER_CUMSUM])
    curva_pbc_O_C = curva_pbc.query(
        f'{cols.CAT_OFERTADA_CASADA} == "O" and {cols.CAT_BUY_SELL} == "C"'
    ).sort_values(by=[cols.FLOAT_BID_POWER_CUMSUM])
    curva_pbc_C_V = curva_pbc.query(
        f'{cols.CAT_OFERTADA_CASADA} == "C" and {cols.CAT_BUY_SELL} == "V"'
    ).sort_values(by=[cols.FLOAT_BID_POWER_CUMSUM])
    curva_pbc_C_C = curva_pbc.query(
        f'{cols.CAT_OFERTADA_CASADA} == "C" and {cols.CAT_BUY_SELL} == "C"'
    ).sort_values(by=[cols.FLOAT_BID_POWER_CUMSUM])

    # get the last price reached (similar to clearing price)
    curva_pbc_C_V_max_price = (
        curva_pbc_C_V.groupby(cols.INT_PERIOD)[cols.FLOAT_BID_PRICE]
        .max()
        .rename("sell_max_price")
    )
    curva_pbc_C_C_min_price = (
        curva_pbc_C_C.groupby(cols.INT_PERIOD)[cols.FLOAT_BID_PRICE]
        .min()
        .rename("buy_min_price")
    )

    # from the offered bids, we want to keep only those that are above the maximum price
    # for selling and below the minimum price for buying
    curva_pbc_O_V_filtered = curva_pbc_O_V.merge(
        curva_pbc_C_V_max_price, on=cols.INT_PERIOD
    ).query(f"{cols.FLOAT_BID_PRICE} > sell_max_price")
    curva_pbc_O_C_filtered = curva_pbc_O_C.merge(
        curva_pbc_C_C_min_price, on=cols.INT_PERIOD
    ).query(f"{cols.FLOAT_BID_PRICE} < buy_min_price")

    # extended the cleared curves with the offered bids that are above/below the clearing price
    curva_pbc_C_V_extended = pd.concat(
        [curva_pbc_C_V, curva_pbc_O_V_filtered], ignore_index=True
    )
    curva_pbc_C_C_extended = pd.concat(
        [curva_pbc_C_C, curva_pbc_O_C_filtered], ignore_index=True
    )

    curva_pbc_C_extended = pd.concat(
        [curva_pbc_C_V_extended, curva_pbc_C_C_extended], ignore_index=True
    )
    curva_pbc_C_extended = curva_pbc_C_extended.drop(columns=[cols.CAT_OFERTADA_CASADA])
    curva_pbc_C_extended[cols.FLOAT_BID_POWER_CUMSUM] = get_float_bid_power_cumsum(
        curva_pbc_C_extended, cod_ofertada_casada_column_name=None
    )

    return curva_pbc_C_extended


def calculate_residual_demand_curves_from_bid_curves(
    curva_pbc: pd.DataFrame | str,
    market_periods_count: int,
    price_points: list[float] | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Calculate the residual demand curves from the bid curves.

    Parameters
    ----------
    curva_pbc : pd.DataFrame | str
        The bid curves as a pandas DataFrame or a path to a CSV file.

    Returns
    -------
    dict[str, pd.DataFrame]
        A dictionary with the residual demand curves for each market.
    """
    if isinstance(curva_pbc, str):
        curva_pbc = parse_curva_pbc_file(curva_pbc)

    if price_points is None:
        price_points = np.arange(MIN_BID_PRICE, MAX_BID_PRICE + 0.5, 0.5)

    curva_pbc_C_extended = calculated_curva_pbc_cleared_extended(curva_pbc)

    curva_pbc_C_extended_rdc = calculate_residual_demand(curva_pbc_C_extended)

    cleared_bids_continued_with_submitted_residual_demand = format_curva_pbc_rdc(
        curva_pbc_C_extended_rdc, price_points, market_periods_count
    )

    return {
        "cleared_bids_continued_with_submitted_residual_demand": cleared_bids_continued_with_submitted_residual_demand
    }
