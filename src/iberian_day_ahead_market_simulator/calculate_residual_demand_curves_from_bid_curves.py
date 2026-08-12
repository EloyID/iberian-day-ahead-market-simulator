import numpy as np
import pandas as pd

from iberian_day_ahead_market_simulator.const import (
    RDC_ENERGY_COLUMNS,
    RDC_PRICE_COLUMNS,
)
from iberian_day_ahead_market_simulator.parse_omie_files import parse_curva_pbc_file

import iberian_day_ahead_market_simulator.columns as cols
from iberian_day_ahead_market_simulator.tools import get_float_bid_power_cumsum


def format_curva_pbc_rdc(
    curva_pbc_C_extended_rdc: pd.DataFrame, price_points: list[float]
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
    cleared_bids_continued_with_submitted_residual_demand = pd.DataFrame(
        {col: price_points for col in RDC_PRICE_COLUMNS}
    )

    for energy_col, period in zip(RDC_ENERGY_COLUMNS, range(1, 25)):
        curva_pbc_period = curva_pbc_C_extended_rdc.query(
            f"{cols.INT_PERIOD} == {period}"
        )
        residual_demand_period = np.interp(
            price_points,
            curva_pbc_period[cols.FLOAT_BID_PRICE],
            curva_pbc_period["residual_demand"],
        )
        cleared_bids_continued_with_submitted_residual_demand[energy_col] = (
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

    # concatenate the two dataframes and sort by price
    curva_pbc_C_extended_rdc = pd.concat(
        [curva_pbc_C_extended_rdc_C, curva_pbc_C_extended_rdc_V], ignore_index=True
    )
    curva_pbc_C_extended_rdc = curva_pbc_C_extended_rdc.sort_values(
        by=[cols.FLOAT_BID_PRICE]  # , "aux_float_bid_power_cumsum"]
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

    # note that we use ffill for sell_cumsum and bfill for buy_cumsum
    # beaause the sell_cumsum is compared to the buy_cumsum from higher prices,
    # and the buy_cumsum is compared to the sell_cumsum from lower prices
    curva_pbc_C_extended_rdc["sell_cumsum"] = curva_pbc_C_extended_rdc.groupby(
        cols.INT_PERIOD
    )["sell_cumsum"].ffill()
    curva_pbc_C_extended_rdc["buy_cumsum"] = curva_pbc_C_extended_rdc.groupby(
        cols.INT_PERIOD
    )["buy_cumsum"].bfill()

    # calculate the residual demand as the difference between the cumulative buy and sell power
    curva_pbc_C_extended_rdc["residual_demand"] = (
        curva_pbc_C_extended_rdc["buy_cumsum"] - curva_pbc_C_extended_rdc["sell_cumsum"]
    )

    return curva_pbc_C_extended_rdc


def calculated_curva_pbc_cleared_extended(curva_pbc: pd.DataFrame) -> pd.DataFrame:
    """Calculate the extended curves concatenating the cleared curve with the offered bids that
    are beyond the clearing price

    Args:
        curva_pbc (pd.DataFrame):

    Returns:
        pd.DataFrame:
    """

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
    curva_pbc: pd.DataFrame | str, price_points: list[float] = None
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
        price_points = np.arange(-500, 1000, 0.5)

    curva_pbc_C_extended = calculated_curva_pbc_cleared_extended(curva_pbc)

    curva_pbc_C_extended_rdc = calculate_residual_demand(curva_pbc_C_extended)

    cleared_bids_continued_with_submitted_residual_demand = format_curva_pbc_rdc(
        curva_pbc_C_extended_rdc, price_points
    )

    return {
        "cleared_bids_continued_with_submitted_residual_demand": cleared_bids_continued_with_submitted_residual_demand
    }
