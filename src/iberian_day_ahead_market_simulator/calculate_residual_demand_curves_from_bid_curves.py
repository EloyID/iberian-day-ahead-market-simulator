import logging
from typing import Literal

import numpy as np
import pandas as pd

from iberian_day_ahead_market_simulator.const import (
    CAT_BIDDING_ZONE_MIBEL,
    MAX_BID_PRICE,
    MIN_BID_PRICE,
    SPAIN_ZONE,
    get_rdc_power_columns,
    get_rdc_price_columns,
)
from iberian_day_ahead_market_simulator.parse_omie_files import (
    parse_curva_pbc_file,
    parse_marginalpdbc_file,
)

import iberian_day_ahead_market_simulator.columns as cols
from iberian_day_ahead_market_simulator.tools import get_float_bid_power_cumsum

logger = logging.getLogger(__name__)

# the curva_pbc rows of the flow between Spain and Portugal in the periods the market
# split: "Exp PT"/"Imp PT" in the Spanish zone's curve and "Exp ES"/"Imp ES" in the
# Portuguese zone's, the same flow seen from either side
SPAIN_PORTUGAL_EXCHANGE_ORDER_TYPES = ["Exp PT", "Imp PT", "Exp ES", "Imp ES"]


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
    curva_pbc_C_extended_rdc[["sell_cumsum", "buy_cumsum"]] = curva_pbc_C_extended_rdc[
        ["sell_cumsum", "buy_cumsum"]
    ].fillna(0)

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


def get_clearing_price_of_rows(
    curva_pbc: pd.DataFrame, marginalpdbc: pd.DataFrame
) -> pd.Series:
    """The clearing price of the bidding zone of each curva_pbc row.

    A row of a period the market split is set against its own zone's price, "ES" or
    "PT"; an "MI" row, of a period the market stayed coupled, against Spain's, which is
    then also Portugal's.

    Args:
        curva_pbc (pd.DataFrame): The bid curves, with int_period and cat_bidding_zone.
        marginalpdbc (pd.DataFrame): The clearing prices, as parse_marginalpdbc_file
            returns them: one row per int_period and cat_bidding_zone ("ES" or "PT"),
            with float_cleared_price in EUR/MWh.

    Returns:
        pd.Series: The clearing price of each row, in EUR/MWh, with curva_pbc's index.

    Raises:
        ValueError: If marginalpdbc has no price for the period and zone of some row.
    """
    price_zones = (
        curva_pbc[cols.CAT_BIDDING_ZONE]
        .astype(str)
        .replace({CAT_BIDDING_ZONE_MIBEL: SPAIN_ZONE})
    )
    clearing_prices = marginalpdbc.astype({cols.CAT_BIDDING_ZONE: str}).set_index(
        [cols.INT_PERIOD, cols.CAT_BIDDING_ZONE]
    )[cols.FLOAT_CLEARED_PRICE]
    row_keys = pd.MultiIndex.from_arrays([curva_pbc[cols.INT_PERIOD], price_zones])
    row_clearing_prices = pd.Series(
        clearing_prices.reindex(row_keys).to_numpy(), index=curva_pbc.index
    )

    if row_clearing_prices.isna().any():
        missing_keys = row_keys[row_clearing_prices.isna().to_numpy()].unique()
        raise ValueError(
            "marginalpdbc has no clearing price for these (period, zone) of the bid "
            f"curves: {list(missing_keys)[:5]}"
        )
    return row_clearing_prices


def calculated_curva_pbc_cleared_extended(
    curva_pbc: pd.DataFrame, marginalpdbc: pd.DataFrame
) -> pd.DataFrame:
    """Calculate the extended curves: the cleared curves continued with the offered bids
    beyond the clearing price.

    Each zone's cleared sells are continued with its offered sells priced above its
    clearing price, and its cleared buys with its offered buys priced below it. The
    threshold is the clearing price itself, not the highest cleared sell or the lowest
    cleared buy: when a buy or a complex order sets the price, the offered sells priced
    between the highest cleared sell and the clearing price are complex orders the market
    rejected although in the money, which are no supply at those prices.

    Args:
        curva_pbc (pd.DataFrame): The bid curves.
        marginalpdbc (pd.DataFrame): The clearing prices, as get_clearing_price_of_rows
            takes them.

    Returns:
        pd.DataFrame: The cleared rows and the offered rows that continue them, without
            cat_ofertada_casada, with float_bid_power_cumsum accumulated over both.
    """
    # operate on a copy: the cumsum column below must not appear in the caller's
    # dataframe, which is typically the whole parsed curva_pbc file
    curva_pbc = curva_pbc.copy()

    curva_pbc[cols.FLOAT_BID_POWER_CUMSUM] = get_float_bid_power_cumsum(curva_pbc)
    curva_pbc["aux_clearing_price"] = get_clearing_price_of_rows(
        curva_pbc, marginalpdbc
    ).to_numpy()

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

    # from the offered bids, keep only the sells priced above their zone's clearing price
    # and the buys priced below it
    curva_pbc_O_V_filtered = curva_pbc_O_V.query(
        f"{cols.FLOAT_BID_PRICE} > aux_clearing_price"
    )
    curva_pbc_O_C_filtered = curva_pbc_O_C.query(
        f"{cols.FLOAT_BID_PRICE} < aux_clearing_price"
    )

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
    curva_pbc_C_extended = curva_pbc_C_extended.drop(
        columns=[cols.CAT_OFERTADA_CASADA, "aux_clearing_price"]
    )
    curva_pbc_C_extended[cols.FLOAT_BID_POWER_CUMSUM] = get_float_bid_power_cumsum(
        curva_pbc_C_extended, cod_ofertada_casada_column_name=None
    )

    return curva_pbc_C_extended


def calculate_residual_demand_curves_from_bid_curves(
    curva_pbc: pd.DataFrame | str,
    marginalpdbc: pd.DataFrame | str,
    market_periods_count: int,
    cat_bidding_zone: Literal["ES", "PT"] = "ES",
    respects_split: bool = True,
    price_points: list[float] | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Calculate the residual demand curves from the bid curves.

    Parameters
    ----------
    curva_pbc : pd.DataFrame | str
        The bid curves as a pandas DataFrame or a path to a CSV file.
    marginalpdbc : pd.DataFrame | str
        The session's clearing prices, as parse_marginalpdbc_file returns them, or a path
        to the marginalpdbc file. Each zone's cleared curves are continued with its
        offered bids beyond its own clearing price.
    market_periods_count : int
        Number of periods the session has, i.e. how many power_<i>/price_<i> columns the
        result carries. It must cover every period the curve holds.
    cat_bidding_zone : Literal["ES", "PT"]
        Bidding zone whose residual demand is wanted when `respects_split`. The rows of
        the other zone are dropped, while the "MI" rows of the periods the market stayed
        coupled are read for either zone. Defaults to "ES".
    respects_split : bool
        If True (the default), the periods when the market splits are read from the curve of
        `cat_bidding_zone`, as that zone's own market. If False, every period is read as
        the whole of MIBEL: the rows of both zones are kept and only the flow between
        Spain and Portugal, internal to MIBEL, is dropped (the
        SPAIN_PORTUGAL_EXCHANGE_ORDER_TYPES rows); `cat_bidding_zone` is then not used.
    price_points : list[float] | None
        Prices the residual demand is sampled at. Defaults to a 0.5 EUR/MWh grid spanning
        MIN_BID_PRICE to MAX_BID_PRICE.

    Returns
    -------
    dict[str, pd.DataFrame]
        "cleared_bids_continued_with_submitted_residual_demand": the residual demand
        curves, one price_<i>/power_<i> pair of columns per period; and
        "curva_pbc_cleared_extended": the extended bid curves they are computed from, as
        calculated_curva_pbc_cleared_extended returns them.

    Raises
    ------
    ValueError
        If the curve holds periods beyond `market_periods_count`, which would silently
        return a curve for only the first part of the session, e.g. asking a 96 period
        quarter-hourly day for 24 periods; or if marginalpdbc has no clearing price for a
        period and zone of the curve.
    """
    if isinstance(curva_pbc, str):
        curva_pbc = parse_curva_pbc_file(curva_pbc)
    if isinstance(marginalpdbc, str):
        marginalpdbc = parse_marginalpdbc_file(marginalpdbc)

    if respects_split:
        # cat_bidding_zone will be "MI" when the market is not split, and "ES" or "PT" when
        # it is. The bid curves will keep either the "MI" or the "ES"/"PT" rows, so filter
        # the dataframe to keep only the relevant ones.
        cod_pais_filtered_analyzed = ["MI", cat_bidding_zone]
        curva_pbc = curva_pbc.query(
            f"{cols.CAT_BIDDING_ZONE} in {cod_pais_filtered_analyzed}"
        )
    else:
        # every period as the whole of MIBEL: in a split period the curves of both zones
        # together, without the flow between them, which is internal to MIBEL
        curva_pbc = curva_pbc[
            ~curva_pbc[cols.CAT_ORDER_TYPE].isin(SPAIN_PORTUGAL_EXCHANGE_ORDER_TYPES)
        ]

    # the period count decides the result's columns but is passed in rather than read from
    # the data, so check it against the curve before any of it is used
    highest_period_in_curve = int(curva_pbc[cols.INT_PERIOD].max())
    if highest_period_in_curve > market_periods_count:
        raise ValueError(
            f"the bid curve holds periods up to {highest_period_in_curve} but "
            f"market_periods_count says the session is {market_periods_count} periods long. "
            f"Pass the session's own period count, otherwise the curves returned cover only "
            f"its first {market_periods_count} periods."
        )

    curva_pbc_C_extended = calculated_curva_pbc_cleared_extended(
        curva_pbc, marginalpdbc
    )

    curva_pbc_C_extended_rdc = calculate_residual_demand(curva_pbc_C_extended)

    if price_points is None:
        price_points = np.arange(MIN_BID_PRICE, MAX_BID_PRICE + 0.5, 0.5)

    cleared_bids_continued_with_submitted_residual_demand = format_curva_pbc_rdc(
        curva_pbc_C_extended_rdc, price_points, market_periods_count
    )

    return {
        "cleared_bids_continued_with_submitted_residual_demand": cleared_bids_continued_with_submitted_residual_demand,
        "curva_pbc_cleared_extended": curva_pbc_C_extended,
    }
