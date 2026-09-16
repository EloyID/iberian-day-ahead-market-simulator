import numpy as np
import pandas as pd

import iberian_day_ahead_market_simulator.columns as cols
from iberian_day_ahead_market_simulator.tools import get_is_not_exclusive_block


def get_cleared_power_from_non_exclusive_block_order(df):
    df = df.copy()
    average_cleared_price = np.average(
        df[cols.FLOAT_CLEARED_PRICE], weights=df[cols.FLOAT_BID_POWER]
    )
    is_block_order_cleared = average_cleared_price >= df[cols.FLOAT_BID_PRICE].iloc[0]
    df[cols.FLOAT_CLEARED_POWER] = (
        df[cols.FLOAT_BID_POWER] if is_block_order_cleared else 0
    )
    return df[cols.FLOAT_CLEARED_POWER]


def get_cleared_power_from_exclusive_block_order_group(df):
    df = df.copy()
    assert df[cols.ID_ORDER].nunique() == 1, "DataFrame must be for a single offer"
    df[cols.FLOAT_CLEARED_POWER] = (
        df.groupby([cols.INT_NUM_BLOCK], observed=True)
        .apply(
            lambda group: pd.DataFrame(
                get_cleared_power_from_non_exclusive_block_order(group)
            ),  # making a df and then taking the series bc of https://stackoverflow.com/a/69232413
            include_groups=False,
        )
        .reset_index(level=[cols.INT_NUM_BLOCK], drop=True)
    )[cols.FLOAT_CLEARED_POWER]
    if (df[cols.FLOAT_CLEARED_POWER] == 0).all():
        return df[cols.FLOAT_CLEARED_POWER]

    # "relative" because we don't know if the bids are quarter-hourly or hourly,
    # if they were quarter-hourly, the welfare would be energy * price,
    # and the energy is the power divided by 4
    df["relative_welfare"] = df[cols.FLOAT_CLEARED_POWER] * (
        df[cols.FLOAT_CLEARED_PRICE] - df[cols.FLOAT_BID_PRICE]
    )
    max_relative_welfare = (
        df.groupby(cols.INT_NUM_BLOCK, observed=True)["relative_welfare"].sum().idxmax()
    )
    df[cols.FLOAT_CLEARED_POWER] = np.where(
        df[cols.INT_NUM_BLOCK] == max_relative_welfare, df[cols.FLOAT_CLEARED_POWER], 0
    )
    return df[cols.FLOAT_CLEARED_POWER]


def calculate_cleared_power_from_exclusive_block_order_groups(df):
    df = df.copy()
    if df.empty:
        return pd.Series(dtype=float)
    cleared_power = (
        df.groupby([cols.ID_ORDER, cols.INT_NUM_EXCL_GROUP], observed=True)[df.columns]
        .apply(get_cleared_power_from_exclusive_block_order_group, include_groups=False)
        .reset_index(level=[cols.ID_ORDER, cols.INT_NUM_EXCL_GROUP], drop=True)
    )
    if cleared_power.shape[0] != df.shape[0]:
        df[cols.FLOAT_CLEARED_POWER] = cleared_power.T
    else:
        df[cols.FLOAT_CLEARED_POWER] = cleared_power
    return df[cols.FLOAT_CLEARED_POWER]


def calculate_cleared_power_from_non_exclusive_block_orders(df):
    df = df.copy()
    assert get_is_not_exclusive_block(df).all()

    if df.empty:
        return pd.Series(dtype=float)

    df[cols.FLOAT_CLEARED_POWER] = (
        df.groupby([cols.ID_ORDER, cols.INT_NUM_BLOCK], observed=True)
        .apply(lambda x: get_cleared_power_from_non_exclusive_block_order(x))
        .reset_index(level=[0, 1], drop=True)
    )
    return df[cols.FLOAT_CLEARED_POWER]


def calculate_cleared_power_from_SCOs(df):
    df = df.copy()
    cleared_power = (
        df.groupby([cols.ID_ORDER], observed=True)[df.columns]
        .apply(get_cleared_power_from_SCO, include_groups=False)
        .reset_index(level=[cols.ID_ORDER], drop=True)
    )
    if len(cleared_power) != len(df):
        df[cols.FLOAT_CLEARED_POWER] = cleared_power.T
    else:
        df[cols.FLOAT_CLEARED_POWER] = cleared_power
    return df[cols.FLOAT_CLEARED_POWER]


def get_cleared_power_from_SCO(df):
    df = df.copy()
    assert df[cols.ID_ORDER].nunique() == 1, "DataFrame must be for a single offer"
    df[cols.FLOAT_CLEARED_POWER] = np.where(
        df[cols.FLOAT_CLEARED_PRICE] >= df[cols.FLOAT_BID_PRICE],
        df[cols.FLOAT_BID_POWER],
        df[cols.FLOAT_MAV],
    )

    # "relative" because we don't know if the bids are quarter-hourly or hourly,
    # if they were quarter-hourly, the welfare would be energy * price,
    # and the energy is the power divided by 4
    relative_collection_rights = (
        df[cols.FLOAT_CLEARED_POWER] * df[cols.FLOAT_CLEARED_PRICE]
    ).sum()
    expected_relative_collection_rights = (
        df[cols.FLOAT_CLEARED_POWER] * df[cols.FLOAT_BID_PRICE]
    ).sum() + df[cols.FLOAT_MIC].iloc[0]
    is_SCO_cleared = relative_collection_rights >= expected_relative_collection_rights
    if not is_SCO_cleared:
        df[cols.FLOAT_CLEARED_POWER] = 0.0
    return df[cols.FLOAT_CLEARED_POWER]


def get_cleared_power_with_price_curve(
    price_curve: np.ndarray, det_cab: pd.DataFrame, market_periods_count: int
):

    price_curve_dict = {
        i: price_curve[i - 1] for i in range(1, market_periods_count + 1)
    }
    det_cab = det_cab.copy()
    det_cab[cols.FLOAT_CLEARED_PRICE] = det_cab[cols.INT_PERIOD].map(price_curve_dict)

    cleared_power = pd.Series(index=det_cab.index, dtype=float)

    det_cab_buy_mask = det_cab[cols.CAT_BUY_SELL] == "C"
    cleared_power.loc[det_cab_buy_mask] = np.where(
        det_cab.loc[det_cab_buy_mask, cols.FLOAT_BID_PRICE]
        >= det_cab.loc[det_cab_buy_mask, cols.FLOAT_CLEARED_PRICE],
        det_cab.loc[det_cab_buy_mask, cols.FLOAT_BID_POWER],
        0,
    )

    det_cab_sell_simple_mask = (det_cab[cols.CAT_BUY_SELL] == "V") & (
        det_cab[cols.CAT_ORDER_TYPE] == "S"
    )
    cleared_power.loc[det_cab_sell_simple_mask] = np.where(
        det_cab.loc[det_cab_sell_simple_mask, cols.FLOAT_BID_PRICE]
        <= det_cab.loc[det_cab_sell_simple_mask, cols.FLOAT_CLEARED_PRICE],
        det_cab.loc[det_cab_sell_simple_mask, cols.FLOAT_BID_POWER],
        0,
    )

    det_cab_sell_C01_mask = (det_cab[cols.CAT_BUY_SELL] == "V") & (
        det_cab[cols.CAT_ORDER_TYPE] == "C01"
    )
    cleared_power.loc[det_cab_sell_C01_mask] = (
        calculate_cleared_power_from_non_exclusive_block_orders(
            det_cab.loc[det_cab_sell_C01_mask]
        )
    )

    det_cab_sell_C02_mask = (det_cab[cols.CAT_BUY_SELL] == "V") & (
        det_cab[cols.CAT_ORDER_TYPE] == "C02"
    )
    cleared_power.loc[det_cab_sell_C02_mask] = calculate_cleared_power_from_SCOs(
        det_cab.loc[det_cab_sell_C02_mask]
    )

    det_cab_sell_C04_mask = (det_cab[cols.CAT_BUY_SELL] == "V") & (
        det_cab[cols.CAT_ORDER_TYPE] == "C04"
    )
    cleared_power.loc[det_cab_sell_C04_mask] = (
        calculate_cleared_power_from_exclusive_block_order_groups(
            det_cab.loc[det_cab_sell_C04_mask]
        )
    )

    assert not cleared_power.isna().any(), "Some cleared_power is NaN"

    return cleared_power


def get_cleared_power_as_simple_bids_with_price_curve(
    price_curve: np.ndarray, det_cab: pd.DataFrame, market_periods_count: int
):

    price_curve_dict = {
        i: price_curve[i - 1] for i in range(1, market_periods_count + 1)
    }
    det_cab = det_cab.copy()
    det_cab[cols.FLOAT_CLEARED_PRICE] = det_cab[cols.INT_PERIOD].map(price_curve_dict)

    cleared_power = pd.Series(index=det_cab.index, dtype=float)

    det_cab_buy_mask = det_cab[cols.CAT_BUY_SELL] == "C"
    cleared_power.loc[det_cab_buy_mask] = np.where(
        det_cab.loc[det_cab_buy_mask, cols.FLOAT_BID_PRICE]
        >= det_cab.loc[det_cab_buy_mask, cols.FLOAT_CLEARED_PRICE],
        det_cab.loc[det_cab_buy_mask, cols.FLOAT_BID_POWER],
        0,
    )

    det_cab_sell_mask = det_cab[cols.CAT_BUY_SELL] == "V"
    cleared_power.loc[det_cab_sell_mask] = np.where(
        det_cab.loc[det_cab_sell_mask, cols.FLOAT_BID_PRICE]
        <= det_cab.loc[det_cab_sell_mask, cols.FLOAT_CLEARED_PRICE],
        det_cab.loc[det_cab_sell_mask, cols.FLOAT_BID_POWER],
        0,
    )

    assert not cleared_power.isna().any(), "Some cleared_power is NaN"

    return cleared_power
