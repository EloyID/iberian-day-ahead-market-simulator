from itertools import groupby
import numpy as np
import pandas as pd

from mibel_simulator.const import (
    CAT_BUY_SELL,
    CAT_ORDER_TYPE,
    DATE_SESION,
    FLOAT_BID_POWER,
    FLOAT_BID_PRICE,
    ID_ORDER,
    ID_SCO,
    INT_NUM_BLOQ,
    INT_NUM_GRUPO_EXCL,
    INT_PERIODO,
    FLOAT_MAR,
    FLOAT_MAV,
    FLOAT_MIC,
)


def get_float_bid_power_cumsum(
    curva_pbc_df,
    date_column_name=DATE_SESION,
    hour_column_name=INT_PERIODO,
    cod_tipo_oferta_column_name=CAT_BUY_SELL,
    cod_ofertada_casada_column_name=None,
    qua_energia_column_name=FLOAT_BID_POWER,
    qua_precio_column_name=FLOAT_BID_PRICE,
) -> pd.Series:
    """
    Calculates the cumulative sum of bid power for each period, buy/sell type, and price within the DET/CAB DataFrame.

    Buy ('V') and sell ('C') bids are sorted and grouped separately, and their cumulative energy is calculated.

    Args:
        curva_pbc_df (pd.DataFrame): DataFrame containing bids.
        date_column_name (str): Name of the column with session date.
        hour_column_name (str): Name of the column with period/hour.
        cod_tipo_oferta_column_name (str): Name of the column with buy/sell type.
        cod_ofertada_casada_column_name (str, optional): Name of the column with offered/matched type. Defaults to None.
        qua_energia_column_name (str): Name of the column with bid power.
        qua_precio_column_name (str): Name of the column with bid price.

    Returns:
        pd.Series: Series with cumulative bid power for each bid.
    """
    groupby_columns = [date_column_name, hour_column_name]
    if cod_ofertada_casada_column_name is not None:
        groupby_columns.append(cod_ofertada_casada_column_name)

    curva_pbc_df_C = (
        curva_pbc_df.query(f"`{cod_tipo_oferta_column_name}` == 'C'")
        .sort_values(
            [qua_precio_column_name, qua_energia_column_name], ascending=[False, True]
        )
        .groupby(groupby_columns)[qua_energia_column_name]
        .cumsum()
    )
    curva_pbc_df_V = (
        curva_pbc_df.query(f"`{cod_tipo_oferta_column_name}` == 'V'")
        .sort_values(
            [qua_precio_column_name, qua_energia_column_name], ascending=[True, True]
        )
        .groupby(groupby_columns)[qua_energia_column_name]
        .cumsum()
    )

    curva_pbc_energy_cumsum = pd.Series(
        np.nan,
        index=curva_pbc_df.index,
    )

    curva_pbc_energy_cumsum.loc[curva_pbc_df_V.index] = curva_pbc_df_V.values
    curva_pbc_energy_cumsum.loc[curva_pbc_df_C.index] = curva_pbc_df_C.values

    return curva_pbc_energy_cumsum


#### get_cat_order_type_column


def is_simple_bid(
    df: pd.DataFrame,
    num_bloq_column: str = INT_NUM_BLOQ,
    num_grupo_excl_column: str = INT_NUM_GRUPO_EXCL,
    mav_column: str = FLOAT_MAV,
    mar_column: str = FLOAT_MAR,
    fijoeuro_column: str = FLOAT_MIC,
) -> pd.Series:
    return (
        (df[num_bloq_column] == 0)
        & (df[num_grupo_excl_column] == 0)
        & (df[mav_column] == 0)
        & (df[mar_column] == 0)
        & (df[fijoeuro_column] == 0)
    )


def is_SCO(
    df: pd.DataFrame,
    mav_column: str = FLOAT_MAV,
    fijoeuro_column: str = FLOAT_MIC,
) -> pd.Series:
    return (df[mav_column] > 0) | (df[fijoeuro_column] > 0)


def is_not_exclusive_block(
    df: pd.DataFrame,
    num_bloq_column: str = INT_NUM_BLOQ,
    num_grupo_excl_column: str = INT_NUM_GRUPO_EXCL,
) -> pd.Series:
    return (df[num_bloq_column] > 0) & (df[num_grupo_excl_column] == 0)


def is_exclusive_block_group(
    df: pd.DataFrame,
    num_grupo_excl_column: str = INT_NUM_GRUPO_EXCL,
) -> pd.Series:
    return df[num_grupo_excl_column] > 0


def get_cat_order_type_column(
    df: pd.DataFrame,
    num_bloq_column: str = INT_NUM_BLOQ,
    num_grupo_excl_column: str = INT_NUM_GRUPO_EXCL,
    mav_column: str = FLOAT_MAV,
    mar_column: str = FLOAT_MAR,
    fijoeuro_column: str = FLOAT_MIC,
) -> pd.Series:
    """
    Assigns the order type category to each row in the DET/CAB DataFrame based on bid characteristics.

    Order types are determined by the values of block number, exclusion group, mav, mar, and float_mic columns.
    Possible order types: Simple ("S"), SCO ("C02"), Non-exclusive block ("C01"), Exclusive block group ("C04").

    Args:
        df (pd.DataFrame): DataFrame containing DET/CAB bids.
        num_bloq_column (str, optional): Name of the block number column. Defaults to INT_NUM_BLOQ.
        num_grupo_excl_column (str, optional): Name of the exclusion group column. Defaults to INT_NUM_GRUPO_EXCL.
        mav_column (str, optional): Name of the mav column. Defaults to FLOAT_MAV.
        mar_column (str, optional): Name of the mar column. Defaults to FLOAT_MAR.
        fijoeuro_column (str, optional): Name of the float_mic column. Defaults to FLOAT_MIC.

    Returns:
        pd.Series: Series of order type codes for each bid.
    """

    df = df.copy()

    df["is_simple_bid"] = is_simple_bid(
        df,
        num_bloq_column,
        num_grupo_excl_column,
        mav_column,
        mar_column,
        fijoeuro_column,
    )
    df["is_SCO"] = is_SCO(df, mav_column, fijoeuro_column)
    df["is_not_exclusive_block"] = is_not_exclusive_block(
        df, num_bloq_column, num_grupo_excl_column
    )
    df["is_exclusive_block_group"] = is_exclusive_block_group(df, num_grupo_excl_column)

    assert (
        df[
            [
                "is_simple_bid",
                "is_SCO",
                "is_not_exclusive_block",
                "is_exclusive_block_group",
            ]
        ].sum(axis=1)
        == 1
    ).all()

    df[CAT_ORDER_TYPE] = np.where(
        df["is_simple_bid"],
        "S",
        np.where(
            df["is_SCO"],
            "C02",
            np.where(
                df["is_not_exclusive_block"],
                "C01",
                np.where(df["is_exclusive_block_group"], "C04", "Error"),
            ),
        ),
    )
    assert (df[CAT_ORDER_TYPE] != "Error").all()
    return df[CAT_ORDER_TYPE]


def filter_scos_with_mic_from_det_cab(
    det_cab_df: pd.DataFrame,
    scos_with_mic_to_keep: list,
    id_order_column: str = ID_ORDER,
    float_mic_column: str = FLOAT_MIC,
) -> pd.DataFrame:
    return det_cab_df.query(
        f"`{id_order_column}` in @scos_with_mic_to_keep or not `{float_mic_column}` > 0"
    ).copy()
