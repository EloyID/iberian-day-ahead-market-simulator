import numpy as np
import pandas as pd

import mibel_simulator.columns as cols
from mibel_simulator.file_paths import UOF_ZONES_FILEPATH
from mibel_simulator.schemas.uof_zones import UOFZonesSchema


def get_float_bid_power_cumsum(
    curva_pbc_df,
    date_column_name=cols.DATE_SESION,
    hour_column_name=cols.INT_PERIODO,
    cod_tipo_oferta_column_name=cols.CAT_BUY_SELL,
    cod_ofertada_casada_column_name=cols.CAT_OFERTADA_CASADA,
    qua_energia_column_name=cols.FLOAT_BID_POWER,
    qua_precio_column_name=cols.FLOAT_BID_PRICE,
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
    groupby_columns = [hour_column_name]
    if cod_ofertada_casada_column_name is not None:
        groupby_columns.append(cod_ofertada_casada_column_name)
    if date_column_name is not None:
        groupby_columns.append(date_column_name)

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


def get_is_simple_bid(
    det_cab: pd.DataFrame,
    id_order_column: str = cols.ID_ORDER,
    num_bloq_column: str = cols.INT_NUM_BLOQ,
    num_grupo_excl_column: str = cols.INT_NUM_GRUPO_EXCL,
    mav_column: str = cols.FLOAT_MAV,
    mar_column: str = cols.FLOAT_MAR,
    fijoeuro_column: str = cols.FLOAT_MIC,
) -> pd.Series:
    """
    Identifies simple bids in the DET/CAB DataFrame.

    A simple bid is defined as a bid that is not a block, not part of an exclusive group,
    has MAR equal to zero, and is not an SCO.

    We check separately for SCOs using the `get_is_SCO` function because is automatically an SCO
    if other bids of the same order have MAV or FIXOEURO greater than zero.

    Args:
        det_cab (pd.DataFrame): DataFrame containing DET/CAB bids.
        id_order_column (str, optional): Name of the order ID column. Defaults to cols.ID_ORDER.
        num_bloq_column (str, optional): Name of the block number column. Defaults to cols.INT_NUM_BLOQ.
        num_grupo_excl_column (str, optional): Name of the exclusion group column. Defaults to cols.INT_NUM_GRUPO_EXCL.
        mav_column (str, optional): Name of the MAV column. Defaults to cols.FLOAT_MAV.
        mar_column (str, optional): Name of the MAR column. Defaults to cols.FLOAT_MAR.
        fijoeuro_column (str, optional): Name of the MIC column. Defaults to cols.FLOAT_MIC.

    Returns:
        pd.Series: Boolean Series indicating which bids are simple bids.
    """
    is_SCO_ = get_is_SCO(det_cab, id_order_column, mav_column, fijoeuro_column)
    return (
        (det_cab[num_bloq_column] == 0)
        & (det_cab[num_grupo_excl_column] == 0)
        & (det_cab[mar_column] == 0)
        & (~is_SCO_)
    )


def get_is_SCO(
    det_cab: pd.DataFrame,
    id_order_column: str = cols.ID_ORDER,
    mav_column: str = cols.FLOAT_MAV,
    fijoeuro_column: str = cols.FLOAT_MIC,
) -> pd.Series:
    """
    Identifies which bids in the DET/CAB DataFrame belong to an SCO (Complex Order).

    An order is considered an SCO if any bid of the same order has MAV > 0 or MIC > 0.
    This function returns a boolean Series indicating, for each row, whether its order is an SCO.

    Args:
        det_cab (pd.DataFrame): DataFrame containing DET/CAB bids.
        id_order_column (str, optional): Name of the order ID column. Defaults to cols.ID_ORDER.
        mav_column (str, optional): Name of the MAV column. Defaults to cols.FLOAT_MAV.
        fijoeuro_column (str, optional): Name of the MIC column. Defaults to cols.FLOAT_MIC.

    Returns:
        pd.Series: Boolean Series indicating which bids are part of an SCO.
    """

    has_sco_attributes = (det_cab[mav_column] > 0) | (det_cab[fijoeuro_column] > 0)
    id_orders_with_sco_attributes = (
        det_cab.loc[has_sco_attributes, id_order_column].unique().tolist()
    )
    return det_cab[id_order_column].isin(id_orders_with_sco_attributes)


def get_is_not_exclusive_block(
    det_cab: pd.DataFrame,
    num_bloq_column: str = cols.INT_NUM_BLOQ,
    num_grupo_excl_column: str = cols.INT_NUM_GRUPO_EXCL,
) -> pd.Series:
    """
    Identifies non-exclusive block bids in the DET/CAB DataFrame.

    A non-exclusive block bid is defined as a bid with block number greater than zero and not belonging to any exclusive group.

    Args:
        det_cab (pd.DataFrame): DataFrame containing DET/CAB bids.
        num_bloq_column (str, optional): Name of the block number column. Defaults to cols.INT_NUM_BLOQ.
        num_grupo_excl_column (str, optional): Name of the exclusion group column. Defaults to cols.INT_NUM_GRUPO_EXCL.

    Returns:
        pd.Series: Boolean Series indicating which bids are non-exclusive block bids.
    """
    return (det_cab[num_bloq_column] > 0) & (det_cab[num_grupo_excl_column] == 0)


def get_is_exclusive_block_group(
    det_cab: pd.DataFrame,
    num_grupo_excl_column: str = cols.INT_NUM_GRUPO_EXCL,
) -> pd.Series:
    """
    Identifies exclusive block group bids in the DET/CAB DataFrame.

    An exclusive block group bid is defined as a bid that belongs to an exclusive group (exclusion group number greater than zero).

    Args:
        det_cab (pd.DataFrame): DataFrame containing DET/CAB bids.
        num_grupo_excl_column (str, optional): Name of the exclusion group column. Defaults to cols.INT_NUM_GRUPO_EXCL.

    Returns:
        pd.Series: Boolean Series indicating which bids are part of an exclusive block group.
    """

    return det_cab[num_grupo_excl_column] > 0


def get_cat_order_type_column(
    det_cab: pd.DataFrame,
    id_order_column: str = cols.ID_ORDER,
    num_bloq_column: str = cols.INT_NUM_BLOQ,
    num_grupo_excl_column: str = cols.INT_NUM_GRUPO_EXCL,
    mav_column: str = cols.FLOAT_MAV,
    mar_column: str = cols.FLOAT_MAR,
    fijoeuro_column: str = cols.FLOAT_MIC,
) -> pd.Series:
    """
    Assigns the order type category to each row in the DET/CAB DataFrame based on bid characteristics.

    Order types are determined by the values of block number, exclusion group, mav, mar, and float_mic columns.
    Possible order types: Simple ("S"), SCO ("C02"), Non-exclusive block ("C01"), Exclusive block group ("C04").

    Args:
        det_cab (pd.DataFrame): DataFrame containing DET/CAB bids.
        num_bloq_column (str, optional): Name of the block number column. Defaults to cols.INT_NUM_BLOQ.
        num_grupo_excl_column (str, optional): Name of the exclusion group column. Defaults to cols.INT_NUM_GRUPO_EXCL.
        mav_column (str, optional): Name of the mav column. Defaults to cols.FLOAT_MAV.
        mar_column (str, optional): Name of the mar column. Defaults to cols.FLOAT_MAR.
        fijoeuro_column (str, optional): Name of the float_mic column. Defaults to cols.FLOAT_MIC.

    Returns:
        pd.Series: Series of order type codes for each bid.
    """

    det_cab = det_cab.copy()

    det_cab[cols.BOOL_IS_SIMPLE_BID] = get_is_simple_bid(
        det_cab,
        id_order_column,
        num_bloq_column,
        num_grupo_excl_column,
        mav_column,
        mar_column,
        fijoeuro_column,
    )
    det_cab[cols.BOOL_IS_SCO] = get_is_SCO(
        det_cab, id_order_column, mav_column, fijoeuro_column
    )
    det_cab[cols.BOOL_IS_NOT_EXCLUSIVE_GROUP] = get_is_not_exclusive_block(
        det_cab, num_bloq_column, num_grupo_excl_column
    )
    det_cab[cols.BOOL_IS_EXCLUSIVE_GROUP] = get_is_exclusive_block_group(
        det_cab, num_grupo_excl_column
    )

    assert (
        det_cab[
            [
                cols.BOOL_IS_SIMPLE_BID,
                cols.BOOL_IS_SCO,
                cols.BOOL_IS_NOT_EXCLUSIVE_GROUP,
                cols.BOOL_IS_EXCLUSIVE_GROUP,
            ]
        ].sum(axis=1)
        == 1
    ).all()

    det_cab[cols.CAT_ORDER_TYPE] = np.where(
        det_cab[cols.BOOL_IS_SIMPLE_BID],
        "S",
        np.where(
            det_cab[cols.BOOL_IS_SCO],
            "C02",
            np.where(
                det_cab[cols.BOOL_IS_NOT_EXCLUSIVE_GROUP],
                "C01",
                np.where(det_cab[cols.BOOL_IS_EXCLUSIVE_GROUP], "C04", "Error"),
            ),
        ),
    )
    assert (det_cab[cols.CAT_ORDER_TYPE] != "Error").all()
    return det_cab[cols.CAT_ORDER_TYPE]


def filter_mic_scos_from_det_cab(
    det_cab_df: pd.DataFrame,
    mic_scos_to_keep: list,
    id_order_column: str = cols.ID_ORDER,
    float_mic_column: str = cols.FLOAT_MIC,
) -> pd.DataFrame:
    return det_cab_df.query(
        f"`{id_order_column}` in @mic_scos_to_keep or not `{float_mic_column}` > 0"
    ).copy()


def concat_provided_uof_zones_with_existing_data(
    user_uof_zones: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge user-provided UOF zones with the packaged reference list.

    The function copies and schema-validates the user dataframe (only `cols.ID_UNIDAD`
    and `cols.CAT_PAIS`), loads the bundled UOF zones CSV and returns a combined dataframe
    ready for downstream use (e.g., deduplication or reconciliation).

    Args:
        user_uof_zones: DataFrame with at least the columns `cols.ID_UNIDAD`
            and `cols.CAT_PAIS` to append to the existing UOF zones list.

    Returns:
        A concatenated DataFrame containing existing and user-provided UOF zones.

    Raises:
        SchemaError: If `user_uof_zones` fails validation against `UOFZonesSchema`.
        FileNotFoundError: If the existing UOF zones CSV cannot be loaded.
    """
    user_uof_zones = user_uof_zones.copy()[[cols.ID_UNIDAD, cols.CAT_PAIS]]
    UOFZonesSchema.validate(user_uof_zones)

    existing_uof_zones = pd.read_csv(UOF_ZONES_FILEPATH)

    user_uof_zones["__origin"] = "user"
    existing_uof_zones["__origin"] = "existing"

    altered_zones = (
        pd.concat([existing_uof_zones, user_uof_zones], ignore_index=True)
        .groupby([cols.ID_UNIDAD])
        .filter(lambda x: len(x[cols.CAT_PAIS].unique()) > 1)
    )
    if not altered_zones.empty:
        altered_zones_list = altered_zones[cols.ID_UNIDAD].unique().tolist()
        # warning
        print(
            f"Warning: The following UOF zones are different from existing in the package and will use the user-provided values: {altered_zones_list}"
        )

    user_unidades = user_uof_zones[cols.ID_UNIDAD].unique().tolist()
    existing_uof_zones_filtered = existing_uof_zones.query(
        f"`{cols.ID_UNIDAD}` not in @user_unidades"
    )
    combined_uof_zones = (
        pd.concat([existing_uof_zones_filtered, user_uof_zones], ignore_index=True)
        .reset_index(drop=True)
        .drop(columns="__origin")
    )

    return combined_uof_zones
