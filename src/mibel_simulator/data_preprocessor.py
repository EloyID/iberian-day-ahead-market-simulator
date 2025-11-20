import logging
import numpy as np
import pandas as pd

from mibel_simulator.pandas_typing import (
    DET_CAB_DAM_SIMULATOR_TYPING,
)
from mibel_simulator.tools import (
    get_cat_order_type_column,
    get_float_bid_power_cumsum,
)
from mibel_simulator.const import (
    CAT_BUY,
    CAT_BUY_SELL,
    CAT_FRONTIER,
    CAT_OFERTADA_CASADA,
    CAT_ORDER_TYPE,
    CAT_PAIS,
    CAT_PAIS_SPAIN,
    CAT_SELL,
    DATE_SESION,
    DET_CAB_DATE_UNIQUE_IDENTIFIERS,
    FLOAT_BID_POWER,
    FLOAT_BID_PRICE,
    FLOAT_EXPORT_CAPACITY,
    FLOAT_IMPORT_CAPACITY,
    FLOAT_MAR,
    FLOAT_MAV,
    FLOAT_MAX_POWER,
    FLOAT_MIC,
    FLOAT_BID_POWER_CUMSUM,
    FLOAT_BID_POWER_CUMSUM_BY_COUNTRY,
    FLOAT_PRICE_FR,
    FRANCE_CODIGOUNIDAD,
    FRANCE_ID_ORDER,
    FRANCE_ID_UNIDAD,
    ID_BLOCK_ORDER,
    ID_BLOCK_ORDER_CHILD,
    ID_BLOCK_ORDER_PARENT,
    ID_INDIVIDUAL_BID,
    ID_ORDER,
    ID_SCO,
    ID_SCO_CHILD,
    ID_SCO_PARENT,
    ID_UNIDAD,
    INT_NUM_BLOQ,
    INT_NUM_GRUPO_EXCL,
    INT_NUM_TRAMO,
    INT_PERIODO,
    PORTUGAL_ZONE,
    SPAIN_ZONE,
    CAT_PAIS,
    CAT_PAIS,
)

logger = logging.getLogger(__name__)


def get_france_det_cab_date_from_price(
    price_france: pd.DataFrame,
    capacidad_inter: pd.DataFrame,
    date: pd.Timestamp | str = "",
):
    """
    Generate DET/CAB-like DataFrame rows for the France-Spain interconnection based on price and capacity data for a specific date.

    This function creates two sets of bids:
    - "Sell" bids: France sells to Spain, using import capacity.
    - "Buy" bids: Spain sells to France, using export capacity.

    Each bid is assigned standard values for required columns and concatenated into a single DataFrame.

    Args:
        price_france (pd.DataFrame): DataFrame with France day-ahead prices. Must contain columns for session period and price. If DATE_SESION column is present, it will be filtered by the provided date.
        capacidad_inter (pd.DataFrame): DataFrame with interconnection capacities. Must contain columns for import and export capacity. If DATE_SESION column is present, it will be filtered by the provided date.
        date (pd.Timestamp | str, optional): Specific date to filter the data. Defaults to "" (no filtering).

    Returns:
        pd.DataFrame: DataFrame in DET/CAB format, ready for use in the DAM simulator.
    """

    if DATE_SESION in price_france.columns:
        if date:
            date_sesion = pd.Timestamp(date)
            price_france = price_france[
                price_france[DATE_SESION].dt.date == date_sesion.date()
            ]
        elif price_france[DATE_SESION].nunique() > 1:
            raise ValueError(
                "price_france contains multiple dates in DATE_SESION column, date parameter must be provided to filter the data."
            )

    if CAT_FRONTIER in capacidad_inter.columns:
        capacidad_inter = capacidad_inter[capacidad_inter[CAT_FRONTIER] == 3].copy()
    if DATE_SESION in capacidad_inter.columns:
        if date:
            date_sesion = pd.Timestamp(date)
            capacidad_inter = capacidad_inter[
                capacidad_inter[DATE_SESION].dt.date == date_sesion.date()
            ]
        elif capacidad_inter[DATE_SESION].nunique() > 1:
            raise ValueError(
                "capacidad_inter contains multiple dates in DATE_SESION column, date parameter must be provided to filter the data."
            )

    sell_det_cab = pd.merge(
        price_france[[INT_PERIODO, FLOAT_PRICE_FR]],
        capacidad_inter[[INT_PERIODO, FLOAT_IMPORT_CAPACITY]],
        how="inner",
        on=INT_PERIODO,
        validate="one_to_one",
    )
    sell_det_cab[CAT_BUY_SELL] = CAT_SELL
    sell_det_cab[FLOAT_BID_POWER] = sell_det_cab[FLOAT_IMPORT_CAPACITY].abs()
    sell_det_cab = sell_det_cab.drop(columns=[FLOAT_IMPORT_CAPACITY])

    buy_det_cab = pd.merge(
        price_france[[INT_PERIODO, FLOAT_PRICE_FR]],
        capacidad_inter[[INT_PERIODO, FLOAT_EXPORT_CAPACITY]],
        how="inner",
        on=INT_PERIODO,
        validate="one_to_one",
    )
    buy_det_cab[CAT_BUY_SELL] = CAT_BUY
    buy_det_cab[FLOAT_BID_POWER] = buy_det_cab[FLOAT_EXPORT_CAPACITY].abs()
    buy_det_cab = buy_det_cab.drop(columns=[FLOAT_EXPORT_CAPACITY])

    det_cab = pd.concat([sell_det_cab, buy_det_cab], ignore_index=True)
    det_cab = (
        det_cab.assign(
            **{
                ID_ORDER: FRANCE_ID_ORDER,
                ID_UNIDAD: FRANCE_ID_UNIDAD,
                FLOAT_MIC: 0,
                FLOAT_MAX_POWER: 99999999,  # High value to avoid issues
                INT_NUM_BLOQ: 1,
                INT_NUM_TRAMO: 1,
                INT_NUM_GRUPO_EXCL: 0,
                FLOAT_MAV: 0,
                FLOAT_MAR: 0,
                CAT_PAIS: CAT_PAIS_SPAIN,  # Spain because it's sell from France to Spain
            }
        )
        .rename(columns={FLOAT_PRICE_FR: FLOAT_BID_PRICE})
        .dropna()
    )
    return det_cab


def get_det_cab_date_id_individual_bid(det_cab_date: pd.DataFrame) -> pd.Series:
    """
    Generates a unique identifier for each individual bid in the DET/CAB DataFrame by concatenating key columns.

    Ensures that each bid has a unique identifier and raises an error if duplicates are found.

    Args:
        det_cab_date (pd.DataFrame): DataFrame containing DET/CAB bids.

    Returns:
        pd.Series: Series of unique identifiers for each bid.
    """
    unique_id = (
        det_cab_date[DET_CAB_DATE_UNIQUE_IDENTIFIERS].astype(str).agg("_".join, axis=1)
    )
    assert (
        not unique_id.duplicated().any()
    ), "There are duplicated unique identifiers in det_cab_date"
    return unique_id


def get_det_cab_date_id_block_order(det_cab_date: pd.DataFrame) -> np.ndarray:
    """
    Generates a unique identifier for block orders in the DET/CAB DataFrame based on order type, order ID, block number, and exclusion group.

    Returns NaN for non-block orders.

    Args:
        det_cab_date (pd.DataFrame): DataFrame containing DET/CAB bids.

    Returns:
        np.ndarray: Array of block order identifiers, or NaN for non-block orders.
    """
    return np.where(
        det_cab_date[CAT_ORDER_TYPE].isin(["C01", "C04"]),
        det_cab_date[ID_ORDER].astype(str)
        + "_B_"
        + det_cab_date[INT_NUM_BLOQ].astype(str)
        + "_GE_"
        + det_cab_date[INT_NUM_GRUPO_EXCL].astype(str),
        np.nan,
    )


def get_det_cab_date_id_sco(det_cab_date: pd.DataFrame) -> np.ndarray:
    """
    Generates a unique identifier for SCO orders in the DET/CAB DataFrame based on order type, order ID, and tramo number.

    Returns NaN for non-SCO orders.

    Args:
        det_cab_date (pd.DataFrame): DataFrame containing DET/CAB bids.

    Returns:
        np.ndarray: Array of SCO order identifiers, or NaN for non-SCO orders.
    """
    return np.where(
        det_cab_date[CAT_ORDER_TYPE] == "C02",
        det_cab_date[ID_ORDER].astype(str)
        + "_SCO_"
        + det_cab_date[INT_NUM_TRAMO].astype(str),
        np.nan,
    )


def get_det_cab_date_for_DAM_simulator(
    det_date: pd.DataFrame,
    cab_date: pd.DataFrame,
    det_cab_fr_date: pd.DataFrame,
    uof_zones: pd.DataFrame,
    zones_default_to_spain: bool = False,
) -> pd.DataFrame:
    """
    Merge DET, CAB, and France DET/CAB data for a single day, assign order types and unique identifiers, and enrich with UOF zone information.

    This function combines DET and CAB data, appends France interconnection bids, assigns order types, computes cumulative bid power, and merges zone information.

    It also handles missing units and assigns default zones if specified.

    Args:
        det_date (pd.DataFrame): DataFrame with DET data for a single day.
        cab_date (pd.DataFrame): DataFrame with CAB data for a single day.
        det_cab_fr_date (pd.DataFrame): DataFrame with France DET/CAB bids for the same day.
        uof_zones (pd.DataFrame): DataFrame mapping units to zones.
        zones_default_to_spain (bool, optional): If True, assign missing units to Spain zone. Defaults to False.

    Raises:
        ValueError: If there are units not found in zone mapping and zones_default_to_spain is False.

    Returns:
        pd.DataFrame: Merged and enriched DET/CAB DataFrame for DAM simulation.
    """

    # Validate input data contains a single day
    if DATE_SESION in det_date.columns:
        assert (
            det_date[DATE_SESION].nunique() == 1
        ), "det_date must contain data for a single day"

    if DATE_SESION in cab_date.columns:
        assert (
            cab_date[DATE_SESION].nunique() == 1
        ), "cab_date must contain data for a single day"

    if DATE_SESION in det_cab_fr_date.columns:
        assert (
            det_cab_fr_date[DATE_SESION].nunique() == 1
        ), "det_cab_fr_date must contain data for a single day"

    # Merge DET and CAB data for the day
    det_cab_date = pd.merge(
        det_date,
        cab_date,
        how="outer",
        on=[DATE_SESION, ID_ORDER],
        suffixes=("_det", "_cab"),
        validate="many_to_one",
        indicator=True,
    )
    assert det_cab_date._merge.isin(["both"]).all()
    det_cab_date = det_cab_date.drop(columns="_merge")

    # Merge with UOF zones
    det_cab_date = det_cab_date.merge(
        uof_zones,
        on=ID_UNIDAD,
        how="left",
        validate="many_to_one",
        indicator=True,
    )

    # Handle units not found in zone mapping
    not_merged_unidades = det_cab_date.query("_merge != 'both'")[ID_UNIDAD].unique()
    if len(not_merged_unidades) > 0:
        if zones_default_to_spain:
            det_cab_date.loc[
                det_cab_date[ID_UNIDAD].isin(not_merged_unidades),
                CAT_PAIS,
            ] = SPAIN_ZONE
            logger.warning(
                f"The following {ID_UNIDAD} in det_cab_date were not found in unidades_zona and have been assigned to {SPAIN_ZONE}: {not_merged_unidades}"
            )
        else:
            raise ValueError(
                f"The following {ID_UNIDAD} in det_cab_date were not found in unidades_zona: {not_merged_unidades}"
            )
    det_cab_date = det_cab_date.drop(columns=["_merge"])

    # Append France interconnection bids
    det_cab_date = pd.concat([det_cab_date, det_cab_fr_date], ignore_index=True)

    # Add columns required for DAM simulator
    det_cab_date[CAT_ORDER_TYPE] = get_cat_order_type_column(det_cab_date)
    det_cab_date[FLOAT_BID_POWER_CUMSUM] = get_float_bid_power_cumsum(
        det_cab_date,
        date_column_name=DATE_SESION,
        hour_column_name=INT_PERIODO,
        cod_tipo_oferta_column_name=CAT_BUY_SELL,
        qua_energia_column_name=FLOAT_BID_POWER,
        qua_precio_column_name=FLOAT_BID_PRICE,
    )
    det_cab_date[ID_INDIVIDUAL_BID] = get_det_cab_date_id_individual_bid(det_cab_date)
    det_cab_date[ID_BLOCK_ORDER] = get_det_cab_date_id_block_order(det_cab_date)
    det_cab_date[ID_SCO] = get_det_cab_date_id_sco(det_cab_date)

    # Recalculate FLOAT_BID_POWER_CUMSUM by country
    det_cab_date = det_cab_date.sort_values(
        by=[INT_PERIODO, CAT_BUY_SELL, FLOAT_BID_POWER_CUMSUM]
    ).copy()
    for country in [SPAIN_ZONE, PORTUGAL_ZONE]:
        det_cab_date.loc[
            (det_cab_date[CAT_PAIS] == country),
            FLOAT_BID_POWER_CUMSUM_BY_COUNTRY,
        ] = get_float_bid_power_cumsum(
            det_cab_date.loc[(det_cab_date[CAT_PAIS] == country)],
            date_column_name=DATE_SESION,
            hour_column_name=INT_PERIODO,
            cod_tipo_oferta_column_name=CAT_BUY_SELL,
            qua_energia_column_name=FLOAT_BID_POWER,
            qua_precio_column_name=FLOAT_BID_PRICE,
        )

    # Set correct typing
    det_cab_date = det_cab_date.astype(DET_CAB_DAM_SIMULATOR_TYPING)
    return det_cab_date


########### sets creation


def get_parent_child_bloques(det_cab_date: pd.DataFrame) -> pd.DataFrame:
    """
    Finds parent-child relationships between non-exclusive block orders in the DET/CAB DataFrame.

    For each block order, identifies the next block order (child) within the same order and exclusion group.

    Args:
        det_cab_date (pd.DataFrame): DataFrame containing DET/CAB bids.

    Returns:
        pd.DataFrame: DataFrame with columns for parent and child block order relationships.
    """
    det_cab_date_bloques_no_exclusivos = (
        det_cab_date.dropna(subset=ID_BLOCK_ORDER)
        .drop_duplicates(ID_BLOCK_ORDER)
        .query(f"{INT_NUM_GRUPO_EXCL} == 0")
        .copy()
    )
    parent_child_bloques = (
        det_cab_date_bloques_no_exclusivos.merge(
            det_cab_date_bloques_no_exclusivos,
            on=[ID_ORDER, INT_NUM_GRUPO_EXCL],
            how="left",
            suffixes=("_parent", "_child"),
        )
        .query(f"{INT_NUM_BLOQ}_parent < {INT_NUM_BLOQ}_child")
        .sort_values([ID_ORDER, f"{INT_NUM_BLOQ}_parent", f"{INT_NUM_BLOQ}_child"])
        .drop_duplicates([ID_ORDER, f"{INT_NUM_BLOQ}_parent"], keep="first")[
            [
                ID_ORDER,
                f"{INT_NUM_BLOQ}_parent",
                INT_NUM_GRUPO_EXCL,
                f"{INT_NUM_BLOQ}_child",
                ID_BLOCK_ORDER_CHILD,
                ID_BLOCK_ORDER_PARENT,
            ]
        ]
    )
    return parent_child_bloques


def get_exclusive_block_orders_grouped(det_cab_date: pd.DataFrame) -> pd.Series:
    """
    Groups exclusive block orders by order ID and exclusion group in the DET/CAB DataFrame.

    Creates a list of block order identifiers for each exclusive group.

    Args:
        det_cab_date (pd.DataFrame): DataFrame containing DET/CAB bids.

    Returns:
        pd.Series: Series mapping (order ID, exclusion group) to lists of block order identifiers.
    """
    exclusive_block_orders_grouped = (
        det_cab_date.query(f"{INT_NUM_GRUPO_EXCL} > 0")
        .drop_duplicates([ID_ORDER, INT_NUM_GRUPO_EXCL, INT_NUM_BLOQ])
        .groupby([ID_ORDER, INT_NUM_GRUPO_EXCL], observed=True)[ID_BLOCK_ORDER]
        .apply(list)
    )
    return exclusive_block_orders_grouped


def get_parent_child_scos(det_cab_date: pd.DataFrame) -> pd.DataFrame:
    """
    Finds parent-child relationships between SCO orders in the DET/CAB DataFrame.

    For each SCO order, identifies the next SCO order (child) within the same order.

    Args:
        det_cab_date (pd.DataFrame): DataFrame containing DET/CAB bids.

    Returns:
        pd.DataFrame: DataFrame with columns for parent and child SCO relationships.
    """
    det_cab_date_scos = (
        det_cab_date.dropna(subset=ID_SCO)
        .drop_duplicates(ID_SCO)[
            [
                ID_ORDER,
                INT_NUM_TRAMO,
                ID_SCO,
            ]
        ]
        .copy()
    )

    parent_child_scos = (
        det_cab_date_scos.merge(
            det_cab_date_scos,
            on=ID_ORDER,
            how="left",
            suffixes=("_parent", "_child"),
        )
        .query(f"{INT_NUM_TRAMO}_parent < {INT_NUM_TRAMO}_child")
        .sort_values([ID_ORDER, f"{INT_NUM_TRAMO}_parent", f"{INT_NUM_TRAMO}_child"])
        .drop_duplicates([ID_ORDER, f"{INT_NUM_TRAMO}_parent"], keep="first")[
            [
                ID_ORDER,
                f"{INT_NUM_TRAMO}_parent",
                f"{INT_NUM_TRAMO}_child",
                ID_SCO_CHILD,
                ID_SCO_PARENT,
            ]
        ]
    )
    return parent_child_scos


def get_sco_bids_tramo_grouped(det_cab_date: pd.DataFrame) -> pd.Series:
    """
    Groups individual bids by SCO order in the DET/CAB DataFrame.

    Creates a list of individual bid identifiers for each SCO.

    Args:
        det_cab_date (pd.DataFrame): DataFrame containing DET/CAB bids.

    Returns:
        pd.Series: Series mapping SCO order identifiers to lists of individual bid identifiers.
    """

    sco_bids_tramo_grouped = (
        det_cab_date.query(f"{ID_SCO}.notna()")
        .groupby([ID_SCO], observed=True)[ID_INDIVIDUAL_BID]
        .apply(list)
    )
    return sco_bids_tramo_grouped


def get_all_mic_scos(det_cab_date: pd.DataFrame) -> list:
    """
    Returns a sorted list of order IDs for all SCOs with a positive MIC value in the DET/CAB DataFrame.

    Args:
        det_cab_date (pd.DataFrame): DataFrame containing DET/CAB bids.

    Returns:
        list: List of order IDs for SCOs with MIC > 0.
    """
    all_mic_scos = (
        det_cab_date.query(f"{FLOAT_MIC} > 0")[ID_ORDER].sort_values().unique().tolist()
    )
    return all_mic_scos
