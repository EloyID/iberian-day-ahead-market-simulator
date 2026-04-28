import numpy as np
import pandas as pd
import pyomo.environ as pyo

import iberian_day_ahead_market_simulator.columns as cols
from iberian_day_ahead_market_simulator.tools import get_float_bid_power_cumsum

from .const import PORTUGAL_ZONE, SPAIN_ZONE


def get_clearing_prices_df(model: pyo.ConcreteModel) -> pd.DataFrame:
    """_summary_

    Args:
        model (pyo.ConcreteModel): _description_

    Returns:
        pd.DataFrame: _description_
    """
    spain_clearing_prices = [
        model.dual[model.c_Balance[p, SPAIN_ZONE]] for p in model.PERIODS
    ]
    portugal_clearing_prices = [
        model.dual[model.c_Balance[p, PORTUGAL_ZONE]] for p in model.PERIODS
    ]
    spain_clearing_price_df = pd.DataFrame(
        {
            cols.INT_PERIOD: list(range(1, 25)),
            cols.FLOAT_CLEARED_PRICE: spain_clearing_prices,
            cols.CAT_BIDDING_ZONE: SPAIN_ZONE,
        }
    )
    portugal_clearing_price_df = pd.DataFrame(
        {
            cols.INT_PERIOD: list(range(1, 25)),
            cols.FLOAT_CLEARED_PRICE: portugal_clearing_prices,
            cols.CAT_BIDDING_ZONE: PORTUGAL_ZONE,
        }
    )
    return pd.concat(
        [spain_clearing_price_df, portugal_clearing_price_df], ignore_index=True
    )


########################## Cleared Energy Series #########################


def get_simple_sellers_cleared_energy_series(model: pyo.ConcreteModel) -> pd.Series:
    """_summary_

    Args:
        model (pyo.ConcreteModel): _description_

    Returns:
        pd.Series: _description_
    """
    simple_cleared_sellers_energy = {
        s: pyo.value(
            model.v_x_SIMPLE_SELLER_BIDS[s] * model.p_quantity_SIMPLE_SELLER_BIDS[s]
        )
        for s in model.SIMPLE_SELLER_BIDS
        if pyo.value(model.v_x_SIMPLE_SELLER_BIDS[s]) > 0
    }
    return pd.Series(simple_cleared_sellers_energy, name=cols.FLOAT_CLEARED_POWER)


def get_block_orders_cleared_energy_series(model: pyo.ConcreteModel) -> pd.Series:
    """_summary_

    Args:
        model (pyo.ConcreteModel): _description_

    Returns:
        pd.Series: _description_
    """
    block_cleared_sellers_energy = {
        s: pyo.value(model.v_x_BLOCK_ORDERS[bo] * model.p_quantity_BLOCK_ORDER_BIDS[s])
        for bo in model.BLOCK_ORDERS
        for s in model.BLOCK_ORDER_BIDS_BY_BLOCK[bo]
        if pyo.value(model.v_x_BLOCK_ORDERS[bo]) > 0
    }
    return pd.Series(block_cleared_sellers_energy, name=cols.FLOAT_CLEARED_POWER)


def get_sco_cleared_energy_series(model: pyo.ConcreteModel) -> pd.Series:
    """_summary_

    Args:
        model (pyo.ConcreteModel): _description_

    Returns:
        pd.Series: _description_
    """
    sco_cleared_sellers_energy = {
        s: pyo.value(model.v_x_SCO_SELLER_BIDS[s] * model.p_quantity_SCO_SELLER_BIDS[s])
        for s in model.SCO_SELLER_BIDS
        if pyo.value(model.v_x_SCO_SELLER_BIDS[s]) > 0
    }
    return pd.Series(sco_cleared_sellers_energy, name=cols.FLOAT_CLEARED_POWER)


def get_buyers_cleared_energy_series(model: pyo.ConcreteModel) -> pd.Series:
    """_summary_

    Args:
        model (pyo.ConcreteModel): _description_

    Returns:
        pd.Series: _description_
    """
    cleared_buyers_energy = {
        b: pyo.value(model.v_x_BUYER_BIDS[b] * model.p_quantity_BUYER_BIDS[b])
        for b in model.BUYER_BIDS
        if pyo.value(model.v_x_BUYER_BIDS[b]) > 0
    }
    return pd.Series(cleared_buyers_energy, name=cols.FLOAT_CLEARED_POWER)


def get_france_export_bids_cleared_energy_series(model: pyo.ConcreteModel) -> pd.Series:
    """_summary_

    Args:
        model (pyo.ConcreteModel): _description_

    Returns:
        pd.Series: _description_
    """
    france_export_cleared_energy = {
        s: pyo.value(
            model.v_x_FRANCE_EXPORT_BIDS[s] * model.p_quantity_FRANCE_EXPORT_BIDS[s]
        )
        for s in model.FRANCE_EXPORT_BIDS
        if pyo.value(model.v_x_FRANCE_EXPORT_BIDS[s]) > 0
    }
    return pd.Series(france_export_cleared_energy, name=cols.FLOAT_CLEARED_POWER)


def get_france_import_bids_cleared_energy_series(model: pyo.ConcreteModel) -> pd.Series:
    """_summary_

    Args:
        model (pyo.ConcreteModel): _description_

    Returns:
        pd.Series: _description_
    """
    france_import_cleared_energy = {
        s: pyo.value(
            model.v_x_FRANCE_IMPORT_BIDS[s] * model.p_quantity_FRANCE_IMPORT_BIDS[s]
        )
        for s in model.FRANCE_IMPORT_BIDS
        if pyo.value(model.v_x_FRANCE_IMPORT_BIDS[s]) > 0
    }
    return pd.Series(france_import_cleared_energy, name=cols.FLOAT_CLEARED_POWER)


def get_cleared_energy_series(model: pyo.ConcreteModel) -> pd.DataFrame:
    """_summary_

    Args:
        model (pyo.ConcreteModel): _description_

    Returns:
        pd.DataFrame: _description_
    """
    simple_sellers_energy = get_simple_sellers_cleared_energy_series(model)
    block_orders_energy = get_block_orders_cleared_energy_series(model)
    sco_energy = get_sco_cleared_energy_series(model)
    cleared_buyers_energy = get_buyers_cleared_energy_series(model)
    france_export_energy = get_france_export_bids_cleared_energy_series(model)
    france_import_energy = get_france_import_bids_cleared_energy_series(model)

    series_list = [
        simple_sellers_energy,
        block_orders_energy,
        sco_energy,
        cleared_buyers_energy,
        france_export_energy,
        france_import_energy,
    ]
    non_empty_series = [s for s in series_list if not s.empty]
    return pd.concat(
        non_empty_series,
        axis=0,
    )


############################ Analyze results #########################


def get_clearing_prices(model: pyo.ConcreteModel) -> pd.DataFrame:
    """_summary_

    Args:
        model (pyo.ConcreteModel): _description_

    Returns:
        pd.DataFrame: _description_
    """
    spain_clearing_prices = [
        model.dual[model.c_Balance[p, SPAIN_ZONE]] for p in model.PERIODS
    ]
    portugal_clearing_prices = [
        model.dual[model.c_Balance[p, PORTUGAL_ZONE]] for p in model.PERIODS
    ]
    clearing_prices = pd.DataFrame(
        {
            "Precio_casacion_ES": spain_clearing_prices,
            "Precio_casacion_PT": portugal_clearing_prices,
        },
        index=list(range(1, 25)),
    )
    return clearing_prices


def get_spain_portugal_transmissions(model: pyo.ConcreteModel) -> pd.DataFrame:
    """_summary_

    Args:
        model (pyo.ConcreteModel): _description_

    Returns:
        pd.DataFrame: _description_
    """
    spain_portugal_transmissions = [
        model.v_transmission_spain_portugal[p]() for p in model.PERIODS
    ]
    spain_portugal_transmissions = pd.DataFrame(
        {
            "Transmision_ES_PT": spain_portugal_transmissions,
        },
        index=list(range(1, 25)),
    )
    return spain_portugal_transmissions


def get_spain_portugal_transmissions_det_cab_df(
    model: pyo.ConcreteModel, dat_sesion: str
) -> pd.DataFrame:
    """_summary_

    Args:
        model (pyo.ConcreteModel): _description_
        dat_sesion (str): _description_

    Returns:
        pd.DataFrame: _description_
    """
    spain_portugal_transmissions = get_spain_portugal_transmissions(model)
    international_flows = []

    for period in model.PERIODS:
        if (
            model.dual[model.c_Balance[period, PORTUGAL_ZONE]]
            != model.dual[model.c_Balance[period, SPAIN_ZONE]]
        ):
            spain_portugal_flow = spain_portugal_transmissions.loc[
                period, "Transmision_ES_PT"
            ]

            spanish_entry = {
                "dat_sesion": dat_sesion,
                cols.INT_PERIOD: period,
                cols.ID_UNIDAD: "MIE",
                cols.CAT_BUY_SELL: "V" if spain_portugal_flow < 0 else "C",
                cols.FLOAT_BID_PRICE: -500 if spain_portugal_flow < 0 else 3000,
                cols.FLOAT_BID_POWER: abs(spain_portugal_flow),
                cols.ID_INDIVIDUAL_BID: f"International_Spain_Portugal_{period}",
                cols.CAT_BIDDING_ZONE: "ES",
                cols.FLOAT_CLEARED_POWER: abs(spain_portugal_flow),
                cols.FLOAT_BID_POWER_CUMSUM: np.nan,
                cols.FLOAT_BID_POWER_CUMSUM_BY_COUNTRY: np.nan,
            }

            portugal_entry = {
                "dat_sesion": dat_sesion,
                cols.INT_PERIOD: period,
                cols.ID_UNIDAD: "MIP",
                cols.CAT_BUY_SELL: "V" if spain_portugal_flow > 0 else "C",
                cols.FLOAT_BID_PRICE: -500 if spain_portugal_flow > 0 else 3000,
                cols.FLOAT_BID_POWER: abs(spain_portugal_flow),
                cols.ID_INDIVIDUAL_BID: f"International_Portugal_Spain_{period}",
                cols.CAT_BIDDING_ZONE: "PT",
                cols.FLOAT_CLEARED_POWER: abs(spain_portugal_flow),
                cols.FLOAT_BID_POWER_CUMSUM: np.nan,
                cols.FLOAT_BID_POWER_CUMSUM_BY_COUNTRY: np.nan,
            }

            international_flows.append(spanish_entry)
            international_flows.append(portugal_entry)

    return pd.DataFrame(international_flows)


def get_det_cab_results(
    model: pyo.ConcreteModel, det_cab: pd.DataFrame
) -> pd.DataFrame:
    """_summary_

    Args:
        model (pyo.ConcreteModel): _description_
        det_cab (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: _description_
    """

    cleared_energy = get_cleared_energy_series(model)
    spain_portugal_transmissions_det_cab = get_spain_portugal_transmissions_det_cab_df(
        model, det_cab["dat_sesion"].iloc[0]
    )

    det_cab_results = (
        det_cab.merge(
            cleared_energy,
            left_on=cols.ID_INDIVIDUAL_BID,
            right_index=True,
            how="outer",
            validate="one_to_one",
            indicator=True,
        )
        .sort_values(
            by=[cols.INT_PERIOD, cols.CAT_BUY_SELL, cols.FLOAT_BID_POWER_CUMSUM]
        )
        .copy()
    )

    assert det_cab_results._merge.isin(["both", "left_only"]).all()
    det_cab_results = det_cab_results.drop(columns="_merge")

    det_cab_results = pd.concat(
        [det_cab_results, spain_portugal_transmissions_det_cab],
        ignore_index=True,
    )

    det_cab_results[cols.FLOAT_CLEARED_POWER_CUMSUM] = get_float_bid_power_cumsum(
        det_cab_results,
        date_column_name="dat_sesion",
        hour_column_name=cols.INT_PERIOD,
        cod_tipo_oferta_column_name=cols.CAT_BUY_SELL,
        cod_ofertada_casada_column_name="cod_ofertada_casada",
        qua_energia_column_name=cols.FLOAT_CLEARED_POWER,
        qua_precio_column_name=cols.FLOAT_BID_PRICE,
    )

    for country in [SPAIN_ZONE, PORTUGAL_ZONE]:
        det_cab_results.loc[
            (det_cab_results[cols.CAT_BIDDING_ZONE] == country),
            cols.FLOAT_CLEARED_POWER_CUMSUM_BY_COUNTRY,
        ] = get_float_bid_power_cumsum(
            det_cab_results.loc[(det_cab_results[cols.CAT_BIDDING_ZONE] == country)],
            date_column_name="dat_sesion",
            hour_column_name=cols.INT_PERIOD,
            cod_tipo_oferta_column_name=cols.CAT_BUY_SELL,
            cod_ofertada_casada_column_name="cod_ofertada_casada",
            qua_energia_column_name=cols.FLOAT_CLEARED_POWER,
            qua_precio_column_name=cols.FLOAT_BID_PRICE,
        )

    return det_cab_results
