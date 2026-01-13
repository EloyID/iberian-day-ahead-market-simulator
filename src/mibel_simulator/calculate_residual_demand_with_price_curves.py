import numpy as np
import pandas as pd

from mibel_simulator.clear_mibel_with_price_curve import (
    get_cleared_power_with_price_curve,
)
from mibel_simulator.const import (
    FRONTIER_MAPPING_REVERSE,
    PORTUGAL_ZONE,
    RDC_ENERGY_COLUMNS,
    RDC_PRICE_COLUMNS,
    SPAIN_ZONE,
)
from mibel_simulator.data_preprocessor import get_det_cab_date_for_simulation
from mibel_simulator.file_paths import UOF_ZONES_FILEPATH
from mibel_simulator.parse_omie_files import (
    parse_cab_file,
    parse_capacidad_inter_file,
    parse_det_file,
)
from mibel_simulator.schemas.cab import CABSchema
from mibel_simulator.schemas.capacidad_inter_pt import CapacidadInterPTSchema
from mibel_simulator.schemas.det import DETSchema
from mibel_simulator.tools import concat_provided_uof_zones_with_existing_data
import mibel_simulator.columns as cols


def format_price_curves(
    price_curves: np.ndarray,
) -> np.ndarray:

    # if 1 dimension check len == 24
    if price_curves.ndim == 1:
        if len(price_curves) != 24:
            raise ValueError("If price_curves is 1D, it must have length 24.")
        # reshape to (1, 24)
        price_curves = price_curves.reshape(1, 24)

    elif price_curves.ndim == 2:
        if price_curves.shape[1] != 24:
            raise ValueError(
                "If price_curves is 2D, it must have shape (n_curves, 24)."
            )

    else:
        raise ValueError("price_curves must be either 1D or 2D numpy array.")

    return price_curves


def calculate_residual_demand_with_saturation_hourly(
    det_cab_date, capacidad_inter_PT_date
):

    capacidad_imp_PT = capacidad_inter_PT_date.set_index(cols.INT_PERIODO)[
        cols.FLOAT_IMPORT_CAPACITY
    ]
    capacidad_exp_PT = capacidad_inter_PT_date.set_index(cols.INT_PERIODO)[
        cols.FLOAT_EXPORT_CAPACITY
    ]

    energy_hourly_cleared_per_country_CV = (
        det_cab_date.groupby(
            [cols.CAT_PAIS, cols.CAT_BUY_SELL, cols.INT_PERIODO], observed=False
        )[cols.FLOAT_CLEARED_POWER]
        .sum()
        .sort_index()
    )
    energy_hourly_cleared_spain_C = energy_hourly_cleared_per_country_CV.loc[
        (SPAIN_ZONE, "C")
    ]
    energy_hourly_cleared_spain_V = energy_hourly_cleared_per_country_CV.loc[
        (SPAIN_ZONE, "V")
    ]
    energy_hourly_cleared_portugal_C = energy_hourly_cleared_per_country_CV.loc[
        (PORTUGAL_ZONE, "C")
    ]
    energy_hourly_cleared_portugal_V = energy_hourly_cleared_per_country_CV.loc[
        (PORTUGAL_ZONE, "V")
    ]

    residual_demand_hourly_spain = energy_hourly_cleared_spain_C.sub(
        energy_hourly_cleared_spain_V, fill_value=0
    )
    residual_demand_hourly_portugal = energy_hourly_cleared_portugal_C.sub(
        energy_hourly_cleared_portugal_V, fill_value=0
    )

    countries_consumption_df = pd.DataFrame(
        {
            # "residual_demand_hourly_spain": residual_demand_hourly_spain,
            "residual_demand_hourly_portugal": residual_demand_hourly_portugal,
            "capacidad_imp_PT": capacidad_imp_PT,
            "capacidad_exp_PT": capacidad_exp_PT,
        }
    )
    # countries_consumption_df["spain_imports"] = np.maximum(
    #     -countries_consumption_df["residual_demand_hourly_spain"],
    #     countries_consumption_df["residual_demand_hourly_portugal"],
    # ).clip(upper=0)
    # countries_consumption_df["spain_exports"] = np.minimum(
    #     -countries_consumption_df["residual_demand_hourly_spain"],
    #     countries_consumption_df["residual_demand_hourly_portugal"],
    # ).clip(lower=0)
    countries_consumption_df["is_spain_import_saturated"] = countries_consumption_df[
        "residual_demand_hourly_portugal"
    ].lt(countries_consumption_df["capacidad_imp_PT"])
    countries_consumption_df["is_spain_export_saturated"] = countries_consumption_df[
        "residual_demand_hourly_portugal"
    ].gt(countries_consumption_df["capacidad_exp_PT"])

    residual_demand = (
        energy_hourly_cleared_spain_C.add(
            energy_hourly_cleared_portugal_C, fill_value=0
        )
        .sub(energy_hourly_cleared_spain_V, fill_value=0)
        .sub(energy_hourly_cleared_portugal_V, fill_value=0)
    )

    residual_demand_spain_import_saturated = energy_hourly_cleared_spain_C.sub(
        energy_hourly_cleared_spain_V, fill_value=0
    ).add(capacidad_imp_PT, fill_value=0)
    residual_demand_spain_export_saturated = energy_hourly_cleared_spain_C.add(
        capacidad_exp_PT, fill_value=0
    ).sub(energy_hourly_cleared_spain_V, fill_value=0)

    residual_demand_with_saturation_hourly = np.where(
        countries_consumption_df["is_spain_import_saturated"],
        residual_demand_spain_import_saturated,
        np.where(
            countries_consumption_df["is_spain_export_saturated"],
            residual_demand_spain_export_saturated,
            residual_demand,
        ),
    )
    residual_demand_with_saturation_hourly = pd.Series(
        residual_demand_with_saturation_hourly,
        index=residual_demand.index,
        name="residual_demand_with_saturation_hourly",
    )
    return residual_demand_with_saturation_hourly


def calculate_residual_demand_with_simple_bids_only(det_cab_date):

    det_cab_date_exchanges_fr = det_cab_date.query(f'{cols.ID_UNIDAD} == "MIEU"').copy()
    # import (venta) negative - export (compra) positive
    det_cab_date_exchanges_fr.loc[:, cols.FLOAT_CLEARED_POWER] = np.where(
        det_cab_date_exchanges_fr[cols.CAT_BUY_SELL] == "C",
        det_cab_date_exchanges_fr[cols.FLOAT_CLEARED_POWER],
        -det_cab_date_exchanges_fr[cols.FLOAT_CLEARED_POWER],
    )

    energy_hourly_cleared_C = (
        det_cab_date.query(f'{cols.CAT_BUY_SELL} == "C"')
        .groupby(cols.INT_PERIODO)[cols.FLOAT_CLEARED_POWER]
        .sum()
        .sort_index()
    )
    energy_hourly_cleared_V_S = (
        det_cab_date.query(f'{cols.CAT_BUY_SELL} == "V" & {cols.CAT_ORDER_TYPE} == "S"')
        .groupby(cols.INT_PERIODO)[cols.FLOAT_CLEARED_POWER]
        .sum()
        .sort_index()
    )
    energy_hourly_cleared_exchanges_fr = (
        det_cab_date_exchanges_fr.groupby(cols.INT_PERIODO)[cols.FLOAT_CLEARED_POWER]
        .sum()
        .sort_index()
    )

    return energy_hourly_cleared_C.add(
        energy_hourly_cleared_exchanges_fr, fill_value=0
    ).sub(energy_hourly_cleared_V_S, fill_value=0)


def calculate_residual_demand_without_saturation_hourly(det_cab_date):

    det_cab_date_exchanges_fr = det_cab_date.query(f'{cols.ID_UNIDAD} == "MIEU"').copy()
    # import (venta) negative - export (compra) positive
    det_cab_date_exchanges_fr.loc[:, cols.FLOAT_CLEARED_POWER] = np.where(
        det_cab_date_exchanges_fr[cols.CAT_BUY_SELL] == "C",
        det_cab_date_exchanges_fr[cols.FLOAT_CLEARED_POWER],
        -det_cab_date_exchanges_fr[cols.FLOAT_CLEARED_POWER],
    )

    energy_hourly_cleared_C = (
        det_cab_date.query(f'{cols.CAT_BUY_SELL} == "C"')
        .groupby(cols.INT_PERIODO)[cols.FLOAT_CLEARED_POWER]
        .sum()
        .sort_index()
    )
    energy_hourly_cleared_V = (
        det_cab_date.query(f'{cols.CAT_BUY_SELL} == "V"')
        .groupby(cols.INT_PERIODO)[cols.FLOAT_CLEARED_POWER]
        .sum()
        .sort_index()
    )
    energy_hourly_cleared_exchanges_fr = (
        det_cab_date_exchanges_fr.groupby(cols.INT_PERIODO)[cols.FLOAT_CLEARED_POWER]
        .sum()
        .sort_index()
    )

    return energy_hourly_cleared_C.add(  # TODO: verificar que este add linea hace falta
        energy_hourly_cleared_exchanges_fr,
        fill_value=0,
    ).sub(energy_hourly_cleared_V, fill_value=0)


def calculate_residual_demand_with_price_curves(
    price_curves: np.ndarray,
    det_date: pd.DataFrame | str,
    cab_date: pd.DataFrame | str,
    capacidad_inter_date: pd.DataFrame | str,
    uof_zones: pd.DataFrame | None = None,
    zones_default_to_spain: bool = False,
) -> dict:

    price_curves = format_price_curves(price_curves)

    if isinstance(det_date, str):
        det_date = parse_det_file(det_date)
    if isinstance(cab_date, str):
        cab_date = parse_cab_file(cab_date)
    if isinstance(capacidad_inter_date, str):
        capacidad_inter_date = parse_capacidad_inter_file(capacidad_inter_date)

    DETSchema.validate(det_date)
    CABSchema.validate(cab_date)
    CapacidadInterPTSchema.validate(capacidad_inter_date)

    if isinstance(uof_zones, pd.DataFrame):
        uof_zones = concat_provided_uof_zones_with_existing_data(uof_zones)
    else:
        uof_zones = pd.read_csv(UOF_ZONES_FILEPATH)

    capacidad_inter_PT_date = capacidad_inter_date.query(
        f"{cols.CAT_FRONTIER} == {FRONTIER_MAPPING_REVERSE['PT']}"
    )

    det_cab_date = get_det_cab_date_for_simulation(
        det_date=det_date,
        cab_date=cab_date,
        uof_zones=uof_zones,
        zones_default_to_spain=zones_default_to_spain,
    )

    residual_demands_with_simple_bids_curves = []
    residual_demands_without_saturation_curves = []
    residual_demands_with_saturation_curves = []

    for price_curve in price_curves:

        det_cab_date_aux = det_cab_date.copy()
        det_cab_date_aux[cols.FLOAT_CLEARED_POWER] = get_cleared_power_with_price_curve(
            price_curve, det_cab_date_aux
        )

        price_series = pd.Series(price_curve, index=RDC_PRICE_COLUMNS)

        residual_demand_with_simple_bids_hourly = (
            calculate_residual_demand_with_simple_bids_only(det_cab_date_aux)
        )
        residual_demand_with_simple_bids_hourly.index = RDC_ENERGY_COLUMNS
        residual_demand_with_simple_bids_hourly = pd.concat(
            [price_series, residual_demand_with_simple_bids_hourly]
        )

        residual_demand_without_saturation_hourly = (
            calculate_residual_demand_without_saturation_hourly(det_cab_date_aux)
        )
        residual_demand_without_saturation_hourly.index = RDC_ENERGY_COLUMNS
        residual_demand_without_saturation_hourly = pd.concat(
            [price_series, residual_demand_without_saturation_hourly]
        )

        residual_demand_with_saturation_hourly = (
            calculate_residual_demand_with_saturation_hourly(
                det_cab_date_aux, capacidad_inter_PT_date
            )
        )
        residual_demand_with_saturation_hourly.index = RDC_ENERGY_COLUMNS
        residual_demand_with_saturation_hourly = pd.concat(
            [price_series, residual_demand_with_saturation_hourly]
        )

        residual_demands_with_simple_bids_curves.append(
            residual_demand_with_simple_bids_hourly
        )
        residual_demands_without_saturation_curves.append(
            residual_demand_without_saturation_hourly
        )
        residual_demands_with_saturation_curves.append(
            residual_demand_with_saturation_hourly
        )

    residual_demands_with_simple_bids_curves_df = pd.DataFrame(
        residual_demands_with_simple_bids_curves
    )
    residual_demands_without_saturation_curves_df = pd.DataFrame(
        residual_demands_without_saturation_curves
    )
    residual_demands_with_saturation_curves_df = pd.DataFrame(
        residual_demands_with_saturation_curves
    )

    return {
        "residual_demands_with_simple_bids_curves": residual_demands_with_simple_bids_curves_df,
        "residual_demands_without_saturation_curves": residual_demands_without_saturation_curves_df,
        "residual_demands_with_saturation_curves": residual_demands_with_saturation_curves_df,
    }
