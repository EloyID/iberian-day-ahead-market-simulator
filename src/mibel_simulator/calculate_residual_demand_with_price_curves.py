import numpy as np
import pandas as pd

from mibel_simulator.clear_mibel_with_price_curve import (
    get_cleared_power_as_simple_bids_with_price_curve,
    get_cleared_power_with_price_curve,
)
from mibel_simulator.const import (
    FRONTIER_MAPPING_REVERSE,
    PORTUGAL_ZONE,
    RDC_ENERGY_COLUMNS,
    RDC_PRICE_COLUMNS,
    SPAIN_ZONE,
)
from mibel_simulator.data_preprocessor import get_det_cab_for_simulation
from mibel_simulator.file_paths import PARTICIPANTS_BIDDING_ZONES_FILEPATH
from mibel_simulator.parse_omie_files import (
    parse_cab_file,
    parse_capacidad_inter_file,
    parse_det_file,
)
from mibel_simulator.schemas.cab import CABSchema
from mibel_simulator.schemas.capacidad_inter_pt import CapacidadInterPTSchema
from mibel_simulator.schemas.det import DETSchema
from mibel_simulator.tools import (
    concat_provided_participants_bidding_zones_with_existing_data,
)
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


def calculate_complex_residual_demand_II_with_market_split(
    det_cab, capacidad_inter_PT_date
):

    capacidad_imp_PT = -capacidad_inter_PT_date.set_index(cols.INT_PERIODO)[
        cols.FLOAT_IMPORT_CAPACITY
    ].abs()
    capacidad_exp_PT = capacidad_inter_PT_date.set_index(cols.INT_PERIODO)[
        cols.FLOAT_EXPORT_CAPACITY
    ].abs()

    energy_hourly_cleared_per_country_CV = (
        det_cab.groupby(
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

    residual_demand_hourly_portugal = energy_hourly_cleared_portugal_C.sub(
        energy_hourly_cleared_portugal_V, fill_value=0
    )

    residual_demand_hourly_from_portugal_with_saturation = pd.Series(
        np.where(
            residual_demand_hourly_portugal.lt(capacidad_imp_PT),
            capacidad_imp_PT,
            np.where(
                residual_demand_hourly_portugal.gt(capacidad_exp_PT),
                capacidad_exp_PT,
                residual_demand_hourly_portugal,
            ),
        ),
        index=residual_demand_hourly_portugal.index,
    )

    residual_demand_with_saturation_hourly = energy_hourly_cleared_spain_C.sub(
        energy_hourly_cleared_spain_V, fill_value=0
    ).add(residual_demand_hourly_from_portugal_with_saturation, fill_value=0)
    residual_demand_with_saturation_hourly.index = residual_demand_hourly_portugal.index
    residual_demand_with_saturation_hourly.name = (
        "complex_residual_demand_II_with_market_split_curves"
    )
    return residual_demand_with_saturation_hourly


def sum_cleared_power_by_period(det_cab, cleared_power_column=cols.FLOAT_CLEARED_POWER):
    return det_cab.groupby(cols.INT_PERIODO)[cleared_power_column].sum().sort_index()


def calculate_complex_residual_demand_I_without_market_split(det_cab):
    energy_hourly_cleared_C = sum_cleared_power_by_period(
        det_cab.query(f'{cols.CAT_BUY_SELL} == "C"'),
    )
    energy_hourly_cleared_V = sum_cleared_power_by_period(
        det_cab.query(f'{cols.CAT_BUY_SELL} == "V"'),
    )

    return energy_hourly_cleared_C.sub(energy_hourly_cleared_V, fill_value=0)


def calculate_only_simple_submitted_relaxed_residual_demand(det_cab):

    energy_hourly_cleared_C = sum_cleared_power_by_period(
        det_cab.query(f'{cols.CAT_BUY_SELL} == "C"'),
        cleared_power_column="float_cleared_power_as_simple_bid",
    )
    energy_hourly_cleared_V_S = sum_cleared_power_by_period(
        det_cab.query(f'{cols.CAT_BUY_SELL} == "V" & {cols.CAT_ORDER_TYPE} == "S"'),
        cleared_power_column="float_cleared_power_as_simple_bid",
    )

    return energy_hourly_cleared_C.sub(energy_hourly_cleared_V_S, fill_value=0)


def calculate_submitted_relaxed_residual_demand(det_cab):

    energy_hourly_cleared_C = sum_cleared_power_by_period(
        det_cab.query(f'{cols.CAT_BUY_SELL} == "C"'),
        cleared_power_column="float_cleared_power_as_simple_bid",
    )
    energy_hourly_cleared_V = sum_cleared_power_by_period(
        det_cab.query(f'{cols.CAT_BUY_SELL} == "V"'),
        cleared_power_column="float_cleared_power_as_simple_bid",
    )

    return energy_hourly_cleared_C.sub(energy_hourly_cleared_V, fill_value=0)


def calculate_residual_demand_with_price_curves(
    price_curves: np.ndarray,
    det: pd.DataFrame | str,
    cab: pd.DataFrame | str,
    capacidad_inter_pbc: pd.DataFrame | str,
    participants_bidding_zones: pd.DataFrame | None = None,
    zones_default_to_spain: bool = False,
) -> dict:

    price_curves = format_price_curves(price_curves)

    if isinstance(det, str):
        det = parse_det_file(det)
    if isinstance(cab, str):
        cab = parse_cab_file(cab)
    if isinstance(capacidad_inter_pbc, str):
        capacidad_inter_pbc = parse_capacidad_inter_file(capacidad_inter_pbc)

    DETSchema.validate(det)
    CABSchema.validate(cab)
    CapacidadInterPTSchema.validate(capacidad_inter_pbc)

    if isinstance(participants_bidding_zones, pd.DataFrame):
        participants_bidding_zones = (
            concat_provided_participants_bidding_zones_with_existing_data(
                participants_bidding_zones
            )
        )
    else:
        participants_bidding_zones = pd.read_csv(PARTICIPANTS_BIDDING_ZONES_FILEPATH)

    capacidad_inter_PT_date = capacidad_inter_pbc.query(
        f"{cols.CAT_FRONTIER} == {FRONTIER_MAPPING_REVERSE['PT']}"
    )

    det_cab = get_det_cab_for_simulation(
        det=det,
        cab=cab,
        participants_bidding_zones=participants_bidding_zones,
        zones_default_to_spain=zones_default_to_spain,
    )

    only_simple_submitted_relaxed_residual_demands = []
    submitted_relaxed_residual_demands = []
    complex_residual_demands_I_without_market_split = []
    complex_residual_demands_II_with_market_split = []

    for price_curve in price_curves:

        det_cab_aux = det_cab.copy()
        price_series = pd.Series(price_curve, index=RDC_PRICE_COLUMNS)

        # Calculate cleared power values
        det_cab_aux["float_cleared_power_as_simple_bid"] = (
            get_cleared_power_as_simple_bids_with_price_curve(price_curve, det_cab_aux)
        )
        det_cab_aux[cols.FLOAT_CLEARED_POWER] = get_cleared_power_with_price_curve(
            price_curve, det_cab_aux
        )

        # Calculate residual demand
        only_simple_submitted_relaxed_residual_demand = (
            calculate_only_simple_submitted_relaxed_residual_demand(det_cab_aux)
        )
        only_simple_submitted_relaxed_residual_demand.index = RDC_ENERGY_COLUMNS
        only_simple_submitted_relaxed_residual_demand = pd.concat(
            [price_series, only_simple_submitted_relaxed_residual_demand]
        )

        submitted_relaxed_residual_demand = calculate_submitted_relaxed_residual_demand(
            det_cab_aux
        )
        submitted_relaxed_residual_demand.index = RDC_ENERGY_COLUMNS
        submitted_relaxed_residual_demand = pd.concat(
            [price_series, submitted_relaxed_residual_demand]
        )

        complex_residual_demand_I_without_market_split = (
            calculate_complex_residual_demand_I_without_market_split(det_cab_aux)
        )
        complex_residual_demand_I_without_market_split.index = RDC_ENERGY_COLUMNS
        complex_residual_demand_I_without_market_split = pd.concat(
            [price_series, complex_residual_demand_I_without_market_split]
        )

        complex_residual_demand_II_with_market_split = (
            calculate_complex_residual_demand_II_with_market_split(
                det_cab_aux, capacidad_inter_PT_date
            )
        )
        complex_residual_demand_II_with_market_split.index = RDC_ENERGY_COLUMNS
        complex_residual_demand_II_with_market_split = pd.concat(
            [price_series, complex_residual_demand_II_with_market_split]
        )

        only_simple_submitted_relaxed_residual_demands.append(
            only_simple_submitted_relaxed_residual_demand
        )
        submitted_relaxed_residual_demands.append(submitted_relaxed_residual_demand)
        complex_residual_demands_I_without_market_split.append(
            complex_residual_demand_I_without_market_split
        )
        complex_residual_demands_II_with_market_split.append(
            complex_residual_demand_II_with_market_split
        )

    # Calculate residual demand submitted_relaxed_residual_demandscurves
    only_simple_submitted_relaxed_residual_demand_df = pd.DataFrame(
        only_simple_submitted_relaxed_residual_demands
    )
    submitted_relaxed_residual_demand_curves_df = pd.DataFrame(
        submitted_relaxed_residual_demands
    )
    complex_residual_demand_I_without_market_split_curves_df = pd.DataFrame(
        complex_residual_demands_I_without_market_split
    )
    complex_residual_demand_II_with_market_split_curves_df = pd.DataFrame(
        complex_residual_demands_II_with_market_split
    )

    return {
        "only_simple_submitted_relaxed_residual_demand": only_simple_submitted_relaxed_residual_demand_df,
        "submitted_relaxed_residual_demand_curves": submitted_relaxed_residual_demand_curves_df,
        "complex_residual_demand_I_without_market_split_curves": complex_residual_demand_I_without_market_split_curves_df,
        "complex_residual_demand_II_with_market_split_curves": complex_residual_demand_II_with_market_split_curves_df,
    }
