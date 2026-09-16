import numpy as np
import pandas as pd

from iberian_day_ahead_market_simulator.clear_mibel_with_price_curve import (
    get_cleared_power_as_simple_bids_with_price_curve,
    get_cleared_power_with_price_curve,
)
from iberian_day_ahead_market_simulator.const import (
    FRONTIER_MAPPING_REVERSE,
    PORTUGAL_ZONE,
    SPAIN_ZONE,
    TOTAL_PERIODS_OPTIONS,
    get_rdc_price_columns,
    get_rdc_power_columns,
)
from iberian_day_ahead_market_simulator.data_preprocessor import (
    get_det_cab_for_simulation,
)
from iberian_day_ahead_market_simulator.file_paths import (
    PARTICIPANTS_BIDDING_ZONES_FILEPATH,
)
from iberian_day_ahead_market_simulator.parse_omie_files import (
    parse_cab_file,
    parse_capacidad_inter_file,
    parse_det_file,
)
from iberian_day_ahead_market_simulator.schemas.cab import CABSchema
from iberian_day_ahead_market_simulator.schemas.capacidad_inter_pt import (
    CapacidadInterPTSchema,
)
from iberian_day_ahead_market_simulator.schemas.det import DETSchema
from iberian_day_ahead_market_simulator.tools import (
    concat_provided_participants_bidding_zones_with_existing_data,
    get_market_periods_count,
    is_QH_market,
)
import iberian_day_ahead_market_simulator.columns as cols


def format_price_curves(
    price_curves: np.ndarray,
) -> np.ndarray:

    # if 1 dimension check len in TOTAL_PERIODS_OPTIONS
    if price_curves.ndim == 1:
        prices_curves_len = len(price_curves)
        if prices_curves_len not in TOTAL_PERIODS_OPTIONS:
            raise ValueError(
                f"If price_curves is 1D, it must have length in {TOTAL_PERIODS_OPTIONS}."
            )
        # reshape to (1, prices_curves_len)
        price_curves = price_curves.reshape(1, prices_curves_len)

    elif price_curves.ndim == 2:
        prices_curves_shape_1 = price_curves.shape[1]
        if prices_curves_shape_1 not in TOTAL_PERIODS_OPTIONS:
            raise ValueError(
                f"If price_curves is 2D, it must have shape (n_curves, {TOTAL_PERIODS_OPTIONS})."
            )

    else:
        raise ValueError("price_curves must be either 1D or 2D numpy array.")

    return price_curves


def calculate_complex_residual_demand_II_with_market_split(
    det_cab,
    capacidad_inter_PBC_pt,
):

    capacidad_imp_PT = -capacidad_inter_PBC_pt.set_index(cols.INT_PERIOD)[
        cols.FLOAT_IMPORT_CAPACITY
    ].abs()
    capacidad_exp_PT = capacidad_inter_PBC_pt.set_index(cols.INT_PERIOD)[
        cols.FLOAT_EXPORT_CAPACITY
    ].abs()

    power_per_period_cleared_per_country_CV = (
        det_cab.groupby(
            [cols.CAT_BIDDING_ZONE, cols.CAT_BUY_SELL, cols.INT_PERIOD], observed=False
        )[cols.FLOAT_CLEARED_POWER]
        .sum()
        .sort_index()
    )
    power_per_period_cleared_spain_C = power_per_period_cleared_per_country_CV.loc[
        (SPAIN_ZONE, "C")
    ]
    power_per_period_cleared_spain_V = power_per_period_cleared_per_country_CV.loc[
        (SPAIN_ZONE, "V")
    ]
    power_per_period_cleared_portugal_C = power_per_period_cleared_per_country_CV.loc[
        (PORTUGAL_ZONE, "C")
    ]
    power_per_period_cleared_portugal_V = power_per_period_cleared_per_country_CV.loc[
        (PORTUGAL_ZONE, "V")
    ]

    residual_demand_per_period_portugal = power_per_period_cleared_portugal_C.sub(
        power_per_period_cleared_portugal_V, fill_value=0
    )

    residual_demand_per_period_from_portugal_with_saturation = pd.Series(
        np.where(
            residual_demand_per_period_portugal.lt(capacidad_imp_PT),
            capacidad_imp_PT,
            np.where(
                residual_demand_per_period_portugal.gt(capacidad_exp_PT),
                capacidad_exp_PT,
                residual_demand_per_period_portugal,
            ),
        ),
        index=residual_demand_per_period_portugal.index,
    )

    residual_demand_with_saturation_per_period = power_per_period_cleared_spain_C.sub(
        power_per_period_cleared_spain_V, fill_value=0
    ).add(residual_demand_per_period_from_portugal_with_saturation, fill_value=0)
    residual_demand_with_saturation_per_period.index = (
        residual_demand_per_period_portugal.index
    )
    residual_demand_with_saturation_per_period.name = (
        "complex_residual_demand_II_with_market_split_curves"
    )
    return residual_demand_with_saturation_per_period


def sum_cleared_power_by_period(det_cab, cleared_power_column=cols.FLOAT_CLEARED_POWER):
    return det_cab.groupby(cols.INT_PERIOD)[cleared_power_column].sum().sort_index()


def calculate_complex_residual_demand_I_without_market_split(det_cab):
    power_per_period_cleared_C = sum_cleared_power_by_period(
        det_cab.query(f'{cols.CAT_BUY_SELL} == "C"'),
    )
    power_per_period_cleared_V = sum_cleared_power_by_period(
        det_cab.query(f'{cols.CAT_BUY_SELL} == "V"'),
    )

    return power_per_period_cleared_C.sub(power_per_period_cleared_V, fill_value=0)


def calculate_only_simple_submitted_relaxed_residual_demand(det_cab):

    power_per_period_cleared_C = sum_cleared_power_by_period(
        det_cab.query(f'{cols.CAT_BUY_SELL} == "C"'),
        cleared_power_column="float_cleared_power_as_simple_bid",
    )
    power_per_period_cleared_V_S = sum_cleared_power_by_period(
        det_cab.query(f'{cols.CAT_BUY_SELL} == "V" & {cols.CAT_ORDER_TYPE} == "S"'),
        cleared_power_column="float_cleared_power_as_simple_bid",
    )

    return power_per_period_cleared_C.sub(power_per_period_cleared_V_S, fill_value=0)


def calculate_submitted_relaxed_residual_demand(det_cab):

    power_per_period_cleared_C = sum_cleared_power_by_period(
        det_cab.query(f'{cols.CAT_BUY_SELL} == "C"'),
        cleared_power_column="float_cleared_power_as_simple_bid",
    )
    power_per_period_cleared_V = sum_cleared_power_by_period(
        det_cab.query(f'{cols.CAT_BUY_SELL} == "V"'),
        cleared_power_column="float_cleared_power_as_simple_bid",
    )

    return power_per_period_cleared_C.sub(power_per_period_cleared_V, fill_value=0)


def substract_reference_curve_from_all_curves(
    curves_df: pd.DataFrame, reference_curve_index: int, rdc_power_columns: list[str]
) -> pd.DataFrame:
    curves_df = curves_df.copy()
    corresponding_df_index = curves_df.index[reference_curve_index]
    reference_power_curve = curves_df.loc[corresponding_df_index, rdc_power_columns]
    curves_df.loc[:, rdc_power_columns] = curves_df.loc[:, rdc_power_columns].sub(
        reference_power_curve
    )
    return curves_df


def calculate_residual_demand_curves_from_bids(
    price_curves: np.ndarray,
    det: pd.DataFrame | str,
    cab: pd.DataFrame | str,
    capacidad_inter_pbc: pd.DataFrame | str,
    participants_bidding_zones: pd.DataFrame | None = None,
    spain_as_default_bidding_zone: bool = False,
    reference_price_curve: np.ndarray | None = None,
) -> dict:

    price_curves = format_price_curves(price_curves)
    if reference_price_curve is not None:
        try:
            reference_price_curve_index = np.where(
                (price_curves == reference_price_curve).all(axis=1)
            )[0][0]
            print("reference_price_curve_index: ", reference_price_curve_index)
        except Exception as e:
            raise ValueError(
                "Error while trying to find the reference price curve in the provided price curves."
            ) from e

    if isinstance(det, str):
        det = parse_det_file(det)
    if isinstance(cab, str):
        cab = parse_cab_file(cab)

    DETSchema.validate(det)
    CABSchema.validate(cab)

    market_periods_count = get_market_periods_count(det)
    is_QH = is_QH_market(market_periods_count)
    det = det.query(f"{cols.INT_PERIOD} <= {market_periods_count}")

    if isinstance(capacidad_inter_pbc, str):
        capacidad_inter_pbc = parse_capacidad_inter_file(
            capacidad_inter_pbc, only_capacity_columns=True, is_QH=is_QH
        )
    CapacidadInterPTSchema.validate(capacidad_inter_pbc)

    rdc_price_columns = get_rdc_price_columns(market_periods_count)
    rdc_power_columns = get_rdc_power_columns(market_periods_count)

    if isinstance(participants_bidding_zones, pd.DataFrame):
        participants_bidding_zones = (
            concat_provided_participants_bidding_zones_with_existing_data(
                participants_bidding_zones
            )
        )
    else:
        participants_bidding_zones = pd.read_csv(PARTICIPANTS_BIDDING_ZONES_FILEPATH)

    capacidad_inter_PBC_pt = capacidad_inter_pbc.query(
        f"{cols.CAT_FRONTIER} == {FRONTIER_MAPPING_REVERSE['PT']}"
    )

    det_cab = get_det_cab_for_simulation(
        det=det,
        cab=cab,
        participants_bidding_zones=participants_bidding_zones,
        spain_as_default_bidding_zone=spain_as_default_bidding_zone,
    )

    only_simple_submitted_relaxed_residual_demands = []
    submitted_relaxed_residual_demands = []
    complex_residual_demands_I_without_market_split = []
    complex_residual_demands_II_with_market_split = []

    for price_curve in price_curves:

        det_cab_aux = det_cab.copy()
        price_series = pd.Series(price_curve, index=rdc_price_columns)

        # Calculate cleared power values
        det_cab_aux["float_cleared_power_as_simple_bid"] = (
            get_cleared_power_as_simple_bids_with_price_curve(
                price_curve, det_cab_aux, market_periods_count
            )
        )
        det_cab_aux[cols.FLOAT_CLEARED_POWER] = get_cleared_power_with_price_curve(
            price_curve, det_cab_aux, market_periods_count
        )

        # Calculate residual demand
        only_simple_submitted_relaxed_residual_demand = (
            calculate_only_simple_submitted_relaxed_residual_demand(det_cab_aux)
        )
        only_simple_submitted_relaxed_residual_demand.index = rdc_power_columns
        only_simple_submitted_relaxed_residual_demand = pd.concat(
            [price_series, only_simple_submitted_relaxed_residual_demand]
        )

        submitted_relaxed_residual_demand = calculate_submitted_relaxed_residual_demand(
            det_cab_aux
        )
        submitted_relaxed_residual_demand.index = rdc_power_columns
        submitted_relaxed_residual_demand = pd.concat(
            [price_series, submitted_relaxed_residual_demand]
        )

        complex_residual_demand_I_without_market_split = (
            calculate_complex_residual_demand_I_without_market_split(det_cab_aux)
        )
        complex_residual_demand_I_without_market_split.index = rdc_power_columns
        complex_residual_demand_I_without_market_split = pd.concat(
            [price_series, complex_residual_demand_I_without_market_split]
        )

        complex_residual_demand_II_with_market_split = (
            calculate_complex_residual_demand_II_with_market_split(
                det_cab_aux, capacidad_inter_PBC_pt
            )
        )
        complex_residual_demand_II_with_market_split.index = rdc_power_columns
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

    if reference_price_curve is not None:
        # substract that curve from all curves
        only_simple_submitted_relaxed_residual_demand_df = (
            substract_reference_curve_from_all_curves(
                only_simple_submitted_relaxed_residual_demand_df,
                reference_price_curve_index,
                rdc_power_columns,
            )
        )
        submitted_relaxed_residual_demand_curves_df = (
            substract_reference_curve_from_all_curves(
                submitted_relaxed_residual_demand_curves_df,
                reference_price_curve_index,
                rdc_power_columns,
            )
        )
        complex_residual_demand_I_without_market_split_curves_df = (
            substract_reference_curve_from_all_curves(
                complex_residual_demand_I_without_market_split_curves_df,
                reference_price_curve_index,
                rdc_power_columns,
            )
        )
        complex_residual_demand_II_with_market_split_curves_df = (
            substract_reference_curve_from_all_curves(
                complex_residual_demand_II_with_market_split_curves_df,
                reference_price_curve_index,
                rdc_power_columns,
            )
        )

    return {
        "only_simple_submitted_relaxed_residual_demand": only_simple_submitted_relaxed_residual_demand_df,
        "submitted_relaxed_residual_demand_curves": submitted_relaxed_residual_demand_curves_df,
        "complex_residual_demand_I_without_market_split_curves": complex_residual_demand_I_without_market_split_curves_df,
        "complex_residual_demand_II_with_market_split_curves": complex_residual_demand_II_with_market_split_curves_df,
    }
