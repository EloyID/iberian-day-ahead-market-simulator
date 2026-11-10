import logging

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
    get_rdc_price_columns,
    get_rdc_power_columns,
)
from iberian_day_ahead_market_simulator.data_preprocessor import (
    get_det_cab_for_simulation,
    get_france_det_cab_from_curva_pbc_uof,
)
from iberian_day_ahead_market_simulator.parse_omie_files import (
    parse_capacidad_inter_file,
)
from iberian_day_ahead_market_simulator.schemas.capacidad_inter_pt import (
    CapacidadInterPTSchema,
)

# format_price_curves and substract_reference_curve_from_all_curves now live in
# validate_inputs and tools, shared with
# calculate_cleared_power_curves_by_order_type_from_bids; they stay importable from this
# module too
from iberian_day_ahead_market_simulator.tools import (
    is_QH_market,
    substract_reference_curve_from_all_curves,
)
from iberian_day_ahead_market_simulator.validate_inputs import (
    format_price_curves,
    get_participants_bidding_zones,
    get_reference_price_curve_index,
    get_validated_det_and_cab,
)
import iberian_day_ahead_market_simulator.columns as cols

logger = logging.getLogger(__name__)


def reindex_over_market_periods(
    residual_demand_per_period: pd.Series, market_periods_count: int | None = None
) -> pd.Series:
    """
    Reindex a per period residual demand over the full range of market periods.

    A market period in which no order at all was placed is simply absent from the
    groupby result. Left as is, it would shift every later period onto the wrong
    power_<i> column; such a period has a residual demand of 0, not a missing one.

    Args:
        residual_demand_per_period (pd.Series): Residual demand indexed by period.
        market_periods_count (int | None): Number of periods of the session. When
            None, the highest period present is used, which only closes the gaps
            between periods and not a gap at the end of the session.

    Returns:
        pd.Series: Residual demand indexed by 1..market_periods_count, with the
            periods that hold no order set to 0.
    """
    if residual_demand_per_period.empty:
        return residual_demand_per_period

    if market_periods_count is None:
        market_periods_count = int(residual_demand_per_period.index.max())

    return residual_demand_per_period.reindex(
        range(1, market_periods_count + 1), fill_value=0
    )


def calculate_complex_residual_demand_II_with_market_split(
    det_cab,
    capacidad_inter_PBC_pt,
    market_periods_count: int | None = None,
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

    # saturate the flow Portugal asks for at the interconnection limits of its own
    # period. Series.clip aligns the bounds on the period index, so the row order of
    # capacidad_inter_PBC_pt cannot leak another period's limit into the comparison.
    # A period without capacity data keeps an unbounded limit, as it did before.
    capacidad_imp_PT = capacidad_imp_PT.reindex(
        residual_demand_per_period_portugal.index, fill_value=-np.inf
    )
    capacidad_exp_PT = capacidad_exp_PT.reindex(
        residual_demand_per_period_portugal.index, fill_value=np.inf
    )
    residual_demand_per_period_from_portugal_with_saturation = (
        residual_demand_per_period_portugal.clip(
            lower=capacidad_imp_PT, upper=capacidad_exp_PT
        )
    )

    residual_demand_with_saturation_per_period = power_per_period_cleared_spain_C.sub(
        power_per_period_cleared_spain_V, fill_value=0
    ).add(residual_demand_per_period_from_portugal_with_saturation, fill_value=0)
    residual_demand_with_saturation_per_period = reindex_over_market_periods(
        residual_demand_with_saturation_per_period, market_periods_count
    )
    residual_demand_with_saturation_per_period.name = (
        "complex_residual_demand_II_with_market_split_curves"
    )
    return residual_demand_with_saturation_per_period


def sum_cleared_power_by_period(det_cab, cleared_power_column=cols.FLOAT_CLEARED_POWER):
    return det_cab.groupby(cols.INT_PERIOD)[cleared_power_column].sum().sort_index()


def calculate_complex_residual_demand_I_without_market_split(
    det_cab, market_periods_count: int | None = None
):
    power_per_period_cleared_C = sum_cleared_power_by_period(
        det_cab.query(f'{cols.CAT_BUY_SELL} == "C"'),
    )
    power_per_period_cleared_V = sum_cleared_power_by_period(
        det_cab.query(f'{cols.CAT_BUY_SELL} == "V"'),
    )

    return reindex_over_market_periods(
        power_per_period_cleared_C.sub(power_per_period_cleared_V, fill_value=0),
        market_periods_count,
    )


def calculate_only_simple_submitted_relaxed_residual_demand(
    det_cab, market_periods_count: int | None = None
):

    power_per_period_cleared_C = sum_cleared_power_by_period(
        det_cab.query(f'{cols.CAT_BUY_SELL} == "C"'),
        cleared_power_column="float_cleared_power_as_simple_bid",
    )
    power_per_period_cleared_V_S = sum_cleared_power_by_period(
        det_cab.query(f'{cols.CAT_BUY_SELL} == "V" & {cols.CAT_ORDER_TYPE} == "S"'),
        cleared_power_column="float_cleared_power_as_simple_bid",
    )

    return reindex_over_market_periods(
        power_per_period_cleared_C.sub(power_per_period_cleared_V_S, fill_value=0),
        market_periods_count,
    )


def calculate_submitted_relaxed_residual_demand(
    det_cab, market_periods_count: int | None = None
):

    power_per_period_cleared_C = sum_cleared_power_by_period(
        det_cab.query(f'{cols.CAT_BUY_SELL} == "C"'),
        cleared_power_column="float_cleared_power_as_simple_bid",
    )
    power_per_period_cleared_V = sum_cleared_power_by_period(
        det_cab.query(f'{cols.CAT_BUY_SELL} == "V"'),
        cleared_power_column="float_cleared_power_as_simple_bid",
    )

    return reindex_over_market_periods(
        power_per_period_cleared_C.sub(power_per_period_cleared_V, fill_value=0),
        market_periods_count,
    )


def calculate_residual_demand_curves_from_bids(
    price_curves: np.ndarray,
    det: pd.DataFrame | str,
    cab: pd.DataFrame | str,
    capacidad_inter_pbc: pd.DataFrame | str,
    curva_pbc_uof: pd.DataFrame | None,
    participants_bidding_zones: pd.DataFrame | None = None,
    spain_as_default_bidding_zone: bool = False,
    reference_price_curve: np.ndarray | None = None,
) -> dict:
    """Residual demand curves built from the individual orders, one per price curve given.

    Parameters
    ----------
    price_curves : np.ndarray
        One price per market period, or a 2D array of such curves (one row each).
    det, cab : pd.DataFrame | str
        The day's order detail and headers, or paths to the raw OMIE files.
    capacidad_inter_pbc : pd.DataFrame | str
        The day's interconnection capacities, or a path to the raw OMIE file.
    curva_pbc_uof : pd.DataFrame | None
        The day's unit-level curve, from which the France interconnection's orders are read
        (OMIE publishes it as the FRANCE_ID_UNIDAD unit, already priced at the extremes of
        the bid range). **DET/CAB do not carry those orders**. Because the
        published prices are extreme, the interconnection is a price taker: it contributes
        the same power at every price swept.
    participants_bidding_zones : pd.DataFrame | None
        Unit to bidding zone mapping; None reads the package's bundled list.
    spain_as_default_bidding_zone : bool
        Put units missing from that mapping in Spain instead of raising.
    reference_price_curve : np.ndarray | None
        When given, every curve is reported relative to this one.

    Returns
    -------
    dict
        One dataframe per residual demand variant, each with a `price_<i>`/`power_<i>`
        column per market period and one row per price curve swept.

    Raises
    ------
    ValueError
        If `price_curves` does not cover exactly the DET file's market periods, or if
        `curva_pbc_uof` holds no cleared rows for the interconnection.
    """

    price_curves = format_price_curves(price_curves)
    if reference_price_curve is not None:
        reference_price_curve_index = get_reference_price_curve_index(
            price_curves, reference_price_curve
        )

    det, cab, market_periods_count = get_validated_det_and_cab(det, cab, price_curves)
    is_QH = is_QH_market(market_periods_count)

    if isinstance(capacidad_inter_pbc, str):
        capacidad_inter_pbc = parse_capacidad_inter_file(
            capacidad_inter_pbc, only_capacity_columns=True, is_QH=is_QH
        )
    CapacidadInterPTSchema.validate(capacidad_inter_pbc)

    rdc_price_columns = get_rdc_price_columns(market_periods_count)
    rdc_power_columns = get_rdc_power_columns(market_periods_count)

    participants_bidding_zones = get_participants_bidding_zones(
        participants_bidding_zones
    )

    capacidad_inter_PBC_pt = capacidad_inter_pbc.query(
        f"{cols.CAT_FRONTIER} == {FRONTIER_MAPPING_REVERSE['PT']}"
    )

    # Read France's orders off the unit-level curve, so that imports from and exports to
    # France take part in the residual demand. DET/CAB never carry them, so without this the
    # whole cross-border volume is simply absent.
    det_cab_fr = get_france_det_cab_from_curva_pbc_uof(curva_pbc_uof)

    det_cab = get_det_cab_for_simulation(
        det=det,
        cab=cab,
        participants_bidding_zones=participants_bidding_zones,
        det_cab_fr=det_cab_fr,
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
            calculate_only_simple_submitted_relaxed_residual_demand(
                det_cab_aux, market_periods_count
            )
        )
        only_simple_submitted_relaxed_residual_demand.index = rdc_power_columns
        only_simple_submitted_relaxed_residual_demand = pd.concat(
            [price_series, only_simple_submitted_relaxed_residual_demand]
        )

        submitted_relaxed_residual_demand = calculate_submitted_relaxed_residual_demand(
            det_cab_aux, market_periods_count
        )
        submitted_relaxed_residual_demand.index = rdc_power_columns
        submitted_relaxed_residual_demand = pd.concat(
            [price_series, submitted_relaxed_residual_demand]
        )

        complex_residual_demand_I_without_market_split = (
            calculate_complex_residual_demand_I_without_market_split(
                det_cab_aux, market_periods_count
            )
        )
        complex_residual_demand_I_without_market_split.index = rdc_power_columns
        complex_residual_demand_I_without_market_split = pd.concat(
            [price_series, complex_residual_demand_I_without_market_split]
        )

        complex_residual_demand_II_with_market_split = (
            calculate_complex_residual_demand_II_with_market_split(
                det_cab_aux, capacidad_inter_PBC_pt, market_periods_count
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
