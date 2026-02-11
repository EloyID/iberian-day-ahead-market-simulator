import logging
from typing import Literal
import numpy as np
import pandas as pd
import pandera.pandas as pa
from mibel_simulator.clearing_process import clear_OMIE_market
import mibel_simulator.columns as cols
from mibel_simulator.const import (
    COD_OFERTA_RESIDUAL_DEMAND_C,
    COD_OFERTA_RESIDUAL_DEMAND_V,
    CODIGO_UNIDAD_RESIDUAL_DEMAND_C,
    CODIGO_UNIDAD_RESIDUAL_DEMAND_V,
    RDC_CAB_C_BASE,
    RDC_CAB_V_BASE,
)
from mibel_simulator.parse_omie_files import (
    parse_cab_file,
    parse_capacidad_inter_file,
    parse_det_file,
)
from mibel_simulator.schemas.cab import CABSchema
from mibel_simulator.schemas.capacidad_inter_pt import CapacidadInterPTSchema
from mibel_simulator.schemas.det import DETSchema
from mibel_simulator.schemas.residual_demand_curves import ResidualDemandCurvesSchema
from mibel_simulator.schemas.sell_profiles import SellProfilesSchema

logger = logging.getLogger(__name__)


def generate_residual_demand_det_cab_and_uof_zone(
    rdc: pd.Series, date_sesion: pd.Timestamp, sell_country: str
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Generate residual demand DET, CAB, and UOF zone dataframes.

    Args:
        rdc (pd.Series): Residual demand curve data.
        date_sesion (pd.Timestamp): Date of the session.
        sell_country (str): Country code for selling.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]: Residual demand DET, CAB, and UOF zone dataframes.
    """
    rdc_sorted = rdc[[f"energy_{i+1}" for i in range(24)]]
    rdc_cab_rows = []
    uof_ids = []
    if (rdc_sorted >= 0).any():
        rdc_cab_rows.append(RDC_CAB_V_BASE)
        uof_ids.append(CODIGO_UNIDAD_RESIDUAL_DEMAND_V)
    if (rdc_sorted < 0).any():
        rdc_cab_rows.append(RDC_CAB_C_BASE)
        uof_ids.append(CODIGO_UNIDAD_RESIDUAL_DEMAND_C)

    periods = list(range(1, 25))
    sell_profile = rdc_sorted.values
    buy_sell = np.where(sell_profile >= 0, "V", "C")
    bid_price = np.where(buy_sell == "V", -500, 3500)
    id_order = np.where(
        buy_sell == "V", COD_OFERTA_RESIDUAL_DEMAND_V, COD_OFERTA_RESIDUAL_DEMAND_C
    )
    bid_power = np.abs(sell_profile)

    rdc_cab = pd.DataFrame(rdc_cab_rows)
    rdc_cab[cols.DATE_SESION] = date_sesion

    rdc_det = pd.DataFrame(
        {
            cols.DATE_SESION: date_sesion,
            cols.ID_ORDER: id_order,
            cols.INT_PERIODO: periods,
            cols.INT_NUM_BLOQ: 0,
            cols.INT_NUM_TRAMO: 1,
            cols.INT_NUM_GRUPO_EXCL: 0,
            cols.FLOAT_BID_PRICE: bid_price,
            cols.FLOAT_BID_POWER: bid_power,
            cols.FLOAT_MAV: 0.0,
            cols.FLOAT_MAR: 0.0,
        }
    ).query(f"{cols.FLOAT_BID_POWER} > 0")

    if rdc_det.empty:
        logger.warning(
            "The generated residual demand DET dataframe is empty. Check the input residual demand curve."
        )
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # Filter rdc_cab to only include relevant ID_ORDERs
    rdc_cab = rdc_cab.query(
        f"{cols.ID_ORDER} in {rdc_det[cols.ID_ORDER].unique().tolist()}"
    )

    uof_zone = pd.DataFrame(
        {
            cols.ID_UNIDAD: uof_ids,
            cols.CAT_PAIS: sell_country,
        }
    )

    return rdc_det, rdc_cab, uof_zone


@pa.check_output(SellProfilesSchema, lazy=True)
def create_homothetic_sell_profiles(
    profile: list[float], scaling_factors: list[float]
) -> pd.DataFrame:
    """Create sell profiles by scaling a base profile with given factors.

    Args:
        profile (list[float]): Base profile to be scaled.
        scaling_factors (list[float]): Factors by which to scale the base profile.

    Returns:
        pd.DataFrame: DataFrame of scaled profiles.
    """
    if len(profile) != 24:
        logging.warning("Base profile length is not 24. Check if this is correct.")

    base_profile = np.array(profile)
    sell_profiles_values = np.array(
        [base_profile * factor for factor in scaling_factors]
    )
    sell_profiles = pd.DataFrame(
        sell_profiles_values,
        index=[f"scale_{factor:.2f}" for factor in scaling_factors],
        columns=[f"energy_{i+1}" for i in range(24)],
    )
    return sell_profiles


def get_clearing_prices_dict(results: dict, sell_country: str) -> dict:
    """
    Extracts the hourly clearing prices for a given country from the market clearing results.

    Args:
        results (dict): Dictionary containing market clearing results, including a DataFrame under the "clearing_prices" key.
        sell_country (str): Country code for which to extract clearing prices (e.g., "ES" for Spain).

    Returns:
        dict: Dictionary mapping 'price_1' to 'price_24' to their corresponding clearing prices for the specified country.
    """

    clearing_prices_country = (
        results["clearing_prices"].query(f"{cols.CAT_PAIS} == '{sell_country}'").copy()
    )
    clearing_prices_country["price_keys"] = "price_" + clearing_prices_country[
        cols.INT_PERIODO
    ].astype(str)
    clearing_prices_dict = (
        clearing_prices_country[["price_keys", cols.FLOAT_CLEARED_PRICE]]
        .set_index("price_keys")[cols.FLOAT_CLEARED_PRICE]
        .to_dict()
    )

    return clearing_prices_dict


@pa.check_input(SellProfilesSchema, "sell_profiles", lazy=True)
@pa.check_output(ResidualDemandCurvesSchema, lazy=True)
def calculate_residual_demand_curves(
    sell_profiles: pd.DataFrame,
    det_date: pd.DataFrame | str,
    cab_date: pd.DataFrame | str,
    capacidad_inter_date: pd.DataFrame | str,
    price_france_date: pd.DataFrame,
    uof_zones: pd.DataFrame | None = None,
    sell_country: str = "ES",
    trials_count: int = 100,
    zones_default_to_spain: bool = True,
    n_jobs: int = 1,
) -> pd.DataFrame:
    """
    Calculates the residual demand curves for a set of sell profiles by simulating market clearing.

    Args:
        sell_profiles (pd.DataFrame): DataFrame with index as profile names and columns as 'energy_1' to 'energy_24' representing hourly energy values.
        det_date (pd.DataFrame | str): DataFrame or path to DET file containing market offer details.
        cab_date (pd.DataFrame | str): DataFrame or path to CAB file containing market header information.
        capacidad_inter_date (pd.DataFrame | str): DataFrame or path to interconnection capacity file.
        price_france_date (pd.DataFrame): DataFrame with France price information.
        uof_zones (pd.DataFrame | None): DataFrame with UOF zone information with columns id_unidad and cat_pais within ('ES', 'PT').
        sell_country (str, optional): Country code for the selling side. Defaults to "ES".
        trials_count (int, optional): Number of trials for the market clearing simulation. Defaults to 100.
        zones_default_to_spain (bool, optional): If True, zones default to Spain. Defaults to True.
        n_jobs (int, optional): Number of parallel jobs for simulation. Defaults to 1.

    Returns:
        pd.DataFrame: DataFrame with index as profile names and columns 'price_1' to 'price_24' (clearing prices per hour) and 'energy_1' to 'energy_24' (energy values per hour).
    """

    residual_demand_curves = pd.DataFrame(
        columns=[f"price_{i+1}" for i in range(24)]
        + [f"energy_{i+1}" for i in range(24)],
        index=sell_profiles.index,
    )

    # if det_date is istring
    if isinstance(det_date, str):
        det_date = parse_det_file(det_date)
    if isinstance(cab_date, str):
        cab_date = parse_cab_file(cab_date)
    if isinstance(capacidad_inter_date, str):
        capacidad_inter_date = parse_capacidad_inter_file(capacidad_inter_date)

    DETSchema.validate(det_date)
    CABSchema.validate(cab_date)
    CapacidadInterPTSchema.validate(capacidad_inter_date)

    for idx, profile in sell_profiles.iterrows():
        # Here you would modify the det_date and cab_date based on the profile
        # For simplicity, we will just pass them as is

        rdc_det, rdc_cab, rdc_uof_zone = generate_residual_demand_det_cab_and_uof_zone(
            profile, det_date[cols.DATE_SESION].iloc[0], sell_country
        )

        if rdc_det.empty or rdc_cab.empty or rdc_uof_zone.empty:
            logger.warning(
                f"Profile {idx} has empty residual demand, the results are similar to a normal clearing of the market."
            )

        det_date_modified = pd.concat([det_date, rdc_det], ignore_index=True)
        cab_date_modified = pd.concat([cab_date, rdc_cab], ignore_index=True)

        if isinstance(uof_zones, pd.DataFrame):
            uof_zones_modified = pd.concat([uof_zones, rdc_uof_zone], ignore_index=True)
        elif not rdc_uof_zone.empty:
            uof_zones_modified = rdc_uof_zone
        else:
            uof_zones_modified = None

        results = clear_OMIE_market(
            det_date=det_date_modified,
            cab_date=cab_date_modified,
            capacidad_inter_date=capacidad_inter_date,
            uof_zones=uof_zones_modified,
            price_france_date=price_france_date,
            trials_count=trials_count,
            zones_default_to_spain=zones_default_to_spain,
            n_jobs=n_jobs,
        )
        energies_dict = profile.to_dict()
        clearing_prices_dict = get_clearing_prices_dict(results, sell_country)
        residual_demand_curves.loc[idx] = {
            **clearing_prices_dict,
            **energies_dict,
        }

    return residual_demand_curves


from mibel_simulator.const import RDC_ENERGY_COLUMNS, RDC_PRICE_COLUMNS


def interpolate_residual_demand_curves(
    target_energy_levels,
    residual_demand_curves,
    target_energy_columns: list[str] = RDC_ENERGY_COLUMNS,
    target_price_columns: list[str] = RDC_PRICE_COLUMNS,
    residual_demand_energy_columns: list[str] = RDC_ENERGY_COLUMNS,
    residual_demand_price_columns: list[str] = RDC_PRICE_COLUMNS,
    extrapolate_action: Literal["limit", "nan", "warning", "raise"] = "warning",
):
    interpolated_prices = {}
    for (
        target_energy_column,
        target_price_column,
        residual_demand_energy_column,
        residual_demand_price_column,
    ) in zip(
        target_energy_columns,
        target_price_columns,
        residual_demand_energy_columns,
        residual_demand_price_columns,
    ):
        target_energy = target_energy_levels[target_energy_column]
        residual_demand_curve_df = (
            residual_demand_curves[
                [residual_demand_energy_column, residual_demand_price_column]
            ]
            .dropna()
            .sort_values(by=residual_demand_energy_column)
        )

        interpolated_price = np.interp(
            target_energy,
            residual_demand_curve_df[residual_demand_energy_column],
            residual_demand_curve_df[residual_demand_price_column],
        )

        if extrapolate_action == "limit":
            pass
        else:
            extrapolated_mask = (
                target_energy
                < residual_demand_curve_df[residual_demand_energy_column].min()
            ) | (
                target_energy
                > residual_demand_curve_df[residual_demand_energy_column].max()
            )

            if extrapolate_action == "raise" and extrapolated_mask.any():
                raise ValueError(
                    "Extrapolation detected in residual demand curve interpolation."
                )
            if extrapolate_action == "warning" and extrapolated_mask.any():
                logging.warning(
                    "Extrapolation detected in residual demand curve interpolation. Resulting values set to NaN."
                )
            if extrapolate_action == "nan":
                interpolated_price[extrapolated_mask] = np.nan

        interpolated_price = pd.Series(
            interpolated_price, index=target_energy.index, name=target_price_column
        )

        interpolated_prices[target_price_column] = interpolated_price

    interpolated_residual_demand_curves = pd.concat(
        [
            target_energy_levels[target_energy_columns],
            pd.DataFrame(interpolated_prices),
        ],
        axis=1,
    )
    return interpolated_residual_demand_curves
