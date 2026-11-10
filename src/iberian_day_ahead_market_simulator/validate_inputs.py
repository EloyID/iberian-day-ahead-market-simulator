"""
Read and validate the inputs of the functions that clear the orders at given price curves.

These helpers live apart from tools because they parse the raw OMIE files, and
parse_omie_files imports tools: in tools they would need an import inside the function
to avoid a circular import.
"""

import logging

import numpy as np
import pandas as pd

import iberian_day_ahead_market_simulator.columns as cols
from iberian_day_ahead_market_simulator.const import TOTAL_PERIODS_OPTIONS
from iberian_day_ahead_market_simulator.file_paths import (
    PARTICIPANTS_BIDDING_ZONES_FILEPATH,
)
from iberian_day_ahead_market_simulator.parse_omie_files import (
    parse_cab_file,
    parse_det_file,
)
from iberian_day_ahead_market_simulator.schemas.cab import CABSchema
from iberian_day_ahead_market_simulator.schemas.det import DETSchema
from iberian_day_ahead_market_simulator.tools import (
    concat_provided_participants_bidding_zones_with_existing_data,
    get_market_periods_count,
)

logger = logging.getLogger(__name__)


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


def get_reference_price_curve_index(
    price_curves: np.ndarray, reference_price_curve: np.ndarray
) -> int:
    """
    Find the row of the price curves that is the reference price curve.

    Args:
        price_curves (np.ndarray): The price curves, one row each, as format_price_curves
            returns them.
        reference_price_curve (np.ndarray): One price per market period.

    Returns:
        int: The position of the first row equal to the reference price curve.

    Raises:
        ValueError: If no row is equal to the reference price curve.
    """
    try:
        reference_price_curve_index = np.where(
            (price_curves == reference_price_curve).all(axis=1)
        )[0][0]
        logger.info("reference_price_curve_index: %s", reference_price_curve_index)
    except Exception as e:
        raise ValueError(
            "Error while trying to find the reference price curve in the provided price curves."
        ) from e
    return reference_price_curve_index


def get_validated_det_and_cab(
    det: pd.DataFrame | str, cab: pd.DataFrame | str, price_curves: np.ndarray
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """
    Read and validate the day's DET and CAB, and check the price curves cover its periods.

    Args:
        det (pd.DataFrame | str): The day's order detail, or a path to the raw OMIE file.
        cab (pd.DataFrame | str): The day's order headers, or a path to the raw OMIE file.
        price_curves (np.ndarray): The price curves, one row each, as format_price_curves
            returns them.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame, int]: The DET, without the periods beyond the
            session's, the CAB, and the session's number of market periods.

    Raises:
        ValueError: If the price curves do not cover exactly the DET file's market periods.
    """
    if isinstance(det, str):
        det = parse_det_file(det)
    if isinstance(cab, str):
        cab = parse_cab_file(cab)

    DETSchema.validate(det)
    CABSchema.validate(cab)

    market_periods_count = get_market_periods_count(det)
    det = det.query(f"{cols.INT_PERIOD} <= {market_periods_count}")

    # the price curves are indexed by market period further down, so a length that
    # disagrees with the DET file has to be reported here rather than as an
    # IndexError from inside the clearing of an individual order
    if price_curves.shape[1] != market_periods_count:
        raise ValueError(
            f"price_curves cover {price_curves.shape[1]} periods but the DET file is a "
            f"{market_periods_count} period session. Pass one price per market period."
        )

    return det, cab, market_periods_count


def get_participants_bidding_zones(
    participants_bidding_zones: pd.DataFrame | None,
) -> pd.DataFrame:
    """
    The unit to bidding zone mapping: the one provided merged into the bundled list, or the
    bundled list alone.

    Args:
        participants_bidding_zones (pd.DataFrame | None): Unit to bidding zone mapping, with
            cols.ID_UNIDAD and cols.CAT_BIDDING_ZONE; None reads the package's bundled list.

    Returns:
        pd.DataFrame: The mapping, one row per unit, with cols.ID_UNIDAD and
            cols.CAT_BIDDING_ZONE.
    """
    if isinstance(participants_bidding_zones, pd.DataFrame):
        return concat_provided_participants_bidding_zones_with_existing_data(
            participants_bidding_zones
        )
    return pd.read_csv(PARTICIPANTS_BIDDING_ZONES_FILEPATH)
