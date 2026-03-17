"""mibel-simulator: MIBEL Iberian day-ahead electricity market clearing simulator.

Main entry points
-----------------
run_mibel_simulator
    Full iterative clearing of the MIBEL day-ahead market for a single session.
calculate_residual_demand_curves
    Compute residual demand curves for a set of sessions.
interpolate_residual_demand_curves
    Interpolate residual demand curves at arbitrary price points.
parse_cab_file / parse_det_file / parse_capacidad_inter_file
    Parse raw OMIE flat-file formats into pandas DataFrames.
"""

__version__ = "0.1.0"
__author__ = "EloyID"

from mibel_simulator.clearing_process import run_mibel_simulator
from mibel_simulator.parse_omie_files import (
    parse_cab_file,
    parse_capacidad_inter_file,
    parse_det_file,
)

__all__ = [
    "run_mibel_simulator",
    "parse_cab_file",
    "parse_det_file",
    "parse_capacidad_inter_file",
]
