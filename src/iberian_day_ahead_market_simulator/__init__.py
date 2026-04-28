"""iberian-day-ahead-market-simulator: MIBEL Iberian day-ahead electricity market clearing simulator.

Main entry points
-----------------
run_iberian_day_ahead_market_simulator
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

from iberian_day_ahead_market_simulator.clearing_process import (
    run_iberian_day_ahead_market_simulator,
)
from iberian_day_ahead_market_simulator.const import FRONTIER_MAPPING_REVERSE
from iberian_day_ahead_market_simulator.parse_omie_files import (
    parse_cab_file,
    parse_capacidad_inter_file,
    parse_det_file,
    parse_price_france_from_entsoe_file,
    parse_marginalpdbc_file,
)
from iberian_day_ahead_market_simulator.plot_helpers import (
    plot_clearing_prices,
    plot_spain_portugal_transmissions,
)

import iberian_day_ahead_market_simulator.columns as cols
from iberian_day_ahead_market_simulator.results_analyze_tools import (
    summary_det_cab_and_curva_pbc_uof,
)

__all__ = [
    "run_iberian_day_ahead_market_simulator",
    "parse_cab_file",
    "parse_det_file",
    "parse_capacidad_inter_file",
    "parse_price_france_from_entsoe_file",
    "parse_marginalpdbc_file",
    "plot_clearing_prices",
    "plot_spain_portugal_transmissions",
    "cols",
    "summary_det_cab_and_curva_pbc_uof",
    "FRONTIER_MAPPING_REVERSE",
]
