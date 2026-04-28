"""
Integration smoke tests for the full market clearing flow.

These tests require an external MIP solver (CBC) to be installed and available in
PATH.  In CI this is satisfied by the "Install CBC solver" step in tests.yml.

Run locally with:
    sudo apt-get install coinor-cbc   # Debian / Ubuntu
    brew install cbc                  # macOS
    pytest tests/test_integration.py -m integration -v
"""

import pandas as pd
import pytest

import iberian_day_ahead_market_simulator.columns as cols
from iberian_day_ahead_market_simulator.clearing_process import run_iterative_loop
from tests.const import STANDARD_TESTING_DATE

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FRONTIER_PT = 2  # FRONTIER_MAPPING["PT"]


@pytest.fixture
def capacidad_inter_pbc_pt_dataframe():
    """Minimal Portugal interconnection capacity DataFrame (3 periods)."""
    return pd.DataFrame(
        {
            cols.DATE_SESION: [STANDARD_TESTING_DATE] * 3,
            cols.CAT_FRONTIER: pd.Categorical(
                [FRONTIER_PT] * 3,
                categories=[2, 3, 4, 5],
            ),
            cols.INT_PERIOD: pd.array([1, 2, 3], dtype="int8"),
            cols.FLOAT_IMPORT_CAPACITY: [500.0, 500.0, 500.0],
            cols.FLOAT_EXPORT_CAPACITY: [500.0, 500.0, 500.0],
        }
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.slow
def test_run_iterative_loop_smoke_cbc(
    full_simplified_det_cab_dataframe,
    capacidad_inter_pbc_pt_dataframe,
):
    """
    Smoke test: run_iterative_loop() completes without error using CBC and
    returns a non-empty iterations_df with the expected result keys.
    """
    pytest.importorskip("pyomo", reason="pyomo is required for this test")

    # Verify CBC is available before running the test
    from pyomo.opt import SolverFactory  # noqa: PLC0415

    cbc = SolverFactory("cbc")
    if not cbc.available():
        pytest.skip("CBC solver binary is not installed; skipping integration test.")

    iterations_df, model, model_binary = run_iterative_loop(
        det_cab=full_simplified_det_cab_dataframe,
        capacidad_inter_pbc_pt=capacidad_inter_pbc_pt_dataframe,
        iterations_count=3,
        solver_factory_type="cbc",
    )

    # Basic structural assertions on iterations_df
    assert iterations_df is not None
    assert len(iterations_df) >= 1, "At least one iteration must have been executed."

    # Every row must have a recorded objective value
    assert cols.FLOAT_OBJECTIVE_VALUE in iterations_df.columns
    assert iterations_df[cols.FLOAT_OBJECTIVE_VALUE].notna().all()

    # Clearing prices must be present and non-empty in every iteration row
    assert cols.CLEARING_PRICES_COLUMN in iterations_df.columns
    for prices in iterations_df[cols.CLEARING_PRICES_COLUMN]:
        assert isinstance(prices, pd.DataFrame)
        assert len(prices) > 0

    # The returned Pyomo models should not be None
    assert model is not None
    assert model_binary is not None
