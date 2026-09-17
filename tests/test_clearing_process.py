"""
Tests for iberian_day_ahead_market_simulator.clearing_process module.

These target pure/orchestration logic that does not require running the MIP
solver. Full end-to-end solving is covered separately by the integration smoke
test in tests/test_integration.py.
"""

import pandas as pd
import pytest

import iberian_day_ahead_market_simulator.columns as cols
from iberian_day_ahead_market_simulator import clearing_process
from iberian_day_ahead_market_simulator.clearing_process import (
    get_id_paradoxical_orders_from_id_orders,
    run_iterative_loop,
)
from tests.const import STANDARD_TESTING_DATE

FRONTIER_PT = 2  # FRONTIER_MAPPING["PT"]


class TestGetIdParadoxicalOrdersFromIdOrders:
    """Test suite for get_id_paradoxical_orders_from_id_orders function."""

    @pytest.fixture
    def det_cab(self):
        return pd.DataFrame(
            {
                cols.ID_ORDER: ["A", "B", "C"],
                cols.ID_PARADOXICAL_ORDERS: ["PGA", "PGB", "PGB"],
            }
        )

    def test_returns_paradoxical_orders_for_matching_ids(self, det_cab):
        result = get_id_paradoxical_orders_from_id_orders(["A", "B"], det_cab)
        assert set(result) == {"PGA", "PGB"}

    def test_raises_when_id_order_not_found(self, det_cab):
        """Regression test for the fixed bug: an id_order absent from det_cab used
        to silently return an incomplete list instead of raising, because the
        check compared the result list's length against itself (always equal)
        instead of against the number of requested id_orders."""
        with pytest.raises(ValueError, match="Mismatch"):
            get_id_paradoxical_orders_from_id_orders(["A", "UNKNOWN"], det_cab)


class TestRunIterativeLoopMarketPeriodsCount:
    """
    run_iterative_loop() always finishes by solving a market model via run_model(),
    which is out of scope for these unit tests (see tests/test_integration.py for
    the solver-backed smoke test). These tests instead target the specific fix
    ("market_periods_count missing argument"): when market_periods_count is not
    provided, it must be defaulted via get_market_periods_count(), and det_cab must
    be filtered down to periods <= market_periods_count before being used further.

    To observe this without invoking the solver, get_all_paradoxical_orders (the
    very next call in run_iterative_loop after the fixed lines) is monkeypatched to
    capture the det_cab it receives and abort the function immediately afterwards.
    """

    @pytest.fixture
    def capacidad_inter_pbc_pt_dataframe(self):
        """Minimal Portugal interconnection capacity DataFrame (3 periods)."""
        return pd.DataFrame(
            {
                cols.DATE_SESION: [pd.Timestamp(STANDARD_TESTING_DATE)] * 3,
                cols.CAT_FRONTIER: pd.Categorical(
                    [FRONTIER_PT] * 3,
                    categories=[2, 3, 4, 5],
                ),
                cols.INT_PERIOD: pd.array([1, 2, 3], dtype="int8"),
                cols.FLOAT_IMPORT_CAPACITY: [-500.0, -500.0, -500.0],
                cols.FLOAT_EXPORT_CAPACITY: [500.0, 500.0, 500.0],
            }
        )

    # To verify without running the solver, the tests monkeypatch
    # get_all_paradoxical_orders (the very next call after the filtering
    # logic) with a stub that:
    # Captures whichever det_cab it was called with, into a captured dict.
    # Immediately raises RuntimeError("stop-before-optimization"), aborting
    # the function before it ever reaches run_model.

    def _make_aborting_stub(self, captured):
        def fake_get_all_paradoxical_orders(det_cab):
            captured["det_cab"] = det_cab
            raise RuntimeError("stop-before-optimization")

        return fake_get_all_paradoxical_orders

    @pytest.fixture
    def det_cab(self, full_simplified_det_cab_dataframe):
        """A schema-valid variant of the shared fixture.

        date_sesion is coerced to datetime64 and the ES (Spain) rows' null
        float_bid_power_cumsum_by_country is filled, since DETCABSchema requires a
        datetime dtype and disallows nulls in that column; neither of those is
        exercised by the market_periods_count filtering this test targets.
        """
        det_cab = full_simplified_det_cab_dataframe.copy()
        det_cab[cols.DATE_SESION] = pd.to_datetime(det_cab[cols.DATE_SESION])
        det_cab[cols.FLOAT_BID_POWER_CUMSUM_BY_COUNTRY] = det_cab[
            cols.FLOAT_BID_POWER_CUMSUM_BY_COUNTRY
        ].fillna(0.0)
        return det_cab

    def test_defaults_market_periods_count_when_not_provided(
        self,
        monkeypatch,
        det_cab,
        capacidad_inter_pbc_pt_dataframe,
    ):
        captured = {}
        # Stub the period-count detection so this test doesn't depend on its
        # internal heuristics (covered separately in tests/test_tools.py); it only
        # needs to prove run_iterative_loop calls it and uses its result to filter.
        monkeypatch.setattr(clearing_process, "get_market_periods_count", lambda det: 2)
        monkeypatch.setattr(
            clearing_process,
            "get_all_paradoxical_orders",
            self._make_aborting_stub(captured),
        )

        with pytest.raises(RuntimeError, match="stop-before-optimization"):
            run_iterative_loop(
                det_cab=det_cab,
                capacidad_inter_pbc_pt=capacidad_inter_pbc_pt_dataframe,
                market_periods_count=None,
            )

        # market_periods_count defaulted to 2 (from the stub), so period-3 rows
        # must have been filtered out of det_cab before it was used further.
        assert sorted(captured["det_cab"][cols.INT_PERIOD].unique()) == [1, 2]

    def test_filters_det_cab_to_explicit_market_periods_count(
        self,
        monkeypatch,
        det_cab,
        capacidad_inter_pbc_pt_dataframe,
    ):
        captured = {}
        monkeypatch.setattr(
            clearing_process,
            "get_all_paradoxical_orders",
            self._make_aborting_stub(captured),
        )

        with pytest.raises(RuntimeError, match="stop-before-optimization"):
            run_iterative_loop(
                det_cab=det_cab,
                capacidad_inter_pbc_pt=capacidad_inter_pbc_pt_dataframe,
                market_periods_count=2,
            )

        assert sorted(captured["det_cab"][cols.INT_PERIOD].unique()) == [1, 2]
