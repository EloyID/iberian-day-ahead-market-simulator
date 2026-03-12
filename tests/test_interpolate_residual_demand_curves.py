import numpy as np
import pandas as pd
import pytest

from mibel_simulator.const import RDC_ENERGY_COLUMNS, RDC_PRICE_COLUMNS
from mibel_simulator.residual_demand_curve import (
    interpolate_residual_demand_curves,
)


def _build_linear_residual_demand_curves():
    """
    Build a simple residual_demand_curves DataFrame with 3 points per hour:
    energies: [0, 100, 200] and prices: [10, 20, 40] for all 24 hours,
    so interpolation is easy to verify.
    """
    rows = ["p0", "p1", "p2"]
    data = {}
    for h in range(1, 25):
        # energies grow linearly
        data[f"energy_{h}"] = [0.0, 100.0, 200.0]
        # prices grow (non-strictly linear ratio for diversity)
        data[f"price_{h}"] = [10.0, 20.0, 40.0]
    df = pd.DataFrame(data, index=rows)
    return df


def _build_target_energy_levels(index_labels=("low", "mid", "high")):
    """
    Build target_energy_levels with all 24 energy columns and flexible rows.
    Default mapping:
      - "low":  -10.0 (below min, triggers extrapolation)
      - "mid":   50.0 (within range)
      - "high": 250.0 (above max, triggers extrapolation)
    Unknown labels get 50.0 by default.
    """
    default_map = {"low": -10.0, "mid": 50.0, "high": 250.0}
    row_values = [default_map.get(lbl, 50.0) for lbl in index_labels]
    df = pd.DataFrame(
        {col: row_values for col in RDC_ENERGY_COLUMNS},
        index=list(index_labels),
    )
    return df


class TestInterpolateResidualDemandCurves:
    def test_within_range_interpolation(self):
        residual = _build_linear_residual_demand_curves()
        # Only use the mid row to avoid extrapolation
        target = _build_target_energy_levels(index_labels=("mid",))

        result = interpolate_residual_demand_curves(
            target, residual, extrapolate_action="nan"
        )
        # Expected price for energy=50 between (0,100) with prices (10,20): 15
        expected_price = 15.0
        for price_col in RDC_PRICE_COLUMNS:
            assert np.allclose(result.loc["mid", price_col], expected_price)
        # Energy columns preserved
        for energy_col in RDC_ENERGY_COLUMNS:
            assert result.loc["mid", energy_col] == 50.0

    def test_extrapolation_limit_behavior(self):
        residual = _build_linear_residual_demand_curves()
        target = _build_target_energy_levels(index_labels=("low", "high"))

        # limit uses np.interp default behavior (clamps to boundary y)
        result = interpolate_residual_demand_curves(
            target, residual, extrapolate_action="limit"
        )

        # For low (-10), expect first price 10.0 for every hour
        for price_col in RDC_PRICE_COLUMNS:
            assert np.isclose(result.loc["low", price_col], 10.0)
        # For high (250), expect last price 40.0 for every hour
        for price_col in RDC_PRICE_COLUMNS:
            assert np.isclose(result.loc["high", price_col], 40.0)

    def test_extrapolation_nan_behavior(self):
        residual = _build_linear_residual_demand_curves()
        target = _build_target_energy_levels(index_labels=("low", "mid", "high"))

        result = interpolate_residual_demand_curves(
            target, residual, extrapolate_action="nan"
        )
        # low and high rows should be NaN on price columns (extrapolated)
        for price_col in RDC_PRICE_COLUMNS:
            assert np.isnan(result.loc["low", price_col])
            assert np.isnan(result.loc["high", price_col])
        # mid row within range -> interpolated value 15.0
        expected_price = 15.0
        for price_col in RDC_PRICE_COLUMNS:
            assert np.isclose(result.loc["mid", price_col], expected_price)

    def test_extrapolation_warning_behavior(self, caplog):
        residual = _build_linear_residual_demand_curves()
        target = _build_target_energy_levels(index_labels=("low", "mid"))

        with caplog.at_level("WARNING"):
            result = interpolate_residual_demand_curves(
                target, residual, extrapolate_action="warning"
            )
            # Check a warning was logged at least once
            assert any(
                "Extrapolation detected" in rec.message for rec in caplog.records
            )
        # low row -> NaN; mid row -> finite interpolated
        for price_col in RDC_PRICE_COLUMNS:
            assert not np.isnan(result.loc["low", price_col])
            assert not np.isnan(result.loc["mid", price_col])

    def test_extrapolation_raise_behavior(self):
        residual = _build_linear_residual_demand_curves()
        target = _build_target_energy_levels(
            index_labels=("low",)
        )  # triggers extrapolation

        with pytest.raises(ValueError):
            interpolate_residual_demand_curves(
                target, residual, extrapolate_action="raise"
            )

    def test_output_structure(self):
        residual = _build_linear_residual_demand_curves()
        target = _build_target_energy_levels(index_labels=("low", "mid", "high"))

        result = interpolate_residual_demand_curves(
            target, residual, extrapolate_action="nan"
        )
        # Columns should include all energies + all prices
        expected_cols = set(RDC_ENERGY_COLUMNS) | set(RDC_PRICE_COLUMNS)
        assert set(result.columns) == expected_cols
        # Index preserved
        assert list(result.index) == ["low", "mid", "high"]
