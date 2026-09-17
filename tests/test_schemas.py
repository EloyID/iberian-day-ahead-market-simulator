"""
Tests for iberian_day_ahead_market_simulator.schemas.residual_demand_curves and
schemas.sell_profiles.

Regression tests for the fixed off-by-one bug: the required/optional column
split used range(1, 24) for required columns and range(24, 101) for optional
ones, so power_24/price_24 - the last hour of an ordinary 24-period day - were
incorrectly optional instead of required. The fix uses range(1, 25)/range(25, 101).
"""

import pandas as pd
import pandera.pandas as pa
import pytest

from iberian_day_ahead_market_simulator.schemas.residual_demand_curves import (
    ResidualDemandCurvesSchema,
)
from iberian_day_ahead_market_simulator.schemas.sell_profiles import (
    SellProfilesSchema,
)


def _hourly_rdc_row(missing_columns: set[str] = frozenset()) -> dict:
    row = {}
    for i in range(1, 25):
        if f"power_{i}" not in missing_columns:
            row[f"power_{i}"] = 100.0
        if f"price_{i}" not in missing_columns:
            row[f"price_{i}"] = 50.0
    return row


class TestResidualDemandCurvesSchema:
    def test_full_24_hour_row_is_valid(self):
        df = pd.DataFrame([_hourly_rdc_row()])
        ResidualDemandCurvesSchema.validate(df)

    def test_missing_power_24_is_rejected(self):
        """power_24 must be required: a row missing it (e.g. an upstream bug
        that drops the last hour) must fail validation, not pass silently."""
        df = pd.DataFrame([_hourly_rdc_row(missing_columns={"power_24"})])
        with pytest.raises(pa.errors.SchemaError):
            ResidualDemandCurvesSchema.validate(df)

    def test_missing_price_24_is_rejected(self):
        df = pd.DataFrame([_hourly_rdc_row(missing_columns={"price_24"})])
        with pytest.raises(pa.errors.SchemaError):
            ResidualDemandCurvesSchema.validate(df)

    def test_periods_beyond_24_remain_optional(self):
        """QH-only columns (25-100) must still be optional for hourly data."""
        df = pd.DataFrame([_hourly_rdc_row()])
        assert "power_25" not in df.columns
        ResidualDemandCurvesSchema.validate(df)


class TestSellProfilesSchema:
    def test_full_24_hour_row_is_valid(self):
        df = pd.DataFrame([{f"power_{i}": 100.0 for i in range(1, 25)}])
        SellProfilesSchema.validate(df)

    def test_missing_power_24_is_rejected(self):
        df = pd.DataFrame([{f"power_{i}": 100.0 for i in range(1, 24)}])
        with pytest.raises(pa.errors.SchemaError):
            SellProfilesSchema.validate(df)
