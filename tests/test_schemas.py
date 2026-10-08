"""
Tests for iberian_day_ahead_market_simulator.schemas.residual_demand_curves and
schemas.sell_profiles.

The last period of a session must not go missing unnoticed, but the session is
23 to 100 periods long (TOTAL_PERIODS_OPTIONS), so a statically required
power_24 cannot express that: it rejects the 23-period daylight saving spring
forward day. Both schemas therefore require only the 23 periods every allowed
length has, and pin the session length with a dataframe level check that the
period columns run from 1 to an allowed length without a gap.

For a residual demand curve that check also catches a dropped last period,
because the power and the price columns then disagree on the session length.
A sell profile has no price columns to cross check against, so there a dropped
last period is indistinguishable from a genuine 23-period session.
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


def _rdc_row(market_periods_count: int) -> dict:
    return {
        **{f"power_{i}": 100.0 for i in range(1, market_periods_count + 1)},
        **{f"price_{i}": 50.0 for i in range(1, market_periods_count + 1)},
    }


class TestResidualDemandCurvesSchema:
    def test_full_24_hour_row_is_valid(self):
        df = pd.DataFrame([_hourly_rdc_row()])
        ResidualDemandCurvesSchema.validate(df)

    def test_missing_power_24_is_rejected(self):
        """A row that drops the last hour of a 24-period day must fail
        validation, not pass silently: its 23 power columns no longer agree
        with its 24 price columns."""
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

    @pytest.mark.parametrize("market_periods_count", [23, 24, 25, 92, 96, 100])
    def test_every_allowed_session_length_is_valid(self, market_periods_count):
        """23 is the daylight saving spring forward day and 92 its quarter
        hourly counterpart; neither may be rejected."""
        df = pd.DataFrame([_rdc_row(market_periods_count)])
        ResidualDemandCurvesSchema.validate(df)

    def test_unknown_session_length_is_rejected(self):
        df = pd.DataFrame([_rdc_row(30)])
        with pytest.raises(pa.errors.SchemaError):
            ResidualDemandCurvesSchema.validate(df)

    def test_gap_in_the_middle_of_the_session_is_rejected(self):
        """power_24 present but power_23 missing is a gap, not a short day."""
        row = _rdc_row(24)
        del row["power_23"]
        del row["price_23"]
        with pytest.raises(pa.errors.SchemaError):
            ResidualDemandCurvesSchema.validate(pd.DataFrame([row]))


class TestSellProfilesSchema:
    def test_full_24_hour_row_is_valid(self):
        df = pd.DataFrame([{f"power_{i}": 100.0 for i in range(1, 25)}])
        SellProfilesSchema.validate(df)

    @pytest.mark.parametrize("market_periods_count", [23, 24, 25, 92, 96, 100])
    def test_every_allowed_session_length_is_valid(self, market_periods_count):
        df = pd.DataFrame(
            [{f"power_{i}": 100.0 for i in range(1, market_periods_count + 1)}]
        )
        SellProfilesSchema.validate(df)

    def test_unknown_session_length_is_rejected(self):
        df = pd.DataFrame([{f"power_{i}": 100.0 for i in range(1, 31)}])
        with pytest.raises(pa.errors.SchemaError):
            SellProfilesSchema.validate(df)

    def test_gap_in_the_middle_of_the_session_is_rejected(self):
        row = {f"power_{i}": 100.0 for i in range(1, 25)}
        del row["power_23"]
        with pytest.raises(pa.errors.SchemaError):
            SellProfilesSchema.validate(pd.DataFrame([row]))
