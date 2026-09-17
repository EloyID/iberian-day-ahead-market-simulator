"""
Tests for iberian_day_ahead_market_simulator.residual_demand_curve module.

Tests the residual demand curve generation and profile creation functions.
"""

import pandas as pd
import pytest

from iberian_day_ahead_market_simulator import columns as cols
from iberian_day_ahead_market_simulator import residual_demand_curve as rdc


class TestCreateHomotheticSellProfiles:
    """Test suite for create_homothetic_sell_profiles function."""

    def test_profile_with_varying_values(self):
        """Test profile creation with varying hourly values."""
        base_profile = list(range(1, 25))  # 1 to 24
        scaling_factors = [1.0, 2.0]

        result = rdc.create_homothetic_sell_profiles(base_profile, scaling_factors)

        # Check first profile (scale 1.0)
        assert list(result.iloc[0]) == base_profile
        # Check second profile (scale 2.0)
        assert list(result.iloc[1]) == [x * 2 for x in base_profile]

    def test_column_names(self):
        """Test that column names are correctly formatted."""
        base_profile = [100.0] * 24
        scaling_factors = [1.0]

        result = rdc.create_homothetic_sell_profiles(base_profile, scaling_factors)

        expected_cols = [f"power_{i+1}" for i in range(24)]
        assert list(result.columns) == expected_cols

    def test_index_names(self):
        """Test that index names are correctly formatted."""
        base_profile = [100.0] * 24
        scaling_factors = [1.0, 2.0, 0.5]

        result = rdc.create_homothetic_sell_profiles(base_profile, scaling_factors)

        assert "scale_1.00" in result.index
        assert "scale_2.00" in result.index
        assert "scale_0.50" in result.index

    def test_negative_values_in_profile(self):
        """Test profile creation with negative values."""
        base_profile = [-100.0] * 24
        scaling_factors = [1.0, 2.0]

        result = rdc.create_homothetic_sell_profiles(base_profile, scaling_factors)

        assert (result.iloc[0] == -100.0).all()
        assert (result.iloc[1] == -200.0).all()

    def test_qh_length_profile_is_accepted_without_warning(self, caplog):
        """A 96-length (QH) profile is a valid option, just like 24 (hourly), and
        must not trigger the "length not allowed" warning."""
        base_profile = list(range(1, 97))  # 1 to 96
        scaling_factors = [1.0]

        with caplog.at_level("WARNING"):
            result = rdc.create_homothetic_sell_profiles(base_profile, scaling_factors)

        assert not any(
            "not in the allowed options" in rec.message for rec in caplog.records
        )
        assert list(result.iloc[0]) == base_profile
        assert list(result.columns) == [f"power_{i+1}" for i in range(96)]

    def test_disallowed_length_profile_warns(self, caplog):
        """A profile length that isn't any of the hourly/QH options logs a
        warning instead of silently proceeding (the resulting DataFrame then also
        fails the function's own output schema, since that schema still expects a
        specific set of period columns)."""
        base_profile = [100.0] * 10  # not a valid hourly or QH period count
        scaling_factors = [1.0]

        with caplog.at_level("WARNING"):
            with pytest.raises(Exception):
                rdc.create_homothetic_sell_profiles(base_profile, scaling_factors)

        assert any(
            "not in the allowed options" in rec.message for rec in caplog.records
        )


class TestGenerateResidualDemandDetCabAndUOFZone:
    """Test suite for generate_residual_demand_det_cab_and_participants_bidding_zone function."""

    def test_mixed_rdc_values(self):
        """Test RDC generation with mixed positive and negative values."""
        rdc_data = {f"power_{i+1}": 100.0 if i < 12 else -100.0 for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-01-01")

        det, cab, uof = (
            rdc.generate_residual_demand_det_cab_and_participants_bidding_zone(
                rdc_series,
                date,
                "ES",
                market_periods_count=24,
            )
        )

        assert not det.empty
        assert not cab.empty
        assert not uof.empty
        # Both positive and negative sell profiles should result in bids
        assert (det[cols.FLOAT_BID_POWER] > 0).all()
        assert len(det) > 0

    def test_zero_rdc_values(self):
        """Test RDC generation with zero values."""
        rdc_data = {f"power_{i+1}": 0.0 for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-01-01")

        det, cab, uof = (
            rdc.generate_residual_demand_det_cab_and_participants_bidding_zone(
                rdc_series,
                date,
                "ES",
                market_periods_count=24,
            )
        )

        assert det.empty  # No non-zero power
        assert cab.empty
        assert uof.empty

    def test_date_assignment(self):
        """Test that date is correctly assigned."""
        rdc_data = {f"power_{i+1}": 100.0 for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-06-15")

        det, cab, uof = (
            rdc.generate_residual_demand_det_cab_and_participants_bidding_zone(
                rdc_series,
                date,
                "ES",
                market_periods_count=24,
            )
        )

        assert (det[cols.DATE_SESION] == date).all()
        assert (cab[cols.DATE_SESION] == date).all()

    def test_country_assignment(self):
        """Test that country is correctly assigned."""
        rdc_data = {f"power_{i+1}": 100.0 for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-01-01")

        det, cab, uof = (
            rdc.generate_residual_demand_det_cab_and_participants_bidding_zone(
                rdc_series,
                date,
                "PT",
                market_periods_count=24,
            )
        )

        assert (uof[cols.CAT_BIDDING_ZONE] == "PT").all()

    def test_period_assignment(self):
        """Test that periods 1-24 are correctly assigned."""
        rdc_data = {f"power_{i+1}": 100.0 for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-01-01")

        det, cab, uof = (
            rdc.generate_residual_demand_det_cab_and_participants_bidding_zone(
                rdc_series,
                date,
                "ES",
                market_periods_count=24,
            )
        )

        assert set(det[cols.INT_PERIOD].unique()) == set(range(1, 25))

    def test_bid_prices_fixed_values(self):
        """Test that bid prices are assigned appropriately."""
        rdc_data = {f"power_{i+1}": (100.0 if i < 12 else -100.0) for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-01-01")

        det, cab, uof = (
            rdc.generate_residual_demand_det_cab_and_participants_bidding_zone(
                rdc_series,
                date,
                "ES",
                market_periods_count=24,
            )
        )

        # Prices should be either -500 or 3500
        assert all(
            price in [-500.0, 3500.0] for price in det[cols.FLOAT_BID_PRICE].values
        )

    def test_all_positive_values(self):
        """Test RDC generation with all positive values (sell orders)."""
        rdc_data = {f"power_{i+1}": 100.0 for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-01-01")

        det, cab, uof = (
            rdc.generate_residual_demand_det_cab_and_participants_bidding_zone(
                rdc_series,
                date,
                "ES",
                market_periods_count=24,
            )
        )

        # All should be sell orders, with bid price of -500
        assert (det[cols.FLOAT_BID_PRICE] == -500.0).all()
        # UOF should only contain sell unit
        assert len(uof) == 1

    def test_all_negative_values(self):
        """Test RDC generation with all negative values (buy orders)."""
        rdc_data = {f"power_{i+1}": -100.0 for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-01-01")

        det, cab, uof = (
            rdc.generate_residual_demand_det_cab_and_participants_bidding_zone(
                rdc_series,
                date,
                "ES",
                market_periods_count=24,
            )
        )

        # All should be buy orders, with bid price of 3500
        assert (det[cols.FLOAT_BID_PRICE] == 3500.0).all()
        # UOF should only contain buy unit
        assert len(uof) == 1

    def test_bid_power_absolute_values(self):
        """Test that bid power is the absolute value of the RDC."""
        rdc_data = {f"power_{i+1}": (-50.0 if i % 2 == 0 else 75.0) for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-01-01")

        det, cab, uof = (
            rdc.generate_residual_demand_det_cab_and_participants_bidding_zone(
                rdc_series,
                date,
                "ES",
                market_periods_count=24,
            )
        )

        # Check that power values are absolute
        assert all(det[cols.FLOAT_BID_POWER] >= 0)
        # For negative RDC values, bid power should be 50.0
        negative_indices = [i for i in range(24) if i % 2 == 0]
        assert all(
            det[det[cols.INT_PERIOD].isin([i + 1 for i in negative_indices])][
                cols.FLOAT_BID_POWER
            ]
            == 50.0
        )
        # For positive RDC values, bid power should be 75.0
        positive_indices = [i for i in range(24) if i % 2 == 1]
        assert all(
            det[det[cols.INT_PERIOD].isin([i + 1 for i in positive_indices])][
                cols.FLOAT_BID_POWER
            ]
            == 75.0
        )

    def test_det_dataframe_structure(self):
        """Test that DET DataFrame has correct structure and columns."""
        rdc_data = {f"power_{i+1}": 100.0 for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-01-01")

        det, cab, uof = (
            rdc.generate_residual_demand_det_cab_and_participants_bidding_zone(
                rdc_series,
                date,
                "ES",
                market_periods_count=24,
            )
        )

        # Check required columns exist
        required_cols = [
            cols.DATE_SESION,
            cols.ID_ORDER,
            cols.INT_PERIOD,
            cols.INT_NUM_BLOCK,
            cols.INT_NUM_SUBORDER,
            cols.INT_NUM_EXCL_GROUP,
            cols.FLOAT_BID_PRICE,
            cols.FLOAT_BID_POWER,
            cols.FLOAT_MAV,
            cols.FLOAT_MAR,
        ]
        for col in required_cols:
            assert col in det.columns

    def test_cab_filtering(self):
        """Test that CAB is filtered to only include relevant ID_ORDERs."""
        rdc_data = {f"power_{i+1}": (100.0 if i < 12 else -100.0) for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-01-01")

        det, cab, uof = (
            rdc.generate_residual_demand_det_cab_and_participants_bidding_zone(
                rdc_series,
                date,
                "ES",
                market_periods_count=24,
            )
        )

        # All ID_ORDER values in CAB should be in DET
        assert set(cab[cols.ID_ORDER].unique()).issubset(
            set(det[cols.ID_ORDER].unique())
        )

    def test_uof_contains_correct_units(self):
        """Test that UOF zone contains correct unit codes."""
        rdc_data = {f"power_{i+1}": (100.0 if i < 12 else -100.0) for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-01-01")

        det, cab, uof = (
            rdc.generate_residual_demand_det_cab_and_participants_bidding_zone(
                rdc_series,
                date,
                "ES",
                market_periods_count=24,
            )
        )

        # When both positive and negative values exist, should have both buy and sell units
        assert len(uof) == 2
        assert cols.ID_UNIDAD in uof.columns
        assert cols.CAT_BIDDING_ZONE in uof.columns

    def test_no_nan_values_in_det(self):
        """Test that DET DataFrame contains no NaN values."""
        rdc_data = {f"power_{i+1}": 100.0 for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-01-01")

        det, cab, uof = (
            rdc.generate_residual_demand_det_cab_and_participants_bidding_zone(
                rdc_series,
                date,
                "ES",
                market_periods_count=24,
            )
        )

        assert not det.isna().any().any()
        assert not cab.isna().any().any()

    def test_zero_filtering(self):
        """Test that zero power bids are filtered out from DET."""
        rdc_data = {f"power_{i+1}": (100.0 if i < 12 else 0.0) for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-01-01")

        det, cab, uof = (
            rdc.generate_residual_demand_det_cab_and_participants_bidding_zone(
                rdc_series,
                date,
                "ES",
                market_periods_count=24,
            )
        )

        # DET should only have 12 rows (periods 1-12 with non-zero power)
        assert len(det) == 12
        # CAB should be filtered accordingly
        assert len(cab) > 0

    def test_multiple_countries(self):
        """Test RDC generation with different countries."""
        rdc_data = {f"power_{i+1}": 100.0 for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-01-01")

        for country in ["ES", "PT"]:
            det, cab, uof = (
                rdc.generate_residual_demand_det_cab_and_participants_bidding_zone(
                    rdc_series,
                    date,
                    country,
                    market_periods_count=24,
                )
            )

            assert (uof[cols.CAT_BIDDING_ZONE] == country).all()

    def test_very_small_values(self):
        """Test RDC generation with very small values."""
        rdc_data = {f"power_{i+1}": 0.001 for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-01-01")

        det, cab, uof = (
            rdc.generate_residual_demand_det_cab_and_participants_bidding_zone(
                rdc_series,
                date,
                "ES",
                market_periods_count=24,
            )
        )

        assert not det.empty
        assert (det[cols.FLOAT_BID_POWER] == 0.001).all()

    def test_very_large_values(self):
        """Test RDC generation with very large values."""
        rdc_data = {f"power_{i+1}": 100000.0 for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-01-01")

        det, cab, uof = (
            rdc.generate_residual_demand_det_cab_and_participants_bidding_zone(
                rdc_series,
                date,
                "ES",
                market_periods_count=24,
            )
        )

        assert not det.empty
        assert (det[cols.FLOAT_BID_POWER] == 100000.0).all()

    def test_qh_market_periods_count_uses_96_periods(self):
        """A QH market_periods_count (96) must produce 96 periods (power_1..
        power_96), not just slice the first 24 like the old hardcoded range(24)
        did."""
        rdc_data = {f"power_{i+1}": 100.0 if i < 48 else -100.0 for i in range(96)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-01-01")

        det, cab, uof = (
            rdc.generate_residual_demand_det_cab_and_participants_bidding_zone(
                rdc_series,
                date,
                "ES",
                market_periods_count=96,
            )
        )

        assert sorted(det[cols.INT_PERIOD].unique().tolist()) == list(range(1, 97))
        assert (det[cols.FLOAT_BID_POWER] == 100.0).all()
        # Both the sell (positive, first 48 periods) and buy (negative, last 48
        # periods) synthetic orders must be present.
        assert set(det[cols.ID_ORDER].unique()) == {
            rdc.COD_OFERTA_RESIDUAL_DEMAND_V,
            rdc.COD_OFERTA_RESIDUAL_DEMAND_C,
        }


class TestGetClearingPricesDict:
    """Test suite for get_clearing_prices_dict function."""

    def test_extract_clearing_prices(self):
        """Test extracting clearing prices for a single country."""
        clearing_prices_df = pd.DataFrame(
            {
                cols.CAT_BIDDING_ZONE: ["ES", "ES", "PT", "PT"],
                cols.INT_PERIOD: [1, 2, 1, 2],
                cols.FLOAT_CLEARED_PRICE: [40.0, 45.0, 38.0, 43.0],
            }
        )

        results = {"clearing_prices": clearing_prices_df}

        prices_dict_ES = rdc.get_clearing_prices_dict(results, "ES")

        assert prices_dict_ES["price_1"] == 40.0
        assert prices_dict_ES["price_2"] == 45.0
        assert len(prices_dict_ES) == 2

        prices_dict_PT = rdc.get_clearing_prices_dict(results, "PT")

        assert prices_dict_PT["price_1"] == 38.0
        assert prices_dict_PT["price_2"] == 43.0
        assert len(prices_dict_PT) == 2

    def test_empty_country_result(self):
        """Test extraction when country has no data."""
        clearing_prices_df = pd.DataFrame(
            {
                cols.CAT_BIDDING_ZONE: ["ES", "PT"],
                cols.INT_PERIOD: [1, 1],
                cols.FLOAT_CLEARED_PRICE: [40.0, 38.0],
            }
        )

        results = {"clearing_prices": clearing_prices_df}

        prices_dict = rdc.get_clearing_prices_dict(results, "FR")

        assert len(prices_dict) == 0
