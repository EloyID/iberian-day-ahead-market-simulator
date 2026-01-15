"""
Tests for mibel_simulator.residual_demand_curve module.

Tests the residual demand curve generation and profile creation functions.
"""

import numpy as np
import pandas as pd
import pytest
from mibel_simulator import columns as cols
from mibel_simulator import residual_demand_curve as rdc
from datetime import datetime


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

        expected_cols = [f"energy_{i+1}" for i in range(24)]
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


class TestGenerateResidualDemandDetCabAndUOFZone:
    """Test suite for generate_residual_demand_det_cab_and_uof_zone function."""

    def test_mixed_rdc_values(self):
        """Test RDC generation with mixed positive and negative values."""
        rdc_data = {f"energy_{i+1}": 100.0 if i < 12 else -100.0 for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-01-01")

        det, cab, uof = rdc.generate_residual_demand_det_cab_and_uof_zone(
            rdc_series, date, "ES"
        )

        assert not det.empty
        assert not cab.empty
        assert not uof.empty
        # Both positive and negative sell profiles should result in bids
        assert (det[cols.FLOAT_BID_POWER] > 0).all()
        assert len(det) > 0

    def test_zero_rdc_values(self):
        """Test RDC generation with zero values."""
        rdc_data = {f"energy_{i+1}": 0.0 for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-01-01")

        det, cab, uof = rdc.generate_residual_demand_det_cab_and_uof_zone(
            rdc_series, date, "ES"
        )

        assert det.empty  # No non-zero power
        assert cab.empty
        assert uof.empty

    def test_date_assignment(self):
        """Test that date is correctly assigned."""
        rdc_data = {f"energy_{i+1}": 100.0 for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-06-15")

        det, cab, uof = rdc.generate_residual_demand_det_cab_and_uof_zone(
            rdc_series, date, "ES"
        )

        assert (det[cols.DATE_SESION] == date).all()
        assert (cab[cols.DATE_SESION] == date).all()

    def test_country_assignment(self):
        """Test that country is correctly assigned."""
        rdc_data = {f"energy_{i+1}": 100.0 for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-01-01")

        det, cab, uof = rdc.generate_residual_demand_det_cab_and_uof_zone(
            rdc_series, date, "PT"
        )

        assert (uof[cols.CAT_PAIS] == "PT").all()

    def test_period_assignment(self):
        """Test that periods 1-24 are correctly assigned."""
        rdc_data = {f"energy_{i+1}": 100.0 for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-01-01")

        det, cab, uof = rdc.generate_residual_demand_det_cab_and_uof_zone(
            rdc_series, date, "ES"
        )

        assert set(det[cols.INT_PERIODO].unique()) == set(range(1, 25))

    def test_bid_prices_fixed_values(self):
        """Test that bid prices are assigned appropriately."""
        rdc_data = {f"energy_{i+1}": (100.0 if i < 12 else -100.0) for i in range(24)}
        rdc_series = pd.Series(rdc_data)
        date = pd.Timestamp("2024-01-01")

        det, cab, uof = rdc.generate_residual_demand_det_cab_and_uof_zone(
            rdc_series, date, "ES"
        )

        # Prices should be either -500 or 3500
        assert all(
            price in [-500.0, 3500.0] for price in det[cols.FLOAT_BID_PRICE].values
        )


class TestGetClearingPricesDict:
    """Test suite for get_clearing_prices_dict function."""

    def test_extract_clearing_prices(self):
        """Test extracting clearing prices for a single country."""
        clearing_prices_df = pd.DataFrame(
            {
                cols.CAT_PAIS: ["ES", "ES", "PT", "PT"],
                cols.INT_PERIODO: [1, 2, 1, 2],
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
                cols.CAT_PAIS: ["ES", "PT"],
                cols.INT_PERIODO: [1, 1],
                cols.FLOAT_CLEARED_PRICE: [40.0, 38.0],
            }
        )

        results = {"clearing_prices": clearing_prices_df}

        prices_dict = rdc.get_clearing_prices_dict(results, "FR")

        assert len(prices_dict) == 0
