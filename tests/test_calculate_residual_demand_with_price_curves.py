"""
Tests for mibel_simulator.calculate_residual_demand_with_price_curves module.

Tests the price curve formatting and residual demand calculation functions.
"""

import numpy as np
import pandas as pd
import pytest
from mibel_simulator import columns as cols
from mibel_simulator.calculate_residual_demand_with_price_curves import (
    format_price_curves,
    calculate_complex_residual_demand_II_with_market_split,
    calculate_only_simple_submitted_relaxed_residual_demand,
    calculate_complex_residual_demand_I_without_market_split,
)
from mibel_simulator.const import SPAIN_ZONE, PORTUGAL_ZONE


class TestFormatPriceCurves:
    """Test suite for format_price_curves function."""

    def test_1d_array_with_24_elements(self):
        """Test formatting of 1D array with 24 elements."""
        price_curve = np.arange(24)
        result = format_price_curves(price_curve)

        assert result.ndim == 2
        assert result.shape == (1, 24)
        np.testing.assert_array_equal(result[0], price_curve)

    def test_2d_array_valid_shape(self):
        """Test formatting of 2D array with valid shape."""
        price_curves = np.random.rand(5, 24)
        result = format_price_curves(price_curves)

        assert result.ndim == 2
        assert result.shape == (5, 24)
        np.testing.assert_array_equal(result, price_curves)

    def test_1d_array_wrong_length_raises(self):
        """Test that 1D array with wrong length raises ValueError."""
        price_curve = np.arange(20)  # Wrong length

        with pytest.raises(ValueError, match="must have length 24"):
            format_price_curves(price_curve)

    def test_2d_array_wrong_shape_raises(self):
        """Test that 2D array with wrong shape raises ValueError."""
        price_curves = np.random.rand(5, 20)  # Wrong number of columns

        with pytest.raises(ValueError, match="must have shape"):
            format_price_curves(price_curves)

    def test_3d_array_raises(self):
        """Test that 3D array raises ValueError."""
        price_curves = np.random.rand(5, 24, 2)

        with pytest.raises(ValueError, match="must be either 1D or 2D"):
            format_price_curves(price_curves)

    def test_empty_array_raises(self):
        """Test that empty array raises ValueError."""
        price_curve = np.array([])

        with pytest.raises(ValueError):
            format_price_curves(price_curve)


class TestCalculateOnlySimpleSubmittedRelaxedResidualDemand:
    """Test suite for calculate_only_simple_submitted_relaxed_residual_demand function."""

    @pytest.fixture
    def det_cab_simple_bids(self):
        """Fixture with simple bids only."""
        return pd.DataFrame(
            {
                cols.INT_PERIODO: [1, 1, 2, 2],
                cols.CAT_BUY_SELL: ["C", "V", "C", "V"],
                cols.CAT_ORDER_TYPE: ["S", "S", "S", "S"],
                cols.ID_UNIDAD: ["UNIT1", "UNIT2", "UNIT1", "UNIT2"],
                cols.FLOAT_CLEARED_POWER: [100.0, 50.0, 120.0, 60.0],
            }
        )

    @pytest.fixture
    def det_cab_with_complex_bids(self):
        """Fixture with simple and complex bids."""
        return pd.DataFrame(
            {
                cols.INT_PERIODO: [1, 1, 1, 2, 2],
                cols.CAT_BUY_SELL: ["C", "V", "V", "C", "V"],
                cols.CAT_ORDER_TYPE: ["S", "S", "C01", "S", "C01"],
                cols.ID_UNIDAD: ["UNIT1", "UNIT2", "UNIT3", "UNIT1", "UNIT3"],
                cols.FLOAT_CLEARED_POWER: [100.0, 50.0, 30.0, 120.0, 40.0],
            }
        )

    @pytest.fixture
    def det_cab_with_france(self):
        """Fixture with France exchanges."""
        return pd.DataFrame(
            {
                cols.INT_PERIODO: [1, 1, 1, 2, 2, 2],
                cols.CAT_BUY_SELL: ["C", "V", "V", "C", "V", "C"],
                cols.CAT_ORDER_TYPE: ["S", "S", "S", "S", "S", "S"],
                cols.ID_UNIDAD: ["UNIT1", "UNIT2", "MIEU", "UNIT1", "UNIT2", "MIEU"],
                cols.FLOAT_CLEARED_POWER: [100.0, 50.0, 20.0, 120.0, 60.0, 25.0],
            }
        )

    def test_simple_bids_only(self, det_cab_simple_bids):
        """Test residual demand with simple bids only."""
        result = calculate_only_simple_submitted_relaxed_residual_demand(
            det_cab_simple_bids
        )

        assert isinstance(result, pd.Series)
        assert len(result) == 2
        # Period 1: 100 - 50 = 50
        assert result.loc[1] == 50.0
        # Period 2: 120 - 60 = 60
        assert result.loc[2] == 60.0

    def test_excludes_complex_bids(self, det_cab_with_complex_bids):
        """Test that complex bids are excluded from calculation."""
        result = calculate_only_simple_submitted_relaxed_residual_demand(
            det_cab_with_complex_bids
        )

        assert isinstance(result, pd.Series)
        # Period 1: 100 - 50 = 50 (C01 bid excluded)
        assert result.loc[1] == 50.0
        # Period 2: 120 - 0 = 120 (C01 bid excluded)
        assert result.loc[2] == 120.0

    def test_france_exchanges_as_export(self, det_cab_with_france):
        """Test France exchanges (sell = negative export)."""
        result = calculate_only_simple_submitted_relaxed_residual_demand(
            det_cab_with_france
        )

        # Period 1: Buy(100) - Sell(70)  = 30
        assert result.loc[1] == pytest.approx(30.0)
        # Period 2: Buy(145) - Sell(60) = 85
        assert result.loc[2] == pytest.approx(85.0)


class TestCalculateResidualDemandIWithoutSaturationHourly:
    """Test suite for calculate_complex_residual_demand_I_without_market_split function."""

    @pytest.fixture
    def det_cab_all_bids(self):
        """Fixture with all types of bids."""
        return pd.DataFrame(
            {
                cols.INT_PERIODO: [1, 1, 1, 2, 2, 2],
                cols.CAT_BUY_SELL: ["C", "V", "V", "C", "V", "V"],
                cols.CAT_ORDER_TYPE: ["S", "S", "C01", "S", "S", "C01"],
                cols.ID_UNIDAD: ["UNIT1", "UNIT2", "UNIT3", "UNIT1", "UNIT2", "UNIT3"],
                cols.FLOAT_CLEARED_POWER: [100.0, 50.0, 30.0, 120.0, 60.0, 40.0],
            }
        )

    @pytest.fixture
    def det_cab_with_france_all(self):
        """Fixture with France exchanges and all bid types."""
        return pd.DataFrame(
            {
                cols.INT_PERIODO: [1, 1, 1, 1, 2, 2, 2],
                cols.CAT_BUY_SELL: ["C", "V", "V", "V", "C", "V", "C"],
                cols.CAT_ORDER_TYPE: ["S", "S", "C01", "S", "S", "C01", "S"],
                cols.ID_UNIDAD: [
                    "UNIT1",
                    "UNIT2",
                    "UNIT3",
                    "MIEU",
                    "UNIT1",
                    "UNIT3",
                    "MIEU",
                ],
                cols.FLOAT_CLEARED_POWER: [100.0, 50.0, 30.0, 20.0, 120.0, 40.0, 25.0],
            }
        )

    def test_includes_all_bid_types(self, det_cab_all_bids):
        """Test that all bid types are included."""
        result = calculate_complex_residual_demand_I_without_market_split(
            det_cab_all_bids
        )

        assert isinstance(result, pd.Series)
        # Period 1: 100 - (50 + 30) = 20
        assert result.loc[1] == 20.0
        # Period 2: 120 - (60 + 40) = 20
        assert result.loc[2] == 20.0

    def test_france_exchanges_included(self, det_cab_with_france_all):
        """Test France exchanges are included."""
        result = calculate_complex_residual_demand_I_without_market_split(
            det_cab_with_france_all
        )

        # Period 1: Buy(100) - Sell(50+30+20) = 0
        assert result.loc[1] == pytest.approx(0.0)
        # Period 2: Buy(120+25) - Sell(40) = 105
        assert result.loc[2] == pytest.approx(105.0)


class TestCalculateResidualDemandWithSaturationHourly:
    """Test suite for calculate_residual_demand_with_saturation_hourly function."""

    @pytest.fixture
    def det_cab_two_countries(self):
        """Fixture with Spain and Portugal bids."""
        return pd.DataFrame(
            {
                cols.INT_PERIODO: [1, 1, 1, 1, 2, 2, 2, 2],
                cols.CAT_BUY_SELL: ["C", "V", "C", "V", "C", "V", "C", "V"],
                cols.CAT_PAIS: [
                    SPAIN_ZONE,
                    SPAIN_ZONE,
                    PORTUGAL_ZONE,
                    PORTUGAL_ZONE,
                    SPAIN_ZONE,
                    SPAIN_ZONE,
                    PORTUGAL_ZONE,
                    PORTUGAL_ZONE,
                ],
                cols.FLOAT_CLEARED_POWER: [
                    100.0,
                    50.0,
                    80.0,
                    60.0,
                    120.0,
                    70.0,
                    90.0,
                    55.0,
                ],
            }
        )

    @pytest.fixture
    def capacidad_inter_pt(self):
        """Fixture with PT interconnection capacities."""
        return pd.DataFrame(
            {
                cols.INT_PERIODO: [1, 2],
                cols.FLOAT_IMPORT_CAPACITY: [100.0, 100.0],
                cols.FLOAT_EXPORT_CAPACITY: [150.0, 150.0],
            }
        )

    @pytest.fixture
    def det_cab_import_saturated(self):
        """Fixture with import saturation scenario."""
        return pd.DataFrame(
            {
                cols.INT_PERIODO: [1, 1, 1, 1, 2, 2, 2, 2],
                cols.CAT_BUY_SELL: ["C", "V", "C", "V", "C", "V", "C", "V"],
                cols.CAT_PAIS: [
                    SPAIN_ZONE,
                    SPAIN_ZONE,
                    PORTUGAL_ZONE,
                    PORTUGAL_ZONE,
                    SPAIN_ZONE,
                    SPAIN_ZONE,
                    PORTUGAL_ZONE,
                    PORTUGAL_ZONE,
                ],
                cols.FLOAT_CLEARED_POWER: [
                    200.0,
                    50.0,
                    30.0,
                    150.0,  # PT demand = -120
                    100.0,
                    40.0,
                    50.0,
                    60.0,
                ],  # Filler for P2
            }
        )

    @pytest.fixture
    def det_cab_export_saturated(self):
        """Fixture with export saturation scenario."""
        return pd.DataFrame(
            {
                cols.INT_PERIODO: [1, 1, 1, 1, 2, 2, 2, 2],
                cols.CAT_BUY_SELL: ["C", "V", "C", "V", "C", "V", "C", "V"],
                cols.CAT_PAIS: [
                    SPAIN_ZONE,
                    SPAIN_ZONE,
                    PORTUGAL_ZONE,
                    PORTUGAL_ZONE,
                    SPAIN_ZONE,
                    SPAIN_ZONE,
                    PORTUGAL_ZONE,
                    PORTUGAL_ZONE,
                ],
                cols.FLOAT_CLEARED_POWER: [
                    200.0,
                    50.0,
                    250.0,
                    50.0,  # PT demand = 200
                    100.0,
                    40.0,
                    50.0,
                    60.0,
                ],  # Filler for P2
            }
        )

    def test_no_saturation(self, det_cab_two_countries, capacidad_inter_pt):
        """Test residual demand without saturation."""
        result = calculate_complex_residual_demand_II_with_market_split(
            det_cab_two_countries, capacidad_inter_pt
        )
        print(det_cab_two_countries)
        print(capacidad_inter_pt)
        print(result)
        assert isinstance(result, pd.Series)
        assert len(result) == 2
        assert result.loc[1] == pytest.approx(70.0)
        assert result.loc[2] == pytest.approx(85.0)

    def test_import_saturation(self, det_cab_import_saturated, capacidad_inter_pt):
        """Test residual demand with import saturation."""
        result = calculate_complex_residual_demand_II_with_market_split(
            det_cab_import_saturated, capacidad_inter_pt
        )

        # PT residual = 30 - 150 = -120 (< -100, so import saturated)
        # Spain saturated: 200 - 50 + 100 = 250
        assert result.loc[1] == pytest.approx(50.0)
        assert result.loc[2] == pytest.approx(50.0)

    def test_export_saturation(self, det_cab_export_saturated, capacidad_inter_pt):
        """Test residual demand with export saturation."""
        result = calculate_complex_residual_demand_II_with_market_split(
            det_cab_export_saturated, capacidad_inter_pt
        )

        # PT residual = 250 - 50 = 200 (> export cap 150)
        # Spain saturated: 200 + 150 - 50 = 300
        assert result.loc[1] == pytest.approx(300.0)
        assert result.loc[2] == pytest.approx(50.0)

    def test_series_name_and_index(self, det_cab_two_countries, capacidad_inter_pt):
        """Test that result has correct name and index."""
        result = calculate_complex_residual_demand_II_with_market_split(
            det_cab_two_countries, capacidad_inter_pt
        )

        assert result.name == "residual_demand_with_saturation_hourly"
        assert list(result.index) == [1, 2]
