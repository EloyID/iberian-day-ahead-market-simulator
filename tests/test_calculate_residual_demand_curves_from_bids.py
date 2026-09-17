"""
Tests for iberian_day_ahead_market_simulator.calculate_residual_demand_curves_from_bids module.

Tests the price curve formatting and residual demand calculation functions.
"""

import numpy as np
import pandas as pd
import pytest

from iberian_day_ahead_market_simulator import columns as cols
from iberian_day_ahead_market_simulator.calculate_residual_demand_curves_from_bids import (
    calculate_complex_residual_demand_I_without_market_split,
    calculate_complex_residual_demand_II_with_market_split,
    calculate_only_simple_submitted_relaxed_residual_demand,
    calculate_submitted_relaxed_residual_demand,
    format_price_curves,
    substract_reference_curve_from_all_curves,
)
from iberian_day_ahead_market_simulator.const import PORTUGAL_ZONE, SPAIN_ZONE


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

        with pytest.raises(ValueError, match="it must have length in"):
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

    @pytest.mark.parametrize("qh_length", [92, 96, 100])
    def test_1d_array_accepts_qh_lengths(self, qh_length):
        """QH markets can have 92, 96 or 100 periods; the length check must accept
        those too, not just the hourly 23/24/25 options."""
        price_curve = np.arange(qh_length)
        result = format_price_curves(price_curve)

        assert result.shape == (1, qh_length)
        np.testing.assert_array_equal(result[0], price_curve)

    def test_2d_array_accepts_qh_length(self):
        """A 2D array of QH-length price curves is also accepted."""
        price_curves = np.random.rand(5, 96)
        result = format_price_curves(price_curves)

        assert result.shape == (5, 96)
        np.testing.assert_array_equal(result, price_curves)


class TestSubstractReferenceCurveFromAllCurves:
    """Test suite for substract_reference_curve_from_all_curves function."""

    @pytest.fixture
    def curves_df(self):
        return pd.DataFrame(
            {
                "price_1": [10.0, 20.0, 30.0],
                "price_2": [11.0, 21.0, 31.0],
                "power_1": [100.0, 150.0, 200.0],
                "power_2": [110.0, 160.0, 210.0],
            }
        )

    def test_subtracts_reference_row_from_power_columns_only(self, curves_df):
        """Only the power columns get the reference row subtracted; price
        columns must be left untouched."""
        result = substract_reference_curve_from_all_curves(
            curves_df, reference_curve_index=1, rdc_power_columns=["power_1", "power_2"]
        )

        pd.testing.assert_series_equal(result["price_1"], curves_df["price_1"])
        pd.testing.assert_series_equal(result["price_2"], curves_df["price_2"])
        # Reference row (index 1: power 150, 160) subtracted from every row.
        expected_power_1 = pd.Series([-50.0, 0.0, 50.0], name="power_1")
        expected_power_2 = pd.Series([-50.0, 0.0, 50.0], name="power_2")
        pd.testing.assert_series_equal(result["power_1"], expected_power_1)
        pd.testing.assert_series_equal(result["power_2"], expected_power_2)

    def test_works_with_a_single_qh_power_column(self, curves_df):
        """The power-column list is caller-supplied (it used to be a hardcoded
        24-hour constant), so a differently-sized/named list of columns, as would
        be used for QH curves, must work too."""
        df = curves_df.rename(columns={"power_1": "power_only"}).drop(
            columns=["power_2"]
        )
        result = substract_reference_curve_from_all_curves(
            df, reference_curve_index=0, rdc_power_columns=["power_only"]
        )

        expected = pd.Series([0.0, 50.0, 100.0], name="power_only")
        pd.testing.assert_series_equal(result["power_only"], expected)


class TestCalculateOnlySimpleSubmittedRelaxedResidualDemand:
    """Test suite for calculate_only_simple_submitted_relaxed_residual_demand function."""

    @pytest.fixture
    def det_cab_simple_bids(self):
        """Fixture with simple bids only."""
        return pd.DataFrame(
            {
                cols.INT_PERIOD: [1, 1, 2, 2],
                cols.CAT_BUY_SELL: ["C", "V", "C", "V"],
                cols.CAT_ORDER_TYPE: ["S", "S", "S", "S"],
                cols.ID_UNIDAD: ["UNIT1", "UNIT2", "UNIT1", "UNIT2"],
                "float_cleared_power_as_simple_bid": [100.0, 50.0, 120.0, 60.0],
                cols.FLOAT_CLEARED_POWER: [0.0, 0.0, 0.0, 0.0],  # Ignored in this test
            }
        )

    @pytest.fixture
    def det_cab_with_complex_bids(self):
        """Fixture with simple and complex bids."""
        return pd.DataFrame(
            {
                cols.INT_PERIOD: [1, 1, 1, 2, 2],
                cols.CAT_BUY_SELL: ["C", "V", "V", "C", "V"],
                cols.CAT_ORDER_TYPE: ["S", "S", "C01", "S", "C01"],
                cols.ID_UNIDAD: ["UNIT1", "UNIT2", "UNIT3", "UNIT1", "UNIT3"],
                "float_cleared_power_as_simple_bid": [100.0, 50.0, 30.0, 120.0, 40.0],
                cols.FLOAT_CLEARED_POWER: [0.0, 0.0, 0.0, 0.0, 0.0],  # Ignored
            }
        )

    @pytest.fixture
    def det_cab_with_france(self):
        """Fixture with France exchanges."""
        return pd.DataFrame(
            {
                # fmt: off
                cols.INT_PERIOD: [1, 1, 1, 2, 2, 2],
                cols.CAT_BUY_SELL: ["C", "V", "V", "C", "V", "C"],
                cols.CAT_ORDER_TYPE: ["S", "S", "S", "S", "S", "S"],
                cols.ID_UNIDAD: ["UNIT1", "UNIT2", "MIEU", "UNIT1", "UNIT2", "MIEU"],
                "float_cleared_power_as_simple_bid": [100.0, 50.0, 20.0, 120.0, 60.0, 25.0],
                cols.FLOAT_CLEARED_POWER: [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                # fmt: on
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


class TestCalculateSubmittedRelaxedResidualDemand:
    """Test suite for calculate_submitted_relaxed_residual_demand function."""

    @pytest.fixture
    def det_cab_all_bids(self):
        """Fixture with all types of bids."""
        return pd.DataFrame(
            {
                # fmt: off
                cols.INT_PERIOD: [1, 1, 1, 2, 2, 2],
                cols.CAT_BUY_SELL: ["C", "V", "V", "C", "V", "V"],
                cols.CAT_ORDER_TYPE: ["S", "S", "C01", "S", "S", "C01"],
                cols.ID_UNIDAD: ["UNIT1", "UNIT2", "UNIT3", "UNIT1", "UNIT2", "UNIT3"],
                "float_cleared_power_as_simple_bid": [100.0, 50.0, 30.0, 120.0, 60.0, 40.0],
                cols.FLOAT_CLEARED_POWER: [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                # fmt: on
            }
        )

    @pytest.fixture
    def det_cab_with_france_all(self):
        """Fixture with France exchanges and all bid types."""
        return pd.DataFrame(
            {
                # fmt: off
                cols.INT_PERIOD: [1, 1, 1, 1, 2, 2, 2],
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
                "float_cleared_power_as_simple_bid": [100.0, 50.0, 30.0, 20.0, 120.0, 40.0, 25.0],
                cols.FLOAT_CLEARED_POWER: [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                # fmt: on
            }
        )

    def test_includes_all_bid_types(self, det_cab_all_bids):
        """Test that all bid types are included."""
        result = calculate_submitted_relaxed_residual_demand(det_cab_all_bids)

        assert isinstance(result, pd.Series)
        # Period 1: 100 - (50 + 30) = 20
        assert result.loc[1] == 20.0
        # Period 2: 120 - (60 + 40) = 20
        assert result.loc[2] == 20.0

    def test_france_exchanges_included(self, det_cab_with_france_all):
        """Test France exchanges are included."""
        result = calculate_submitted_relaxed_residual_demand(det_cab_with_france_all)

        # Period 1: Buy(100) - Sell(50+30+20) = 0
        assert result.loc[1] == pytest.approx(0.0)
        # Period 2: Buy(120+25) - Sell(40) = 105
        assert result.loc[2] == pytest.approx(105.0)


@pytest.fixture
def det_cab_two_countries():
    """Fixture with Spain and Portugal bids."""
    return pd.DataFrame(
        {
            cols.INT_PERIOD: [1, 1, 1, 1, 2, 2, 2, 2],
            cols.CAT_BUY_SELL: ["C", "V", "C", "V", "C", "V", "C", "V"],
            cols.CAT_BIDDING_ZONE: [
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
def capacidad_inter_pt():
    """Fixture with PT interconnection capacities."""
    return pd.DataFrame(
        {
            cols.INT_PERIOD: [1, 2],
            cols.FLOAT_IMPORT_CAPACITY: [100.0, 100.0],
            cols.FLOAT_EXPORT_CAPACITY: [150.0, 150.0],
        }
    )


@pytest.fixture
def det_cab_import_saturated():
    """Fixture with import saturation scenario."""
    return pd.DataFrame(
        {
            cols.INT_PERIOD: [1, 1, 1, 1, 2, 2, 2, 2],
            cols.CAT_BUY_SELL: ["C", "V", "C", "V", "C", "V", "C", "V"],
            cols.CAT_BIDDING_ZONE: [
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
def det_cab_export_saturated():
    """Fixture with export saturation scenario."""
    return pd.DataFrame(
        {
            cols.INT_PERIOD: [1, 1, 1, 1, 2, 2, 2, 2],
            cols.CAT_BUY_SELL: ["C", "V", "C", "V", "C", "V", "C", "V"],
            cols.CAT_BIDDING_ZONE: [
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


class TestCalculateComplexResidualDemandIWithMarketSplit:
    """Test suite for calculate_complex_residual_demand_II_with_market_split function."""

    def test_no_saturation(self, det_cab_two_countries):
        """Test residual demand without saturation."""
        result = calculate_complex_residual_demand_I_without_market_split(
            det_cab_two_countries
        )
        print(det_cab_two_countries)
        print(result)
        assert isinstance(result, pd.Series)
        assert len(result) == 2
        assert result.loc[1] == pytest.approx(70.0)
        assert result.loc[2] == pytest.approx(85.0)

    def test_import_saturation(self, det_cab_import_saturated):
        """Test residual demand with import saturation."""
        result = calculate_complex_residual_demand_I_without_market_split(
            det_cab_import_saturated
        )

        # PT residual = 30 - 150 = -120 (< -100, so import saturated)
        # Spain: 200 - 50 + -120 = 30 (We ignore the saturation here)
        assert result.loc[1] == pytest.approx(30.0)
        assert result.loc[2] == pytest.approx(50.0)

    def test_export_saturation(self, det_cab_export_saturated):
        """Test residual demand with export saturation."""
        result = calculate_complex_residual_demand_I_without_market_split(
            det_cab_export_saturated
        )

        # PT residual = 250 - 50 = 200 (> export cap 150)
        # Spain saturated: 200 + 200 - 50 = 350 (We ignore the saturation here)
        assert result.loc[1] == pytest.approx(350.0)
        assert result.loc[2] == pytest.approx(50.0)

    def test_series_index(self, det_cab_two_countries):
        """Test that result has correct name and index."""
        result = calculate_complex_residual_demand_I_without_market_split(
            det_cab_two_countries
        )

        assert list(result.index) == [1, 2]


class TestCalculateComplexResidualDemandIIWithMarketSplit:
    """Test suite for calculate_complex_residual_demand_II_with_market_split function."""

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
        # Spain saturated: 200 - 50 - 100 = 50
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

        assert result.name == "complex_residual_demand_II_with_market_split_curves"
        assert list(result.index) == [1, 2]
