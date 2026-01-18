"""
Tests for mibel_simulator.clear_mibel_with_price_curve module.

Tests the market clearing logic for different order types with price curves.
"""

import numpy as np
import pandas as pd
import pytest
from mibel_simulator import columns as cols
from mibel_simulator.clear_mibel_with_price_curve import (
    calculate_cleared_energy_from_SCOs,
    calculate_cleared_energy_from_exclusive_block_order_groups,
    get_cleared_energy_from_non_exclusive_block_order,
    get_cleared_energy_from_SCO,
)


class TestGetClearedEnergyFromNonExclusiveBlockOrder:
    """Test suite for get_cleared_energy_from_non_exclusive_block_order function."""

    @pytest.fixture
    def block_order_data(self):
        """Single block order with multiple periods."""
        return pd.DataFrame(
            {
                cols.FLOAT_BID_PRICE: [34.0, 34.0, 34.0],
                cols.FLOAT_BID_POWER: [100.0, 110.0, 120.0],
            }
        )

    def test_block_clears_when_average_price_above_bid(self, block_order_data):
        """Test block order clears when weighted average price >= bid price."""
        block_order_data = block_order_data.copy()
        block_order_data[cols.FLOAT_CLEARED_PRICE] = [35.0, 36.0, 37.0]
        result = get_cleared_energy_from_non_exclusive_block_order(block_order_data)

        # Average price = (35*100 + 36*110 + 37*120) / (100+110+120)
        # = (3500 + 3960 + 4440) / 330 = 11900 / 330 = 36.06
        # 36.06 >= 34.0, so all cleared at full power
        expected = pd.Series([100.0, 110.0, 120.0], dtype=float)
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_block_not_clears_when_average_price_below_bid(self, block_order_data):
        """Test block order doesn't clear when weighted average price < bid price."""
        block_order_data = block_order_data.copy()
        block_order_data[cols.FLOAT_CLEARED_PRICE] = [30.0, 31.0, 32.0]

        result = get_cleared_energy_from_non_exclusive_block_order(block_order_data)

        # Let me recalculate: (3000 + 3410 + 3840) / 330 = 10250 / 330 = 31.06
        # 31.06 < 33.0, so not cleared (all zeros)
        expected = pd.Series([0, 0, 0], dtype="int64")
        pd.testing.assert_series_equal(result, expected, check_names=False)

        block_order_data[cols.FLOAT_CLEARED_PRICE] = [30.0, 31.0, 35.0]

        result = get_cleared_energy_from_non_exclusive_block_order(block_order_data)

        # Let me recalculate: (3000 + 3410 + 4200) / 330 = 10610 / 330 = 32.15
        # 32.15 < 33.0, so not cleared (all zeros)
        expected = pd.Series([0, 0, 0], dtype="int64")
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_preserves_index(self, block_order_data):
        """Test that function preserves the original index."""
        block_order_data = block_order_data.copy()
        block_order_data[cols.FLOAT_CLEARED_PRICE] = [35.0, 36.0, 37.0]
        block_order_data.index = [10, 20, 30]

        result = get_cleared_energy_from_non_exclusive_block_order(block_order_data)

        assert list(result.index) == [10, 20, 30]


class TestGetClearedEnergyFromSCO:
    """Test suite for get_cleared_energy_from_SCO function."""

    @pytest.fixture
    def sco_order(self):
        """SCO order that clears."""
        return pd.DataFrame(
            {
                cols.INT_PERIODO: [1, 2, 3],
                # cols.FLOAT_CLEARED_PRICE: [40.0, 41.0, 42.0],
                cols.FLOAT_BID_PRICE: [35.0, 35.0, 35.0],
                cols.FLOAT_BID_POWER: [100.0, 110.0, 120.0],
                cols.FLOAT_MAV: [50.0, 50.0, 50.0],
                cols.FLOAT_MIC: [1000.0, 1000.0, 1000.0],
                cols.ID_ORDER: ["SCO_ORDER", "SCO_ORDER", "SCO_ORDER"],
            }
        )

    def test_sco_clears_when_collection_sufficient(self, sco_order):
        """Test SCO clears when collection rights >= expected."""
        sco_order = sco_order.copy()
        sco_order[cols.FLOAT_CLEARED_PRICE] = [40.0, 41.0, 42.0]
        result = get_cleared_energy_from_SCO(sco_order)

        # Cleared energy where price >= bid: [100, 110, 120]
        # Collection = 100*40 + 110*41 + 120*42 = 4000 + 4510 + 5040 = 13550
        # Expected = 100*35 + 110*35 + 120*35 + 1000 = 8050 + 1000 = 9050
        # 13550 >= 9050, so clears
        expected = pd.Series([100.0, 110.0, 120.0], dtype=float)
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_sco_not_clears_when_collection_insufficient(self, sco_order):
        """Test SCO doesn't clear when collection rights < expected."""
        sco_order = sco_order.copy()
        sco_order[cols.FLOAT_CLEARED_PRICE] = [34.0, 34.0, 34.0]
        result = get_cleared_energy_from_SCO(sco_order)

        # All zeros when not clearing
        expected = pd.Series([0.0, 0.0, 0.0], dtype=float)
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_sco_not_clears_partially(self, sco_order):
        """Test SCO doesn't clear when collection rights < expected."""
        sco_order = sco_order.copy()
        sco_order[cols.FLOAT_CLEARED_PRICE] = [40.0, 41.0, 30.0]
        result = get_cleared_energy_from_SCO(sco_order)

        # Collection = 100*40 + 110*41 + 50*30 = 4000 + 4510 + 1500 = 10010
        # Expected = 100*35 + 110*35 + 50*35 + 1000 = 10100
        # 10010 < 10100, so doesn't clear

        # But it could not clear the last period, and it would clear the first two
        # Collection = 100*40 + 110*41 = 4000 + 4510 = 8510
        # Expected = 100*35 + 110*35 + 1000 = 8350
        # 8510 >= 8350, so first two periods would clear
        # But we don't want it to partially clear, not resepecting MAV

        # All zeros when not clearing
        expected = pd.Series([0.0, 0.0, 0.0], dtype=float)
        pd.testing.assert_series_equal(result, expected, check_names=False)


class TestGetClearedEnergyFromExclusiveBlockOrderGroups:
    """Test suite for get_cleared_energy_from_exclusive_block_order_groups function."""

    @pytest.fixture
    def block_group_data(self):
        """Exclusive block order group with multiple blocks."""
        return pd.DataFrame(
            {
                cols.ID_ORDER: ["BLOCK1", "BLOCK1", "BLOCK2", "BLOCK2"],
                cols.INT_NUM_BLOQ: [1, 1, 2, 2],
                cols.INT_NUM_GRUPO_EXCL: [1, 1, 1, 1],
                cols.FLOAT_CLEARED_PRICE: [35.0, 40.0, 30.0, 35.0],
                cols.FLOAT_BID_PRICE: [34.0, 34.0, 34.0, 34.0],
                cols.FLOAT_BID_POWER: [100.0, 110.0, 80.0, 90.0],
            }
        )

    def test_clears_only_highest_clearing_block(self, block_group_data):
        """Test only the block with highest clearing price clears."""
        result = calculate_cleared_energy_from_exclusive_block_order_groups(
            block_group_data
        )
        expected = pd.Series([100.0, 110.0, 0.0, 0.0], dtype=float)
        pd.testing.assert_series_equal(result, expected, check_names=False)


class TestCalculateClearedEnergyFromNonExclusiveBlockOrders:
    """Test suite for calculate_cleared_energy_from_non_exclusive_block_orders function."""

    @pytest.fixture
    def multiple_blocks(self):
        """Multiple non-exclusive block orders."""
        return pd.DataFrame(
            {
                cols.ID_ORDER: ["BLOCK1", "BLOCK1", "BLOCK1", "BLOCK2", "BLOCK2"],
                cols.INT_NUM_BLOQ: [1, 1, 1, 2, 2],
                cols.INT_NUM_GRUPO_EXCL: [0, 0, 0, 0, 0],
                cols.FLOAT_CLEARED_PRICE: [35.0, 36.0, 37.0, 30.0, 35.0],
                cols.FLOAT_BID_PRICE: [34.0, 34.0, 34.0, 33.0, 33.0],
                cols.FLOAT_BID_POWER: [100.0, 110.0, 120.0, 80.0, 90.0],
                cols.CAT_BUY_SELL: ["V", "V", "V", "V", "V"],
                cols.CAT_ORDER_TYPE: ["C01", "C01", "C01", "C01", "C01"],
            }
        )

    def test_clears_all_matching_blocks(self, multiple_blocks):
        """Test that all matching block orders are cleared."""
        result = calculate_cleared_energy_from_exclusive_block_order_groups(
            multiple_blocks
        )
        expected = pd.Series([100.0, 110.0, 120.0, 0.0, 0.0], dtype=float)
        pd.testing.assert_series_equal(result, expected, check_names=False)


class TestCalculateClearedEnergyFromSCOs:
    """Test suite for calculate_cleared_energy_from_SCOs function."""

    @pytest.fixture
    def multiple_scos(self):
        """Multiple SCO orders."""
        return pd.DataFrame(
            {
                cols.ID_ORDER: ["SCO1", "SCO1", "SCO1", "SCO2", "SCO2"],
                cols.INT_PERIODO: [1, 2, 3, 1, 2],
                cols.FLOAT_CLEARED_PRICE: [40.0, 41.0, 42.0, 39.0, 40.0],
                cols.FLOAT_BID_PRICE: [35.0, 35.0, 35.0, 38.0, 38.0],
                cols.FLOAT_BID_POWER: [100.0, 110.0, 120.0, 80.0, 90.0],
                cols.FLOAT_MAV: [50.0, 50.0, 50.0, 40.0, 40.0],
                cols.FLOAT_MIC: [1000.0, 1000.0, 1000.0, 2000.0, 2000.0],
                cols.CAT_BUY_SELL: ["V", "V", "V", "V", "V"],
                cols.CAT_ORDER_TYPE: ["C02", "C02", "C02", "C02", "C02"],
            }
        )

    def test_processes_all_scos(self, multiple_scos):
        """Test that all SCO orders are processed."""
        result = calculate_cleared_energy_from_SCOs(multiple_scos)
        expected = pd.Series([100.0, 110.0, 120.0, 0.0, 0.0], dtype=float)
        pd.testing.assert_series_equal(result, expected, check_names=False)
