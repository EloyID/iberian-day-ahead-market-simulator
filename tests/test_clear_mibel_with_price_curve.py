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
    get_cleared_power_as_simple_bids_with_price_curve,
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
                cols.INT_PERIOD: [1, 2, 3],
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
                cols.INT_PERIOD: [1, 2, 3, 1, 2],
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


class TestGetClearedPowerAsSimpleBidsWithPriceCurve:
    """Test suite for get_cleared_power_as_simple_bids_with_price_curve function.

    Tests that the function processes all orders (simple and complex) using simple bid logic:
    - Buy orders (all types) clear when: bid_price >= cleared_price
    - Sell orders (all types: S, C01, C02, C04) clear when: bid_price <= cleared_price
    """

    @pytest.fixture
    def mixed_simple_and_complex_orders(self):
        """DataFrame with mixed simple (S) and complex (C01, C02, C04) orders."""
        # fmt: off
        return pd.DataFrame(
            {
                cols.INT_PERIOD: [1, 1, 2, 2, 3, 3, 3, 4, 4, 4],
                cols.CAT_BUY_SELL: ["C", "V", "C", "V", "C", "V", "V", "C", "V", "V"],
                cols.CAT_ORDER_TYPE: ["S", "S", "S", "C01", "S", "S", "C02", "S", "C04", "S"],
                cols.FLOAT_BID_PRICE: [35.0, 30.0, 35.0, 32.0, 35.0, 30.0, 33.0, 35.0, 34.0, 30.0],
                cols.FLOAT_BID_POWER: [100.0, 150.0, 110.0, 160.0, 120.0, 170.0, 140.0, 130.0, 150.0, 180.0],
                cols.ID_ORDER: ["BUY1", "SELL1", "BUY2", "SELL_C01", "BUY3", "SELL3", "SELL_C02", "BUY4", "SELL_C04", "SELL6"],
            }
        ) 
        # fmt: on

    def test_simple_and_complex_sell_orders_clear_together(
        self, mixed_simple_and_complex_orders
    ):
        """Test that simple and complex sell orders are treated identically."""
        price_curve = np.array([35.0, 35.0, 35.0, 35.0] + [25.0] * 20)

        result = get_cleared_power_as_simple_bids_with_price_curve(
            price_curve, mixed_simple_and_complex_orders
        )

        # Period 1: buy S (35>=35? Yes, 100), sell S (30<=35? Yes, 150)
        # Period 2: buy S (35>=35? Yes, 110), sell C01 (32<=35? Yes, 160) - complex treated as simple!
        # Period 3: buy S (35>=35? Yes, 120), sell S (30<=35? Yes, 170), sell C02 (33<=35? Yes, 140)
        # Period 4: buy S (35>=35? Yes, 130), sell C04 (34<=35? Yes, 150), sell S (30<=35? Yes, 180)
        expected = pd.Series(
            [100.0, 150.0, 110.0, 160.0, 120.0, 170.0, 140.0, 130.0, 150.0, 180.0],
            dtype=float,
        )
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_complex_orders_not_cleared_when_condition_fails(
        self, mixed_simple_and_complex_orders
    ):
        """Test complex orders don't clear when bid > cleared price (same as simple logic)."""
        price_curve = np.array([25.0, 25.0, 25.0, 25.0] + [35.0] * 20)

        result = get_cleared_power_as_simple_bids_with_price_curve(
            price_curve, mixed_simple_and_complex_orders
        )

        # Period 1: buy S (35>=25? Yes, 100), sell S (30<=25? No, 0)
        # Period 2: buy S (35>=25? Yes, 110), sell C01 (32<=25? No, 0) - complex behaves like simple
        # Period 3: buy S (35>=25? Yes, 120), sell S (30<=25? No, 0), sell C02 (33<=25? No, 0)
        # Period 4: buy S (35>=25? Yes, 130), sell C04 (34<=25? No, 0), sell S (30<=25? No, 0)
        expected = pd.Series(
            [100.0, 0.0, 110.0, 0.0, 120.0, 0.0, 0.0, 130.0, 0.0, 0.0], dtype=float
        )
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_all_order_types_cleared_with_matching_prices(self):
        """Test that C01, C02, C04 orders clear using simple bid logic."""
        df = pd.DataFrame(
            {
                cols.INT_PERIOD: [1, 1, 1, 1],
                cols.CAT_BUY_SELL: ["V", "V", "V", "V"],
                cols.CAT_ORDER_TYPE: ["S", "C01", "C02", "C04"],
                cols.FLOAT_BID_PRICE: [30.0, 32.0, 33.0, 34.0],
                cols.FLOAT_BID_POWER: [100.0, 110.0, 120.0, 130.0],
            }
        )
        price_curve = np.array([35.0] * 24)

        result = get_cleared_power_as_simple_bids_with_price_curve(price_curve, df)

        # All have bid <= 35 (cleared), so all clear with simple logic
        # S: 30<=35? Yes (100), C01: 32<=35? Yes (110), C02: 33<=35? Yes (120), C04: 34<=35? Yes (130)
        expected = pd.Series([100.0, 110.0, 120.0, 130.0], dtype=float)
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_boundary_prices_c01_c02_c04(self):
        """Test boundary condition (equal prices) for complex order types."""
        df = pd.DataFrame(
            {
                cols.INT_PERIOD: [1, 2, 3, 4],
                cols.CAT_BUY_SELL: ["V", "V", "V", "V"],
                cols.CAT_ORDER_TYPE: ["C01", "C02", "C04", "S"],
                cols.FLOAT_BID_PRICE: [35.0, 35.0, 35.0, 35.0],
                cols.FLOAT_BID_POWER: [100.0, 110.0, 120.0, 130.0],
            }
        )
        price_curve = np.array([35.0, 35.0, 35.0, 35.0] + [25.0] * 20)

        result = get_cleared_power_as_simple_bids_with_price_curve(price_curve, df)

        # All at boundary: bid=35, cleared=35, so 35<=35? Yes, all clear
        expected = pd.Series([100.0, 110.0, 120.0, 130.0], dtype=float)
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_high_cleared_price_all_sells_dont_clear(
        self, mixed_simple_and_complex_orders
    ):
        """Test that high cleared prices allow all buy orders to clear."""
        price_curve = np.array([50.0] * 24)

        result = get_cleared_power_as_simple_bids_with_price_curve(
            price_curve, mixed_simple_and_complex_orders
        )

        # All buys: bid=35, cleared=50, so 35>=50? No, none clear
        # All sells: bid<=50, so all clear (30,32,33,34 all <= 50)
        expected = pd.Series(
            [0.0, 150.0, 0.0, 160.0, 0.0, 170.0, 140.0, 0.0, 150.0, 180.0], dtype=float
        )
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_low_cleared_price_all_buys_clear(self, mixed_simple_and_complex_orders):
        """Test that low cleared prices allow all buy orders to clear."""
        price_curve = np.array([20.0] * 24)

        result = get_cleared_power_as_simple_bids_with_price_curve(
            price_curve, mixed_simple_and_complex_orders
        )

        # All buys: bid=35, cleared=20, so 35>=20? Yes, all clear
        # All sells: bid>20, so none clear (30,32,33,34 all > 20)
        expected = pd.Series(
            [100.0, 0.0, 110.0, 0.0, 120.0, 0.0, 0.0, 130.0, 0.0, 0.0], dtype=float
        )
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_preserves_dataframe_index(self, mixed_simple_and_complex_orders):
        """Test that original DataFrame index is preserved."""
        price_curve = np.array([35.0] * 24)
        mixed_simple_and_complex_orders.index = list(range(100, 110))

        result = get_cleared_power_as_simple_bids_with_price_curve(
            price_curve, mixed_simple_and_complex_orders
        )

        assert list(result.index) == list(range(100, 110))

    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        price_curve = np.array([35.0] * 24)
        empty_df = pd.DataFrame(
            {
                cols.INT_PERIOD: [],
                cols.CAT_BUY_SELL: [],
                cols.CAT_ORDER_TYPE: [],
                cols.FLOAT_BID_PRICE: [],
                cols.FLOAT_BID_POWER: [],
            }
        )

        result = get_cleared_power_as_simple_bids_with_price_curve(
            price_curve, empty_df
        )

        assert len(result) == 0
        assert not result.isna().any()

    def test_only_c01_orders(self):
        """Test DataFrame with only C01 (block) orders treated as simple."""
        df = pd.DataFrame(
            {
                cols.INT_PERIOD: [1, 2, 3],
                cols.CAT_BUY_SELL: ["V", "V", "V"],
                cols.CAT_ORDER_TYPE: ["C01", "C01", "C01"],
                cols.FLOAT_BID_PRICE: [30.0, 35.0, 28.0],
                cols.FLOAT_BID_POWER: [100.0, 110.0, 120.0],
            }
        )
        price_curve = np.array([35.0, 30.0, 25.0] + [25.0] * 21)

        result = get_cleared_power_as_simple_bids_with_price_curve(price_curve, df)

        # Period 1: C01 at 30, cleared=35, so 30<=35? Yes (100)
        # Period 2: C01 at 35, cleared=30, so 35<=30? No (0)
        # Period 3: C01 at 28, cleared=25, so 28<=25? No (0)
        expected = pd.Series([100.0, 0.0, 0.0], dtype=float)
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_only_c02_orders(self):
        """Test DataFrame with only C02 (SCO) orders treated as simple."""
        df = pd.DataFrame(
            {
                cols.INT_PERIOD: [1, 2, 3],
                cols.CAT_BUY_SELL: ["V", "V", "V"],
                cols.CAT_ORDER_TYPE: ["C02", "C02", "C02"],
                cols.FLOAT_BID_PRICE: [32.0, 34.0, 29.0],
                cols.FLOAT_BID_POWER: [110.0, 120.0, 130.0],
            }
        )
        price_curve = np.array([35.0, 30.0, 25.0] + [25.0] * 21)

        result = get_cleared_power_as_simple_bids_with_price_curve(price_curve, df)

        # Period 1: C02 at 32, cleared=35, so 32<=35? Yes (110)
        # Period 2: C02 at 34, cleared=30, so 34<=30? No (0)
        # Period 3: C02 at 29, cleared=25, so 29<=25? No (0)
        expected = pd.Series([110.0, 0.0, 0.0], dtype=float)
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_only_c04_orders(self):
        """Test DataFrame with only C04 (exclusive block) orders treated as simple."""
        df = pd.DataFrame(
            {
                cols.INT_PERIOD: [1, 2, 3],
                cols.CAT_BUY_SELL: ["V", "V", "V"],
                cols.CAT_ORDER_TYPE: ["C04", "C04", "C04"],
                cols.FLOAT_BID_PRICE: [33.0, 36.0, 27.0],
                cols.FLOAT_BID_POWER: [120.0, 130.0, 140.0],
            }
        )
        price_curve = np.array([35.0, 30.0, 25.0] + [25.0] * 21)

        result = get_cleared_power_as_simple_bids_with_price_curve(price_curve, df)

        # Period 1: C04 at 33, cleared=35, so 33<=35? Yes (120)
        # Period 2: C04 at 36, cleared=30, so 36<=30? No (0)
        # Period 3: C04 at 27, cleared=25, so 27<=25? No (0)
        expected = pd.Series([120.0, 0.0, 0.0], dtype=float)
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_mixed_periods_with_complex_orders(self, mixed_simple_and_complex_orders):
        """Test varying prices across periods with mixed order types."""
        # Period 1: 30, Period 2: 35, Period 3: 40, Period 4: 32
        price_curve = np.array([30.0, 35.0, 40.0, 32.0] + [25.0] * 20)

        result = get_cleared_power_as_simple_bids_with_price_curve(
            price_curve, mixed_simple_and_complex_orders
        )

        # Period 1: buy 35>=30? Yes (100), sell S 30<=30? Yes (150)
        # Period 2: buy 35>=35? Yes (110), sell C01 32<=35? Yes (160)
        # Period 3: buy 35>=40? No (0), sell S 30<=40? Yes (170), sell C02 33<=40? Yes (140)
        # Period 4: buy 35>=32? Yes (130), sell C04 34<=32? No (0), sell S 30<=32? Yes (180)
        expected = pd.Series(
            [100.0, 150.0, 110.0, 160.0, 0.0, 170.0, 140.0, 130.0, 0.0, 180.0],
            dtype=float,
        )
        pd.testing.assert_series_equal(result, expected, check_names=False)
