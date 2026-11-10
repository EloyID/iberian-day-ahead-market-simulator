"""
Tests for the iberian_day_ahead_market_simulator.calculate_cleared_power_curves_by_order_type_from_bids
module.

The session of the conftest fixtures has one order of each type, alike in every one of
its 24 periods, so the cleared power of each type follows from its bid prices:

- at 20 EUR/MWh only the buy (bid at 100) clears: the simple sell is bid at 30, the block
  order at 40 and 60, the exclusive group at 35 and 45, and the scalable complex order,
  bid at 20, would not earn its 1,000 EUR minimum income;
- at 50 EUR/MWh every type clears: the simple sell's 100 MW, the block order's 50 MW block
  at 40 but not its block at 60, the scalable complex order's 80 MW, and the exclusive
  group's 60 MW block at 35, which earns more than its block at 45.
"""

import numpy as np
import pandas as pd
import pytest

from iberian_day_ahead_market_simulator import columns as cols
from iberian_day_ahead_market_simulator.calculate_cleared_power_curves_by_order_type_from_bids import (
    calculate_cleared_power_curves_by_order_type_from_bids,
    _sum_cleared_power_by_period_and_bid_type,
    CLEARED_POWER_BID_TYPES,
)
from iberian_day_ahead_market_simulator.const import (
    get_rdc_power_columns,
    get_rdc_price_columns,
)

LOW_PRICE_CURVE = np.full(24, 20.0)
HIGH_PRICE_CURVE = np.full(24, 50.0)

CLEARED_POWER_AT_LOW_PRICE_MW = {
    "cleared_power_buy": 400.0,
    "cleared_power_sell_simple": 0.0,
    "cleared_power_sell_C01": 0.0,
    "cleared_power_sell_C02": 0.0,
    "cleared_power_sell_C04": 0.0,
}
CLEARED_POWER_AT_HIGH_PRICE_MW = {
    "cleared_power_buy": 400.0,
    "cleared_power_sell_simple": 100.0,
    "cleared_power_sell_C01": 50.0,
    "cleared_power_sell_C02": 80.0,
    "cleared_power_sell_C04": 60.0,
}


class TestCalculateClearedPowerCurvesByOrderTypeFromBids:
    """Test suite for calculate_cleared_power_curves_by_order_type_from_bids function."""

    @pytest.fixture
    def cleared_power_curves_by_bid_type(
        self,
        hourly_session_det_dataframe,
        hourly_session_cab_dataframe,
        hourly_session_participants_bidding_zones_dataframe,
    ):
        return calculate_cleared_power_curves_by_order_type_from_bids(
            np.vstack([LOW_PRICE_CURVE, HIGH_PRICE_CURVE]),
            hourly_session_det_dataframe,
            hourly_session_cab_dataframe,
            participants_bidding_zones=hourly_session_participants_bidding_zones_dataframe,
        )

    def test_returns_one_set_of_curves_per_bid_type(
        self, cleared_power_curves_by_bid_type
    ):
        assert list(cleared_power_curves_by_bid_type) == list(CLEARED_POWER_BID_TYPES)

    def test_each_set_has_a_row_per_price_curve_and_a_column_pair_per_period(
        self, cleared_power_curves_by_bid_type
    ):
        for cleared_power_curves_df in cleared_power_curves_by_bid_type.values():
            assert len(cleared_power_curves_df) == 2
            assert list(cleared_power_curves_df.columns) == (
                get_rdc_price_columns(24) + get_rdc_power_columns(24)
            )

    def test_price_columns_are_the_price_curves(self, cleared_power_curves_by_bid_type):
        for cleared_power_curves_df in cleared_power_curves_by_bid_type.values():
            np.testing.assert_array_equal(
                cleared_power_curves_df[get_rdc_price_columns(24)].to_numpy(),
                np.vstack([LOW_PRICE_CURVE, HIGH_PRICE_CURVE]),
            )

    @pytest.mark.parametrize(
        "row, expected_cleared_power_mw",
        [(0, CLEARED_POWER_AT_LOW_PRICE_MW), (1, CLEARED_POWER_AT_HIGH_PRICE_MW)],
    )
    def test_cleared_power_of_each_bid_type(
        self, cleared_power_curves_by_bid_type, row, expected_cleared_power_mw
    ):
        for bid_type, curves_df in cleared_power_curves_by_bid_type.items():
            cleared_power = curves_df.loc[row, get_rdc_power_columns(24)]
            expected_cleared_power = [expected_cleared_power_mw[bid_type]] * 24
            assert cleared_power.tolist() == expected_cleared_power, bid_type

    def test_the_two_bidding_zones_are_summed_together(
        self, cleared_power_curves_by_bid_type
    ):
        """The block order's unit is in Portugal, every other one in Spain."""
        assert cleared_power_curves_by_bid_type["cleared_power_sell_C01"].loc[
            1, "power_1"
        ] == pytest.approx(50.0)

    def test_reference_price_curve_makes_every_curve_relative_to_it(
        self,
        hourly_session_det_dataframe,
        hourly_session_cab_dataframe,
        hourly_session_participants_bidding_zones_dataframe,
    ):
        participants_bidding_zones = hourly_session_participants_bidding_zones_dataframe
        cleared_power_curves_by_bid_type = (
            calculate_cleared_power_curves_by_order_type_from_bids(
                np.vstack([LOW_PRICE_CURVE, HIGH_PRICE_CURVE]),
                hourly_session_det_dataframe,
                hourly_session_cab_dataframe,
                participants_bidding_zones=participants_bidding_zones,
                reference_price_curve=LOW_PRICE_CURVE,
            )
        )

        for bid_type, curves_df in cleared_power_curves_by_bid_type.items():
            power_columns_df = curves_df[get_rdc_power_columns(24)]
            change_from_reference_mw = (
                CLEARED_POWER_AT_HIGH_PRICE_MW[bid_type]
                - CLEARED_POWER_AT_LOW_PRICE_MW[bid_type]
            )
            # the reference row is 0 MW, the other is its change from the reference
            assert power_columns_df.loc[0].tolist() == [0.0] * 24, bid_type
            assert power_columns_df.loc[1].tolist() == (
                [change_from_reference_mw] * 24
            ), bid_type

    def test_price_curves_of_another_length_raise(
        self,
        hourly_session_det_dataframe,
        hourly_session_cab_dataframe,
        hourly_session_participants_bidding_zones_dataframe,
    ):
        with pytest.raises(ValueError, match="price_curves cover 96 periods"):
            calculate_cleared_power_curves_by_order_type_from_bids(
                np.full(96, 50.0),
                hourly_session_det_dataframe,
                hourly_session_cab_dataframe,
                participants_bidding_zones=hourly_session_participants_bidding_zones_dataframe,
            )


class TestSumClearedPowerByPeriodAndBidType:
    """Test suite for _sum_cleared_power_by_period_and_bid_type function."""

    @pytest.fixture
    def det_cab(self):
        return pd.DataFrame(
            # fmt: off
            {
                cols.INT_PERIOD:            [1,     1,      2,      1,      1,      2,      1       ],
                cols.CAT_BUY_SELL:          ["C",   "V",    "C",    "V",    "V",    "V",    "V"     ],
                cols.CAT_ORDER_TYPE:        ["S",   "C01",  "S",    "S",    "S",    "C01",  "C04"   ],
                cols.CAT_BIDDING_ZONE:      ["ES",  "ES",   "ES",   "ES",   "PT",   "PT",   "ES"    ],
                cols.FLOAT_CLEARED_POWER:   [100.0, 10.0,   120.0,  50.0,   5.0,    30.0,   20.0    ],
            }
            # fmt: on
        )

    def test_returns_every_bid_type(self, det_cab):
        cleared_power_by_bid_type = _sum_cleared_power_by_period_and_bid_type(
            det_cab, 2
        )

        assert list(cleared_power_by_bid_type) == list(CLEARED_POWER_BID_TYPES)

    def test_buys_of_every_order_type_are_summed_together(self, det_cab):
        cleared_power_by_bid_type = _sum_cleared_power_by_period_and_bid_type(
            det_cab, 2
        )

        assert cleared_power_by_bid_type["cleared_power_buy"].tolist() == [100.0, 120.0]

    def test_sells_are_summed_by_order_type_over_both_zones(self, det_cab):
        cleared_power_by_bid_type = _sum_cleared_power_by_period_and_bid_type(
            det_cab, 2
        )

        # period 1: 50 MW in Spain and 5 MW in Portugal
        assert cleared_power_by_bid_type["cleared_power_sell_simple"].tolist() == [
            55.0,
            0.0,
        ]
        assert cleared_power_by_bid_type["cleared_power_sell_C01"].tolist() == [
            10.0,
            30.0,
        ]
        assert cleared_power_by_bid_type["cleared_power_sell_C04"].tolist() == [
            20.0,
            0.0,
        ]

    def test_a_bid_type_without_any_order_is_0_mw_in_every_period(self, det_cab):
        cleared_power_by_bid_type = _sum_cleared_power_by_period_and_bid_type(
            det_cab, 3
        )

        assert cleared_power_by_bid_type["cleared_power_sell_C02"].tolist() == [
            0.0,
            0.0,
            0.0,
        ]
        # every bid type covers the session's periods, also the one without orders
        assert all(
            cleared_power.index.tolist() == [1, 2, 3]
            for cleared_power in cleared_power_by_bid_type.values()
        )
