"""
Tests for iberian_day_ahead_market_simulator.calculate_residual_demand_curves_from_bid_curves.

This module computes the residual demand curve directly from the published bid
curves (curva_pbc), as opposed to re-clearing the market from the individual
bids (calculate_residual_demand_curves_from_bids) or running full simulations
(residual_demand_curve.calculate_residual_demand_curves).

The residual demand at price ``p`` for a period is defined as

    residual_demand(p) = buy_volume(price >= p) - sell_volume(price <= p)

because a buyer bidding at or above ``p`` is willing to buy at ``p``, and a
seller bidding at or below ``p`` is willing to sell at ``p``.

Tests marked ``xfail(strict=True)`` encode confirmed defects: they assert the
value the definition above requires, and will start failing (as XPASS) the
moment the defect is fixed, which is the signal to drop the marker.
"""

import pandas as pd
import pytest

from iberian_day_ahead_market_simulator import columns as cols
from iberian_day_ahead_market_simulator.const import MAX_BID_PRICE, MIN_BID_PRICE
from iberian_day_ahead_market_simulator.calculate_residual_demand_curves_from_bid_curves import (
    calculate_residual_demand,
    calculate_residual_demand_curves_from_bid_curves,
    calculated_curva_pbc_cleared_extended,
    format_curva_pbc_rdc,
)

DATE_SESION = pd.Timestamp("2026-02-18")

CURVA_PBC_COLUMNS = [
    cols.DATE_SESION,
    cols.INT_PERIOD,
    cols.CAT_BUY_SELL,
    cols.CAT_OFERTADA_CASADA,
    cols.FLOAT_BID_PRICE,
    cols.FLOAT_BID_POWER,
]


def _curva_pbc(rows: list[tuple]) -> pd.DataFrame:
    """Build a curva_pbc DataFrame from (period, buy_sell, O/C, price, power) tuples."""
    return pd.DataFrame(
        [(DATE_SESION, *row) for row in rows], columns=CURVA_PBC_COLUMNS
    )


def _residual_demand_by_price(rdc: pd.DataFrame, period: int) -> dict[float, float]:
    """Map each bid price of a period to its computed residual demand."""
    period_rdc = rdc.query(f"{cols.INT_PERIOD} == {period}")
    return dict(zip(period_rdc[cols.FLOAT_BID_PRICE], period_rdc["residual_demand"]))


class TestCalculatedCurvaPbcClearedExtended:
    """Test suite for calculated_curva_pbc_cleared_extended function."""

    @pytest.fixture
    def curva_pbc(self):
        """Cleared bids plus offered bids both inside and outside the cleared range.

        Cleared sells reach 20 EUR/MWh, so only the offered sell at 90 extends
        the curve; the offered sell at 10 is already inside the cleared range.
        Cleared buys bottom out at 20, so only the offered buy at 5 extends it.
        """
        return _curva_pbc(
            [
                (1, "V", "C", -500.0, 50.0),
                (1, "V", "C", 20.0, 30.0),
                (1, "V", "O", -500.0, 50.0),  # cleared
                (1, "V", "O", 10.0, 99.0),  # inside cleared range -> dropped
                (1, "V", "O", 20.0, 30.0),  # cleared
                (1, "V", "O", 90.0, 70.0),  # above clearing -> kept
                (1, "C", "C", 180.0, 60.0),
                (1, "C", "C", 20.0, 25.0),
                (1, "C", "O", 200.0, 99.0),  # inside cleared range -> dropped
                (1, "C", "O", 180.0, 60.0),  # cleared
                (1, "C", "O", 20.0, 25.0),  # cleared
                (1, "C", "O", 5.0, 40.0),  # below clearing -> kept
            ]
        )

    def test_keeps_offered_bids_outside_the_cleared_price_range(self, curva_pbc):
        extended = calculated_curva_pbc_cleared_extended(curva_pbc.copy())

        sell_prices = sorted(
            extended.query(f'{cols.CAT_BUY_SELL} == "V"')[cols.FLOAT_BID_PRICE]
        )
        buy_prices = sorted(
            extended.query(f'{cols.CAT_BUY_SELL} == "C"')[cols.FLOAT_BID_PRICE]
        )

        assert sell_prices == [-500.0, 20.0, 90.0]
        assert buy_prices == [5.0, 20.0, 180.0]

    def test_drops_the_ofertada_casada_column(self, curva_pbc):
        extended = calculated_curva_pbc_cleared_extended(curva_pbc.copy())
        assert cols.CAT_OFERTADA_CASADA not in extended.columns

    def test_cumsum_is_recomputed_over_the_whole_extended_curve(self, curva_pbc):
        """The extended curve must be cumulated as one curve per period, not
        separately for the cleared ('C') and offered ('O') halves."""
        extended = calculated_curva_pbc_cleared_extended(curva_pbc.copy())

        sells = extended.query(f'{cols.CAT_BUY_SELL} == "V"').sort_values(
            cols.FLOAT_BID_PRICE
        )
        buys = extended.query(f'{cols.CAT_BUY_SELL} == "C"').sort_values(
            cols.FLOAT_BID_PRICE, ascending=False
        )

        # sells cumulate from the cheapest up: 50, 50+30, 50+30+70
        assert sells[cols.FLOAT_BID_POWER_CUMSUM].tolist() == [50.0, 80.0, 150.0]
        # buys cumulate from the most expensive down: 60, 60+25, 60+25+40
        assert buys[cols.FLOAT_BID_POWER_CUMSUM].tolist() == [60.0, 85.0, 125.0]

    def test_does_not_mutate_the_caller_dataframe(self, curva_pbc):
        columns_before = list(curva_pbc.columns)
        calculated_curva_pbc_cleared_extended(curva_pbc)
        assert list(curva_pbc.columns) == columns_before


class TestCalculateResidualDemand:
    """Test suite for calculate_residual_demand function."""

    def test_buy_above_minus_sell_below_at_every_bid_price(self):
        """Buy and sell prices strictly interleave, so every row can see the
        opposite side both below and above it."""
        curva_pbc = _curva_pbc(
            [
                (1, "V", "C", 30.0, 80.0),
                (1, "V", "C", 55.0, 40.0),
                (1, "C", "C", 40.0, 50.0),
                (1, "C", "C", 60.0, 100.0),
            ]
        )
        extended = calculated_curva_pbc_cleared_extended(curva_pbc)

        residual_demand = _residual_demand_by_price(
            calculate_residual_demand(extended), period=1
        )

        # buy(>=30)=150, sell(<=30)=80
        assert residual_demand[30.0] == pytest.approx(70.0)
        # buy(>=40)=150, sell(<=40)=80
        assert residual_demand[40.0] == pytest.approx(70.0)
        # buy(>=55)=100, sell(<=55)=120
        assert residual_demand[55.0] == pytest.approx(-20.0)
        # buy(>=60)=100, sell(<=60)=120
        assert residual_demand[60.0] == pytest.approx(-20.0)

    def test_residual_demand_is_monotonically_non_increasing_in_price(self):
        curva_pbc = _curva_pbc(
            [
                (1, "V", "C", -500.0, 50.0),
                (1, "V", "C", 15.0, 30.0),
                (1, "V", "C", 95.0, 70.0),
                (1, "C", "C", 180.0, 60.0),
                (1, "C", "C", 25.0, 25.0),
                (1, "C", "C", 5.0, 40.0),
            ]
        )
        extended = calculated_curva_pbc_cleared_extended(curva_pbc)

        rdc = calculate_residual_demand(extended).sort_values(cols.FLOAT_BID_PRICE)

        residual_demand = rdc["residual_demand"].tolist()
        assert residual_demand == sorted(residual_demand, reverse=True)

    def test_periods_are_calculated_independently(self):
        curva_pbc = _curva_pbc(
            [
                (1, "V", "C", 30.0, 80.0),
                (1, "C", "C", 60.0, 100.0),
                (2, "V", "C", 30.0, 10.0),
                (2, "C", "C", 60.0, 500.0),
            ]
        )
        extended = calculated_curva_pbc_cleared_extended(curva_pbc)

        rdc = calculate_residual_demand(extended)

        assert _residual_demand_by_price(rdc, 1)[30.0] == pytest.approx(20.0)
        assert _residual_demand_by_price(rdc, 2)[30.0] == pytest.approx(490.0)

    def test_tied_buy_and_sell_price(self):
        """A buy and a sell bid at the same price is the normal situation at the
        clearing price, so this is not an exotic edge case."""
        curva_pbc = _curva_pbc(
            [
                (1, "C", "C", 50.0, 100.0),
                (1, "V", "C", 50.0, 80.0),
            ]
        )
        extended = calculated_curva_pbc_cleared_extended(curva_pbc)

        rdc = calculate_residual_demand(extended)

        # buy(>=50)=100, sell(<=50)=80 -> 20, for every row at that price
        assert rdc["residual_demand"].dropna().unique().tolist() == [
            pytest.approx(20.0)
        ]

    def test_extreme_price_on_one_side_only_does_not_produce_nan(self):
        """Below the cheapest sell bid no power is sold, and above the priciest
        buy bid no power is bought: both are 0, not unknown."""
        curva_pbc = _curva_pbc(
            [
                (1, "C", "C", 10.0, 100.0),
                (1, "V", "C", 50.0, 80.0),
            ]
        )
        extended = calculated_curva_pbc_cleared_extended(curva_pbc)

        rdc = calculate_residual_demand(extended)

        residual_demand = _residual_demand_by_price(rdc, 1)
        # at p=10 nothing is sold yet -> 100 - 0
        assert residual_demand[10.0] == pytest.approx(100.0)
        # at p=50 the buy bid at 10 no longer applies -> 0 - 80
        assert residual_demand[50.0] == pytest.approx(-80.0)

    def test_yields_one_point_per_price(self):
        """np.interp has no defined behaviour for a repeated sample point, so a
        price must not appear twice within a period."""
        curva_pbc = _curva_pbc(
            [
                (1, "C", "C", 50.0, 100.0),
                (1, "V", "C", 50.0, 80.0),
                (1, "C", "C", 20.0, 10.0),
                (1, "V", "C", 90.0, 15.0),
            ]
        )
        extended = calculated_curva_pbc_cleared_extended(curva_pbc)

        rdc = calculate_residual_demand(extended)

        assert not rdc.duplicated(subset=[cols.INT_PERIOD, cols.FLOAT_BID_PRICE]).any()

    def test_never_leaves_an_unknown_residual_demand(self):
        curva_pbc = _curva_pbc(
            [
                (1, "C", "C", 10.0, 100.0),
                (1, "V", "C", 50.0, 80.0),
                (2, "V", "C", 5.0, 20.0),
                (2, "C", "C", 5.0, 70.0),
            ]
        )
        extended = calculated_curva_pbc_cleared_extended(curva_pbc)

        rdc = calculate_residual_demand(extended)

        assert rdc["residual_demand"].notna().all()


class TestFormatCurvaPbcRdc:
    """Test suite for format_curva_pbc_rdc function."""

    @pytest.fixture
    def rdc(self):
        curva_pbc = _curva_pbc(
            [
                (1, "V", "C", 30.0, 80.0),
                (1, "V", "C", 55.0, 40.0),
                (1, "C", "C", 40.0, 50.0),
                (1, "C", "C", 60.0, 100.0),
            ]
        )
        return calculate_residual_demand(
            calculated_curva_pbc_cleared_extended(curva_pbc)
        )

    def test_returns_one_row_per_price_point(self, rdc):
        formatted = format_curva_pbc_rdc(rdc, [30.0, 45.0, 60.0], 1)

        assert list(formatted.columns) == ["price_1", "power_1"]
        assert formatted["price_1"].tolist() == [30.0, 45.0, 60.0]

    def test_interpolates_linearly_between_bid_prices(self, rdc):
        formatted = format_curva_pbc_rdc(rdc, [40.0, 47.5, 55.0], 1)

        # between (40, 70) and (55, -20) the midpoint is (47.5, 25)
        assert formatted["power_1"].tolist() == pytest.approx([70.0, 25.0, -20.0])

    def test_clamps_outside_the_bid_price_range(self, rdc):
        """np.interp clamps rather than extrapolating, which is the intended
        behaviour here: outside the bid range the residual demand is flat."""
        formatted = format_curva_pbc_rdc(rdc, [-1000.0, 5000.0], 1)

        assert formatted["power_1"].tolist() == pytest.approx([70.0, -20.0])

    def test_period_without_any_bid_is_reported_clearly(self, rdc):
        """``rdc`` only covers period 1, but 2 periods are requested."""
        formatted = format_curva_pbc_rdc(rdc, [40.0], 2)

        assert formatted["power_2"].isna().all()


class TestCalculateResidualDemandCurvesFromBidCurves:
    """Test suite for the calculate_residual_demand_curves_from_bid_curves entry point."""

    @pytest.fixture
    def curva_pbc(self):
        return _curva_pbc(
            [
                (1, "V", "C", -500.0, 50.0),
                (1, "V", "C", 15.0, 30.0),
                (1, "V", "O", -500.0, 50.0),  # cleared
                (1, "V", "O", 15.0, 30.0),  # cleared
                (1, "V", "O", 90.0, 70.0),
                (1, "C", "C", 180.0, 60.0),
                (1, "C", "C", 25.0, 25.0),
                (1, "C", "O", 180.0, 60.0),  # cleared
                (1, "C", "O", 25.0, 25.0),  # cleared
                (1, "C", "O", 5.0, 40.0),
                (2, "V", "C", -500.0, 40.0),
                (2, "V", "C", 30.0, 20.0),
                (2, "V", "O", -500.0, 40.0),  # cleared
                (2, "V", "O", 30.0, 20.0),  # cleared
                (2, "V", "O", 100.0, 60.0),
                (2, "C", "C", 180.0, 55.0),
                (2, "C", "C", 40.0, 15.0),
                (2, "C", "O", 180.0, 55.0),  # cleared
                (2, "C", "O", 40.0, 15.0),  # cleared
                (2, "C", "O", 10.0, 35.0),
            ]
        )

    def test_returns_the_documented_key(self, curva_pbc):
        result = calculate_residual_demand_curves_from_bid_curves(
            curva_pbc, 2, price_points=[0.0, 50.0]
        )

        assert list(result) == ["cleared_bids_continued_with_submitted_residual_demand"]

    def test_produces_price_and_power_columns_for_every_period(self, curva_pbc):
        result = calculate_residual_demand_curves_from_bid_curves(
            curva_pbc, 2, price_points=[0.0, 50.0]
        )["cleared_bids_continued_with_submitted_residual_demand"]

        assert set(result.columns) == {"price_1", "price_2", "power_1", "power_2"}
        assert len(result) == 2

    def test_residual_demand_decreases_as_price_increases(self, curva_pbc):
        price_points = [-100.0, 0.0, 50.0, 120.0, 200.0]

        result = calculate_residual_demand_curves_from_bid_curves(
            curva_pbc, 2, price_points=price_points
        )["cleared_bids_continued_with_submitted_residual_demand"]

        for power_column in ("power_1", "power_2"):
            powers = result[power_column].tolist()
            assert powers == sorted(powers, reverse=True), power_column

    def test_default_price_points_span_the_whole_bid_price_range(self, curva_pbc):
        """The default grid must not stop short of MAX_BID_PRICE, or the curve is
        silently truncated where expensive orders still sit."""
        result = calculate_residual_demand_curves_from_bid_curves(curva_pbc, 2)[
            "cleared_bids_continued_with_submitted_residual_demand"
        ]

        assert result["price_1"].min() == pytest.approx(MIN_BID_PRICE)
        assert result["price_1"].max() == pytest.approx(MAX_BID_PRICE)
