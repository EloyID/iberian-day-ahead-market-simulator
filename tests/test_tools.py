"""
Tests for iberian_day_ahead_market_simulator.tools module.

Tests the utility functions for bid processing and analysis.
"""

import numpy as np
import pandas as pd
import pytest

from iberian_day_ahead_market_simulator import columns as cols
from iberian_day_ahead_market_simulator import tools
from iberian_day_ahead_market_simulator.file_paths import (
    PARTICIPANTS_BIDDING_ZONES_FILEPATH,
)


class TestGetFloatBidPowerCumsum:
    """Test suite for get_float_bid_power_cumsum function."""

    def test_get_float_bid_power_cumsum(self, full_simplified_det_cab_dataframe):
        float_bid_power_cumsum = tools.get_float_bid_power_cumsum(
            full_simplified_det_cab_dataframe,
            date_column_name=None,
            cod_ofertada_casada_column_name=None,
        )
        pd.testing.assert_series_equal(
            float_bid_power_cumsum,
            full_simplified_det_cab_dataframe[cols.FLOAT_BID_POWER_CUMSUM].rename(None),
        )

    def test_cumsum_output_is_series(self):
        """Test that function returns a Series."""
        df = pd.DataFrame(
            {
                cols.INT_PERIOD: [1, 1, 1],
                cols.CAT_BUY_SELL: ["V", "V", "V"],
                cols.CAT_OFERTADA_CASADA: [None, None, None],
                cols.DATE_SESION: [pd.Timestamp("2024-01-01")] * 3,
                cols.FLOAT_BID_PRICE: [10.0, 15.0, 20.0],
                cols.FLOAT_BID_POWER: [100.0, 200.0, 150.0],
            }
        )

        result = tools.get_float_bid_power_cumsum(df, date_column_name=None)

        assert isinstance(result, pd.Series)
        assert len(result) == len(df)

    def test_cumsum_no_negative_values(self):
        """Test that cumulative sum doesn't produce negative values."""
        df = pd.DataFrame(
            {
                cols.INT_PERIOD: [1, 1, 1],
                cols.CAT_BUY_SELL: ["V", "V", "V"],
                cols.CAT_OFERTADA_CASADA: [None, None, None],
                cols.DATE_SESION: [pd.Timestamp("2024-01-01")] * 3,
                cols.FLOAT_BID_PRICE: [10.0, 15.0, 20.0],
                cols.FLOAT_BID_POWER: [100.0, 200.0, 150.0],
            }
        )

        result = tools.get_float_bid_power_cumsum(df, date_column_name=None)

        # All non-NaN values should be positive or zero
        assert (result.dropna() >= 0).all()

    def test_cumsum_handles_mixed_buysell(self):
        """Test that mixed buy/sell are processed separately."""
        df = pd.DataFrame(
            {
                cols.INT_PERIOD: [1, 1, 1, 1],
                cols.CAT_BUY_SELL: ["V", "V", "C", "C"],
                cols.CAT_OFERTADA_CASADA: [None, None, None, None],
                cols.DATE_SESION: [pd.Timestamp("2024-01-01")] * 4,
                cols.FLOAT_BID_PRICE: [10.0, 15.0, 20.0, 25.0],
                cols.FLOAT_BID_POWER: [100.0, 200.0, 100.0, 200.0],
            }
        )

        result = tools.get_float_bid_power_cumsum(df, date_column_name=None)

        # Should have result for each bid
        assert len(result) == 4

    def test_cumsum_empty_dataframe(self):
        """Test function handles empty dataframe."""
        df = pd.DataFrame(
            {
                cols.INT_PERIOD: [],
                cols.CAT_BUY_SELL: [],
                cols.CAT_OFERTADA_CASADA: [],
                cols.DATE_SESION: [],
                cols.FLOAT_BID_PRICE: [],
                cols.FLOAT_BID_POWER: [],
            }
        )

        result = tools.get_float_bid_power_cumsum(df, date_column_name=None)
        assert len(result) == 0

    def test_cumsum_single_row(self):
        """Test function with single row."""
        df = pd.DataFrame(
            {
                cols.INT_PERIOD: [1],
                cols.CAT_BUY_SELL: ["V"],
                cols.CAT_OFERTADA_CASADA: [None],
                cols.DATE_SESION: [pd.Timestamp("2024-01-01")],
                cols.FLOAT_BID_PRICE: [10.0],
                cols.FLOAT_BID_POWER: [100.0],
            }
        )

        result = tools.get_float_bid_power_cumsum(df, date_column_name=None)
        assert len(result) == 1


class TestGetIsSimpleBid:
    """Test suite for get_is_simple_bid function."""

    def test_simple_bid_identification(self, full_simplified_det_cab_dataframe):
        """Test identification of simple bids."""

        result = tools.get_is_simple_bid(full_simplified_det_cab_dataframe)
        expected_result = full_simplified_det_cab_dataframe.id_unidad.isin(
            ["UNIT_SIMPLE", "UNIT_BUY", "MIEU"]
        )
        # First two should be simple, third is a block order
        assert np.array_equal(result.values, expected_result.values)


class TestGetIsSCO:
    """Test suite for get_is_SCO function."""

    def test_sco_with_mav(self, full_simplified_det_cab_dataframe):
        """Test SCO identification."""
        result = tools.get_is_SCO(full_simplified_det_cab_dataframe)
        expected_result = full_simplified_det_cab_dataframe.id_unidad.isin(
            ["UNIT_SCO_MIC", "UNIT_SCO_MAV"]
        )
        # First two should be simple, third is a block order
        assert np.array_equal(result.values, expected_result.values)


class TestConcatProvidedParticipantBiddingZonesWithExistingData:
    """Test suite for concat_provided_participants_bidding_zones_with_existing_data function."""

    def test_concat_new_participants_bidding_zones(self):
        """Test concatenating new UOF zones with existing data."""
        new_df = pd.DataFrame(
            {
                cols.ID_UNIDAD: ["UNIT3", "UNIT4"],
                cols.CAT_BIDDING_ZONE: ["ES", "PT"],
            }
        )

        result = tools.concat_provided_participants_bidding_zones_with_existing_data(
            new_df
        )

        # Should contain the new units
        assert "UNIT3" in result[cols.ID_UNIDAD].values
        assert "UNIT4" in result[cols.ID_UNIDAD].values

    def test_concat_overlapping_units(self):
        """Test concatenating with overlapping unit IDs."""

        existing_participants_bidding_zones = pd.read_csv(
            PARTICIPANTS_BIDDING_ZONES_FILEPATH
        )
        example_unit = existing_participants_bidding_zones.iloc[0][cols.ID_UNIDAD]

        # We don't know the country of the existing unit, so just use "ES" for test
        example_country = "ES"
        new_df = pd.DataFrame(
            {
                cols.ID_UNIDAD: [example_unit, "UNIT3"],
                cols.CAT_BIDDING_ZONE: [example_country, "ES"],
            }
        )

        result = tools.concat_provided_participants_bidding_zones_with_existing_data(
            new_df
        )

        # Should handle overlapping units
        assert "UNIT3" in result[cols.ID_UNIDAD].values
        assert (
            result.loc[
                result[cols.ID_UNIDAD] == example_unit, cols.CAT_BIDDING_ZONE
            ].values[0]
            == example_country
        )

        # We don't know the country of the existing unit, so just use "ES" for test
        example_country = "PT"
        new_df = pd.DataFrame(
            {
                cols.ID_UNIDAD: [example_unit, "UNIT3"],
                cols.CAT_BIDDING_ZONE: [example_country, "ES"],
            }
        )

        result = tools.concat_provided_participants_bidding_zones_with_existing_data(
            new_df
        )

        # Should handle overlapping units
        assert "UNIT3" in result[cols.ID_UNIDAD].values
        assert (
            result.loc[
                result[cols.ID_UNIDAD] == example_unit, cols.CAT_BIDDING_ZONE
            ].values[0]
            == example_country
        )

    def test_concat_single_unit(self):
        """Test concatenating with single new unit."""
        new_df = pd.DataFrame(
            {
                cols.ID_UNIDAD: ["UNIT5"],
                cols.CAT_BIDDING_ZONE: ["PT"],
            }
        )

        result = tools.concat_provided_participants_bidding_zones_with_existing_data(
            new_df
        )

        assert "UNIT5" in result[cols.ID_UNIDAD].values
        assert (
            result.loc[result[cols.ID_UNIDAD] == "UNIT5", cols.CAT_BIDDING_ZONE].values[
                0
            ]
            == "PT"
        )


class TestFilterParadoxicalOrdersFromDetCab:
    """Test suite for filter_paradoxical_orders_from_det_cab function."""

    def test_filter_paradoxical_orders(self, full_simplified_det_cab_dataframe):
        """Test filtering paradox groups from DET/CAB."""
        paradoxical_orders = {
            "ids_mic_scos": ["ID_SCO_MIC_SCO"],
            "ids_bid_blocks": [
                "ID_BLOCK_B_1_GE_0",
                "ID_BLOCK_B_2_GE_0",
                "ID_EXCL_BLOCK_B_1_GE_1",
                "ID_EXCL_BLOCK_B_2_GE_1",
            ],
        }  # Order 1 is a MIC SCO
        result = tools.filter_paradoxical_orders_from_det_cab(
            full_simplified_det_cab_dataframe, paradoxical_orders
        )

        # Should include rows where ID_ORDER is in paradoxical_orders OR where FLOAT_MIC is not > 0
        assert len(result) == 30

    def test_filter_out_all_paradoxical_orders(self, full_simplified_det_cab_dataframe):
        """Test filtering paradox groups from DET/CAB."""
        paradoxical_orders = {
            "ids_mic_scos": [],
            "ids_bid_blocks": [],
        }  # Order 1 is a MIC SCO
        result = tools.filter_paradoxical_orders_from_det_cab(
            full_simplified_det_cab_dataframe, paradoxical_orders
        )

        # Should include rows where ID_ORDER is in paradoxical_orders  OR where FLOAT_MIC is not > 0
        assert len(result) == 15

    def test_filter_out_bid_blocks_paradoxical_orders(
        self, full_simplified_det_cab_dataframe
    ):
        """Test filtering paradox groups from DET/CAB."""
        paradoxical_orders = {
            "ids_mic_scos": ["ID_SCO_MIC_SCO"],
            "ids_bid_blocks": [],
        }  # Order 1 is a MIC SCO
        result = tools.filter_paradoxical_orders_from_det_cab(
            full_simplified_det_cab_dataframe, paradoxical_orders
        )

        # Should include rows where ID_ORDER is in paradoxical_orders  OR where FLOAT_MIC is not > 0
        assert len(result) == 18

    def test_filter_out_mic_sco_paradoxical_orders(
        self, full_simplified_det_cab_dataframe
    ):
        """Test filtering paradox groups from DET/CAB."""
        paradoxical_orders = {
            "ids_mic_scos": [],
            "ids_bid_blocks": [
                "ID_BLOCK_B_1_GE_0",
                "ID_BLOCK_B_2_GE_0",
                "ID_EXCL_BLOCK_B_1_GE_1",
                "ID_EXCL_BLOCK_B_2_GE_1",
            ],
        }  # Order 1 is a MIC SCO
        result = tools.filter_paradoxical_orders_from_det_cab(
            full_simplified_det_cab_dataframe, paradoxical_orders
        )

        # Should include rows where ID_ORDER is in paradoxical_orders  OR where FLOAT_MIC is not > 0
        assert len(result) == 27


class TestIsMarketPresenceResidual:
    """Test suite for is_market_presence_residual function."""

    def _det_with_period_counts(self, period_counts: dict) -> pd.DataFrame:
        periods = []
        for period, count in period_counts.items():
            periods.extend([period] * count)
        return pd.DataFrame({cols.INT_PERIOD: periods})

    def test_residual_period_detected(self):
        """A period with far fewer bids than the median is flagged as residual."""
        det = self._det_with_period_counts({1: 100, 2: 100, 3: 100, 25: 1})
        assert tools.is_market_presence_residual(det, 25) == True

    def test_non_residual_period_not_detected(self):
        """A period with a comparable bid count to the median is not residual."""
        det = self._det_with_period_counts({1: 100, 2: 100, 3: 100, 25: 100})
        assert tools.is_market_presence_residual(det, 25) == False

    def test_custom_threshold(self):
        """A period just above/below a custom threshold is classified accordingly."""
        det = self._det_with_period_counts({1: 100, 2: 100, 25: 30})
        # 30 / 100 == 0.3, so with threshold 0.5 it is residual...
        assert tools.is_market_presence_residual(det, 25, threshold=0.5) == True
        # ...but with threshold 0.2 it is not.
        assert tools.is_market_presence_residual(det, 25, threshold=0.2) == False


class TestGetMarketPeriodsCount:
    """Test suite for get_market_periods_count function."""

    def _det_with_max_period(
        self, max_period: int, other_count: int = 100
    ) -> pd.DataFrame:
        periods = []
        for period in range(1, max_period + 1):
            periods.extend([period] * other_count)
        return pd.DataFrame({cols.INT_PERIOD: periods})

    def test_standard_hourly_market_24_periods(self):
        """A regular hourly market with 24 evenly distributed periods stays at 24."""
        det = self._det_with_max_period(24)
        assert tools.get_market_periods_count(det) == 24

    def test_hourly_market_with_residual_25th_period(self):
        """A 25th period with negligible bids is dropped, and 24 is not residual either."""
        periods = []
        for period in range(1, 25):
            periods.extend([period] * 100)
        periods.append(25)
        det = pd.DataFrame({cols.INT_PERIOD: periods})
        assert tools.get_market_periods_count(det) == 24

    def test_hourly_market_with_genuine_25_periods(self):
        """When period 25 is not residual (DST fall-back day), it is kept."""
        det = self._det_with_max_period(25)
        assert tools.get_market_periods_count(det) == 25

    def test_hourly_market_23_periods(self):
        """A 23-period day (DST spring-forward) is returned as-is."""
        det = self._det_with_max_period(23)
        assert tools.get_market_periods_count(det) == 23

    def test_standard_qh_market_96_periods(self):
        """A regular QH market with 96 evenly distributed periods stays at 96."""
        det = self._det_with_max_period(96)
        assert tools.get_market_periods_count(det) == 96

    def test_qh_market_with_residual_97th_period(self):
        """A 97th+ QH period with negligible bids is dropped down to 96."""
        periods = []
        for period in range(1, 97):
            periods.extend([period] * 100)
        periods.extend([97, 98, 99, 100])
        det = pd.DataFrame({cols.INT_PERIOD: periods})
        assert tools.get_market_periods_count(det) == 96

    def test_qh_market_with_genuine_100_periods(self):
        """When period 100 is not residual, the 100-period count is kept."""
        det = self._det_with_max_period(100)
        assert tools.get_market_periods_count(det) == 100

    def test_qh_market_92_periods(self):
        """A 92-period QH day (DST spring-forward) is returned as-is."""
        det = self._det_with_max_period(92)
        assert tools.get_market_periods_count(det) == 92

    def test_qh_market_with_period_97_entirely_absent(self):
        """Regression test for the fixed bug: the QH residual check used to
        always test a hardcoded period 97 regardless of the actual max period.
        If period 97 had zero rows (while periods 98-100 were fully populated,
        a plausible OMIE data quirk), value_counts().loc[97] raised a bare
        KeyError instead of the day being correctly recognized as a genuine
        100-period market. The fix checks the actual max period (100) instead."""
        periods = []
        for period in range(1, 97):
            periods.extend([period] * 100)
        for period in [98, 99, 100]:
            periods.extend([period] * 100)
        det = pd.DataFrame({cols.INT_PERIOD: periods})
        assert tools.get_market_periods_count(det) == 100

    def test_qh_market_without_complete_100_periods(self):
        """Regression test for the fixed bug: if only 98 not residual periods
        available, the function should still return 100."""
        det = self._det_with_max_period(98)
        assert tools.get_market_periods_count(det) == 100

    def test_raises_for_unrecognized_hourly_period_count(self):
        """A max period count outside the known hourly/QH options (e.g. a
        corrupted file, or an intermediate count like 26) must raise a clear
        error instead of silently returning None."""
        det = self._det_with_max_period(26)
        with pytest.raises(ValueError, match="Unexpected number of periods"):
            tools.get_market_periods_count(det)


class TestIsQHMarket:
    """Test suite for is_QH_market function."""

    def test_hourly_period_counts_are_not_qh(self):
        for market_periods_count in [23, 24, 25]:
            assert tools.is_QH_market(market_periods_count) is False

    def test_qh_period_counts_are_qh(self):
        for market_periods_count in [92, 96, 100]:
            assert tools.is_QH_market(market_periods_count) is True


class TestGetPowerEnergyScalator:
    """Test suite for get_power_energy_scalator function."""

    def test_qh_market_scalator_is_4(self):
        assert tools.get_power_energy_scalator(True) == 4

    def test_hourly_market_scalator_is_1(self):
        assert tools.get_power_energy_scalator(False) == 1


class TestTransformHxQxPeriodToInt:
    """Test suite for transform_hxqx_period_to_int function."""

    def test_transforms_first_hour(self):
        result = tools.transform_hxqx_period_to_int(
            pd.Series(["H1Q1", "H1Q2", "H1Q3", "H1Q4"])
        )
        pd.testing.assert_series_equal(result, pd.Series([1, 2, 3, 4]))

    def test_transforms_later_hour(self):
        result = tools.transform_hxqx_period_to_int(
            pd.Series(["H24Q1", "H24Q2", "H24Q3", "H24Q4"])
        )
        pd.testing.assert_series_equal(result, pd.Series([93, 94, 95, 96]))

    def test_transforms_double_digit_hour(self):
        result = tools.transform_hxqx_period_to_int(pd.Series(["H10Q3"]))
        pd.testing.assert_series_equal(result, pd.Series([39]))
