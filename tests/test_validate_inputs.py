import numpy as np
import pandas as pd
import pytest

from iberian_day_ahead_market_simulator import validate_inputs
from iberian_day_ahead_market_simulator import columns as cols
from iberian_day_ahead_market_simulator.file_paths import (
    PARTICIPANTS_BIDDING_ZONES_FILEPATH,
)


class TestGetReferencePriceCurveIndex:
    """Test suite for get_reference_price_curve_index function."""

    @pytest.fixture
    def price_curves(self):
        return np.array([[10.0] * 24, [20.0] * 24, [30.0] * 24])

    def test_finds_the_row_equal_to_the_reference(self, price_curves):
        assert (
            validate_inputs.get_reference_price_curve_index(
                price_curves, np.array([20.0] * 24)
            )
            == 1
        )

    def test_reference_not_among_the_price_curves_raises(self, price_curves):
        with pytest.raises(ValueError, match="reference price curve"):
            validate_inputs.get_reference_price_curve_index(
                price_curves, np.array([25.0] * 24)
            )


class TestGetValidatedDetAndCab:
    """Test suite for get_validated_det_and_cab function."""

    def test_returns_the_session_period_count(
        self, hourly_session_det_dataframe, hourly_session_cab_dataframe
    ):
        det, cab, market_periods_count = validate_inputs.get_validated_det_and_cab(
            hourly_session_det_dataframe,
            hourly_session_cab_dataframe,
            np.full((1, 24), 50.0),
        )

        assert market_periods_count == 24
        assert len(det) == len(hourly_session_det_dataframe)
        pd.testing.assert_frame_equal(cab, hourly_session_cab_dataframe)

    def test_drops_a_residual_period_beyond_the_session(
        self, hourly_session_det_dataframe, hourly_session_cab_dataframe
    ):
        """OMIE's hourly DET files may hold a few bids for a 25th period."""
        residual_period_row = hourly_session_det_dataframe.iloc[[0]].assign(
            **{cols.INT_PERIOD: 25}
        )
        det_with_period_25 = pd.concat(
            [hourly_session_det_dataframe, residual_period_row], ignore_index=True
        )

        det, _, market_periods_count = validate_inputs.get_validated_det_and_cab(
            det_with_period_25, hourly_session_cab_dataframe, np.full((1, 24), 50.0)
        )

        assert market_periods_count == 24
        assert det[cols.INT_PERIOD].max() == 24

    def test_price_curves_of_another_length_raise(
        self, hourly_session_det_dataframe, hourly_session_cab_dataframe
    ):
        with pytest.raises(ValueError, match="price_curves cover 96 periods"):
            validate_inputs.get_validated_det_and_cab(
                hourly_session_det_dataframe,
                hourly_session_cab_dataframe,
                np.full((1, 96), 50.0),
            )


class TestGetParticipantsBiddingZones:
    """Test suite for get_participants_bidding_zones function."""

    def test_none_reads_the_bundled_list(self):
        pd.testing.assert_frame_equal(
            validate_inputs.get_participants_bidding_zones(None),
            pd.read_csv(PARTICIPANTS_BIDDING_ZONES_FILEPATH),
        )

    def test_provided_units_are_added_to_the_bundled_list(self):
        provided = pd.DataFrame(
            {cols.ID_UNIDAD: ["UNIT_NOT_BUNDLED"], cols.CAT_BIDDING_ZONE: ["PT"]}
        )

        participants_bidding_zones = validate_inputs.get_participants_bidding_zones(
            provided
        )

        assert len(participants_bidding_zones) == (
            len(pd.read_csv(PARTICIPANTS_BIDDING_ZONES_FILEPATH)) + 1
        )
        assert (
            participants_bidding_zones.set_index(cols.ID_UNIDAD).loc[
                "UNIT_NOT_BUNDLED", cols.CAT_BIDDING_ZONE
            ]
            == "PT"
        )
