"""
Tests for mibel_simulator.tools module.

Tests the utility functions for bid processing and analysis.
"""

import numpy as np
import pandas as pd
import pytest
from mibel_simulator import columns as cols
from mibel_simulator import tools
from mibel_simulator.file_paths import UOF_ZONES_FILEPATH


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
                cols.INT_PERIODO: [1, 1, 1],
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
                cols.INT_PERIODO: [1, 1, 1],
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
                cols.INT_PERIODO: [1, 1, 1, 1],
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
                cols.INT_PERIODO: [],
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
                cols.INT_PERIODO: [1],
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


class TestConcatProvidedUOFZonesWithExistingData:
    """Test suite for concat_provided_uof_zones_with_existing_data function."""

    def test_concat_new_uof_zones(self):
        """Test concatenating new UOF zones with existing data."""
        new_df = pd.DataFrame(
            {
                cols.ID_UNIDAD: ["UNIT3", "UNIT4"],
                cols.CAT_PAIS: ["ES", "PT"],
            }
        )

        result = tools.concat_provided_uof_zones_with_existing_data(new_df)

        # Should contain the new units
        assert "UNIT3" in result[cols.ID_UNIDAD].values
        assert "UNIT4" in result[cols.ID_UNIDAD].values

    def test_concat_overlapping_units(self):
        """Test concatenating with overlapping unit IDs."""

        existing_uof_zones = pd.read_csv(UOF_ZONES_FILEPATH)
        example_unit = existing_uof_zones.iloc[0][cols.ID_UNIDAD]

        # We don't know the country of the existing unit, so just use "ES" for test
        example_country = "ES"
        new_df = pd.DataFrame(
            {
                cols.ID_UNIDAD: [example_unit, "UNIT3"],
                cols.CAT_PAIS: [example_country, "ES"],
            }
        )

        result = tools.concat_provided_uof_zones_with_existing_data(new_df)

        # Should handle overlapping units
        assert "UNIT3" in result[cols.ID_UNIDAD].values
        assert (
            result.loc[result[cols.ID_UNIDAD] == example_unit, cols.CAT_PAIS].values[0]
            == example_country
        )

        # We don't know the country of the existing unit, so just use "ES" for test
        example_country = "PT"
        new_df = pd.DataFrame(
            {
                cols.ID_UNIDAD: [example_unit, "UNIT3"],
                cols.CAT_PAIS: [example_country, "ES"],
            }
        )

        result = tools.concat_provided_uof_zones_with_existing_data(new_df)

        # Should handle overlapping units
        assert "UNIT3" in result[cols.ID_UNIDAD].values
        assert (
            result.loc[result[cols.ID_UNIDAD] == example_unit, cols.CAT_PAIS].values[0]
            == example_country
        )

    def test_concat_single_unit(self):
        """Test concatenating with single new unit."""
        new_df = pd.DataFrame(
            {
                cols.ID_UNIDAD: ["UNIT5"],
                cols.CAT_PAIS: ["PT"],
            }
        )

        result = tools.concat_provided_uof_zones_with_existing_data(new_df)

        assert "UNIT5" in result[cols.ID_UNIDAD].values
        assert (
            result.loc[result[cols.ID_UNIDAD] == "UNIT5", cols.CAT_PAIS].values[0]
            == "PT"
        )


class TestFilterParadoxGroupsFromDetCab:
    """Test suite for filter_paradox_groups_from_det_cab function."""

    def test_filter_paradox_groups(self, full_simplified_det_cab_dataframe):
        """Test filtering paradox groups from DET/CAB."""
        paradox_groups = {
            "ids_mic_scos": ["ID_SCO_MIC_SCO"],
            "ids_bid_blocks": [
                "ID_BLOCK_B_1_GE_0",
                "ID_BLOCK_B_2_GE_0",
                "ID_EXCL_BLOCK_B_1_GE_1",
                "ID_EXCL_BLOCK_B_2_GE_1",
            ],
        }  # Order 1 is a MIC SCO
        result = tools.filter_paradox_groups_from_det_cab(
            full_simplified_det_cab_dataframe, paradox_groups
        )

        # Should include rows where ID_ORDER is in paradox_groups OR where FLOAT_MIC is not > 0
        assert len(result) == 30

    def test_filter_out_all_paradox_groups(self, full_simplified_det_cab_dataframe):
        """Test filtering paradox groups from DET/CAB."""
        paradox_groups = {
            "ids_mic_scos": [],
            "ids_bid_blocks": [],
        }  # Order 1 is a MIC SCO
        result = tools.filter_paradox_groups_from_det_cab(
            full_simplified_det_cab_dataframe, paradox_groups
        )

        # Should include rows where ID_ORDER is in paradox_groups  OR where FLOAT_MIC is not > 0
        assert len(result) == 15

    def test_filter_out_bid_blocks_paradox_groups(
        self, full_simplified_det_cab_dataframe
    ):
        """Test filtering paradox groups from DET/CAB."""
        paradox_groups = {
            "ids_mic_scos": ["ID_SCO_MIC_SCO"],
            "ids_bid_blocks": [],
        }  # Order 1 is a MIC SCO
        result = tools.filter_paradox_groups_from_det_cab(
            full_simplified_det_cab_dataframe, paradox_groups
        )

        # Should include rows where ID_ORDER is in paradox_groups  OR where FLOAT_MIC is not > 0
        assert len(result) == 18

    def test_filter_out_mic_sco_paradox_groups(self, full_simplified_det_cab_dataframe):
        """Test filtering paradox groups from DET/CAB."""
        paradox_groups = {
            "ids_mic_scos": [],
            "ids_bid_blocks": [
                "ID_BLOCK_B_1_GE_0",
                "ID_BLOCK_B_2_GE_0",
                "ID_EXCL_BLOCK_B_1_GE_1",
                "ID_EXCL_BLOCK_B_2_GE_1",
            ],
        }  # Order 1 is a MIC SCO
        result = tools.filter_paradox_groups_from_det_cab(
            full_simplified_det_cab_dataframe, paradox_groups
        )

        # Should include rows where ID_ORDER is in paradox_groups  OR where FLOAT_MIC is not > 0
        assert len(result) == 27
