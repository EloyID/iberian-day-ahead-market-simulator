import pytest
import pandas as pd
from mibel_simulator import columns as cols
from mibel_simulator.paradox_groups_tools import (
    check_are_paradox_groups_tested,
    transform_ids_paradox_groups_list_to_dict,
    transform_paradox_groups_dict_to_ids_list,
)


# Realistic test data
MIC_SCO_IDS = ["9706994", "9706993", "9707198", "9708010", "9708011"]
BID_BLOCK_IDS = [
    "9707582_B_2_GE_0",
    "9707925_B_1_GE_0",
    "9707924_B_1_GE_0",
    "9706993_B_4_GE_1",
    "9706995_B_1_GE_1",
]


class TestTransformIdsParadoxGroupsListToDict:
    """Test transformation functions between list and dict representations."""

    def test_transform_only_mic_scos(self):
        """Test transformation with only MIC SCO IDs (no 'GE')."""
        ids = ["9706994", "9707198", "9708010"]
        result = transform_ids_paradox_groups_list_to_dict(ids)

        assert cols.IDS_MIC_SCOS in result
        assert cols.IDS_BID_BLOCKS in result
        assert set(result[cols.IDS_MIC_SCOS]) == set(ids)
        assert result[cols.IDS_BID_BLOCKS] == []

    def test_transform_only_bid_blocks(self):
        """Test transformation with only bid block IDs (containing 'GE')."""
        ids = ["9707582_B_2_GE_0", "9707925_B_1_GE_0", "9707924_B_1_GE_0"]
        result = transform_ids_paradox_groups_list_to_dict(ids)

        assert result[cols.IDS_MIC_SCOS] == []
        assert set(result[cols.IDS_BID_BLOCKS]) == set(ids)

    def test_transform_mixed_ids(self):
        """Test transformation with mixed MIC SCO and bid block IDs."""
        ids = ["9706994", "9707582_B_2_GE_0", "9707198", "9707925_B_1_GE_0"]
        result = transform_ids_paradox_groups_list_to_dict(ids)

        expected_mic_scos = ["9706994", "9707198"]
        expected_bid_blocks = ["9707582_B_2_GE_0", "9707925_B_1_GE_0"]

        assert set(result[cols.IDS_MIC_SCOS]) == set(expected_mic_scos)
        assert set(result[cols.IDS_BID_BLOCKS]) == set(expected_bid_blocks)

    def test_transform_empty_list(self):
        """Test transformation with an empty list."""
        result = transform_ids_paradox_groups_list_to_dict([])

        assert result[cols.IDS_MIC_SCOS] == []
        assert result[cols.IDS_BID_BLOCKS] == []

    def test_roundtrip_transformation(self):
        """Test that transforming list->dict->list preserves IDs."""
        ids = MIC_SCO_IDS[:3] + BID_BLOCK_IDS[:2]
        result_dict = transform_ids_paradox_groups_list_to_dict(ids)
        result_list = transform_paradox_groups_dict_to_ids_list(result_dict)

        assert set(ids) == set(result_list)


class TestTransformParadoxGroupsDictToIdsList:
    """Test transformation from dict to list representation of paradox groups."""

    def test_dict_to_list_preserves_order(self):
        """Test that dict to list concatenates MIC SCOs first, then bid blocks."""
        paradox_dict = {
            cols.IDS_MIC_SCOS: ["9706994", "9707198"],
            cols.IDS_BID_BLOCKS: ["9707582_B_2_GE_0", "9707925_B_1_GE_0"],
        }
        result = transform_paradox_groups_dict_to_ids_list(paradox_dict)

        # First elements should be MIC SCOs
        assert result[:2] == ["9706994", "9707198"]
        # Last elements should be bid blocks
        assert result[2:] == ["9707582_B_2_GE_0", "9707925_B_1_GE_0"]

    def test_dict_to_list_empty_dict(self):
        """Test dict to list with empty entries."""
        paradox_dict = {
            cols.IDS_MIC_SCOS: [],
            cols.IDS_BID_BLOCKS: [],
        }
        result = transform_paradox_groups_dict_to_ids_list(paradox_dict)

        assert result == []


class TestCheckAreParadoxGroupsTested:
    """Test function for checking if paradox groups have been tested."""

    def test_exact_match_found(self):
        """Test that exact matches are detected."""
        combo1 = transform_ids_paradox_groups_list_to_dict(
            ["9706994", "9707582_B_2_GE_0"]
        )
        combo2 = transform_ids_paradox_groups_list_to_dict(
            ["9707198", "9707925_B_1_GE_0"]
        )
        trials_df = pd.DataFrame({cols.PARADOX_GROUPS_COLUMN: [combo1, combo2]})

        assert check_are_paradox_groups_tested(
            trials_df, ["9706994", "9707582_B_2_GE_0"]
        )
        assert check_are_paradox_groups_tested(
            trials_df, ["9707198", "9707925_B_1_GE_0"]
        )

    def test_order_independence(self):
        """Test that order of IDs doesn't matter for matching."""
        combo = transform_ids_paradox_groups_list_to_dict(
            ["9706994", "9707198", "9707582_B_2_GE_0"]
        )
        trials_df = pd.DataFrame({cols.PARADOX_GROUPS_COLUMN: [combo]})

        # Different orderings should all match
        assert check_are_paradox_groups_tested(
            trials_df, ["9706994", "9707198", "9707582_B_2_GE_0"]
        )
        assert check_are_paradox_groups_tested(
            trials_df, ["9707198", "9706994", "9707582_B_2_GE_0"]
        )
        assert check_are_paradox_groups_tested(
            trials_df, ["9707582_B_2_GE_0", "9706994", "9707198"]
        )

    def test_no_match_different_ids(self):
        """Test that different combinations are not matched."""
        combo = transform_ids_paradox_groups_list_to_dict(["9706994", "9707198"])
        trials_df = pd.DataFrame({cols.PARADOX_GROUPS_COLUMN: [combo]})

        assert not check_are_paradox_groups_tested(trials_df, ["9706994", "9708010"])
        assert not check_are_paradox_groups_tested(
            trials_df, ["9707582_B_2_GE_0", "9707925_B_1_GE_0"]
        )

    def test_subset_not_matched(self):
        """Test that subsets of tested combinations are not matched."""
        combo = transform_ids_paradox_groups_list_to_dict(
            ["9706994", "9707198", "9708010"]
        )
        trials_df = pd.DataFrame({cols.PARADOX_GROUPS_COLUMN: [combo]})

        assert not check_are_paradox_groups_tested(trials_df, ["9706994", "9707198"])
        assert not check_are_paradox_groups_tested(trials_df, ["9706994"])

    def test_superset_not_matched(self):
        """Test that supersets of tested combinations are not matched."""
        combo = transform_ids_paradox_groups_list_to_dict(["9706994", "9707198"])
        trials_df = pd.DataFrame({cols.PARADOX_GROUPS_COLUMN: [combo]})

        assert not check_are_paradox_groups_tested(
            trials_df, ["9706994", "9707198", "9708010"]
        )

    def test_empty_trials_df(self):
        """Test with empty trials DataFrame."""
        trials_df = pd.DataFrame({cols.PARADOX_GROUPS_COLUMN: []})

        assert not check_are_paradox_groups_tested(trials_df, ["9706994"])
        assert not check_are_paradox_groups_tested(trials_df, [])

    def test_empty_combination(self):
        """Test checking for an empty combination."""
        combo = transform_ids_paradox_groups_list_to_dict([])
        trials_df = pd.DataFrame({cols.PARADOX_GROUPS_COLUMN: [combo]})

        assert check_are_paradox_groups_tested(trials_df, [])

    def test_multiple_trials(self):
        """Test with multiple trials in the DataFrame."""
        combos = [
            transform_ids_paradox_groups_list_to_dict(["9706994"]),
            transform_ids_paradox_groups_list_to_dict(["9707198", "9708010"]),
            transform_ids_paradox_groups_list_to_dict(["9707582_B_2_GE_0"]),
        ]
        trials_df = pd.DataFrame({cols.PARADOX_GROUPS_COLUMN: combos})

        assert check_are_paradox_groups_tested(trials_df, ["9706994"])
        assert check_are_paradox_groups_tested(trials_df, ["9707198", "9708010"])
        assert check_are_paradox_groups_tested(trials_df, ["9707582_B_2_GE_0"])
        assert not check_are_paradox_groups_tested(trials_df, ["9706993"])
