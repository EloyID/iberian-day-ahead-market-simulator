import pandas as pd

from mibel_simulator import columns as cols
from mibel_simulator.paradoxal_orders_tools import (
    check_are_paradoxal_orders_tested,
    transform_ids_paradoxal_orders_list_to_dict,
    transform_paradoxal_orders_dict_to_ids_list,
)

# Realistic test data
MIC_SCO_IDS = [
    "9706994_SCO",
    "9706993_SCO",
    "9707198_SCO",
    "9708010_SCO",
    "9708011_SCO",
]
BID_BLOCK_IDS = [
    "9707582_B_2_GE_0",
    "9707925_B_1_GE_0",
    "9707924_B_1_GE_0",
    "9706993_B_4_GE_1",
    "9706995_B_1_GE_1",
]


class TestTransformIdsParadoxalOrdersListToDict:
    """Test transformation functions between list and dict representations."""

    def test_transform_only_mic_scos(self):
        """Test transformation with only MIC SCO IDs (no 'GE')."""
        ids = ["9706994_SCO", "9707198_SCO", "9708010_SCO"]
        result = transform_ids_paradoxal_orders_list_to_dict(ids)

        assert cols.IDS_MIC_SCOS in result
        assert cols.IDS_BID_BLOCKS in result
        assert set(result[cols.IDS_MIC_SCOS]) == set(ids)
        assert result[cols.IDS_BID_BLOCKS] == []

    def test_transform_only_bid_blocks(self):
        """Test transformation with only bid block IDs (containing 'GE')."""
        ids = ["9707582_B_2_GE_0", "9707925_B_1_GE_0", "9707924_B_1_GE_0"]
        result = transform_ids_paradoxal_orders_list_to_dict(ids)

        assert result[cols.IDS_MIC_SCOS] == []
        assert set(result[cols.IDS_BID_BLOCKS]) == set(ids)

    def test_transform_mixed_ids(self):
        """Test transformation with mixed MIC SCO and bid block IDs."""
        ids = ["9706994_SCO", "9707582_B_2_GE_0", "9707198_SCO", "9707925_B_1_GE_0"]
        result = transform_ids_paradoxal_orders_list_to_dict(ids)

        expected_mic_scos = ["9706994_SCO", "9707198_SCO"]
        expected_bid_blocks = ["9707582_B_2_GE_0", "9707925_B_1_GE_0"]

        assert set(result[cols.IDS_MIC_SCOS]) == set(expected_mic_scos)
        assert set(result[cols.IDS_BID_BLOCKS]) == set(expected_bid_blocks)

    def test_transform_empty_list(self):
        """Test transformation with an empty list."""
        result = transform_ids_paradoxal_orders_list_to_dict([])

        assert result[cols.IDS_MIC_SCOS] == []
        assert result[cols.IDS_BID_BLOCKS] == []

    def test_roundtrip_transformation(self):
        """Test that transforming list->dict->list preserves IDs."""
        ids = MIC_SCO_IDS[:3] + BID_BLOCK_IDS[:2]
        result_dict = transform_ids_paradoxal_orders_list_to_dict(ids)
        result_list = transform_paradoxal_orders_dict_to_ids_list(result_dict)

        assert set(ids) == set(result_list)


class TestTransformParadoxalOrdersDictToIdsList:
    """Test transformation from dict to list representation of paradox groups."""

    def test_dict_to_list_preserves_order(self):
        """Test that dict to list concatenates MIC SCOs first, then bid blocks."""
        paradox_dict = {
            cols.IDS_MIC_SCOS: ["9706994_SCO", "9707198_SCO"],
            cols.IDS_BID_BLOCKS: ["9707582_B_2_GE_0", "9707925_B_1_GE_0"],
        }
        result = transform_paradoxal_orders_dict_to_ids_list(paradox_dict)

        # First elements should be MIC SCOs
        assert result[:2] == ["9706994_SCO", "9707198_SCO"]
        # Last elements should be bid blocks
        assert result[2:] == ["9707582_B_2_GE_0", "9707925_B_1_GE_0"]

    def test_dict_to_list_empty_dict(self):
        """Test dict to list with empty entries."""
        paradox_dict = {
            cols.IDS_MIC_SCOS: [],
            cols.IDS_BID_BLOCKS: [],
        }
        result = transform_paradoxal_orders_dict_to_ids_list(paradox_dict)

        assert result == []


class TestCheckAreParadoxalOrdersTested:
    """Test function for checking if paradox groups have been tested."""

    def test_exact_match_found(self):
        """Test that exact matches are detected."""
        combo1 = transform_ids_paradoxal_orders_list_to_dict(
            ["9706994_SCO", "9707582_B_2_GE_0"]
        )
        combo2 = transform_ids_paradoxal_orders_list_to_dict(
            ["9707198_SCO", "9707925_B_1_GE_0"]
        )
        iterations_df = pd.DataFrame({cols.PARADOXAL_ORDERS_COLUMN: [combo1, combo2]})

        assert check_are_paradoxal_orders_tested(
            iterations_df, ["9706994_SCO", "9707582_B_2_GE_0"]
        )
        assert check_are_paradoxal_orders_tested(
            iterations_df, ["9707198_SCO", "9707925_B_1_GE_0"]
        )

    def test_order_independence(self):
        """Test that order of IDs doesn't matter for matching."""
        combo = transform_ids_paradoxal_orders_list_to_dict(
            ["9706994_SCO", "9707198_SCO", "9707582_B_2_GE_0"]
        )
        iterations_df = pd.DataFrame({cols.PARADOXAL_ORDERS_COLUMN: [combo]})

        # Different orderings should all match
        assert check_are_paradoxal_orders_tested(
            iterations_df, ["9706994_SCO", "9707198_SCO", "9707582_B_2_GE_0"]
        )
        assert check_are_paradoxal_orders_tested(
            iterations_df, ["9707198_SCO", "9706994_SCO", "9707582_B_2_GE_0"]
        )
        assert check_are_paradoxal_orders_tested(
            iterations_df, ["9707582_B_2_GE_0", "9706994_SCO", "9707198_SCO"]
        )

    def test_no_match_different_ids(self):
        """Test that different combinations are not matched."""
        combo = transform_ids_paradoxal_orders_list_to_dict(
            ["9706994_SCO", "9707198_SCO"]
        )
        iterations_df = pd.DataFrame({cols.PARADOXAL_ORDERS_COLUMN: [combo]})

        assert not check_are_paradoxal_orders_tested(
            iterations_df, ["9706994_SCO", "9708010_SCO"]
        )
        assert not check_are_paradoxal_orders_tested(
            iterations_df, ["9707582_B_2_GE_0", "9707925_B_1_GE_0"]
        )

    def test_subset_not_matched(self):
        """Test that subsets of tested combinations are not matched."""
        combo = transform_ids_paradoxal_orders_list_to_dict(
            ["9706994_SCO", "9707198_SCO", "9708010_SCO"]
        )
        iterations_df = pd.DataFrame({cols.PARADOXAL_ORDERS_COLUMN: [combo]})

        assert not check_are_paradoxal_orders_tested(
            iterations_df, ["9706994_SCO", "9707198_SCO"]
        )
        assert not check_are_paradoxal_orders_tested(iterations_df, ["9706994_SCO"])

    def test_superset_not_matched(self):
        """Test that supersets of tested combinations are not matched."""
        combo = transform_ids_paradoxal_orders_list_to_dict(
            ["9706994_SCO", "9707198_SCO"]
        )
        iterations_df = pd.DataFrame({cols.PARADOXAL_ORDERS_COLUMN: [combo]})

        assert not check_are_paradoxal_orders_tested(
            iterations_df, ["9706994_SCO", "9707198_SCO", "9708010_SCO"]
        )

    def test_empty_iterations_df(self):
        """Test with empty iterations DataFrame."""
        iterations_df = pd.DataFrame({cols.PARADOXAL_ORDERS_COLUMN: []})

        assert not check_are_paradoxal_orders_tested(iterations_df, ["9706994_SCO"])
        assert not check_are_paradoxal_orders_tested(iterations_df, [])

    def test_empty_combination(self):
        """Test checking for an empty combination."""
        combo = transform_ids_paradoxal_orders_list_to_dict([])
        iterations_df = pd.DataFrame({cols.PARADOXAL_ORDERS_COLUMN: [combo]})

        assert check_are_paradoxal_orders_tested(iterations_df, [])

    def test_multiple_iterations(self):
        """Test with multiple iterations in the DataFrame."""
        combos = [
            transform_ids_paradoxal_orders_list_to_dict(["9706994_SCO"]),
            transform_ids_paradoxal_orders_list_to_dict(["9707198_SCO", "9708010_SCO"]),
            transform_ids_paradoxal_orders_list_to_dict(["9707582_B_2_GE_0"]),
        ]
        iterations_df = pd.DataFrame({cols.PARADOXAL_ORDERS_COLUMN: combos})

        assert check_are_paradoxal_orders_tested(iterations_df, ["9706994_SCO"])
        assert check_are_paradoxal_orders_tested(
            iterations_df, ["9707198_SCO", "9708010_SCO"]
        )
        assert check_are_paradoxal_orders_tested(iterations_df, ["9707582_B_2_GE_0"])
        assert not check_are_paradoxal_orders_tested(iterations_df, ["9706993_SCO"])
