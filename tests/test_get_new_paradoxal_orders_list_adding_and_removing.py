import pandas as pd
import pytest

from iberian_day_ahead_market_simulator import columns as cols
from iberian_day_ahead_market_simulator.get_new_paradoxal_orders_list_adding_and_removing import (
    get_new_paradoxal_orders_list_adding_and_removing,
    get_paradoxal_orders_combinations,
)
from iberian_day_ahead_market_simulator.paradoxal_orders_tools import (
    transform_ids_paradoxal_orders_list_to_dict,
    transform_paradoxal_orders_dict_to_ids_list,
)


class TestGetParadoxalOrdersCombinations:
    """Test function for generating paradox group combinations."""

    def test_basic_combinations(self):
        """Test basic combination generation."""
        df = pd.DataFrame(
            {"float_ratio_net_income_bid_power_merit": [10, 20, 30, 40]},
            index=["A", "B", "C", "D"],
        )

        combos = get_paradoxal_orders_combinations(df, "A", elements_in_combinations=2)

        # Should return all pairs including 'A'
        assert all("A" in c for c in combos)
        assert len(combos) == 3  # (A,B), (A,C), (A,D)

    def test_combinations_sorted_by_merit(self):
        """Test that combinations are sorted by sum of merit values."""
        df = pd.DataFrame(
            {"float_ratio_net_income_bid_power_merit": [10, 20, 30, 40]},
            index=["A", "B", "C", "D"],
        )

        combos = get_paradoxal_orders_combinations(df, "A", elements_in_combinations=2)

        # First combo should have highest sum (A=10, D=40 => 50)
        assert "D" in combos[0]
        # Last combo should have lowest sum (A=10, B=20 => 30)
        assert "B" in combos[-1]

    def test_larger_combinations(self):
        """Test with larger combination sizes."""
        df = pd.DataFrame(
            {"float_ratio_net_income_bid_power_merit": [10, 20, 30, 40, 50]},
            index=["A", "B", "C", "D", "E"],
        )

        combos = get_paradoxal_orders_combinations(df, "A", elements_in_combinations=3)

        assert all("A" in c for c in combos)
        assert all(len(c) == 3 for c in combos)
        # C(4,2) = 6 combinations
        assert len(combos) == 6

    def test_not_enough_elements_raises_assertion(self):
        """Test that assertion is raised when not enough elements."""
        df = pd.DataFrame(
            {"float_ratio_net_income_bid_power_merit": [10, 20]}, index=["A", "B"]
        )

        with pytest.raises(AssertionError, match="Not enough paradox groups"):
            get_paradoxal_orders_combinations(df, "A", elements_in_combinations=3)

    def test_single_element_combination(self):
        """Test with single element combinations."""
        df = pd.DataFrame(
            {"float_ratio_net_income_bid_power_merit": [10, 20, 30]},
            index=["A", "B", "C"],
        )

        combos = get_paradoxal_orders_combinations(df, "A", elements_in_combinations=1)

        assert combos == [["A"]]


class TestGetNewParadoxalOrdersListAddingAndRemoving:
    """Test function for proposing new paradox group combinations."""

    def test_basic_new_combination(self):
        """Test basic generation of new combinations."""
        leftout = pd.DataFrame(
            {cols.FLOAT_RATIO_NET_INCOME_BID_POWER: [10, 20]},
            index=["9706994_SCO", "9707198_SCO"],
        )
        cleared = pd.DataFrame(
            {cols.FLOAT_RATIO_NET_INCOME_CLEARED_POWER: [5]},
            index=["9708010_SCO"],
        )
        iterations_df = pd.DataFrame({cols.PARADOXAL_ORDERS_COLUMN: []})
        starting = transform_ids_paradoxal_orders_list_to_dict(["9708010_SCO"])

        result = get_new_paradoxal_orders_list_adding_and_removing(
            leftout,
            cleared,
            iterations_df,
            starting,
            paradoxal_orders_combinations_count=2,
        )

        assert isinstance(result, list)
        assert len(result) > 0  # Should return at least some combinations
        assert all(isinstance(r, dict) for r in result)
        # Should propose combinations that add leftout groups
        result_ids = [
            set(transform_paradoxal_orders_dict_to_ids_list(d)) for d in result
        ]
        assert any("9706994_SCO" in ids or "9707198_SCO" in ids for ids in result_ids)

    def test_avoids_tested_combinations(self):
        """Test that already tested combinations are not returned."""
        leftout = pd.DataFrame(
            {cols.FLOAT_RATIO_NET_INCOME_BID_POWER: [10, 20]},
            index=["9706994_SCO", "9707198_SCO"],
        )
        cleared = pd.DataFrame(
            {cols.FLOAT_RATIO_NET_INCOME_CLEARED_POWER: [5]},
            index=["9708010_SCO"],
        )

        # Mark one combination as already tested
        tested_combo = transform_ids_paradoxal_orders_list_to_dict(
            ["9708010_SCO", "9706994_SCO"]
        )
        iterations_df = pd.DataFrame({cols.PARADOXAL_ORDERS_COLUMN: [tested_combo]})
        starting = transform_ids_paradoxal_orders_list_to_dict(["9708010_SCO"])

        result = get_new_paradoxal_orders_list_adding_and_removing(
            leftout,
            cleared,
            iterations_df,
            starting,
            paradoxal_orders_combinations_count=5,
        )

        # Should not return the already tested combination
        for d in result:
            ids = set(transform_paradoxal_orders_dict_to_ids_list(d))
            assert ids != {"9708010_SCO", "9706994_SCO"}

    def test_removes_cleared_groups(self):
        """Test that cleared groups can be removed in new combinations."""
        leftout = pd.DataFrame(
            {cols.FLOAT_RATIO_NET_INCOME_BID_POWER: [30]},  # High merit
            index=["9706994_SCO"],
        )
        cleared = pd.DataFrame(
            {cols.FLOAT_RATIO_NET_INCOME_CLEARED_POWER: [5]},  # Low (negative) merit
            index=["9708010_SCO"],
        )
        iterations_df = pd.DataFrame({cols.PARADOXAL_ORDERS_COLUMN: []})
        starting = transform_ids_paradoxal_orders_list_to_dict(["9708010_SCO"])

        result = get_new_paradoxal_orders_list_adding_and_removing(
            leftout,
            cleared,
            iterations_df,
            starting,
            paradoxal_orders_combinations_count=5,
        )

        # Should propose swapping out low-merit cleared for high-merit leftout
        result_ids = [
            set(transform_paradoxal_orders_dict_to_ids_list(d)) for d in result
        ]
        # At least one result should have leftout but not cleared
        assert any(
            "9706994_SCO" in ids and "9708010_SCO" not in ids for ids in result_ids
        )

    def test_empty_inputs(self):
        """Test with empty leftout and cleared DataFrames."""
        empty_leftout = pd.DataFrame({cols.FLOAT_RATIO_NET_INCOME_BID_POWER: []})
        empty_cleared = pd.DataFrame({cols.FLOAT_RATIO_NET_INCOME_CLEARED_POWER: []})
        iterations_df = pd.DataFrame({cols.PARADOXAL_ORDERS_COLUMN: []})
        starting = transform_ids_paradoxal_orders_list_to_dict([])

        result = get_new_paradoxal_orders_list_adding_and_removing(
            empty_leftout,
            empty_cleared,
            iterations_df,
            starting,
            paradoxal_orders_combinations_count=1,
        )

        assert result == []

    def test_respects_max_combinations_count(self):
        """Test that the function respects the max combinations count."""
        leftout = pd.DataFrame(
            {cols.FLOAT_RATIO_NET_INCOME_BID_POWER: [10, 20, 30, 40, 50]},
            index=["L1_SCO", "L2_SCO", "L3_SCO", "L4_SCO", "L5_SCO"],
        )
        cleared = pd.DataFrame(
            {cols.FLOAT_RATIO_NET_INCOME_CLEARED_POWER: [5]},
            index=["C1_SCO"],
        )
        iterations_df = pd.DataFrame({cols.PARADOXAL_ORDERS_COLUMN: []})
        starting = transform_ids_paradoxal_orders_list_to_dict(["C1_SCO"])

        result = get_new_paradoxal_orders_list_adding_and_removing(
            leftout,
            cleared,
            iterations_df,
            starting,
            paradoxal_orders_combinations_count=3,
        )

        assert len(result) <= 3

    def test_mixed_mic_scos_and_bid_blocks(self):
        """Test with realistic mix of MIC SCOs and bid blocks."""
        leftout = pd.DataFrame(
            {cols.FLOAT_RATIO_NET_INCOME_BID_POWER: [10, 20, 30]},
            index=["9706994_SCO", "9707582_B_2_GE_0", "9707198_SCO"],
        )
        cleared = pd.DataFrame(
            {cols.FLOAT_RATIO_NET_INCOME_CLEARED_POWER: [5, 8]},
            index=["9708010_SCO", "9707925_B_1_GE_0"],
        )
        iterations_df = pd.DataFrame({cols.PARADOXAL_ORDERS_COLUMN: []})
        starting = transform_ids_paradoxal_orders_list_to_dict(
            ["9708010_SCO", "9707925_B_1_GE_0"]
        )

        result = get_new_paradoxal_orders_list_adding_and_removing(
            leftout,
            cleared,
            iterations_df,
            starting,
            paradoxal_orders_combinations_count=3,
        )

        assert len(result) > 0
        # Verify the structure is correct (has both MIC SCOs and bid blocks keys)
        for d in result:
            assert cols.IDS_MIC_SCOS in d
            assert cols.IDS_BID_BLOCKS in d

    def test_multiple_swaps_in_combination(self):
        """Test that multiple groups can be swapped in a single combination."""
        leftout = pd.DataFrame(
            {cols.FLOAT_RATIO_NET_INCOME_BID_POWER: [30, 40]},
            index=["L1_SCO", "L2_SCO"],
        )
        cleared = pd.DataFrame(
            {cols.FLOAT_RATIO_NET_INCOME_CLEARED_POWER: [5, 6]},
            index=["C1_SCO", "C2_SCO"],
        )
        iterations_df = pd.DataFrame({cols.PARADOXAL_ORDERS_COLUMN: []})
        starting = transform_ids_paradoxal_orders_list_to_dict(["C1_SCO", "C2_SCO"])

        result = get_new_paradoxal_orders_list_adding_and_removing(
            leftout,
            cleared,
            iterations_df,
            starting,
            paradoxal_orders_combinations_count=10,
        )

        # Should include combinations that swap multiple groups
        result_ids = [
            set(transform_paradoxal_orders_dict_to_ids_list(d)) for d in result
        ]
        # At least one should have both leftout groups
        assert any("L1_SCO" in ids and "L2_SCO" in ids for ids in result_ids)
