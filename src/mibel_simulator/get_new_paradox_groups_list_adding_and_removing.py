import numpy as np
import pandas as pd
from mibel_simulator import columns as cols
from mibel_simulator.paradox_groups_tools import (
    check_are_paradox_groups_tested,
    transform_ids_paradox_groups_list_to_dict,
    transform_paradox_groups_dict_to_ids_list,
)

from itertools import combinations


def get_paradox_groups_combinations(
    paradox_groups_summary_sorted: pd.DataFrame,
    reference_id_paradox_groups: str,
    elements_in_combinations: int = 4,
):
    """ """
    secondary_ids_paradox_groups = paradox_groups_summary_sorted.query(
        "index != @reference_id_paradox_groups"
    ).index.to_list()
    assert len(secondary_ids_paradox_groups) >= elements_in_combinations - 1, (
        "Not enough paradox groups to create combinations. "
        f"Available: {len(secondary_ids_paradox_groups)}, "
        f"Requested: {elements_in_combinations}."
    )
    assert len(secondary_ids_paradox_groups) + 1 == len(paradox_groups_summary_sorted)
    ids_paradox_groups_combinations = [
        list(ids_paradox_groups) + [reference_id_paradox_groups]
        for ids_paradox_groups in combinations(
            secondary_ids_paradox_groups, elements_in_combinations - 1
        )
    ]
    ids_paradox_groups_combinations_sorted = sorted(
        ids_paradox_groups_combinations,
        key=lambda x: sum(
            paradox_groups_summary_sorted.loc[list(x)][
                "float_ratio_net_income_bid_power_merit"
            ]
        ),
        reverse=True,
    )
    return ids_paradox_groups_combinations_sorted


def get_new_paradox_groups_list_adding_and_removing(
    leftout_paradox_groups_summary: pd.DataFrame,
    cleared_paradox_groups_summary: pd.DataFrame,
    iterations_df: pd.DataFrame,
    starting_paradox_groups: dict,
    paradox_groups_combinations_count: int = 1,
) -> list[dict]:
    """
    Proposes new paradox order combinations by adding left-out paradox orders to the current combination.

    This function sorts left-out paradox orders by their net income per bid power, then proposes new combinations
    by adding the most promising left-out paradox orders to the current set. It avoids combinations that have already
    been tested and can propose combinations with more than one left-out paradox order if beneficial. Returns up to
    paradox_groups_combinations_count new combinations.

    Args:
        leftout_paradox_groups_summary (pd.DataFrame): Summary DataFrame of left-out paradox orders with financial metrics.
        iterations_df (pd.DataFrame): DataFrame of all previous iteration results.
        starting_paradox_groups (dict): Dictionary of paradox order IDs with MIC in the current iteration.
        paradox_groups_combinations_count (int, optional): Maximum number of new combinations to return. Defaults to 1.

    Returns:
        pd.Series: Series of new paradox order combinations (as lists) to try in the next iterations.
    """

    paradox_groups_summary_sorted = pd.concat(
        [
            leftout_paradox_groups_summary.copy().eval(
                f"""
                float_ratio_net_income_bid_power_merit = {cols.FLOAT_RATIO_NET_INCOME_BID_POWER}
                """
            ),
            cleared_paradox_groups_summary.copy().eval(
                f"""
                float_ratio_net_income_bid_power_merit = - {cols.FLOAT_RATIO_NET_INCOME_CLEARED_POWER}
                """
            ),
        ]
    ).sort_values(by="float_ratio_net_income_bid_power_merit", ascending=False)

    # Create initial new combinations by adding single left-out paradox orders
    new_paradox_groups = []

    keep_creating_combinations = True
    rows_to_consider = 1
    created_combinations_count = 0

    while keep_creating_combinations and rows_to_consider <= len(
        paradox_groups_summary_sorted
    ):

        reference_id_paradox_groups = paradox_groups_summary_sorted.iloc[
            rows_to_consider - 1
        ].name
        max_elements_in_combinations = min(4, rows_to_consider)
        for elements_in_combinations in range(1, max_elements_in_combinations + 1):
            paradox_groups_ids_combinations_to_switch_generator = (
                get_paradox_groups_combinations(
                    paradox_groups_summary_sorted.head(rows_to_consider),
                    reference_id_paradox_groups=reference_id_paradox_groups,
                    elements_in_combinations=elements_in_combinations,
                )
            )

            for (
                paradox_groups_ids
            ) in paradox_groups_ids_combinations_to_switch_generator:
                new_iteration_ids_paradox_groups = (
                    transform_paradox_groups_dict_to_ids_list(
                        starting_paradox_groups
                    ).copy()
                )
                for id_paradox_group in paradox_groups_ids:
                    if id_paradox_group in leftout_paradox_groups_summary.index:
                        new_iteration_ids_paradox_groups.append(id_paradox_group)
                    else:
                        new_iteration_ids_paradox_groups.remove(id_paradox_group)

                are_paradox_groups_tested = check_are_paradox_groups_tested(
                    iterations_df, new_iteration_ids_paradox_groups
                )
                if not are_paradox_groups_tested:
                    new_iteration_paradox_groups = (
                        transform_ids_paradox_groups_list_to_dict(
                            new_iteration_ids_paradox_groups
                        )
                    )
                    new_paradox_groups.append(new_iteration_paradox_groups)
                    created_combinations_count += 1
                    if created_combinations_count >= paradox_groups_combinations_count:
                        keep_creating_combinations = False
                        break

        rows_to_consider += 1

    return new_paradox_groups
