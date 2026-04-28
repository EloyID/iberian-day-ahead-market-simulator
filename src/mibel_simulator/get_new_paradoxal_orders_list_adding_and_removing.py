from itertools import combinations

import pandas as pd

from iberian_day_ahead_market_simulator import columns as cols
from iberian_day_ahead_market_simulator.paradoxal_orders_tools import (
    check_are_paradoxal_orders_tested,
    transform_ids_paradoxal_orders_list_to_dict,
    transform_paradoxal_orders_dict_to_ids_list,
)


def get_paradoxal_orders_combinations(
    paradoxal_orders_summary_sorted: pd.DataFrame,
    reference_id_paradoxal_orders: str,
    elements_in_combinations: int = 4,
):
    """ """
    secondary_ids_paradoxal_orders = paradoxal_orders_summary_sorted.query(
        "index != @reference_id_paradoxal_orders"
    ).index.to_list()
    assert len(secondary_ids_paradoxal_orders) >= elements_in_combinations - 1, (
        "Not enough paradox groups to create combinations. "
        f"Available: {len(secondary_ids_paradoxal_orders)}, "
        f"Requested: {elements_in_combinations}."
    )
    assert len(secondary_ids_paradoxal_orders) + 1 == len(
        paradoxal_orders_summary_sorted
    )
    ids_paradoxal_orders_combinations = [
        list(ids_paradoxal_orders) + [reference_id_paradoxal_orders]
        for ids_paradoxal_orders in combinations(
            secondary_ids_paradoxal_orders, elements_in_combinations - 1
        )
    ]
    ids_paradoxal_orders_combinations_sorted = sorted(
        ids_paradoxal_orders_combinations,
        key=lambda x: sum(
            paradoxal_orders_summary_sorted.loc[list(x)][
                "float_ratio_net_income_bid_power_merit"
            ]
        ),
        reverse=True,
    )
    return ids_paradoxal_orders_combinations_sorted


def get_new_paradoxal_orders_list_adding_and_removing(
    leftout_paradoxal_orders_summary: pd.DataFrame,
    cleared_paradoxal_orders_summary: pd.DataFrame,
    iterations_df: pd.DataFrame,
    starting_paradoxal_orders: dict,
    paradoxal_orders_combinations_count: int = 1,
) -> list[dict]:
    """
    Proposes new paradox order combinations by adding left-out paradox orders to the current combination.

    This function sorts left-out paradox orders by their net income per bid power, then proposes new combinations
    by adding the most promising left-out paradox orders to the current set. It avoids combinations that have already
    been tested and can propose combinations with more than one left-out paradox order if beneficial. Returns up to
    paradoxal_orders_combinations_count new combinations.

    Args:
        leftout_paradoxal_orders_summary (pd.DataFrame): Summary DataFrame of left-out paradox orders with financial metrics.
        iterations_df (pd.DataFrame): DataFrame of all previous iteration results.
        starting_paradoxal_orders (dict): Dictionary of paradox order IDs with MIC in the current iteration.
        paradoxal_orders_combinations_count (int, optional): Maximum number of new combinations to return. Defaults to 1.

    Returns:
        pd.Series: Series of new paradox order combinations (as lists) to try in the next iterations.
    """

    paradoxal_orders_summary_sorted = pd.concat(
        [
            leftout_paradoxal_orders_summary.copy().eval(
                f"""
                float_ratio_net_income_bid_power_merit = {cols.FLOAT_RATIO_NET_INCOME_BID_POWER}
                """
            ),
            cleared_paradoxal_orders_summary.copy().eval(
                f"""
                float_ratio_net_income_bid_power_merit = - {cols.FLOAT_RATIO_NET_INCOME_CLEARED_POWER}
                """
            ),
        ]
    ).sort_values(by="float_ratio_net_income_bid_power_merit", ascending=False)

    # Create initial new combinations by adding single left-out paradox orders
    new_paradoxal_orders = []

    keep_creating_combinations = True
    rows_to_consider = 1
    created_combinations_count = 0

    while keep_creating_combinations and rows_to_consider <= len(
        paradoxal_orders_summary_sorted
    ):

        reference_id_paradoxal_orders = paradoxal_orders_summary_sorted.iloc[
            rows_to_consider - 1
        ].name
        max_elements_in_combinations = min(4, rows_to_consider)
        for elements_in_combinations in range(1, max_elements_in_combinations + 1):
            paradoxal_orders_ids_combinations_to_switch_generator = (
                get_paradoxal_orders_combinations(
                    paradoxal_orders_summary_sorted.head(rows_to_consider),
                    reference_id_paradoxal_orders=reference_id_paradoxal_orders,
                    elements_in_combinations=elements_in_combinations,
                )
            )

            for (
                paradoxal_orders_ids
            ) in paradoxal_orders_ids_combinations_to_switch_generator:
                new_iteration_ids_paradoxal_orders = (
                    transform_paradoxal_orders_dict_to_ids_list(
                        starting_paradoxal_orders
                    ).copy()
                )
                for id_paradoxal_order in paradoxal_orders_ids:
                    if id_paradoxal_order in leftout_paradoxal_orders_summary.index:
                        new_iteration_ids_paradoxal_orders.append(id_paradoxal_order)
                    else:
                        new_iteration_ids_paradoxal_orders.remove(id_paradoxal_order)

                are_paradoxal_orders_tested = check_are_paradoxal_orders_tested(
                    iterations_df, new_iteration_ids_paradoxal_orders
                )
                if not are_paradoxal_orders_tested:
                    new_iteration_paradoxal_orders = (
                        transform_ids_paradoxal_orders_list_to_dict(
                            new_iteration_ids_paradoxal_orders
                        )
                    )
                    new_paradoxal_orders.append(new_iteration_paradoxal_orders)
                    created_combinations_count += 1
                    if (
                        created_combinations_count
                        >= paradoxal_orders_combinations_count
                    ):
                        keep_creating_combinations = False
                        break

        rows_to_consider += 1

    return new_paradoxal_orders
