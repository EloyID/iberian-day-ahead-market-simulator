import pandas as pd

from mibel_simulator import columns as cols


def check_are_paradoxal_orders_tested(
    iterations_df: pd.DataFrame, ids_paradoxal_orders_combination: list
) -> bool:
    """
    Checks if a given combination of paradox orders has already been tested in previous iterations.

    Compares the provided combination against all combinations stored in the iterations DataFrame,
    returning True if an identical combination has already been tried, and False otherwise.

    Args:
        iterations_df (pd.DataFrame): DataFrame containing results of previous iterations, including tested paradox order combinations.
        ids_paradoxal_orders_combination (list): List of paradox order IDs representing the combination to check.

    Returns:
        bool: True if the combination has already been tested, False otherwise.
    """

    for paradoxal_orders_tried in iterations_df[cols.PARADOXAL_ORDERS_COLUMN]:
        ids_paradoxal_orders_tried = transform_paradoxal_orders_dict_to_ids_list(
            paradoxal_orders_tried
        )
        if set(ids_paradoxal_orders_tried) == set(ids_paradoxal_orders_combination):
            return True
    return False


def transform_ids_paradoxal_orders_list_to_dict(
    ids_paradoxal_orders: list,
) -> dict:
    """
    Transforms a list of paradox order IDs into a dictionary separating MIC SCOs and bid blocks.

    Args:
        ids_paradoxal_orders (list): List of paradox order IDs.
    Returns:
        dict: Dictionary with get_new_paradoxal_orders_list_adding_and_removingkeys 'ids_mic_scos' and 'ids_bid_blocks' containing respective IDs.
    """
    ids_mic_scos = [id for id in ids_paradoxal_orders if "SCO" in str(id)]
    ids_bid_blocks = [id for id in ids_paradoxal_orders if "GE" in str(id)]
    assert len(ids_mic_scos) + len(ids_bid_blocks) == len(
        ids_paradoxal_orders
    ), "Error: Some paradox group IDs do not contain 'SCO' or 'GE'."
    return {
        cols.IDS_MIC_SCOS: ids_mic_scos,
        cols.IDS_BID_BLOCKS: ids_bid_blocks,
    }


def transform_paradoxal_orders_dict_to_ids_list(
    paradoxal_orders_dict: dict,
) -> list:
    """
    Transforms a dict of paradox groups into a list of paradox order IDs.

    Args:
        paradoxal_orders_dict (dict): Dictionary with keys 'ids_mic_scos' and 'ids_bid_blocks' containing respective IDs.
    Returns:
        list: List of paradox order IDs.
    """
    ids_mic_scos = paradoxal_orders_dict[cols.IDS_MIC_SCOS]
    ids_bid_blocks = paradoxal_orders_dict[cols.IDS_BID_BLOCKS]
    return ids_mic_scos + ids_bid_blocks
