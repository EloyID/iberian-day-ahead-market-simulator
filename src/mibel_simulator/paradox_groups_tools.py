import pandas as pd
from mibel_simulator import columns as cols


def check_are_paradox_groups_tested(
    trials_df: pd.DataFrame, ids_paradox_groups_combination: list
) -> bool:
    """
    Checks if a given combination of paradox orders has already been tested in previous trials.

    Compares the provided combination against all combinations stored in the trials DataFrame,
    returning True if an identical combination has already been tried, and False otherwise.

    Args:
        trials_df (pd.DataFrame): DataFrame containing results of previous trials, including tested paradox order combinations.
        ids_paradox_groups_combination (list): List of paradox order IDs representing the combination to check.

    Returns:
        bool: True if the combination has already been tested, False otherwise.
    """

    for paradox_groups_tried in trials_df[cols.PARADOX_GROUPS_COLUMN]:
        ids_paradox_groups_tried = transform_paradox_groups_dict_to_ids_list(
            paradox_groups_tried
        )
        if set(ids_paradox_groups_tried) == set(ids_paradox_groups_combination):
            return True
    return False


def transform_ids_paradox_groups_list_to_dict(
    ids_paradox_groups: list,
) -> dict:
    """
    Transforms a list of paradox order IDs into a dictionary separating MIC SCOs and bid blocks.

    Args:
        ids_paradox_groups (list): List of paradox order IDs.
    Returns:
        dict: Dictionary with get_new_paradox_groups_list_adding_and_removingkeys 'ids_mic_scos' and 'ids_bid_blocks' containing respective IDs.
    """
    ids_mic_scos = [id for id in ids_paradox_groups if "SCO" in str(id)]
    ids_bid_blocks = [id for id in ids_paradox_groups if "GE" in str(id)]
    assert len(ids_mic_scos) + len(ids_bid_blocks) == len(
        ids_paradox_groups
    ), "Error: Some paradox group IDs do not contain 'SCO' or 'GE'."
    return {
        cols.IDS_MIC_SCOS: ids_mic_scos,
        cols.IDS_BID_BLOCKS: ids_bid_blocks,
    }


def transform_paradox_groups_dict_to_ids_list(
    paradox_groups_dict: dict,
) -> list:
    """
    Transforms a dict of paradox groups into a list of paradox order IDs.

    Args:
        paradox_groups_dict (dict): Dictionary with keys 'ids_mic_scos' and 'ids_bid_blocks' containing respective IDs.
    Returns:
        list: List of paradox order IDs.
    """
    ids_mic_scos = paradox_groups_dict[cols.IDS_MIC_SCOS]
    ids_bid_blocks = paradox_groups_dict[cols.IDS_BID_BLOCKS]
    return ids_mic_scos + ids_bid_blocks
