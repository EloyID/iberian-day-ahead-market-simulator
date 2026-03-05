########################### Iterative Loop #########################


from itertools import combinations
import logging
import multiprocessing
import numpy as np
import pandas as pd
from mibel_simulator.get_new_paradox_groups_list_adding_and_removing import (
    get_new_paradox_groups_list_adding_and_removing,
)
import mibel_simulator.columns as cols
import pandera.pandas as pa
import warnings

from mibel_simulator.const import FRONTIER_MAPPING_REVERSE, TRIALS_DF_COLUMNS
from mibel_simulator.data_preprocessor import (
    get_all_paradox_groups,
    get_det_cab_for_simulation,
    get_france_det_cab_from_price,
)
from mibel_simulator.file_paths import UOF_ZONES_FILEPATH
from mibel_simulator.paradox_groups_tools import (
    check_are_paradox_groups_tested,
    transform_ids_paradox_groups_list_to_dict,
    transform_paradox_groups_dict_to_ids_list,
)
from mibel_simulator.schemas import (
    CABSchema,
    CapacidadInterPTSchema,
    ClearingPricesSchema,
    DETCABSchema,
    DETSchema,
    ExclusiveBlockOrdersGroupedSchema,
    TrialsSchema,
)
from mibel_simulator.parse_omie_files import (
    parse_cab_file,
    parse_capacidad_inter_file,
    parse_det_file,
)
from mibel_simulator.run_model import run_model
from mibel_simulator.model_info_extraction import (
    get_cleared_energy_series,
    get_clearing_prices_df,
    get_spain_portugal_transmissions,
)
from mibel_simulator.schemas.cleared_det_cab import ClearedDetCabSchema
from mibel_simulator.schemas.spain_portugal_transmissions import (
    SpainPortugaLTransmissionsSchema,
)
from mibel_simulator.tools import (
    concat_provided_uof_zones_with_existing_data,
    filter_paradox_groups_from_det_cab,
)
import pyomo.environ as pyo
from pandera.typing import DataFrame, Series

logger = logging.getLogger(__name__)


def get_cleared_paradox_groups_summary(
    det_cab_paradox_groups_filtered: pd.DataFrame,
    cleared_energy_df: pd.DataFrame,
    clearing_price_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Aggregates results for paradox orders that were matched in the trial.

    Merges DET/CAB data with cleared energy and clearing prices, computes financial metrics, and groups by order ID.

    Args:
        det_cab_paradox_groups_filtered (pd.DataFrame): DET/CAB DataFrame for paradox orders in the trial.
        cleared_energy_df (pd.DataFrame): DataFrame of cleared energy per bid.
        clearing_price_df (pd.DataFrame): DataFrame of clearing prices per period and zone.

    Returns:
        pd.DataFrame: DataFrame grouped by order ID with financial results for matched paradox orders.
    """

    cleared_det_cab = (
        det_cab_paradox_groups_filtered.merge(
            cleared_energy_df,
            left_on=cols.ID_INDIVIDUAL_BID,
            right_index=True,
            how="outer",
            validate="one_to_one",
            indicator=True,
        )
        .sort_values(
            by=[cols.INT_PERIODO, cols.CAT_BUY_SELL, cols.FLOAT_BID_POWER_CUMSUM]
        )
        .copy()
    )
    assert cleared_det_cab._merge.isin(["both", "left_only"]).all()
    cleared_det_cab = cleared_det_cab.drop(columns="_merge")

    cleared_paradox_groups_df = (
        cleared_det_cab.query(
            f"({cols.FLOAT_MIC} > 0 or {cols.INT_NUM_BLOQ} > 0) and {cols.FLOAT_CLEARED_POWER} > 0"
        )
        .copy()
        .merge(
            clearing_price_df,
            on=[cols.INT_PERIODO, cols.CAT_PAIS],
            how="left",
            validate="many_to_one",
            indicator=True,
        )
    )
    assert cleared_paradox_groups_df._merge.isin(["both"]).all()
    cleared_paradox_groups_df = cleared_paradox_groups_df.drop(columns="_merge")

    cleared_paradox_groups_df = cleared_paradox_groups_df.eval(
        f"""
        {cols.FLOAT_COLLECTION_RIGHTS} = {cols.FLOAT_CLEARED_POWER} * {cols.FLOAT_CLEARED_PRICE}
        {cols.FLOAT_VARIABLE_COST} = {cols.FLOAT_CLEARED_POWER} * {cols.FLOAT_BID_PRICE}
        """
    )
    cleared_paradox_groups_df_grouped = (
        cleared_paradox_groups_df.groupby([cols.ID_PARADOX_GROUPS], observed=True)
        .agg(
            {
                cols.FLOAT_COLLECTION_RIGHTS: "sum",
                cols.FLOAT_VARIABLE_COST: "sum",
                cols.FLOAT_MIC: "first",
                cols.FLOAT_CLEARED_POWER: "sum",
            }
        )
        .eval(
            f"""
            {cols.FLOAT_NET_INCOME} = {cols.FLOAT_COLLECTION_RIGHTS} - ( {cols.FLOAT_VARIABLE_COST} + {cols.FLOAT_MIC} )
            {cols.FLOAT_RATIO_NET_INCOME_CLEARED_POWER} = {cols.FLOAT_NET_INCOME} / {cols.FLOAT_CLEARED_POWER}            
            """
        )
    )

    return cleared_paradox_groups_df_grouped


def get_leftout_paradox_groups_summary(
    det_cab: pd.DataFrame,
    all_paradox_groups: dict,
    trial_paradox_groups: dict,
    clearing_price_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Aggregates financial results for paradox orders not included in the current trial.

    Merges left-out paradox orders with clearing prices, computes financial metrics, and groups by order ID.

    Args:
        det_cab (pd.DataFrame): Full DET/CAB DataFrame.
        all_paradox_groups (dict): Dictionary of all paradox order IDs.
        trial_paradox_groups (dict): Dictionary of paradox order IDs included in the trial.
        clearing_price_df (pd.DataFrame): DataFrame of clearing prices per period and zone.

    Returns:
        pd.DataFrame: DataFrame grouped by order ID with financial results for left-out paradox orders.
    """

    det_cab = det_cab.copy().merge(
        clearing_price_df,
        on=[cols.INT_PERIODO, cols.CAT_PAIS],
        how="left",
        validate="many_to_one",
    )

    all_scos = all_paradox_groups[cols.IDS_MIC_SCOS]
    trial_scos = trial_paradox_groups[cols.IDS_MIC_SCOS]
    left_out_scos = set(all_scos) - set(trial_scos)

    all_bid_blocks = all_paradox_groups[cols.IDS_BID_BLOCKS]
    trial_bid_blocks = trial_paradox_groups[cols.IDS_BID_BLOCKS]
    left_out_bid_blocks = set(all_bid_blocks) - set(trial_bid_blocks)

    det_cab_scos = (
        det_cab.query(f"{cols.ID_SCO} in @left_out_scos")
        .assign(
            **{
                cols.FLOAT_MAXIMIZED_COMPETITIVE_BID_POWER: lambda df: np.where(
                    df[cols.FLOAT_CLEARED_PRICE] >= df[cols.FLOAT_BID_PRICE],
                    df[cols.FLOAT_BID_POWER],
                    df[cols.FLOAT_MAV],
                ),
            }
        )
        .eval(
            f"""
            {cols.FLOAT_FIX_COST} = {cols.FLOAT_MIC}
            {cols.FLOAT_VARIABLE_COST} = {cols.FLOAT_BID_PRICE}
            """
        )
    )
    det_cab_bid_blocks = (
        det_cab.query(f" {cols.ID_BLOCK_ORDER} in @left_out_bid_blocks")
        .assign(
            **{
                cols.FLOAT_MAXIMIZED_COMPETITIVE_BID_POWER: lambda df: np.where(
                    df[cols.FLOAT_CLEARED_PRICE] >= df[cols.FLOAT_BID_PRICE],
                    df[cols.FLOAT_BID_POWER],
                    df[cols.FLOAT_MAR] * df[cols.FLOAT_BID_POWER],
                ),
            }
        )
        .eval(
            f"""
            {cols.FLOAT_FIX_COST} = 0
            {cols.FLOAT_VARIABLE_COST} = {cols.FLOAT_BID_PRICE}
            """
        )
    )

    det_cab_paradox_groups = (
        pd.concat(
            [
                det_cab_scos,
                det_cab_bid_blocks,
            ],
            ignore_index=True,
        )
        .eval(
            f"""
            {cols.FLOAT_COLLECTION_RIGHTS} = {cols.FLOAT_MAXIMIZED_COMPETITIVE_BID_POWER} * {cols.FLOAT_CLEARED_PRICE}
            {cols.FLOAT_TOTAL_VARIABLE_COST} = {cols.FLOAT_MAXIMIZED_COMPETITIVE_BID_POWER} * {cols.FLOAT_VARIABLE_COST}
            """
        )
        .groupby([cols.ID_PARADOX_GROUPS], observed=True)
        .agg(
            {
                cols.FLOAT_COLLECTION_RIGHTS: "sum",
                cols.FLOAT_TOTAL_VARIABLE_COST: "sum",
                cols.FLOAT_FIX_COST: "first",
                cols.FLOAT_MAXIMIZED_COMPETITIVE_BID_POWER: "sum",
            }
        )
        .eval(
            f"""
            {cols.FLOAT_NET_INCOME} = {cols.FLOAT_COLLECTION_RIGHTS} - ( {cols.FLOAT_TOTAL_VARIABLE_COST} + {cols.FLOAT_FIX_COST} )
            {cols.FLOAT_RATIO_NET_INCOME_BID_POWER} = {cols.FLOAT_NET_INCOME} / {cols.FLOAT_MAXIMIZED_COMPETITIVE_BID_POWER}
            """
        )
    )

    assert det_cab_paradox_groups[cols.FLOAT_NET_INCOME].notna().all()
    assert det_cab_paradox_groups[cols.FLOAT_RATIO_NET_INCOME_BID_POWER].notna().all()
    return det_cab_paradox_groups


#### ITERATIVE LOOP


def sort_trials_df_by_most_promising(trials_df: pd.DataFrame) -> pd.DataFrame:
    """
    Sorts the trials DataFrame to prioritize the most promising SCO combinations.
    Trials with successful status are sorted by objective value and SCO count; unsuccessful trials are sorted by SCO count and objective value.

    Args:
        trials_df (pd.DataFrame): DataFrame containing results of previous trials.

    Returns:
        pd.DataFrame: Sorted DataFrame of trials.
    """

    trials_df_status_false = trials_df.query(
        f"{cols.BOOL_IS_EXPECTED_INCOME_RESPECTED} == False"
    )
    trials_df_status_true = trials_df.query(
        f"{cols.BOOL_IS_EXPECTED_INCOME_RESPECTED} == True"
    )
    sorted_promising_trials_df = pd.concat(
        [
            trials_df_status_true.sort_values(
                by=[
                    cols.FLOAT_OBJECTIVE_VALUE,
                    cols.INT_PARADOX_GROUPS_COUNT,
                ],
                ascending=[False, True],
            ),
            trials_df_status_false.sort_values(
                by=[
                    cols.INT_PARADOX_GROUPS_COUNT,
                    cols.FLOAT_OBJECTIVE_VALUE,
                ],
                ascending=[True, False],
            ),
        ],
        ignore_index=True,
    )
    return sorted_promising_trials_df


def get_best_trial(
    trials_df: pd.DataFrame, mic_respected_only: bool = True
) -> pd.Series:
    """
    Selects and returns the best trial from the trials DataFrame.

    If mic_respected_only is True, only considers trials where the MIC constraint is respected.
    The best trial is determined by sorting for the highest objective value and, in case of ties,
    the lowest number of SCOs with MIC. If mic_respected_only is False, considers all trials.

    Args:
        trials_df (pd.DataFrame): DataFrame containing results of all trials.
        mic_respected_only (bool, optional): If True, only consider trials where MIC is respected. Defaults to True.

    Returns:
        pd.Series: The row of the best trial in the DataFrame.
    """
    if mic_respected_only:
        trials_df = trials_df.query(f"{cols.BOOL_IS_EXPECTED_INCOME_RESPECTED} == True")
    sorted_trials_df = sort_trials_df_by_most_promising(trials_df)
    return sorted_trials_df.iloc[0]


def get_new_paradox_groups_list_by_removing_underperforming_ones(
    trial_cleared_paradox_groups_summary: pd.DataFrame,
    trials_df: pd.DataFrame,
    paradox_groups_combination: dict,
    int_paradox_groups_count: int = 1,
) -> pd.Series:
    """
    Proposes new paradox order combinations by removing underperforming paradox orders from the current combination.

    This function identifies paradox orders with negative net income per cleared power, removes them one by one from the current combination,
    and checks if the resulting combinations have already been tested. Returns up to int_paradox_groups_count new combinations that have not been tested yet.

    Args:
        trial_cleared_paradox_groups_summary (pd.DataFrame): Summary DataFrame of cleared paradox orders for the current trial.
        trials_df (pd.DataFrame): DataFrame of all previous trial results.
        paradox_groups_combination (dict): Dictionary of paradox orders in the current trial.
        int_paradox_groups_count (int, optional): Maximum number of new combinations to return. Defaults to 1.

    Returns:
        pd.Series: Series of new paradox order combinations (as lists) to try in the next trials.
    """
    trial_cleared_paradox_groups_summary = trial_cleared_paradox_groups_summary.query(
        f"{cols.FLOAT_RATIO_NET_INCOME_CLEARED_POWER} < 0"
    ).sort_values(by=cols.FLOAT_RATIO_NET_INCOME_CLEARED_POWER, ascending=True)
    new_paradox_groups_df = pd.DataFrame(
        {
            cols.PARADOX_GROUPS_COLUMN: np.nan,
            cols.BOOL_ARE_PARADOX_GROUPS_TESTED: np.nan,
        },
        index=trial_cleared_paradox_groups_summary.index,
    ).astype(
        {cols.PARADOX_GROUPS_COLUMN: object, cols.BOOL_ARE_PARADOX_GROUPS_TESTED: bool}
    )

    paradox_group_ids_left = transform_paradox_groups_dict_to_ids_list(
        paradox_groups_combination
    )

    for index, row in new_paradox_groups_df.iterrows():
        new_trial_paradox_groups = list(set(paradox_group_ids_left) - set([index]))
        are_paradox_groups_tested = check_are_paradox_groups_tested(
            trials_df, new_trial_paradox_groups
        )

        new_paradox_groups_df.at[index, cols.PARADOX_GROUPS_COLUMN] = (
            new_trial_paradox_groups
        )
        new_paradox_groups_df.at[index, cols.BOOL_ARE_PARADOX_GROUPS_TESTED] = (
            are_paradox_groups_tested
        )

        paradox_group_ids_left = new_trial_paradox_groups

    new_paradox_groups_df = new_paradox_groups_df.query(
        f"{cols.BOOL_ARE_PARADOX_GROUPS_TESTED} == False"
    ).head(int_paradox_groups_count)

    return (
        new_paradox_groups_df[cols.PARADOX_GROUPS_COLUMN]
        .apply(transform_ids_paradox_groups_list_to_dict)
        .tolist()
    )


def define_new_paradox_groups_list(
    trials_df: pd.DataFrame,
    det_cab: pd.DataFrame,
    all_paradox_groups: dict,
    int_paradox_groups_count: int = 1,
) -> list[dict]:
    """
    Defines a new combination of paradox groups with MIC to try in the next trial.

    Based on previous trial results, either adds promising left-out paradox groups or removes underperforming ones, ensuring combinations are not repeated.

    Args:
        trials_df (pd.DataFrame): DataFrame of previous trial results.
        det_cab (pd.DataFrame): Full DET/CAB DataFrame.
        all_paradox_groups (dict): Dictionary of all paradox groups with MIC.
        int_paradox_groups_count (int, optional): Maximum number of MIC paradox group combinations to propose. Defaults to 1.

    Returns:
        list[dict]: List of paradox groups for the next trial.
    """

    # Get the most promising trial
    sorted_promising_trials_df = sort_trials_df_by_most_promising(trials_df)
    int_paradox_groups_count_start = int_paradox_groups_count
    new_paradox_groups_list = []
    for index, row in sorted_promising_trials_df.iterrows():

        logger.info(
            f"--ALGORITHM--: Most promising combination: {row[cols.PARADOX_GROUPS_COLUMN]}"
        )

        cleared_energy = row[cols.CLEARED_ENERGY_COLUMN]
        clearing_prices = row[cols.CLEARING_PRICES_COLUMN]
        paradox_groups = row[cols.PARADOX_GROUPS_COLUMN]
        is_expected_income_respected = row[cols.BOOL_IS_EXPECTED_INCOME_RESPECTED]

        if is_expected_income_respected:
            logger.info("--ALGORITHM--: MIC is respected")
            leftout_paradox_groups_summary = get_leftout_paradox_groups_summary(
                det_cab, all_paradox_groups, paradox_groups, clearing_prices
            ).sort_values(by=cols.FLOAT_RATIO_NET_INCOME_BID_POWER, ascending=False)
            det_cab_paradox_groups_filtered = filter_paradox_groups_from_det_cab(
                det_cab, paradox_groups
            )
            trial_cleared_paradox_groups_summary = get_cleared_paradox_groups_summary(
                det_cab_paradox_groups_filtered,
                cleared_energy,
                clearing_prices,
            )
            new_paradox_groups_list.extend(
                get_new_paradox_groups_list_adding_and_removing(
                    leftout_paradox_groups_summary,
                    trial_cleared_paradox_groups_summary,
                    trials_df,
                    paradox_groups,
                    int_paradox_groups_count,
                )
            )

        else:
            det_cab_paradox_groups_filtered = filter_paradox_groups_from_det_cab(
                det_cab, paradox_groups
            )
            trial_cleared_paradox_groups_summary = get_cleared_paradox_groups_summary(
                det_cab_paradox_groups_filtered,
                cleared_energy,
                clearing_prices,
            )
            new_paradox_groups_list.extend(
                get_new_paradox_groups_list_by_removing_underperforming_ones(
                    trial_cleared_paradox_groups_summary,
                    trials_df,
                    paradox_groups,
                    int_paradox_groups_count,
                )
            )

        int_paradox_groups_count = int_paradox_groups_count_start - len(
            new_paradox_groups_list
        )
        if int_paradox_groups_count <= 0:
            break

    return new_paradox_groups_list


def iterative_function(
    args: tuple[
        pd.DataFrame,
        pd.DataFrame,
        list,
        pd.Series | None,
    ],
) -> pd.DataFrame:
    """
    Runs a single market clearing trial for a given combination of SCOs with MIC.

    This function filters the DET/CAB data for the current SCOs with MIC, runs the market model,
    extracts relevant results, and returns a DataFrame summarizing the trial.

    Args:
        args (tuple): Tuple containing:
            - det_cab (pd.DataFrame): Full DET/CAB DataFrame.
            - capacidad_inter_PT_date (pd.DataFrame): DataFrame of interconnection capacities for Portugal.
            - paradox_groups (list): List of SCO order IDs with MIC for this trial.
            - france_fixed_exchange (pd.Series, optional): Series with fixed exchange values for France. Defaults to None.

    Returns:
        pd.DataFrame: DataFrame with the results of the trial (one row).
    """

    (
        det_cab,
        capacidad_inter_PT_date,
        paradox_groups,
        france_fixed_exchange,
    ) = args

    # Keep only SCOs in the current trial
    det_cab_paradox_groups_filtered = filter_paradox_groups_from_det_cab(
        det_cab, paradox_groups
    )

    # Run market model
    model, _, results = run_model(
        det_cab_paradox_groups_filtered,
        capacidad_inter_PT_date,
        france_fixed_exchange,
    )

    # Extract information from the model
    cleared_energy = get_cleared_energy_series(model)
    clearing_prices = get_clearing_prices_df(model)
    cleared_paradox_groups_summary = get_cleared_paradox_groups_summary(
        det_cab_paradox_groups_filtered, cleared_energy, clearing_prices
    )
    welfare = pyo.value(model.OBJ)
    bool_is_expected_income_respected = (
        cleared_paradox_groups_summary[cols.FLOAT_NET_INCOME] >= 0
    ).all()

    ids_mic_scos = paradox_groups[cols.IDS_MIC_SCOS]
    ids_bid_blocks = paradox_groups[cols.IDS_BID_BLOCKS]

    # Update trials_df with current trial results
    trial_df_entry = {
        cols.PARADOX_GROUPS_COLUMN: [paradox_groups],
        cols.IDS_MIC_SCOS: [ids_mic_scos],
        cols.IDS_BID_BLOCKS: [ids_bid_blocks],
        cols.IDS_PARADOX_GROUPS: [ids_mic_scos + ids_bid_blocks],
        cols.FLOAT_OBJECTIVE_VALUE: [welfare],
        cols.BOOL_IS_EXPECTED_INCOME_RESPECTED: [bool_is_expected_income_respected],
        cols.SOLVER_RESULTS_COLUMN: [results],
        cols.INT_MIC_SCOS_COUNT: [len(ids_mic_scos)],
        cols.INT_BID_BLOCKS_COUNT: [len(ids_bid_blocks)],
        cols.INT_PARADOX_GROUPS_COUNT: [len(ids_mic_scos) + len(ids_bid_blocks)],
        cols.CLEARED_ENERGY_COLUMN: [cleared_energy],
        cols.CLEARING_PRICES_COLUMN: [clearing_prices],
        cols.SPAIN_PORTUGAL_TRANSMISSIONS_COLUMN: [
            get_spain_portugal_transmissions(model)
        ],
    }

    return pd.DataFrame(trial_df_entry)


def check_if_success_at_first_trial(
    first_trial_df: pd.Series,
    is_trials_df_provided: bool,
    is_trial_paradox_groups_provided: bool,
) -> bool:
    """
    Checks if the first trial was successful, i.e., all SCOs with MIC were correctly cleared on the first attempt.

    Args:
        results (list): List of DataFrames with trial results, where each DataFrame contains the results of a single trial.
        is_trials_df_provided (bool): Indicates if a previous trials DataFrame was provided (i.e., not the first run).
        is_trial_paradox_groups_provided (bool): Indicates if an initial paradox orders combination was provided.

    Returns:
        bool: True if the first trial was successful and it was the initial run (no previous trials or initial combination provided), False otherwise.
    """
    success_at_first_trial = (
        not is_trials_df_provided
        and not is_trial_paradox_groups_provided
        and first_trial_df[cols.BOOL_IS_EXPECTED_INCOME_RESPECTED]
    )
    if success_at_first_trial:
        logger.info(
            "--ALGORITHM--: First trial with all SCOs correctly cleared, finishing"
        )
    return success_at_first_trial


@pa.check_input(DETCABSchema, "det_cab", lazy=True)
@pa.check_input(CapacidadInterPTSchema, "capacidad_inter_pt_date", lazy=True)
def run_iterative_loop(
    det_cab: DataFrame,
    capacidad_inter_pt_date: DataFrame,
    france_fixed_exchange: pd.Series | None = None,
    trials_count: int = 100,
    trial_ids_mic_scos: list | None = None,
    trial_ids_bid_blocks: list | None = None,
    trials_df: pd.DataFrame | None = None,
    n_jobs: int = 1,
) -> tuple[pd.DataFrame, pyo.ConcreteModel, pyo.ConcreteModel]:
    """
    Runs the iterative clearing process, optimizing combinations of SCOs with MIC.

    For each trial, filters SCOs, runs the market model, collects results, and updates the trial DataFrame. Continues until all combinations are tested or a successful result is found.

    Args:
        det_cab (pd.DataFrame): Full DET/CAB DataFrame.
        capacidad_inter_pt_date (pd.DataFrame): DataFrame of interconnection capacities for Portugal.
        france_fixed_exchange (pd.Series, optional): Series with fixed exchange values for France. Defaults to None.
        trial_ids_mic_scos (list, optional): Initial SCOs with MIC for the first trial.
        trial_ids_bid_blocks (list, optional): Initial bid blocks for the first trial.
        trials_df (pd.DataFrame, optional): Existing trials DataFrame to continue from.

    Returns:
        tuple: (trials_df, best_model, best_model_binary)
            trials_df (pd.DataFrame): DataFrame of all trial results.
            best_model: Pyomo model object for the best trial.
            best_model_binary: Pyomo model object for the best trial (binary version).
    """

    is_trials_df_provided = trials_df is not None
    is_trial_ids_mic_scos_provided = trial_ids_mic_scos is not None
    is_trial_ids_bid_blocks_provided = trial_ids_bid_blocks is not None
    if is_trial_ids_mic_scos_provided != is_trial_ids_bid_blocks_provided:
        raise ValueError(
            "Both trial_ids_mic_scos and trial_ids_bid_blocks must be provided together or none at all."
        )
    are_trial_ids_paradox_groups_provided = (
        is_trial_ids_mic_scos_provided and is_trial_ids_bid_blocks_provided
    )

    all_paradox_groups = get_all_paradox_groups(det_cab)

    if trials_count // n_jobs < 5 and not is_trials_df_provided:
        logger.warning(
            "--ALGORITHM--: It is recommended trials_count to be at least 5 times n_jobs."
        )

    #### INITIALIZATION ####
    # Initialize trials_df if not provided
    if trials_df is None:
        trials_df = pd.DataFrame(columns=TRIALS_DF_COLUMNS)

    # If trial_paradox_groups is provided, start with that combination
    if are_trial_ids_paradox_groups_provided:
        next_trials_paradox_groups = [
            {
                cols.IDS_MIC_SCOS: trial_ids_mic_scos,
                cols.IDS_BID_BLOCKS: trial_ids_bid_blocks,
            }
        ]
    # If trials_df is defined, define a new combination based on previous trials
    elif not trials_df.empty:
        next_trials_paradox_groups = define_new_paradox_groups_list(
            trials_df, det_cab, all_paradox_groups, min(n_jobs, trials_count)
        )
        if next_trials_paradox_groups is False:
            logger.info("--ALGORITHM--: All combinations tried, finishing")
            return trials_df
    # Otherwise start with all paradox orders
    else:
        next_trials_paradox_groups = [all_paradox_groups]

    #### ITERATIVE LOOP ####

    completed_trials = 0

    while completed_trials < trials_count:

        args_list = [
            (
                det_cab,
                capacidad_inter_pt_date,
                trial_paradox_groups,
                france_fixed_exchange,
            )
            for trial_paradox_groups in next_trials_paradox_groups
        ]
        with multiprocessing.Pool(processes=n_jobs) as pool:
            results = pool.map(iterative_function, args_list)
            completed_trials += len(results)
            trials_df = pd.concat([trials_df] + results, ignore_index=True)

            if check_if_success_at_first_trial(
                trials_df.iloc[0],
                is_trials_df_provided,
                are_trial_ids_paradox_groups_provided,
            ):
                logger.info("--ALGORITHM--: Success at first trial, finishing")
                break

            ### hasta aqui
        new_trial_int_paradox_groups_count = min(
            n_jobs, trials_count - completed_trials
        )
        logger.info(
            f"--ALGORITHM--: Completed trials: {completed_trials}/{trials_count}"
        )
        next_trials_paradox_groups = define_new_paradox_groups_list(
            trials_df,
            det_cab,
            all_paradox_groups,
            new_trial_int_paradox_groups_count,
        )
        # TODO: this is a quickfix so the loop ends when no new combinations are found
        # but it should iterate with other options too
        if len(next_trials_paradox_groups) == 0:
            break

    try:
        TrialsSchema.validate(trials_df, lazy=True)
    except pa.errors.SchemaErrors as e:
        warnings.warn(f"Pandera validation warning: {e}")

    best_trial = get_best_trial(trials_df, mic_respected_only=False)

    det_cab_scos_filtered = filter_paradox_groups_from_det_cab(
        det_cab, best_trial[cols.PARADOX_GROUPS_COLUMN]
    )

    # Run market model
    best_model, best_model_binary, results = run_model(
        det_cab=det_cab_scos_filtered,
        capacidad_inter_PT_date=capacidad_inter_pt_date,
        france_fixed_exchange=france_fixed_exchange,
    )

    return trials_df, best_model, best_model_binary


def run_mibel_simulator(
    det: pd.DataFrame | str,
    cab: pd.DataFrame | str,
    capacidad_inter_date: pd.DataFrame | str,
    price_france_date: pd.DataFrame,
    uof_zones: pd.DataFrame | None = None,
    trials_count: int = 100,
    starting_trials_df: pd.DataFrame = None,
    france_fixed_exchange: pd.Series | None = None,
    zones_default_to_spain: bool = False,
    trial_ids_mic_scos: list | None = None,
    trial_ids_bid_blocks: list | None = None,
    n_jobs: int = 1,
) -> dict:
    """
    Runs the full MIBEL clearing process for a given day, including data
    preprocessing, structure building, and iterative optimization.

    This function prepares all necessary data structures from the provided DET,
    CAB, UOF zones, interconnection capacity, and France price data. It then
    runs the iterative clearing process, optimizing combinations of SCOs
    with MIC, and returns the results and best models.

    Args:
        det (pd.DataFrame | str): DET DataFrame for the studied day or path to DET file.
        cab (pd.DataFrame | str): CAB DataFrame for the studied day or path to CAB file.
        capacidad_inter_date (pd.DataFrame | str): DataFrame of interconnection capacities for the studied day or path to file.
        price_france_date (pd.DataFrame): DataFrame of France prices for the studied day.
        uof_zones (pd.DataFrame | None): DataFrame mapping units to zones.
        trials_count (int, optional): Maximum number of optimization trials to run. Defaults to 100.
        starting_trials_df (pd.DataFrame, optional): Existing trials DataFrame to continue from. Defaults to None.
        france_fixed_exchange (pd.Series, optional): Series with fixed exchange values for France. Defaults to None.
        zones_default_to_spain (bool, optional): Whether the missing uof zones are assumed to be Spain. Defaults to False.
        trial_ids_mic_scos (list | None, optional): List of MIC SCOs for trial. Defaults to None.
        trial_ids_bid_blocks (list | None, optional): List of bid blocks for trial. Defaults to None.
        n_jobs (int, optional): Number of parallel jobs to run. Defaults to 1.
    Returns:
        tuple: (trials_df, best_model, best_model_binary)
            trials_df (pd.DataFrame): DataFrame of all trial results.
            best_model (pyo.ConcreteModel): Pyomo model object for the best trial.
            best_model_binary (pyo.ConcreteModel): Pyomo model object for the best trial (binary version).
    """

    if isinstance(det, str):
        det = parse_det_file(det)
    if isinstance(cab, str):
        cab = parse_cab_file(cab)
    if isinstance(capacidad_inter_date, str):
        capacidad_inter_date = parse_capacidad_inter_file(capacidad_inter_date)

    DETSchema.validate(det, lazy=True)
    CABSchema.validate(cab, lazy=True)
    CapacidadInterPTSchema.validate(capacidad_inter_date, lazy=True)

    if isinstance(uof_zones, pd.DataFrame):
        uof_zones = concat_provided_uof_zones_with_existing_data(uof_zones)
    else:
        uof_zones = pd.read_csv(UOF_ZONES_FILEPATH)

    capacidad_inter_pt_date = capacidad_inter_date.query(
        f"{cols.CAT_FRONTIER} == {FRONTIER_MAPPING_REVERSE['PT']}"
    )
    det_cab_fr_date = get_france_det_cab_from_price(
        price_france_date, capacidad_inter_date
    )
    det_cab = get_det_cab_for_simulation(
        det=det,
        cab=cab,
        uof_zones=uof_zones,
        det_cab_fr_date=det_cab_fr_date,
        zones_default_to_spain=zones_default_to_spain,
    )

    trials_df, model, model_binary = run_iterative_loop(
        det_cab=det_cab,
        capacidad_inter_pt_date=capacidad_inter_pt_date,
        trials_count=trials_count,
        trials_df=starting_trials_df,
        trial_ids_mic_scos=trial_ids_mic_scos,
        trial_ids_bid_blocks=trial_ids_bid_blocks,
        france_fixed_exchange=france_fixed_exchange,
        n_jobs=n_jobs,
    )

    best_trial = get_best_trial(trials_df, mic_respected_only=False)
    cleared_energy = best_trial.cleared_energy
    cleared_det_cab = det_cab.merge(
        cleared_energy,
        left_on=cols.ID_INDIVIDUAL_BID,
        right_index=True,
        how="outer",
        validate="one_to_one",
        indicator=True,
    )

    try:
        ClearedDetCabSchema.validate(cleared_det_cab, lazy=True)
    except pa.errors.SchemaErrors as e:
        warnings.warn(f"Pandera validation warning in ClearedDetCabSchema: {e}")
    try:
        ClearingPricesSchema.validate(best_trial.clearing_prices, lazy=True)
    except pa.errors.SchemaErrors as e:
        warnings.warn(f"Pandera validation warning in ClearingPricesSchema: {e}")
    try:
        SpainPortugaLTransmissionsSchema.validate(
            best_trial.spain_portugal_transmissions, lazy=True
        )
    except pa.errors.SchemaErrors as e:
        warnings.warn(
            f"Pandera validation warning in SpainPortugaLTransmissionsSchema: {e}"
        )

    results_dict = {
        "model_binary_fixed": model,
        "model_binary_not_fixed": model_binary,
        "model_trial_info": best_trial,
        "cleared_det_cab": cleared_det_cab,
        "clearing_prices": best_trial.clearing_prices,
        "spain_portugal_transmissions": best_trial.spain_portugal_transmissions,
        "trials_df": trials_df,
    }

    return results_dict
