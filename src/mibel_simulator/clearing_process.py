########################### Iterative Loop #########################


from itertools import combinations
import logging
import multiprocessing
import numpy as np
import pandas as pd
import mibel_simulator.columns as cols
import pandera.pandas as pa
import warnings

from mibel_simulator.const import FRONTIER_MAPPING_REVERSE, TRIALS_DF_COLUMNS
from mibel_simulator.data_preprocessor import (
    get_all_mic_scos,
    get_det_cab_date_for_simulation,
    get_exclusive_block_orders_grouped,
    get_france_det_cab_date_from_price,
)
from mibel_simulator.file_paths import UOF_ZONES_FILEPATH
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
    filter_mic_scos_from_det_cab,
)
import pyomo.environ as pyo
from pandera.typing import DataFrame, Series

logger = logging.getLogger(__name__)


def check_are_mic_scos_tested(trials_df: pd.DataFrame, scos_combination: list) -> bool:
    """
    Checks if a given combination of SCOs with MIC has already been tested in previous trials.

    Compares the provided combination against all combinations stored in the trials DataFrame,
    returning True if an identical combination has already been tried, and False otherwise.

    Args:
        trials_df (pd.DataFrame): DataFrame containing results of previous trials, including tested SCO combinations.
        scos_combination (list): List of SCO order IDs with MIC representing the combination to check.

    Returns:
        bool: True if the combination has already been tested, False otherwise.
    """

    for scos_tried in trials_df[cols.MIC_SCOS_COLUMN]:
        if set(scos_tried) == set(scos_combination):
            return True
    return False


def get_trial_cleared_mic_scos_summary(
    det_cab_date: pd.DataFrame,
    cleared_energy_df: pd.DataFrame,
    clearing_price_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Aggregates results for SCO orders that were matched in the trial.

    Merges DET/CAB data with cleared energy and clearing prices, computes financial metrics, and groups by order ID.

    Args:
        det_cab_date (pd.DataFrame): DET/CAB DataFrame for SCOs in the trial.
        cleared_energy_df (pd.DataFrame): DataFrame of cleared energy per bid.
        clearing_price_df (pd.DataFrame): DataFrame of clearing prices per period and zone.

    Returns:
        pd.DataFrame: DataFrame grouped by order ID with financial results for matched SCOs.
    """

    cleared_det_cab_date = (
        det_cab_date.merge(
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
    assert cleared_det_cab_date._merge.isin(["both", "left_only"]).all()
    cleared_det_cab_date = cleared_det_cab_date.drop(columns="_merge")

    cleared_mic_scos_df = (
        cleared_det_cab_date.query(
            f"{cols.FLOAT_MIC} > 0 and {cols.FLOAT_CLEARED_POWER} > 0"
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
    assert cleared_mic_scos_df._merge.isin(["both"]).all()
    cleared_mic_scos_df = cleared_mic_scos_df.drop(columns="_merge")

    cleared_mic_scos_df = cleared_mic_scos_df.eval(
        f"""
        {cols.FLOAT_COLLECTION_RIGHTS} = {cols.FLOAT_CLEARED_POWER} * {cols.FLOAT_CLEARED_PRICE}
        {cols.FLOAT_VARIABLE_COST} = {cols.FLOAT_CLEARED_POWER} * {cols.FLOAT_BID_PRICE}
        """
    )
    cleared_mic_scos_df_grouped = (
        cleared_mic_scos_df.groupby([cols.ID_ORDER], observed=True)
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

    return cleared_mic_scos_df_grouped


def get_leftout_mic_scos_summary(
    det_cab_date: pd.DataFrame,
    all_scos: list,
    trial_scos: list,
    clearing_price_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Aggregates financial results for SCOs not included in the current trial.

    Merges left-out SCOs with clearing prices, computes financial metrics, and groups by order ID.

    Args:
        det_cab_date (pd.DataFrame): Full DET/CAB DataFrame.
        all_scos (list): List of all SCO order IDs.
        trial_scos (list): List of SCO order IDs included in the trial.
        clearing_price_df (pd.DataFrame): DataFrame of clearing prices per period and zone.

    Returns:
        pd.DataFrame: DataFrame grouped by order ID with financial results for left-out SCOs.
    """

    left_out_scos = set(all_scos) - set(trial_scos)
    return (
        det_cab_date.query(f"{cols.ID_ORDER} in @left_out_scos")
        .merge(
            clearing_price_df,
            on=[cols.INT_PERIODO, cols.CAT_PAIS],
            how="left",
            validate="many_to_one",
        )
        .assign(
            **{
                cols.FLOAT_MAXIMIZED_COMPETITIVE_BID_POWER: lambda df: np.where(
                    df[cols.FLOAT_CLEARED_PRICE] >= df[cols.FLOAT_BID_PRICE],
                    df[cols.FLOAT_BID_POWER],
                    df[cols.FLOAT_MAV],
                )
            }
        )
        .eval(
            f"""
            {cols.FLOAT_COLLECTION_RIGHTS} = {cols.FLOAT_MAXIMIZED_COMPETITIVE_BID_POWER} * {cols.FLOAT_CLEARED_PRICE}
            {cols.FLOAT_VARIABLE_COST} = {cols.FLOAT_MAXIMIZED_COMPETITIVE_BID_POWER} * {cols.FLOAT_BID_PRICE}
            """
        )
        .groupby([cols.ID_ORDER], observed=True)
        .agg(
            {
                cols.FLOAT_COLLECTION_RIGHTS: "sum",
                cols.FLOAT_VARIABLE_COST: "sum",
                cols.FLOAT_MIC: "first",
                cols.FLOAT_MAXIMIZED_COMPETITIVE_BID_POWER: "sum",
            }
        )
        .eval(
            f"""
            {cols.FLOAT_NET_INCOME} = {cols.FLOAT_COLLECTION_RIGHTS} - ( {cols.FLOAT_VARIABLE_COST} + {cols.FLOAT_MIC} )
            {cols.FLOAT_RATIO_NET_INCOME_BID_POWER} = {cols.FLOAT_NET_INCOME} / {cols.FLOAT_MAXIMIZED_COMPETITIVE_BID_POWER}
            """
        )
    )


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

    trials_df_status_false = trials_df.query(f"{cols.BOOL_IS_MIC_RESPECTED} == False")
    trials_df_status_true = trials_df.query(f"{cols.BOOL_IS_MIC_RESPECTED} == True")
    sorted_promising_trials_df = pd.concat(
        [
            trials_df_status_true.sort_values(
                by=[cols.FLOAT_OBJECTIVE_VALUE, cols.INT_MIC_SCOS_COUNT],
                ascending=[False, True],
            ),
            trials_df_status_false.sort_values(
                by=[cols.INT_MIC_SCOS_COUNT, cols.FLOAT_OBJECTIVE_VALUE],
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
        trials_df = trials_df.query(f"{cols.BOOL_IS_MIC_RESPECTED} == True")
    sorted_trials_df = sort_trials_df_by_most_promising(trials_df)
    return sorted_trials_df.iloc[0]


def get_combinations_generator(
    leftout_mic_scos_summary_combinations: pd.DataFrame,
    combinations_count: int,
    reverse: bool = True,
) -> list[tuple]:
    """
    Generate sorted combinations of left-out MIC SCOs.

    This function creates all possible combinations (of a given size) from the index of
    `leftout_mic_scos_summary_combinations`, sorts them by the sum of their
    FLOAT_RATIO_NET_INCOME_BID_POWER, and returns them in the specified order.

    Args:
        leftout_mic_scos_summary_combinations (pd.DataFrame): DataFrame indexed by SCO IDs,
            containing at least the FLOAT_RATIO_NET_INCOME_BID_POWER column.
        combinations_count (int): The number of SCOs in each combination.
        reverse (bool, optional): If True, sort combinations in descending order of
            total FLOAT_RATIO_NET_INCOME_BID_POWER. If False, ascending. Defaults to True.

    Returns:
        list: List of tuples, each tuple is a combination of SCO IDs, sorted by the sum
            of FLOAT_RATIO_NET_INCOME_BID_POWER.
    """

    scos_combinations = leftout_mic_scos_summary_combinations.index.tolist()
    index_combinations = combinations(scos_combinations, combinations_count)
    index_cobinations_sorted = sorted(
        index_combinations,
        key=lambda t: sum(
            leftout_mic_scos_summary_combinations.loc[list(t)][
                cols.FLOAT_RATIO_NET_INCOME_BID_POWER
            ]
        ),
        reverse=reverse,
    )
    return index_cobinations_sorted


def get_new_mic_scos_by_adding_left_out_scos(
    leftout_mic_scos_summary: pd.DataFrame,
    trials_df: pd.DataFrame,
    starting_mic_scos: list,
    mic_scos_combinations_count: int = 1,
) -> pd.Series:
    """
    Proposes new SCO-with-MIC combinations by adding left-out SCOs to the current combination.

    This function sorts left-out SCOs by their net income per bid power, then proposes new combinations
    by adding the most promising left-out SCOs to the current set. It avoids combinations that have already
    been tested and can propose combinations with more than one left-out SCO if beneficial. Returns up to
    mic_scos_combinations_count new combinations.

    Args:
        leftout_mic_scos_summary (pd.DataFrame): Summary DataFrame of left-out SCOs with financial metrics.
        trials_df (pd.DataFrame): DataFrame of all previous trial results.
        starting_mic_scos (list): List of SCO order IDs with MIC in the current trial.
        mic_scos_combinations_count (int, optional): Maximum number of new combinations to return. Defaults to 1.

    Returns:
        pd.Series: Series of new SCO-with-MIC combinations (as lists) to try in the next trials.
    """
    starting_mic_scos_count = len(starting_mic_scos)

    # Sort left-out SCOs by ratio net income / bid power
    leftout_mic_scos_summary_sorted = leftout_mic_scos_summary.sort_values(
        by=cols.FLOAT_RATIO_NET_INCOME_BID_POWER, ascending=False
    )

    # Create initial new combinations by adding single left-out SCOs
    new_mic_scos_df = pd.DataFrame(
        {
            cols.ID_ORDER: leftout_mic_scos_summary_sorted.index,
            cols.FLOAT_RATIO_NET_INCOME_BID_POWER: leftout_mic_scos_summary[
                cols.FLOAT_RATIO_NET_INCOME_BID_POWER
            ].values,
            cols.INT_MIC_SCOS_COUNT: 1 + starting_mic_scos_count,
        }
    )
    new_mic_scos_df[cols.MIC_SCOS_COLUMN] = new_mic_scos_df[cols.ID_ORDER].apply(
        lambda sco: starting_mic_scos + [sco]
    )
    new_mic_scos_df[cols.BOOL_ARE_MIC_SCOS_TESTED] = new_mic_scos_df[
        cols.MIC_SCOS_COLUMN
    ].apply(lambda sco: check_are_mic_scos_tested(trials_df, sco + starting_mic_scos))

    # Filter out already tested combinations
    new_mic_scos_df = new_mic_scos_df.query(
        f"{cols.BOOL_ARE_MIC_SCOS_TESTED} == False"
    ).drop(columns=[cols.ID_ORDER])

    # If the number of new combinations is greater than the desired, keep only the top ones
    if len(new_mic_scos_df) > mic_scos_combinations_count:
        new_mic_scos_df = new_mic_scos_df.head(mic_scos_combinations_count)

    min_ratio = new_mic_scos_df[cols.FLOAT_RATIO_NET_INCOME_BID_POWER].min()
    create_combinations = True
    combinations_count = 2

    while create_combinations and combinations_count <= 4:
        # Generate combinations of left-out SCOs, sorted by their combined ratio net income / bid power
        leftout_mic_scos_sorted = get_combinations_generator(
            leftout_mic_scos_summary_sorted, combinations_count
        )
        new_mic_scos_added_in_iteration = False
        for leftout_mic_scos in leftout_mic_scos_sorted:
            new_trial_mic_scos = starting_mic_scos + list(leftout_mic_scos)
            are_mic_scos_tested = check_are_mic_scos_tested(
                trials_df, new_trial_mic_scos
            )
            if not are_mic_scos_tested:
                ratio_net_income_bid_power = np.average(
                    leftout_mic_scos_summary.loc[list(leftout_mic_scos)][
                        cols.FLOAT_RATIO_NET_INCOME_BID_POWER
                    ],
                    weights=leftout_mic_scos_summary.loc[list(leftout_mic_scos)][
                        cols.FLOAT_MAXIMIZED_COMPETITIVE_BID_POWER
                    ],
                )

                # Add only if the ratio is better than the minimum of the current new combinations or
                # if we still need more combinations
                if (
                    ratio_net_income_bid_power > min_ratio
                    or len(new_mic_scos_df) < mic_scos_combinations_count
                ):
                    new_mic_scos_entry = pd.DataFrame(
                        {
                            cols.MIC_SCOS_COLUMN: [new_trial_mic_scos],
                            cols.FLOAT_RATIO_NET_INCOME_BID_POWER: [
                                ratio_net_income_bid_power
                            ],
                            cols.BOOL_ARE_MIC_SCOS_TESTED: [False],
                            cols.INT_MIC_SCOS_COUNT: [
                                combinations_count + starting_mic_scos_count
                            ],
                        }
                    )

                    new_mic_scos_df = (
                        pd.concat(
                            [new_mic_scos_df, new_mic_scos_entry], ignore_index=True
                        )
                        .sort_values(
                            by=cols.FLOAT_RATIO_NET_INCOME_BID_POWER,
                            ascending=False,
                        )
                        .head(mic_scos_combinations_count)
                    )

                    new_mic_scos_added_in_iteration = True

                else:
                    break

        if (
            not new_mic_scos_added_in_iteration
            and len(new_mic_scos_df) >= mic_scos_combinations_count
        ):
            create_combinations = False

        else:
            combinations_count += 1

    return new_mic_scos_df.head(mic_scos_combinations_count)[cols.MIC_SCOS_COLUMN]


def get_new_mic_scos_by_removing_underperforming_scos(
    trial_cleared_mic_scos_summary: pd.DataFrame,
    trials_df: pd.DataFrame,
    scos_combination: list,
    int_mic_scos_count: int = 1,
) -> pd.Series:
    """
    Proposes new SCO-with-MIC combinations by removing underperforming SCOs from the current combination.

    This function identifies SCOs with negative net income per cleared power, removes them one by one from the current combination,
    and checks if the resulting combinations have already been tested. Returns up to int_mic_scos_count new combinations that have not been tested yet.

    Args:
        trial_cleared_mic_scos_summary (pd.DataFrame): Summary DataFrame of cleared SCOs with MIC for the current trial.
        trials_df (pd.DataFrame): DataFrame of all previous trial results.
        scos_combination (list): List of SCO order IDs with MIC in the current trial.
        int_mic_scos_count (int, optional): Maximum number of new combinations to return. Defaults to 1.

    Returns:
        pd.Series: Series of new SCO-with-MIC combinations (as lists) to try in the next trials.
    """
    trial_cleared_mic_scos_summary = trial_cleared_mic_scos_summary.query(
        f"{cols.FLOAT_RATIO_NET_INCOME_CLEARED_POWER} < 0"
    ).sort_values(by=cols.FLOAT_RATIO_NET_INCOME_CLEARED_POWER, ascending=True)
    new_mic_scos_df = pd.DataFrame(
        {cols.MIC_SCOS_COLUMN: np.nan, cols.BOOL_ARE_MIC_SCOS_TESTED: np.nan},
        index=trial_cleared_mic_scos_summary.index,
    ).astype({cols.MIC_SCOS_COLUMN: object, cols.BOOL_ARE_MIC_SCOS_TESTED: bool})

    mic_scos_left = scos_combination.copy()

    for index, row in new_mic_scos_df.iterrows():

        new_trial_mic_scos = list(set(mic_scos_left) - set([index]))
        are_mic_scos_tested = check_are_mic_scos_tested(trials_df, new_trial_mic_scos)

        new_mic_scos_df.at[index, cols.MIC_SCOS_COLUMN] = new_trial_mic_scos
        new_mic_scos_df.at[index, cols.BOOL_ARE_MIC_SCOS_TESTED] = are_mic_scos_tested

        mic_scos_left = new_trial_mic_scos

    new_mic_scos_df = new_mic_scos_df.query(
        f"{cols.BOOL_ARE_MIC_SCOS_TESTED} == False"
    ).head(int_mic_scos_count)

    return new_mic_scos_df[cols.MIC_SCOS_COLUMN]


def define_new_trial_mic_scos(
    trials_df: pd.DataFrame,
    det_cab_date: pd.DataFrame,
    all_mic_scos: list,
    int_mic_scos_count: int = 1,
) -> list:
    """
    Defines a new combination of SCOs with MIC to try in the next trial.

    Based on previous trial results, either adds promising left-out SCOs or removes underperforming ones, ensuring combinations are not repeated.

    Args:
        trials_df (pd.DataFrame): DataFrame of previous trial results.
        det_cab_date (pd.DataFrame): Full DET/CAB DataFrame.
        all_mic_scos (list): List of all SCO order IDs with MIC.
        int_mic_scos_count (int, optional): Maximum number of MIC SCO combinations to propose. Defaults to 1.

    Returns:
        list: List of SCO order IDs for the next trial.
    """

    # Get the most promising trial
    sorted_promising_trials_df = sort_trials_df_by_most_promising(trials_df)

    for index, row in sorted_promising_trials_df.iterrows():

        logger.info(
            f"--ALGORITHM--: Most promising combination: {row[cols.MIC_SCOS_COLUMN]}"
        )

        cleared_energy = row[cols.CLEARED_ENERGY_COLUMN]
        clearing_prices = row[cols.CLEARING_PRICES_COLUMN]
        trial_mic_scos = row[cols.MIC_SCOS_COLUMN]
        mic_scos = row[cols.MIC_SCOS_COLUMN]
        bool_is_mic_respected = row[cols.BOOL_IS_MIC_RESPECTED]

        if bool_is_mic_respected:
            logger.info("--ALGORITHM--: MIC is respected")
            leftout_mic_scos_summary = get_leftout_mic_scos_summary(
                det_cab_date, all_mic_scos, mic_scos, clearing_prices
            ).sort_values(by=cols.FLOAT_RATIO_NET_INCOME_BID_POWER, ascending=False)
            return get_new_mic_scos_by_adding_left_out_scos(
                leftout_mic_scos_summary, trials_df, mic_scos, int_mic_scos_count
            )

        else:
            det_cab_date_mic_scos_filtered = filter_mic_scos_from_det_cab(
                det_cab_date, trial_mic_scos
            )
            trial_cleared_mic_scos_summary = get_trial_cleared_mic_scos_summary(
                det_cab_date_mic_scos_filtered, cleared_energy, clearing_prices
            )
            return get_new_mic_scos_by_removing_underperforming_scos(
                trial_cleared_mic_scos_summary,
                trials_df,
                mic_scos,
                int_mic_scos_count,
            )


def iterative_function(
    args: tuple[
        pd.DataFrame,
        pd.DataFrame,
        pd.DataFrame,
        pd.DataFrame,
        list,
    ],
) -> pd.DataFrame:
    """
    Runs a single market clearing trial for a given combination of SCOs with MIC.

    This function filters the DET/CAB data for the current SCOs with MIC, runs the market model,
    extracts relevant results, and returns a DataFrame summarizing the trial.

    Args:
        args (tuple): Tuple containing:
            - det_cab_date (pd.DataFrame): Full DET/CAB DataFrame.
            - capacidad_inter_PT_date (pd.DataFrame): DataFrame of interconnection capacities for Portugal.
            - exclusive_block_orders_grouped (pd.DataFrame): DataFrame of exclusive block order groups.
            - current_trial_mic_scos (list): List of SCO order IDs with MIC for this trial.

    Returns:
        pd.DataFrame: DataFrame with the results of the trial (one row).
    """

    (
        det_cab_date,
        capacidad_inter_PT_date,
        exclusive_block_orders_grouped,
        current_trial_mic_scos,
    ) = args

    # Keep only SCOs in the current trial
    det_cab_date_scos_filtered = filter_mic_scos_from_det_cab(
        det_cab_date, current_trial_mic_scos
    )

    # Run market model
    model, _, results = run_model(
        det_cab_date_scos_filtered,
        capacidad_inter_PT_date,
        exclusive_block_orders_grouped,
    )

    # Extract information from the model
    cleared_energy = get_cleared_energy_series(model)
    clearing_prices = get_clearing_prices_df(model)
    trial_results_sco_casadas_grouped = get_trial_cleared_mic_scos_summary(
        det_cab_date_scos_filtered, cleared_energy, clearing_prices
    )
    welfare = pyo.value(model.OBJ)
    bool_is_mic_respected = (
        trial_results_sco_casadas_grouped[cols.FLOAT_NET_INCOME] >= 0
    ).all()

    # Update trials_df with current trial results
    trial_df_entry = {
        cols.MIC_SCOS_COLUMN: [current_trial_mic_scos],
        cols.FLOAT_OBJECTIVE_VALUE: [welfare],
        cols.BOOL_IS_MIC_RESPECTED: [bool_is_mic_respected],
        cols.SOLVER_RESULTS_COLUMN: [results],
        cols.INT_MIC_SCOS_COUNT: [len(current_trial_mic_scos)],
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
    is_trial_mic_scos_provided: bool,
) -> bool:
    """
    Checks if the first trial was successful, i.e., all SCOs with MIC were correctly cleared on the first attempt.

    Args:
        results (list): List of DataFrames with trial results, where each DataFrame contains the results of a single trial.
        is_trials_df_provided (bool): Indicates if a previous trials DataFrame was provided (i.e., not the first run).
        is_trial_mic_scos_provided (bool): Indicates if an initial SCOs-with-MIC combination was provided.

    Returns:
        bool: True if the first trial was successful and it was the initial run (no previous trials or initial combination provided), False otherwise.
    """
    success_at_first_trial = (
        not is_trials_df_provided
        and not is_trial_mic_scos_provided
        and first_trial_df[cols.BOOL_IS_MIC_RESPECTED]
    )
    if success_at_first_trial:
        logger.info(
            "--ALGORITHM--: First trial with all SCOs correctly cleared, finishing"
        )
    return success_at_first_trial


@pa.check_input(DETCABSchema, "det_cab_date")
@pa.check_input(CapacidadInterPTSchema, "capacidad_inter_pt_date")
@pa.check_input(ExclusiveBlockOrdersGroupedSchema, "exclusive_block_orders_grouped")
def run_iterative_loop(
    det_cab_date: DataFrame,
    capacidad_inter_pt_date: DataFrame,
    exclusive_block_orders_grouped: DataFrame,
    all_mic_scos: list,
    trials_count: int = 100,
    trial_mic_scos: list | None = None,
    trials_df: pd.DataFrame | None = None,
    n_jobs: int = 1,
) -> tuple[pd.DataFrame, pyo.ConcreteModel, pyo.ConcreteModel]:
    """
    Runs the iterative clearing process, optimizing combinations of SCOs with MIC.

    For each trial, filters SCOs, runs the market model, collects results, and updates the trial DataFrame. Continues until all combinations are tested or a successful result is found.

    Args:
        det_cab_date (pd.DataFrame): Full DET/CAB DataFrame.
        capacidad_inter_pt_date (pd.DataFrame): DataFrame of interconnection capacities for Portugal.
        exclusive_block_orders_grouped (pd.DataFrame): DataFrame of exclusive block order groups.
        all_mic_scos (list): List of all SCO order IDs with MIC.
        trials_count (int, optional): Maximum number of trials to run. Defaults to 100.
        trial_mic_scos (list, optional): Initial SCOs with MIC for the first trial.
        trials_df (pd.DataFrame, optional): Existing trials DataFrame to continue from.

    Returns:
        tuple: (trials_df, best_model, best_model_binary)
            trials_df (pd.DataFrame): DataFrame of all trial results.
            best_model: Pyomo model object for the best trial.
            best_model_binary: Pyomo model object for the best trial (binary version).
    """

    is_trials_df_provided = trials_df is not None
    is_trial_mic_scos_provided = trial_mic_scos is not None

    if trials_count // n_jobs < 5 and not is_trials_df_provided:
        logger.warning(
            "--ALGORITHM--: It is recommended trials_count to be at least 5 times n_jobs."
        )

    #### INITIALIZATION ####
    # Initialize trials_df if not provided
    if trials_df is None:
        trials_df = pd.DataFrame(columns=TRIALS_DF_COLUMNS)

    # If trial_mic_scos is provided, start with that combination
    if trial_mic_scos is not None:
        next_trials_mic_sco = [trial_mic_scos.copy()]
    # If trials_df is defined, define a new combination based on previous trials
    elif not trials_df.empty:
        next_trials_mic_sco = define_new_trial_mic_scos(
            trials_df, det_cab_date, all_mic_scos
        )
        if next_trials_mic_sco is False:
            logger.info("--ALGORITHM--: All combinations tried, finishing")
            return trials_df
    # Otherwise start with all SCOs with MIC
    else:
        next_trials_mic_sco = [all_mic_scos.copy()]

    #### ITERATIVE LOOP ####

    completed_trials = 0

    while completed_trials < trials_count:

        args_list = [
            (
                det_cab_date,
                capacidad_inter_pt_date,
                exclusive_block_orders_grouped,
                trial_mic_scos,
            )
            for trial_mic_scos in next_trials_mic_sco
        ]
        with multiprocessing.Pool(processes=n_jobs) as pool:
            results = pool.map(iterative_function, args_list)
            completed_trials += len(results)
            trials_df = pd.concat([trials_df] + results, ignore_index=True)

            if check_if_success_at_first_trial(
                trials_df.iloc[0], is_trials_df_provided, is_trial_mic_scos_provided
            ):
                logger.info("--ALGORITHM--: Success at first trial, finishing")
                break

            ### hasta aqui
        new_trial_int_mic_scos_count = min(n_jobs, trials_count - completed_trials)
        logger.info(
            f"--ALGORITHM--: Completed trials: {completed_trials}/{trials_count}"
        )
        next_trials_mic_sco = define_new_trial_mic_scos(
            trials_df, det_cab_date, all_mic_scos, new_trial_int_mic_scos_count
        )
        # TODO: this is a quickfix so the loop ends when no new combinations are found
        # but it should iterate with other options too
        if len(next_trials_mic_sco) == 0:
            break

    try:
        TrialsSchema.validate(trials_df, lazy=True)
    except pa.errors.SchemaErrors as e:
        warnings.warn(f"Pandera validation warning: {e}")

    best_trial = get_best_trial(trials_df, mic_respected_only=False)

    det_cab_date_scos_filtered = filter_mic_scos_from_det_cab(
        det_cab_date, best_trial[cols.MIC_SCOS_COLUMN]
    )

    # Run market model
    best_model, best_model_binary, results = run_model(
        det_cab_date=det_cab_date_scos_filtered,
        capacidad_inter_PT_date=capacidad_inter_pt_date,
        exclusive_block_orders_grouped=exclusive_block_orders_grouped,
    )

    return trials_df, best_model, best_model_binary


def clear_OMIE_market(
    det_date: pd.DataFrame | str,
    cab_date: pd.DataFrame | str,
    capacidad_inter_date: pd.DataFrame | str,
    price_france_date: pd.DataFrame,
    uof_zones: pd.DataFrame | None = None,
    trials_count: int = 100,
    starting_trials_df: pd.DataFrame = None,
    zones_default_to_spain: bool = False,
    trial_mic_scos: list | None = None,
    n_jobs: int = 1,
) -> dict:
    """
    Runs the full OMIE market clearing process for a given day, including data
    preprocessing, structure building, and iterative optimization.

    This function prepares all necessary data structures from the provided DET,
    CAB, UOF zones, interconnection capacity, and France price data. It then
    runs the iterative clearing process, optimizing combinations of SCOs
    with MIC, and returns the results and best models.

    Args:
        det_date (pd.DataFrame | str): DET DataFrame for the studied day or path to DET file.
        cab_date (pd.DataFrame | str): CAB DataFrame for the studied day or path to CAB file.
        capacidad_inter_date (pd.DataFrame | str): DataFrame of interconnection capacities for the studied day or path to file.
        price_france_date (pd.DataFrame): DataFrame of France prices for the studied day.
        uof_zones (pd.DataFrame | None): DataFrame mapping units to zones.
        trials_count (int, optional): Maximum number of optimization trials to run. Defaults to 100.
        starting_trials_df (pd.DataFrame, optional): Existing trials DataFrame to continue from. Defaults to None.
        zones_default_to_spain (bool, optional): Whether the missing uof zones are assumed to be Spain. Defaults to False.
        trial_mic_scos (list | None, optional): List of MIC SCOs for trial. Defaults to None.
        n_jobs (int, optional): Number of parallel jobs to run. Defaults to 1.
    Returns:
        tuple: (trials_df, best_model, best_model_binary)
            trials_df (pd.DataFrame): DataFrame of all trial results.
            best_model (pyo.ConcreteModel): Pyomo model object for the best trial.
            best_model_binary (pyo.ConcreteModel): Pyomo model object for the best trial (binary version).
    """

    if isinstance(det_date, str):
        det_date = parse_det_file(det_date)
    if isinstance(cab_date, str):
        cab_date = parse_cab_file(cab_date)
    if isinstance(capacidad_inter_date, str):
        capacidad_inter_date = parse_capacidad_inter_file(capacidad_inter_date)

    DETSchema.validate(det_date)
    CABSchema.validate(cab_date)
    CapacidadInterPTSchema.validate(capacidad_inter_date)

    if isinstance(uof_zones, pd.DataFrame):
        uof_zones = concat_provided_uof_zones_with_existing_data(uof_zones)
    else:
        uof_zones = pd.read_csv(UOF_ZONES_FILEPATH)

    capacidad_inter_pt_date = capacidad_inter_date.query(
        f"{cols.CAT_FRONTIER} == {FRONTIER_MAPPING_REVERSE['PT']}"
    )
    det_cab_fr_date = get_france_det_cab_date_from_price(
        price_france_date, capacidad_inter_date
    )
    det_cab_date = get_det_cab_date_for_simulation(
        det_date=det_date,
        cab_date=cab_date,
        uof_zones=uof_zones,
        det_cab_fr_date=det_cab_fr_date,
        zones_default_to_spain=zones_default_to_spain,
    )
    exclusive_block_orders_grouped = get_exclusive_block_orders_grouped(det_cab_date)
    all_mic_scos = get_all_mic_scos(det_cab_date)

    trials_df, model, model_binary = run_iterative_loop(
        det_cab_date=det_cab_date,
        capacidad_inter_pt_date=capacidad_inter_pt_date,
        exclusive_block_orders_grouped=exclusive_block_orders_grouped,
        trials_count=trials_count,
        all_mic_scos=all_mic_scos,
        trials_df=starting_trials_df,
        trial_mic_scos=trial_mic_scos,
        n_jobs=n_jobs,
    )

    best_trial = get_best_trial(trials_df, mic_respected_only=False)
    cleared_energy = best_trial.cleared_energy
    cleared_det_cab_date = det_cab_date.merge(
        cleared_energy,
        left_on=cols.ID_INDIVIDUAL_BID,
        right_index=True,
        how="outer",
        validate="one_to_one",
        indicator=True,
    )

    try:
        ClearedDetCabSchema.validate(cleared_det_cab_date, lazy=True)
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
        "cleared_det_cab_date": cleared_det_cab_date,
        "clearing_prices": best_trial.clearing_prices,
        "spain_portugal_transmissions": best_trial.spain_portugal_transmissions,
        "trials_df": trials_df,
    }

    return results_dict
