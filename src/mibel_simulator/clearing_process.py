########################### Iterative Loop #########################


from itertools import combinations
import logging
import multiprocessing
import numpy as np
import pandas as pd
from mibel_simulator.get_new_paradoxal_orders_list_adding_and_removing import (
    get_new_paradoxal_orders_list_adding_and_removing,
)
import mibel_simulator.columns as cols
import pandera.pandas as pa
import warnings

from mibel_simulator.const import FRONTIER_MAPPING_REVERSE, ITERATIONS_DF_COLUMNS
from mibel_simulator.data_preprocessor import (
    get_all_paradoxal_orders,
    get_det_cab_for_simulation,
    get_france_det_cab_from_price,
)
from mibel_simulator.file_paths import PARTICIPANTS_BIDDING_ZONES_FILEPATH
from mibel_simulator.paradoxal_orders_tools import (
    check_are_paradoxal_orders_tested,
    transform_ids_paradoxal_orders_list_to_dict,
    transform_paradoxal_orders_dict_to_ids_list,
)
from mibel_simulator.schemas import (
    CABSchema,
    CapacidadInterPTSchema,
    ClearingPricesSchema,
    DETCABSchema,
    DETSchema,
    ExclusiveBlockOrdersGroupedSchema,
    IterationsSchema,
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
    concat_provided_participants_bidding_zones_with_existing_data,
    filter_paradoxal_orders_from_det_cab,
)
import pyomo.environ as pyo
from pandera.typing import DataFrame, Series

logger = logging.getLogger(__name__)


def get_cleared_paradoxal_orders_summary(
    det_cab_paradoxal_orders_filtered: pd.DataFrame,
    cleared_energy_df: pd.DataFrame,
    clearing_price_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Aggregates results for paradox orders that were matched in the iteration.

    Merges DET/CAB data with cleared energy and clearing prices, computes financial metrics, and groups by order ID.

    Args:
        det_cab_paradoxal_orders_filtered (pd.DataFrame): DET/CAB DataFrame for paradox orders in the iteration.
        cleared_energy_df (pd.DataFrame): DataFrame of cleared energy per bid.
        clearing_price_df (pd.DataFrame): DataFrame of clearing prices per period and zone.

    Returns:
        pd.DataFrame: DataFrame grouped by order ID with financial results for matched paradox orders.
    """

    cleared_det_cab = (
        det_cab_paradoxal_orders_filtered.merge(
            cleared_energy_df,
            left_on=cols.ID_INDIVIDUAL_BID,
            right_index=True,
            how="outer",
            validate="one_to_one",
            indicator=True,
        )
        .sort_values(
            by=[cols.INT_PERIOD, cols.CAT_BUY_SELL, cols.FLOAT_BID_POWER_CUMSUM]
        )
        .copy()
    )
    assert cleared_det_cab._merge.isin(["both", "left_only"]).all()
    cleared_det_cab = cleared_det_cab.drop(columns="_merge")

    cleared_paradoxal_orders_df = (
        cleared_det_cab.query(
            f"({cols.FLOAT_MIC} > 0 or {cols.INT_NUM_BLOQ} > 0) and {cols.FLOAT_CLEARED_POWER} > 0"
        )
        .copy()
        .merge(
            clearing_price_df,
            on=[cols.INT_PERIOD, cols.CAT_BIDDING_ZONE],
            how="left",
            validate="many_to_one",
            indicator=True,
        )
    )
    assert cleared_paradoxal_orders_df._merge.isin(["both"]).all()
    cleared_paradoxal_orders_df = cleared_paradoxal_orders_df.drop(columns="_merge")

    cleared_paradoxal_orders_df = cleared_paradoxal_orders_df.eval(
        f"""
        {cols.FLOAT_COLLECTION_RIGHTS} = {cols.FLOAT_CLEARED_POWER} * {cols.FLOAT_CLEARED_PRICE}
        {cols.FLOAT_VARIABLE_COST} = {cols.FLOAT_CLEARED_POWER} * {cols.FLOAT_BID_PRICE}
        """
    )
    cleared_paradoxal_orders_df_grouped = (
        cleared_paradoxal_orders_df.groupby([cols.ID_PARADOXAL_ORDERS], observed=True)
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

    return cleared_paradoxal_orders_df_grouped


def get_leftout_paradoxal_orders_summary(
    det_cab: pd.DataFrame,
    all_paradoxal_orders: dict,
    iteration_paradoxal_orders: dict,
    clearing_price_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Aggregates financial results for paradox orders not included in the current iteration.

    Merges left-out paradox orders with clearing prices, computes financial metrics, and groups by order ID.

    Args:
        det_cab (pd.DataFrame): Full DET/CAB DataFrame.
        all_paradoxal_orders (dict): Dictionary of all paradox order IDs.
        iteration_paradoxal_orders (dict): Dictionary of paradox order IDs included in the iteration.
        clearing_price_df (pd.DataFrame): DataFrame of clearing prices per period and zone.

    Returns:
        pd.DataFrame: DataFrame grouped by order ID with financial results for left-out paradox orders.
    """

    det_cab = det_cab.copy().merge(
        clearing_price_df,
        on=[cols.INT_PERIOD, cols.CAT_BIDDING_ZONE],
        how="left",
        validate="many_to_one",
    )

    all_scos = all_paradoxal_orders[cols.IDS_MIC_SCOS]
    iteration_scos = iteration_paradoxal_orders[cols.IDS_MIC_SCOS]
    left_out_scos = set(all_scos) - set(iteration_scos)

    all_bid_blocks = all_paradoxal_orders[cols.IDS_BID_BLOCKS]
    iteration_bid_blocks = iteration_paradoxal_orders[cols.IDS_BID_BLOCKS]
    left_out_bid_blocks = set(all_bid_blocks) - set(iteration_bid_blocks)

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

    det_cab_paradoxal_orders = (
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
        .groupby([cols.ID_PARADOXAL_ORDERS], observed=True)
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

    assert det_cab_paradoxal_orders[cols.FLOAT_NET_INCOME].notna().all()
    assert det_cab_paradoxal_orders[cols.FLOAT_RATIO_NET_INCOME_BID_POWER].notna().all()
    return det_cab_paradoxal_orders


#### ITERATIVE LOOP


def sort_iterations_df_by_most_promising(iterations_df: pd.DataFrame) -> pd.DataFrame:
    """
    Sorts the iterations DataFrame to prioritize the most promising SCO combinations.
    Iterations with successful status are sorted by objective value and SCO count; unsuccessful iterations are sorted by SCO count and objective value.

    Args:
        iterations_df (pd.DataFrame): DataFrame containing results of previous iterations.

    Returns:
        pd.DataFrame: Sorted DataFrame of iterations.
    """

    iterations_df_status_false = iterations_df.query(
        f"{cols.BOOL_IS_EXPECTED_INCOME_RESPECTED} == False"
    )
    iterations_df_status_true = iterations_df.query(
        f"{cols.BOOL_IS_EXPECTED_INCOME_RESPECTED} == True"
    )
    sorted_promising_iterations_df = pd.concat(
        [
            iterations_df_status_true.sort_values(
                by=[
                    cols.FLOAT_OBJECTIVE_VALUE,
                    cols.INT_PARADOXAL_ORDERS_COUNT,
                ],
                ascending=[False, True],
            ),
            iterations_df_status_false.sort_values(
                by=[
                    cols.INT_PARADOXAL_ORDERS_COUNT,
                    cols.FLOAT_OBJECTIVE_VALUE,
                ],
                ascending=[True, False],
            ),
        ],
        ignore_index=True,
    )
    return sorted_promising_iterations_df


def get_best_iteration(
    iterations_df: pd.DataFrame, mic_respected_only: bool = True
) -> pd.Series:
    """
    Selects and returns the best iteration from the iterations DataFrame.

    If mic_respected_only is True, only considers iterations where the MIC constraint is respected.
    The best iteration is determined by sorting for the highest objective value and, in case of ties,
    the lowest number of SCOs with MIC. If mic_respected_only is False, considers all iterations.

    Args:
        iterations_df (pd.DataFrame): DataFrame containing results of all iterations.
        mic_respected_only (bool, optional): If True, only consider iterations where MIC is respected. Defaults to True.

    Returns:
        pd.Series: The row of the best iteration in the DataFrame.
    """
    if mic_respected_only:
        iterations_df = iterations_df.query(
            f"{cols.BOOL_IS_EXPECTED_INCOME_RESPECTED} == True"
        )
    sorted_iterations_df = sort_iterations_df_by_most_promising(iterations_df)
    return sorted_iterations_df.iloc[0]


def get_new_paradoxal_orders_list_by_removing_underperforming_ones(
    iteration_cleared_paradoxal_orders_summary: pd.DataFrame,
    iterations_df: pd.DataFrame,
    paradoxal_orders_combination: dict,
    int_paradoxal_orders_count: int = 1,
) -> pd.Series:
    """
    Proposes new paradox order combinations by removing underperforming paradox orders from the current combination.

    This function identifies paradox orders with negative net income per cleared power, removes them one by one from the current combination,
    and checks if the resulting combinations have already been tested. Returns up to int_paradoxal_orders_count new combinations that have not been tested yet.

    Args:
        iteration_cleared_paradoxal_orders_summary (pd.DataFrame): Summary DataFrame of cleared paradox orders for the current iteration.
        iterations_df (pd.DataFrame): DataFrame of all previous iteration results.
        paradoxal_orders_combination (dict): Dictionary of paradox orders in the current iteration.
        int_paradoxal_orders_count (int, optional): Maximum number of new combinations to return. Defaults to 1.

    Returns:
        pd.Series: Series of new paradox order combinations (as lists) to try in the next iterations.
    """
    iteration_cleared_paradoxal_orders_summary = (
        iteration_cleared_paradoxal_orders_summary.query(
            f"{cols.FLOAT_RATIO_NET_INCOME_CLEARED_POWER} < 0"
        ).sort_values(by=cols.FLOAT_RATIO_NET_INCOME_CLEARED_POWER, ascending=True)
    )
    new_paradoxal_orders_df = pd.DataFrame(
        {
            cols.PARADOXAL_ORDERS_COLUMN: np.nan,
            cols.BOOL_ARE_PARADOXAL_ORDERS_TESTED: np.nan,
        },
        index=iteration_cleared_paradoxal_orders_summary.index,
    ).astype(
        {
            cols.PARADOXAL_ORDERS_COLUMN: object,
            cols.BOOL_ARE_PARADOXAL_ORDERS_TESTED: bool,
        }
    )

    paradoxal_order_ids_left = transform_paradoxal_orders_dict_to_ids_list(
        paradoxal_orders_combination
    )

    for index, row in new_paradoxal_orders_df.iterrows():
        new_iteration_paradoxal_orders = list(
            set(paradoxal_order_ids_left) - set([index])
        )
        are_paradoxal_orders_tested = check_are_paradoxal_orders_tested(
            iterations_df, new_iteration_paradoxal_orders
        )

        new_paradoxal_orders_df.at[index, cols.PARADOXAL_ORDERS_COLUMN] = (
            new_iteration_paradoxal_orders
        )
        new_paradoxal_orders_df.at[index, cols.BOOL_ARE_PARADOXAL_ORDERS_TESTED] = (
            are_paradoxal_orders_tested
        )

        paradoxal_order_ids_left = new_iteration_paradoxal_orders

    new_paradoxal_orders_df = new_paradoxal_orders_df.query(
        f"{cols.BOOL_ARE_PARADOXAL_ORDERS_TESTED} == False"
    ).head(int_paradoxal_orders_count)

    return (
        new_paradoxal_orders_df[cols.PARADOXAL_ORDERS_COLUMN]
        .apply(transform_ids_paradoxal_orders_list_to_dict)
        .tolist()
    )


def define_new_paradoxal_orders_list(
    iterations_df: pd.DataFrame,
    det_cab: pd.DataFrame,
    all_paradoxal_orders: dict,
    int_paradoxal_orders_count: int = 1,
) -> list[dict]:
    """
    Defines a new combination of paradox groups with MIC to try in the next iteration.

    Based on previous iteration results, either adds promising left-out paradox groups or removes underperforming ones, ensuring combinations are not repeated.

    Args:
        iterations_df (pd.DataFrame): DataFrame of previous iteration results.
        det_cab (pd.DataFrame): Full DET/CAB DataFrame.
        all_paradoxal_orders (dict): Dictionary of all paradox groups with MIC.
        int_paradoxal_orders_count (int, optional): Maximum number of MIC paradox group combinations to propose. Defaults to 1.

    Returns:
        list[dict]: List of paradox groups for the next iteration.
    """

    # Get the most promising iteration
    sorted_promising_iterations_df = sort_iterations_df_by_most_promising(iterations_df)
    int_paradoxal_orders_count_start = int_paradoxal_orders_count
    new_paradoxal_orders_list = []
    for index, row in sorted_promising_iterations_df.iterrows():

        logger.info(
            f"--ALGORITHM--: Most promising combination: {row[cols.PARADOXAL_ORDERS_COLUMN]}"
        )

        cleared_energy = row[cols.CLEARED_ENERGY_COLUMN]
        clearing_prices = row[cols.CLEARING_PRICES_COLUMN]
        paradoxal_orders = row[cols.PARADOXAL_ORDERS_COLUMN]
        is_expected_income_respected = row[cols.BOOL_IS_EXPECTED_INCOME_RESPECTED]

        if is_expected_income_respected:
            logger.info("--ALGORITHM--: MIC is respected")
            leftout_paradoxal_orders_summary = get_leftout_paradoxal_orders_summary(
                det_cab, all_paradoxal_orders, paradoxal_orders, clearing_prices
            ).sort_values(by=cols.FLOAT_RATIO_NET_INCOME_BID_POWER, ascending=False)
            det_cab_paradoxal_orders_filtered = filter_paradoxal_orders_from_det_cab(
                det_cab, paradoxal_orders
            )
            iteration_cleared_paradoxal_orders_summary = (
                get_cleared_paradoxal_orders_summary(
                    det_cab_paradoxal_orders_filtered,
                    cleared_energy,
                    clearing_prices,
                )
            )
            new_paradoxal_orders_list.extend(
                get_new_paradoxal_orders_list_adding_and_removing(
                    leftout_paradoxal_orders_summary,
                    iteration_cleared_paradoxal_orders_summary,
                    iterations_df,
                    paradoxal_orders,
                    int_paradoxal_orders_count,
                )
            )

        else:
            det_cab_paradoxal_orders_filtered = filter_paradoxal_orders_from_det_cab(
                det_cab, paradoxal_orders
            )
            iteration_cleared_paradoxal_orders_summary = (
                get_cleared_paradoxal_orders_summary(
                    det_cab_paradoxal_orders_filtered,
                    cleared_energy,
                    clearing_prices,
                )
            )
            new_paradoxal_orders_list.extend(
                get_new_paradoxal_orders_list_by_removing_underperforming_ones(
                    iteration_cleared_paradoxal_orders_summary,
                    iterations_df,
                    paradoxal_orders,
                    int_paradoxal_orders_count,
                )
            )

        int_paradoxal_orders_count = int_paradoxal_orders_count_start - len(
            new_paradoxal_orders_list
        )
        if int_paradoxal_orders_count <= 0:
            break

    return new_paradoxal_orders_list


def iterative_function(
    args: tuple[
        pd.DataFrame,
        pd.DataFrame,
        list,
        pd.Series | None,
    ],
) -> pd.DataFrame:
    """
    Runs a single market clearing iteration for a given combination of SCOs with MIC.

    This function filters the DET/CAB data for the current SCOs with MIC, runs the market model,
    extracts relevant results, and returns a DataFrame summarizing the iteration.

    Args:
        args (tuple): Tuple containing:
            - det_cab (pd.DataFrame): Full DET/CAB DataFrame.
            - capacidad_inter_PBC_pt (pd.DataFrame): DataFrame of interconnection capacities for Portugal.
            - paradoxal_orders (list): List of SCO order IDs with MIC for this iteration.
            - france_fixed_exchange (pd.Series, optional): Series with fixed exchange values for France. Defaults to None.

    Returns:
        pd.DataFrame: DataFrame with the results of the iteration (one row).
    """

    (
        det_cab,
        capacidad_inter_PBC_pt,
        paradoxal_orders,
        france_fixed_exchange,
    ) = args

    # Keep only SCOs in the current iteration
    det_cab_paradoxal_orders_filtered = filter_paradoxal_orders_from_det_cab(
        det_cab, paradoxal_orders
    )

    # Run market model
    model, _, results = run_model(
        det_cab_paradoxal_orders_filtered,
        capacidad_inter_PBC_pt,
        france_fixed_exchange,
    )

    # Extract information from the model
    cleared_energy = get_cleared_energy_series(model)
    clearing_prices = get_clearing_prices_df(model)
    cleared_paradoxal_orders_summary = get_cleared_paradoxal_orders_summary(
        det_cab_paradoxal_orders_filtered, cleared_energy, clearing_prices
    )
    welfare = pyo.value(model.OBJ)
    bool_is_expected_income_respected = (
        cleared_paradoxal_orders_summary[cols.FLOAT_NET_INCOME] >= 0
    ).all()

    ids_mic_scos = paradoxal_orders[cols.IDS_MIC_SCOS]
    ids_bid_blocks = paradoxal_orders[cols.IDS_BID_BLOCKS]

    # Update iterations_df with current iteration results
    iteration_df_entry = {
        cols.PARADOXAL_ORDERS_COLUMN: [paradoxal_orders],
        cols.IDS_MIC_SCOS: [ids_mic_scos],
        cols.IDS_BID_BLOCKS: [ids_bid_blocks],
        cols.IDS_PARADOXAL_ORDERS: [ids_mic_scos + ids_bid_blocks],
        cols.FLOAT_OBJECTIVE_VALUE: [welfare],
        cols.BOOL_IS_EXPECTED_INCOME_RESPECTED: [bool_is_expected_income_respected],
        cols.SOLVER_RESULTS_COLUMN: [results],
        cols.INT_MIC_SCOS_COUNT: [len(ids_mic_scos)],
        cols.INT_BID_BLOCKS_COUNT: [len(ids_bid_blocks)],
        cols.INT_PARADOXAL_ORDERS_COUNT: [len(ids_mic_scos) + len(ids_bid_blocks)],
        cols.CLEARED_ENERGY_COLUMN: [cleared_energy],
        cols.CLEARING_PRICES_COLUMN: [clearing_prices],
        cols.SPAIN_PORTUGAL_TRANSMISSIONS_COLUMN: [
            get_spain_portugal_transmissions(model)
        ],
    }

    return pd.DataFrame(iteration_df_entry)


def check_if_success_at_first_iteration(
    first_iteration_df: pd.Series,
    is_iterations_df_provided: bool,
    is_iteration_paradoxal_orders_provided: bool,
) -> bool:
    """
    Checks if the first iteration was successful, i.e., all SCOs with MIC were correctly cleared on the first attempt.

    Args:
        results (list): List of DataFrames with iteration results, where each DataFrame contains the results of a single iteration.
        is_iterations_df_provided (bool): Indicates if a previous iterations DataFrame was provided (i.e., not the first run).
        is_iteration_paradoxal_orders_provided (bool): Indicates if an initial paradox orders combination was provided.

    Returns:
        bool: True if the first iteration was successful and it was the initial run (no previous iterations or initial combination provided), False otherwise.
    """
    success_at_first_iteration = (
        not is_iterations_df_provided
        and not is_iteration_paradoxal_orders_provided
        and first_iteration_df[cols.BOOL_IS_EXPECTED_INCOME_RESPECTED]
    )
    if success_at_first_iteration:
        logger.info(
            "--ALGORITHM--: First iteration with all SCOs correctly cleared, finishing"
        )
    return success_at_first_iteration


@pa.check_input(DETCABSchema, "det_cab", lazy=True)
@pa.check_input(CapacidadInterPTSchema, "capacidad_inter_pbc_pt", lazy=True)
def run_iterative_loop(
    det_cab: DataFrame,
    capacidad_inter_pbc_pt: DataFrame,
    france_fixed_exchange: pd.Series | None = None,
    iterations_count: int = 100,
    iteration_ids_mic_scos: list | None = None,
    iteration_ids_bid_blocks: list | None = None,
    iterations_df: pd.DataFrame | None = None,
    n_jobs: int = 1,
) -> tuple[pd.DataFrame, pyo.ConcreteModel, pyo.ConcreteModel]:
    """
    Runs the iterative clearing process, optimizing combinations of SCOs with MIC.

    For each iteration, filters SCOs, runs the market model, collects results, and updates the iteration DataFrame. Continues until all combinations are tested or a successful result is found.

    Args:
        det_cab (pd.DataFrame): Full DET/CAB DataFrame.
        capacidad_inter_pbc_pt (pd.DataFrame): DataFrame of interconnection capacities for Portugal.
        france_fixed_exchange (pd.Series, optional): Series with fixed exchange values for France. Defaults to None.
        iteration_ids_mic_scos (list, optional): Initial SCOs with MIC for the first iteration.
        iteration_ids_bid_blocks (list, optional): Initial bid blocks for the first iteration.
        iterations_df (pd.DataFrame, optional): Existing iterations DataFrame to continue from.

    Returns:
        tuple: (iterations_df, best_model, best_model_binary)
            iterations_df (pd.DataFrame): DataFrame of all iteration results.
            best_model: Pyomo model object for the best iteration.
            best_model_binary: Pyomo model object for the best iteration (binary version).
    """

    is_iterations_df_provided = iterations_df is not None
    is_iteration_ids_mic_scos_provided = iteration_ids_mic_scos is not None
    is_iteration_ids_bid_blocks_provided = iteration_ids_bid_blocks is not None
    if is_iteration_ids_mic_scos_provided != is_iteration_ids_bid_blocks_provided:
        raise ValueError(
            "Both iteration_ids_mic_scos and iteration_ids_bid_blocks must be provided together or none at all."
        )
    are_iteration_ids_paradoxal_orders_provided = (
        is_iteration_ids_mic_scos_provided and is_iteration_ids_bid_blocks_provided
    )

    all_paradoxal_orders = get_all_paradoxal_orders(det_cab)

    if iterations_count // n_jobs < 5 and not is_iterations_df_provided:
        logger.warning(
            "--ALGORITHM--: It is recommended iterations_count to be at least 5 times n_jobs."
        )

    #### INITIALIZATION ####
    # Initialize iterations_df if not provided
    if iterations_df is None:
        iterations_df = pd.DataFrame(columns=ITERATIONS_DF_COLUMNS)

    # If iteration_paradoxal_orders is provided, start with that combination
    if are_iteration_ids_paradoxal_orders_provided:
        next_iterations_paradoxal_orders = [
            {
                cols.IDS_MIC_SCOS: iteration_ids_mic_scos,
                cols.IDS_BID_BLOCKS: iteration_ids_bid_blocks,
            }
        ]
    # If iterations_df is defined, define a new combination based on previous iterations
    elif not iterations_df.empty:
        next_iterations_paradoxal_orders = define_new_paradoxal_orders_list(
            iterations_df, det_cab, all_paradoxal_orders, min(n_jobs, iterations_count)
        )
        if next_iterations_paradoxal_orders is False:
            logger.info("--ALGORITHM--: All combinations tried, finishing")
            return iterations_df
    # Otherwise start with all paradox orders
    else:
        next_iterations_paradoxal_orders = [all_paradoxal_orders]

    #### ITERATIVE LOOP ####

    completed_iterations = 0

    while completed_iterations < iterations_count:

        args_list = [
            (
                det_cab,
                capacidad_inter_pbc_pt,
                iteration_paradoxal_orders,
                france_fixed_exchange,
            )
            for iteration_paradoxal_orders in next_iterations_paradoxal_orders
        ]
        with multiprocessing.Pool(processes=n_jobs) as pool:
            results = pool.map(iterative_function, args_list)
            completed_iterations += len(results)
            iterations_df = pd.concat([iterations_df] + results, ignore_index=True)

            if check_if_success_at_first_iteration(
                iterations_df.iloc[0],
                is_iterations_df_provided,
                are_iteration_ids_paradoxal_orders_provided,
            ):
                logger.info("--ALGORITHM--: Success at first iteration, finishing")
                break

            ### hasta aqui
        new_iteration_int_paradoxal_orders_count = min(
            n_jobs, iterations_count - completed_iterations
        )
        logger.info(
            f"--ALGORITHM--: Completed iterations: {completed_iterations}/{iterations_count}"
        )
        next_iterations_paradoxal_orders = define_new_paradoxal_orders_list(
            iterations_df,
            det_cab,
            all_paradoxal_orders,
            new_iteration_int_paradoxal_orders_count,
        )
        # TODO: this is a quickfix so the loop ends when no new combinations are found
        # but it should iterate with other options too
        if len(next_iterations_paradoxal_orders) == 0:
            break

    try:
        IterationsSchema.validate(iterations_df, lazy=True)
    except pa.errors.SchemaErrors as e:
        warnings.warn(f"Pandera validation warning: {e}")

    best_iteration = get_best_iteration(iterations_df, mic_respected_only=False)

    det_cab_scos_filtered = filter_paradoxal_orders_from_det_cab(
        det_cab, best_iteration[cols.PARADOXAL_ORDERS_COLUMN]
    )

    # Run market model
    best_model, best_model_binary, results = run_model(
        det_cab=det_cab_scos_filtered,
        capacidad_inter_PBC_pt=capacidad_inter_pbc_pt,
        france_fixed_exchange=france_fixed_exchange,
    )

    return iterations_df, best_model, best_model_binary


def run_mibel_simulator(
    det: pd.DataFrame | str,
    cab: pd.DataFrame | str,
    capacidad_inter_pbc: pd.DataFrame | str,
    france_day_ahead_prices: pd.DataFrame,
    participants_bidding_zones: pd.DataFrame | None = None,
    iterations_count: int = 100,
    starting_iterations_df: pd.DataFrame = None,
    france_fixed_exchange: pd.Series | None = None,
    spain_as_default_bidding_zone: bool = False,
    iteration_ids_mic_scos: list | None = None,
    iteration_ids_bid_blocks: list | None = None,
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
        capacidad_inter_pbc (pd.DataFrame | str): DataFrame of interconnection capacities for the studied day or path to file.
        france_day_ahead_prices (pd.DataFrame): DataFrame of France prices for the studied day.
        participants_bidding_zones (pd.DataFrame | None): DataFrame mapping units to zones.
        iterations_count (int, optional): Maximum number of optimization iterations to run. Defaults to 100.
        starting_iterations_df (pd.DataFrame, optional): Existing iterations DataFrame to continue from. Defaults to None.
        france_fixed_exchange (pd.Series, optional): Series with fixed exchange values for France. Defaults to None.
        spain_as_default_bidding_zone (bool, optional): Whether the missing uof zones are assumed to be Spain. Defaults to False.
        iteration_ids_mic_scos (list | None, optional): List of MIC SCOs for iteration. Defaults to None.
        iteration_ids_bid_blocks (list | None, optional): List of bid blocks for iteration. Defaults to None.
        n_jobs (int, optional): Number of parallel jobs to run. Defaults to 1.
    Returns:
        tuple: (iterations_df, best_model, best_model_binary)
            iterations_df (pd.DataFrame): DataFrame of all iteration results.
            best_model (pyo.ConcreteModel): Pyomo model object for the best iteration.
            best_model_binary (pyo.ConcreteModel): Pyomo model object for the best iteration (binary version).
    """

    if isinstance(det, str):
        det = parse_det_file(det)
    if isinstance(cab, str):
        cab = parse_cab_file(cab)
    if isinstance(capacidad_inter_pbc, str):
        capacidad_inter_pbc = parse_capacidad_inter_file(capacidad_inter_pbc)

    DETSchema.validate(det, lazy=True)
    CABSchema.validate(cab, lazy=True)
    CapacidadInterPTSchema.validate(capacidad_inter_pbc, lazy=True)

    if isinstance(participants_bidding_zones, pd.DataFrame):
        participants_bidding_zones = (
            concat_provided_participants_bidding_zones_with_existing_data(
                participants_bidding_zones
            )
        )
    else:
        participants_bidding_zones = pd.read_csv(PARTICIPANTS_BIDDING_ZONES_FILEPATH)

    capacidad_inter_pbc_pt = capacidad_inter_pbc.query(
        f"{cols.CAT_FRONTIER} == {FRONTIER_MAPPING_REVERSE['PT']}"
    )
    det_cab_fr = get_france_det_cab_from_price(
        france_day_ahead_prices, capacidad_inter_pbc
    )
    det_cab = get_det_cab_for_simulation(
        det=det,
        cab=cab,
        participants_bidding_zones=participants_bidding_zones,
        det_cab_fr=det_cab_fr,
        spain_as_default_bidding_zone=spain_as_default_bidding_zone,
    )

    iterations_df, model, model_binary = run_iterative_loop(
        det_cab=det_cab,
        capacidad_inter_pbc_pt=capacidad_inter_pbc_pt,
        iterations_count=iterations_count,
        iterations_df=starting_iterations_df,
        iteration_ids_mic_scos=iteration_ids_mic_scos,
        iteration_ids_bid_blocks=iteration_ids_bid_blocks,
        france_fixed_exchange=france_fixed_exchange,
        n_jobs=n_jobs,
    )

    best_iteration = get_best_iteration(iterations_df, mic_respected_only=False)
    cleared_energy = best_iteration.cleared_energy
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
        ClearingPricesSchema.validate(best_iteration.clearing_prices, lazy=True)
    except pa.errors.SchemaErrors as e:
        warnings.warn(f"Pandera validation warning in ClearingPricesSchema: {e}")
    try:
        SpainPortugaLTransmissionsSchema.validate(
            best_iteration.spain_portugal_transmissions, lazy=True
        )
    except pa.errors.SchemaErrors as e:
        warnings.warn(
            f"Pandera validation warning in SpainPortugaLTransmissionsSchema: {e}"
        )

    results_dict = {
        "model_binary_fixed": model,
        "model_binary_not_fixed": model_binary,
        "model_iteration_info": best_iteration,
        "cleared_det_cab": cleared_det_cab,
        "clearing_prices": best_iteration.clearing_prices,
        "spain_portugal_transmissions": best_iteration.spain_portugal_transmissions,
        "iterations_df": iterations_df,
    }

    return results_dict
