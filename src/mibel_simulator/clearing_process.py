########################### Iterative Loop #########################


import gc
from itertools import combinations
import logging
import pandas as pd
from mibel_simulator.const import (
    CAT_BUY_SELL,
    CAT_PAIS,
    FLOAT_BID_POWER,
    FLOAT_BID_POWER_CUMSUM,
    FLOAT_BID_PRICE,
    FLOAT_MIC,
    ID_INDIVIDUAL_BID,
    ID_ORDER,
    INT_PERIODO,
    SCOS_WITH_MIC_COLUMN,
    SCOS_WITH_MIC_COUNT_COLUMN,
    TRIALS_DF_COLUMNS,
    OBJECTIVE_VALUE_COLUMN,
    IS_MIC_RESPECTED_COLUMN,
    SOLVER_RESULTS_COLUMN,
    CLEARED_ENERGY_COLUMN,
    CLEARING_PRICES_COLUMN,
    SPAIN_PORTUGAL_TRANSMISSION_COLUMN,
)
from mibel_simulator.model import run_market_model
from mibel_simulator.model_info_extraction import (
    get_cleared_energy_series,
    get_clearing_prices_df,
    get_spain_portugal_transmissions,
)
from mibel_simulator.tools import filter_scos_with_mic_from_det_cab
import pyomo.environ as pyo

logger = logging.getLogger(__name__)


def check_if_sco_combination_tried(
    trials_df: pd.DataFrame, scos_combination: list
) -> bool:
    """_summary_

    Args:
        trials_df (pd.DataFrame): _description_
        scos_combination (list): _description_

    Returns:
        bool: _description_
    """
    for scos_tried in trials_df[SCOS_WITH_MIC_COLUMN]:
        if set(scos_tried) == set(scos_combination):
            return True
    return False


def get_trial_results_sco_casadas_grouped(
    det_cab_date_scos_filtered: pd.DataFrame,
    cleared_energy: pd.DataFrame,
    clearing_price_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Aggregates results for SCO orders that were matched in the trial.

    Merges filtered DET/CAB data with cleared energy and clearing prices, computes financial metrics, and groups by order ID.

    Args:
        det_cab_date_scos_filtered (pd.DataFrame): Filtered DET/CAB DataFrame for SCOs in the trial.
        cleared_energy (pd.DataFrame): DataFrame of cleared energy per bid.
        clearing_price_df (pd.DataFrame): DataFrame of clearing prices per period and zone.

    Returns:
        pd.DataFrame: DataFrame grouped by order ID with financial results for matched SCOs.
    """

    trial_results = (
        det_cab_date_scos_filtered.merge(
            cleared_energy,
            left_on=ID_INDIVIDUAL_BID,
            right_index=True,
            how="outer",
            validate="one_to_one",
            indicator=True,
        )
        .sort_values(by=[INT_PERIODO, CAT_BUY_SELL, FLOAT_BID_POWER_CUMSUM])
        .copy()
    )
    assert trial_results._merge.isin(["both", "left_only"]).all()
    trial_results = trial_results.drop(columns="_merge")

    trial_results_sco_casadas = (
        trial_results.query(f"{FLOAT_MIC} > 0 and `Potencia_casada` > 0")
        .copy()
        .merge(
            clearing_price_df,
            on=[INT_PERIODO, CAT_PAIS],
            how="left",
            validate="many_to_one",
            indicator=True,
        )
    )
    assert trial_results_sco_casadas._merge.isin(["both"]).all()
    trial_results_sco_casadas = trial_results_sco_casadas.drop(columns="_merge")
    trial_results_sco_casadas = trial_results_sco_casadas.eval(
        f"""
        `Derechos_de_cobro` = `Potencia_casada` * `Precio_casación`
        `Coste_variable` = `Potencia_casada` * {FLOAT_BID_PRICE}
        """
    )
    trial_results_sco_casadas_grouped = (
        trial_results_sco_casadas.groupby([ID_ORDER], observed=True)
        .agg({"Derechos_de_cobro": "sum", "Coste_variable": "sum", FLOAT_MIC: "first"})
        .eval(
            f"`Beneficio_neto` = `Derechos_de_cobro` - ( `Coste_variable` + `{FLOAT_MIC}` )"
        )
    )

    return trial_results_sco_casadas_grouped


def get_left_out_scos_grouped(
    det_cab_date: pd.DataFrame,
    all_scos: list,
    trial_scos: list,
    clearing_prices: pd.DataFrame,
) -> pd.DataFrame:
    """
    Aggregates financial results for SCOs not included in the current trial.

    Merges left-out SCOs with clearing prices, computes financial metrics, and groups by order ID.

    Args:
        det_cab_date (pd.DataFrame): Full DET/CAB DataFrame.
        all_scos (list): List of all SCO order IDs.
        trial_scos (list): List of SCO order IDs included in the trial.
        clearing_prices (pd.DataFrame): DataFrame of clearing prices per period and zone.

    Returns:
        pd.DataFrame: DataFrame grouped by order ID with financial results for left-out SCOs.
    """

    left_out_scos = set(all_scos) - set(trial_scos)
    return (
        det_cab_date.query(f"{ID_ORDER} in @left_out_scos")
        .merge(
            clearing_prices,
            on=[INT_PERIODO, CAT_PAIS],
            how="left",
            validate="many_to_one",
        )
        .eval(
            f"""
            `Derechos_de_cobro` = {FLOAT_BID_POWER} * `Precio_casación`
            `Coste_variable` = {FLOAT_BID_POWER} * {FLOAT_BID_PRICE}
            """
        )
        .groupby([ID_ORDER], observed=True)
        .agg(
            {
                "Derechos_de_cobro": "sum",
                "Coste_variable": "sum",
                FLOAT_MIC: "first",
            }
        )
        .eval(
            f"""
            `Beneficio_neto` = `Derechos_de_cobro` - ( `Coste_variable` + `{FLOAT_MIC}` )
            `Ratio_Beneficio_Coste_variable` = `Beneficio_neto` / `Coste_variable`
            """
        )
    )


#### ITERATIVE LOOP


def sort_trials_df_by_most_promising(trials_df):
    """
    Sorts the trials DataFrame to prioritize the most promising SCO combinations.

    Trials with successful status are sorted by objective value and SCO count; unsuccessful trials are sorted by SCO count and objective value.

    Args:
        trials_df (pd.DataFrame): DataFrame containing results of previous trials.

    Returns:
        pd.DataFrame: Sorted DataFrame of trials.
    """

    trials_df_status_false = trials_df.query(f"{IS_MIC_RESPECTED_COLUMN} == False")
    trials_df_status_true = trials_df.query(f"{IS_MIC_RESPECTED_COLUMN} == True")
    sorted_promising_trials_df = pd.concat(
        [
            trials_df_status_true.sort_values(
                by=[OBJECTIVE_VALUE_COLUMN, SCOS_WITH_MIC_COUNT_COLUMN],
                ascending=[False, True],
            ),
            trials_df_status_false.sort_values(
                by=[SCOS_WITH_MIC_COUNT_COLUMN, OBJECTIVE_VALUE_COLUMN],
                ascending=[True, False],
            ),
        ],
        ignore_index=True,
    )
    return sorted_promising_trials_df


def get_best_trial(
    trials_df: pd.DataFrame, mic_respected_only: bool = True
) -> pd.Series:
    """_summary_

    Args:
        trials_df (pd.DataFrame): _description_
        mic_respected_only (bool, optional): _description_. Defaults to True.

    Returns:
        pd.Series: _description_
    """
    if mic_respected_only:
        best_trial = (
            trials_df.query(f"{IS_MIC_RESPECTED_COLUMN} == True")
            .sort_values(by=OBJECTIVE_VALUE_COLUMN, ascending=False)
            .iloc[0]
        )
    else:
        sorted_promising_trials_df = sort_trials_df_by_most_promising(trials_df)
        best_trial = sorted_promising_trials_df.iloc[0]
    return best_trial


def define_new_trial_scos_with_mic(
    trials_df: pd.DataFrame, det_cab_date: pd.DataFrame, all_scos_with_mic: list
) -> list | bool:
    """
    Defines a new combination of SCOs with MIC to try in the next trial.

    Based on previous trial results, either adds promising left-out SCOs or removes underperforming ones, ensuring combinations are not repeated.

    Args:
        trials_df (pd.DataFrame): DataFrame of previous trial results.
        det_cab_date (pd.DataFrame): Full DET/CAB DataFrame.
        all_scos_with_mic (list): List of all SCO order IDs with MIC.

    Returns:
        list | bool: List of SCO order IDs for the next trial, or False if all combinations have been tried.
    """

    # Get the most promising trial
    sorted_promising_trials_df = sort_trials_df_by_most_promising(trials_df)
    most_promising_trial = sorted_promising_trials_df.iloc[0]

    cleared_energy = most_promising_trial[CLEARED_ENERGY_COLUMN]
    clearing_prices = most_promising_trial[CLEARING_PRICES_COLUMN]
    most_promising_trial_scos_with_mic = most_promising_trial[SCOS_WITH_MIC_COLUMN]

    det_cab_date_scos_with_mic_filtered = filter_scos_with_mic_from_det_cab(
        det_cab_date, most_promising_trial_scos_with_mic
    )
    trial_results_sco_with_mic_casadas_grouped = get_trial_results_sco_casadas_grouped(
        det_cab_date_scos_with_mic_filtered, cleared_energy, clearing_prices
    )

    # logger.info("--ALGORITHM--: Iteration: ", len(trials_df))
    for index, row in sorted_promising_trials_df.iterrows():

        logger.info(
            f"--ALGORITHM--: Most promising combination: {row[SCOS_WITH_MIC_COLUMN]}"
        )
        most_promising_sco_combinations = row[SCOS_WITH_MIC_COLUMN]
        is_mic_respected = row[IS_MIC_RESPECTED_COLUMN]
        if is_mic_respected:
            logger.info("--ALGORITHM--: MIC is respected")

            has_combination_been_tested = True
            left_out_socs_grouped = get_left_out_scos_grouped(
                det_cab_date,
                all_scos_with_mic,
                most_promising_sco_combinations,
                clearing_prices,
            ).sort_values(by="Ratio_Beneficio_Coste_variable", ascending=False)
            for entries_to_combine in range(1, len(left_out_socs_grouped) + 1):
                logger.info(f"--ALGORITHM--: entries_to_combine: {entries_to_combine}")
                index_combinations = combinations(
                    left_out_socs_grouped.index.tolist(), entries_to_combine
                )

                for indexes in index_combinations:
                    new_trial_scos_with_mic = most_promising_sco_combinations + list(
                        indexes
                    )
                    has_combination_been_tested = check_if_sco_combination_tried(
                        trials_df, new_trial_scos_with_mic
                    )
                    if not has_combination_been_tested:
                        trial_scos_with_mic = new_trial_scos_with_mic
                        logger.info(
                            f"--ALGORITHM--: Trying new combination: {trial_scos_with_mic}"
                        )
                        break
                if not has_combination_been_tested:
                    break

            if has_combination_been_tested:
                logger.info("--ALGORITHM--: All combinations tried, finishing")
                break

        else:

            trial_results_sco_casadas_grouped_sorted = (
                trial_results_sco_with_mic_casadas_grouped.sort_values(
                    by="Beneficio_neto"
                )
            )

            for entries_to_delete in range(
                1, len(trial_results_sco_casadas_grouped_sorted) + 1
            ):
                index_combinations = combinations(
                    trial_results_sco_casadas_grouped_sorted.index.tolist(),
                    entries_to_delete,
                )
                has_combination_been_tested = True
                for indexes in index_combinations:
                    new_trial_scos_with_mic = list(
                        set(most_promising_sco_combinations) - set(indexes)
                    )
                    has_combination_been_tested = check_if_sco_combination_tried(
                        trials_df, new_trial_scos_with_mic
                    )
                    if not has_combination_been_tested:
                        logger.info(
                            f"--ALGORITHM--: Trying new combination removing {indexes} from {most_promising_sco_combinations}"
                        )
                        trial_scos_with_mic = new_trial_scos_with_mic
                        break
                if not has_combination_been_tested:
                    break

                else:
                    logger.info(
                        "--ALGORITHM--: All combinations removing have been tried",
                    )
                    break

        if not has_combination_been_tested:
            return trial_scos_with_mic

    if has_combination_been_tested:
        logger.info("--ALGORITHM--: All combinations tried, finishing")
        return False
    logger.info("-----------------------------------------------------")


# trial (SCOs included)
# 1. optimize
# 2. get precios de casación and welfare
# 3. todas las casaciones sco son correctas?
# 3.No. Eliminar las no correctas e ir a 1.
# 3.Si. Es la primera vez?
#      Sí. Fin
#      No.
#           Calcular derechos_de_cobro de las SCO descartadas
#           Añadir la más prometedora, si la combinación no ha sido probada, si no, la segunda


def run_iterative_loop(
    det_cab_date: pd.DataFrame,
    capacidad_inter_date: pd.DataFrame,
    parent_child_scos: pd.DataFrame,
    parent_child_bloques: pd.DataFrame,
    exclusive_block_orders_grouped: pd.DataFrame,
    sco_bids_tramo_grouped: pd.DataFrame,
    all_scos_with_mic: list,
    trials_count: int = 100,
    trial_scos_with_mic: list | None = None,
    trials_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pyo.ConcreteModel, pyo.ConcreteModel]:
    """
    Runs the iterative DAM clearing process, optimizing combinations of SCOs with MIC.

    For each trial, filters SCOs, runs the market model, collects results, and updates the trial DataFrame. Continues until all combinations are tested or a successful result is found.

    Args:
        det_cab_date (pd.DataFrame): Full DET/CAB DataFrame.
        capacidad_inter_date (pd.DataFrame): DataFrame of interconnection capacities.
        parent_child_scos (pd.DataFrame): DataFrame of parent-child SCO relationships.
        parent_child_bloques (pd.DataFrame): DataFrame of parent-child block order relationships.
        exclusive_block_orders_grouped (pd.DataFrame): DataFrame of exclusive block order groups.
        sco_bids_tramo_grouped (pd.DataFrame): DataFrame of grouped SCO bids by tramo.
        all_scos_with_mic (list): List of all SCO order IDs with MIC.
        trials_count (int, optional): Maximum number of trials to run. Defaults to 100.
        trial_scos_with_mic (list, optional): Initial SCOs with MIC for the first trial.
        trials_df (pd.DataFrame, optional): Existing trials DataFrame to continue from.

    Returns:
        tuple: (trials_df, best_model, best_model_binary)
            trials_df (pd.DataFrame): DataFrame of all trial results.
            best_model: Pyomo model object for the best trial.
            best_model_binary: Pyomo model object for the best trial (binary version).
    """

    #### INITIALIZATION ####

    # Filter capacity for Portugal only
    if CAT_PAIS in capacidad_inter_date.columns:
        capacidad_inter_PT_date = capacidad_inter_date.query(
            f"`{CAT_PAIS}` == '2'"
        ).copy()
    else:
        capacidad_inter_PT_date = capacidad_inter_date.copy()

    # Initialize trials_df if not provided
    if trials_df is None:
        trials_df = pd.DataFrame(columns=TRIALS_DF_COLUMNS)

    # Define initial trial SCOs with MIC if not provided
    if trial_scos_with_mic is not None:
        current_trial_scos_with_mic = trial_scos_with_mic.copy()
    # Start with all SCOs with MIC if trials_df is empty
    elif trial_scos_with_mic is None and trials_df.empty:
        current_trial_scos_with_mic = all_scos_with_mic.copy()
    # Otherwise, define a new combination based on previous trials
    elif trial_scos_with_mic is None and not trials_df.empty:
        current_trial_scos_with_mic = define_new_trial_scos_with_mic(
            trials_df, det_cab_date, all_scos_with_mic
        )
        if current_trial_scos_with_mic is False:
            logger.info("--ALGORITHM--: All combinations tried, finishing")
            return trials_df

    #### ITERATIVE LOOP ####

    best_model = None
    best_model_binary = None

    for trial in range(trials_count):
        logger.info(f"trial: {trial}")

        # Keep only SCOs in the current trial
        det_cab_date_scos_filtered = filter_scos_with_mic_from_det_cab(
            det_cab_date, current_trial_scos_with_mic
        )

        # Run market model
        model, model_binary, results = run_market_model(
            det_cab_date_scos_filtered,
            capacidad_inter_PT_date,
            parent_child_scos,
            parent_child_bloques,
            exclusive_block_orders_grouped,
            sco_bids_tramo_grouped,
        )

        # Extract information from the model
        cleared_energy = get_cleared_energy_series(model)
        clearing_prices = get_clearing_prices_df(model)
        trial_results_sco_casadas_grouped = get_trial_results_sco_casadas_grouped(
            det_cab_date_scos_filtered, cleared_energy, clearing_prices
        )
        welfare = pyo.value(model.OBJ)
        is_mic_respected = (
            trial_results_sco_casadas_grouped["Beneficio_neto"] >= 0
        ).all()

        # Update trials_df with current trial results
        trial_df_entry = {
            SCOS_WITH_MIC_COLUMN: [current_trial_scos_with_mic],
            OBJECTIVE_VALUE_COLUMN: [welfare],
            IS_MIC_RESPECTED_COLUMN: [is_mic_respected],
            SOLVER_RESULTS_COLUMN: [results],
            SCOS_WITH_MIC_COUNT_COLUMN: [len(current_trial_scos_with_mic)],
            CLEARED_ENERGY_COLUMN: [cleared_energy],
            CLEARING_PRICES_COLUMN: [clearing_prices],
            SPAIN_PORTUGAL_TRANSMISSION_COLUMN: [
                get_spain_portugal_transmissions(model)
            ],
        }
        trials_df = pd.concat(
            [trials_df, pd.DataFrame(trial_df_entry)], ignore_index=True
        )

        # Update best model if current trial is the best so far
        best_trial = get_best_trial(trials_df, mic_respected_only=False)
        is_current_trial_best = set(best_trial[SCOS_WITH_MIC_COLUMN]) == set(
            current_trial_scos_with_mic
        )
        if is_current_trial_best:
            best_model = model
            best_model_binary = model_binary

        # If the optimization of all data was successful in the first trial, finish
        if (
            trial == 0
            and is_mic_respected
            and len(trials_df) == 1
            and trial_scos_with_mic is None
        ):

            logger.info(
                "--ALGORITHM--: First trial with all SCOs correctly cleared, finishing"
            )
            break

        current_trial_scos_with_mic = define_new_trial_scos_with_mic(
            trials_df, det_cab_date, all_scos_with_mic
        )
        if current_trial_scos_with_mic is False:
            logger.info("--ALGORITHM--: All combinations tried, finishing")
            break

    return trials_df, best_model, best_model_binary
