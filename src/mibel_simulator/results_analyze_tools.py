import numpy as np
import pandas as pd

from mibel_simulator.columns import INT_NUM_BLOQ
from mibel_simulator.schemas import det_cab


def compare_det_cab_date_and_curva_pbc_uof(
    det_cab_date: pd.DataFrame, curva_pbc_uof: pd.DataFrame
):

    curva_pbc_uof_casada = curva_pbc_uof.query(
        'cod_ofertada_casada == "C" and cod_simple_block_orders != "S"'
    )

    det_cab_date_energy_cleared_by_hour_and_unidad = (
        det_cab_date.query("float_cleared_power > 0 and cat_order_type != 'S'")
        .groupby(["id_unidad", "int_periodo"])["float_cleared_power"]
        .sum()
    ).unstack()

    display(
        "Total cleared energy in det_cab_date by complex orders",
        det_cab_date_energy_cleared_by_hour_and_unidad,
    )

    curva_pbc_uof_energy_cleared_by_hour_and_unidad = (
        curva_pbc_uof_casada.groupby(["id_unidad", "qua_hora"])["qua_energia"].sum()
    ).unstack()

    display(
        "Total cleared energy in curva_pbc_uof_casada by complex orders",
        curva_pbc_uof_energy_cleared_by_hour_and_unidad,
    )

    for i in range(1, 25):
        if i not in det_cab_date_energy_cleared_by_hour_and_unidad.columns:
            det_cab_date_energy_cleared_by_hour_and_unidad[i] = np.nan
        if i not in curva_pbc_uof_energy_cleared_by_hour_and_unidad.columns:
            curva_pbc_uof_energy_cleared_by_hour_and_unidad[i] = np.nan
    ordered_columns = list(range(1, 25))
    det_cab_date_energy_cleared_by_hour_and_unidad = (
        det_cab_date_energy_cleared_by_hour_and_unidad[ordered_columns]
    )
    curva_pbc_uof_energy_cleared_by_hour_and_unidad = (
        curva_pbc_uof_energy_cleared_by_hour_and_unidad[ordered_columns]
    )

    cleared_energy_merged = pd.merge(
        det_cab_date_energy_cleared_by_hour_and_unidad,
        curva_pbc_uof_energy_cleared_by_hour_and_unidad,
        left_index=True,
        right_index=True,
        suffixes=("_calculated", "_reference"),
        how="outer",
    ).fillna(0)

    calculated_columns = [
        col for col in cleared_energy_merged.columns if col.endswith("_calculated")
    ]
    reference_columns = [
        col for col in cleared_energy_merged.columns if col.endswith("_reference")
    ]

    # use np.close tol 0.001 to compare each row
    cleared_energy_merged["same_cleared_energy"] = cleared_energy_merged.apply(
        lambda row: all(
            abs(row[calc_col] - row[ref_col]) < 0.001
            for calc_col, ref_col in zip(calculated_columns, reference_columns)
        ),
        axis=1,
    )

    same_cleared_energy_groups = cleared_energy_merged.query(
        "same_cleared_energy == True"
    ).index.tolist()
    same_cleared_energy_groups_count = len(same_cleared_energy_groups)
    different_cleared_energy_groups = cleared_energy_merged.query(
        "same_cleared_energy == False"
    ).index.tolist()
    different_cleared_energy_groups_count = len(different_cleared_energy_groups)

    display(
        f"Paradox groups with same cleared energy ({same_cleared_energy_groups_count}): {same_cleared_energy_groups}"
    )
    display(
        f"Paradox groups with different cleared energy ({different_cleared_energy_groups_count}): {different_cleared_energy_groups}"
    )
    columns_zipped = zip(calculated_columns, reference_columns)
    columns_zipped = [[col1, col2] for col1, col2 in columns_zipped]
    columns_zipped_flattened = sum(columns_zipped, [])
    display(
        cleared_energy_merged.query("same_cleared_energy == False")[
            columns_zipped_flattened
        ]
    )

    reference_C01_groups = (
        curva_pbc_uof.query('cod_simple_block_orders == "C01"')
        .id_unidad.unique()
        .tolist()
    )
    calculated_C01_groups = (
        det_cab_date.query('cat_order_type == "C01"')["id_unidad"].unique().tolist()
    )
    C01_groups = set(reference_C01_groups).union(set(calculated_C01_groups))
    reference_C02_groups = (
        curva_pbc_uof.query('cod_simple_block_orders == "C02"')
        .id_unidad.unique()
        .tolist()
    )
    calculated_C02_groups = (
        det_cab_date.query('cat_order_type == "C02"')["id_unidad"].unique().tolist()
    )
    C02_groups = set(reference_C02_groups).union(set(calculated_C02_groups))
    reference_C04_groups = (
        curva_pbc_uof.query('cod_simple_block_orders == "C04"')
        .id_unidad.unique()
        .tolist()
    )
    calculated_C04_groups = (
        det_cab_date.query('cat_order_type == "C04"')["id_unidad"].unique().tolist()
    )
    C04_groups = set(reference_C04_groups).union(set(calculated_C04_groups))

    display(f"========== Reference C01 groups: {reference_C01_groups}")
    if set(reference_C01_groups) != set(calculated_C01_groups):
        display("Discrepancy in C01 (BLOCKS) groups:")
        display(f"Calculated C01 groups: {calculated_C01_groups}")

    cleared_energy_merged_C01_discrepating = cleared_energy_merged.query(
        "same_cleared_energy == False and id_unidad in @C01_groups"
    )
    display("Cleared energy discrepancies C01:", cleared_energy_merged_C01_discrepating)
    if not cleared_energy_merged_C01_discrepating.empty:
        cleared_energy_merged_C01_discrepating_reference = (
            cleared_energy_merged_C01_discrepating[reference_columns]
            .T.eval('index = index.str.replace("_reference", "")')
            .set_index("index")
        )
        cleared_energy_merged_C01_discrepating_calculated = (
            cleared_energy_merged_C01_discrepating[calculated_columns]
            .T.eval('index = index.str.replace("_calculated", "")')
            .set_index("index")
        )
        cleared_energy_merged_C01_discrepating_diff = (
            cleared_energy_merged_C01_discrepating_calculated
            - cleared_energy_merged_C01_discrepating_reference
        )
        cleared_energy_merged_C01_discrepating = (
            pd.concat(
                {
                    "calculated": cleared_energy_merged_C01_discrepating_calculated,
                    "reference": cleared_energy_merged_C01_discrepating_reference,
                    "zdifference": cleared_energy_merged_C01_discrepating_diff,
                },
                axis=1,
            )
            .swaplevel(0, 1, axis=1)
            .sort_index(axis=1)
        )
        display(
            "Cleared energy discrepancies reference pivoted:",
            # column coolwarm style to highlight differences only zdifference columns
            cleared_energy_merged_C01_discrepating.style.background_gradient(
                cmap="coolwarm",
                axis=0,
                subset=cleared_energy_merged_C01_discrepating.columns[
                    cleared_energy_merged_C01_discrepating.columns.get_level_values(1)
                    == "zdifference"
                ],
            ),
        )
    else:
        display("No discrepancies in cleared energy for C01 orders.")

    display(f"========== Reference C02 groups: {reference_C02_groups}")
    if set(reference_C02_groups) != set(calculated_C02_groups):
        display("Discrepancy in C02 (SCO) groups:")
        display(f"Calculated C02 groups: {calculated_C02_groups}")

    cleared_energy_merged_C02_discrepating = cleared_energy_merged.query(
        "same_cleared_energy == False and id_unidad in @C02_groups"
    )
    if not cleared_energy_merged_C02_discrepating.empty:

        cleared_energy_merged_C02_discrepating_reference = (
            cleared_energy_merged_C02_discrepating[reference_columns]
            .T.eval('index = index.str.replace("_reference", "")')
            .set_index("index")
        )
        cleared_energy_merged_C02_discrepating_calculated = (
            cleared_energy_merged_C02_discrepating[calculated_columns]
            .T.eval('index = index.str.replace("_calculated", "")')
            .set_index("index")
        )
        cleared_energy_merged_C02_discrepating_diff = (
            cleared_energy_merged_C02_discrepating_calculated
            - cleared_energy_merged_C02_discrepating_reference
        )

        cleared_energy_merged_C02_discrepating = (
            pd.concat(
                {
                    "calculated": cleared_energy_merged_C02_discrepating_calculated,
                    "reference": cleared_energy_merged_C02_discrepating_reference,
                    "zdifference": cleared_energy_merged_C02_discrepating_diff,
                },
                axis=1,
            )
            .swaplevel(0, 1, axis=1)
            .sort_index(axis=1)
        )

        display(
            "Cleared energy discrepancies reference pivoted:",
            # column coolwarm style to highlight differences only zdifference columns
            cleared_energy_merged_C02_discrepating.style.background_gradient(
                cmap="coolwarm",
                axis=0,
                subset=cleared_energy_merged_C02_discrepating.columns[
                    cleared_energy_merged_C02_discrepating.columns.get_level_values(1)
                    == "zdifference"
                ],
            ),
        )
    else:
        display("No discrepancies in cleared energy for C02 orders.")

    display(f"========== Reference C04 groups: {reference_C04_groups}")
    if set(reference_C04_groups) != set(calculated_C04_groups):
        display("Discrepancy in C04 (EXCLUSIVE BLOCK) groups:")
        display(f"Calculated C04 groups: {calculated_C04_groups}")

    cleared_energy_merged_C04_discrepating = cleared_energy_merged.query(
        "same_cleared_energy == False and id_unidad in @C04_groups"
    )
    if not cleared_energy_merged_C04_discrepating.empty:

        cleared_energy_merged_C04_discrepating_reference = (
            cleared_energy_merged_C04_discrepating[reference_columns]
            .T.eval('index = index.str.replace("_reference", "")')
            .set_index("index")
        )
        cleared_energy_merged_C04_discrepating_calculated = (
            cleared_energy_merged_C04_discrepating[calculated_columns]
            .T.eval('index = index.str.replace("_calculated", "")')
            .set_index("index")
        )
        cleared_energy_merged_C04_discrepating_diff = (
            cleared_energy_merged_C04_discrepating_calculated
            - cleared_energy_merged_C04_discrepating_reference
        )
        cleared_energy_merged_C04_discrepating = (
            pd.concat(
                {
                    "calculated": cleared_energy_merged_C04_discrepating_calculated,
                    "reference": cleared_energy_merged_C04_discrepating_reference,
                    "zdifference": cleared_energy_merged_C04_discrepating_diff,
                },
                axis=1,
            )
            .swaplevel(0, 1, axis=1)
            .sort_index(axis=1)
        )
        display(
            "Cleared energy discrepancies reference pivoted:",
            # column coolwarm style to highlight differences only zdifference columns
            cleared_energy_merged_C04_discrepating.style.background_gradient(
                cmap="coolwarm",
                axis=0,
                subset=cleared_energy_merged_C04_discrepating.columns[
                    cleared_energy_merged_C04_discrepating.columns.get_level_values(1)
                    == "zdifference"
                ],
            ),
        )

    else:
        display("No discrepancies in cleared energy for C04 orders.")

    if "MIEU" in same_cleared_energy_groups:
        display(
            "========== MIEU (Exchange with France9 group has matching cleared energy."
        )
    else:
        display(
            "========== MIEU (Exchange with France9 group has DISCREPANT cleared energy."
        )

    MIEU_calculated = (
        det_cab_date.query("float_cleared_power > 0 and id_unidad == 'MIEU'")
        .groupby(["cat_buy_sell", "int_periodo"])["float_cleared_power"]
        .sum()
    ).unstack(level=0)
    MIEU_reference = (
        curva_pbc_uof_casada.query("id_unidad == 'MIEU'")
        .groupby(["cod_tipo_oferta", "qua_hora"])["qua_energia"]
        .sum()
    ).unstack(level=0)

    MIEU_cleared_energy_merged = (
        pd.merge(
            MIEU_calculated.reset_index(),
            MIEU_reference.reset_index(),
            left_on="int_periodo",
            right_on="qua_hora",
            suffixes=("_calculated", "_reference"),
            how="outer",
        )
        .fillna(0)
        .set_index("int_periodo")
    )
    MIEU_cleared_energy_merged_columns_sorted = sorted(
        MIEU_cleared_energy_merged.columns
    )
    MIEU_cleared_energy_merged = MIEU_cleared_energy_merged[
        MIEU_cleared_energy_merged_columns_sorted
    ]

    display(
        "MIEU cleared energy details:",
        MIEU_cleared_energy_merged.style.background_gradient(cmap="coolwarm"),
    )


def reconstruct_C04_orders_price_in_curva_pbc_uof_C_V_C04(
    curva_pbc_uof_C_V_C04: pd.DataFrame,
    det_cab_date: pd.DataFrame,
) -> pd.DataFrame:
    det_cab_date_C04 = det_cab_date.query('cat_order_type == "C04"')
    curva_pbc_uof_C_V_C04 = curva_pbc_uof_C_V_C04.copy()

    for name, groups in curva_pbc_uof_C_V_C04.groupby(["id_unidad"]):
        id_unidad = name
        det_cab_date_C04_unidad = det_cab_date_C04.query("id_unidad == @id_unidad")
        int_num_bloq_cats = det_cab_date_C04_unidad[INT_NUM_BLOQ].unique().tolist()
        for int_num_bloq_cat in int_num_bloq_cats:
            det_cab_date_C04_unidad_bloq = det_cab_date_C04_unidad.query(
                "@INT_NUM_BLOQ == @int_num_bloq_cat"
            )
            if len(det_cab_date_C04_unidad_bloq) != len(groups):
                continue
            if set(det_cab_date_C04_unidad_bloq["int_periodo"]) != set(
                groups["qua_hora"]
            ):
                continue

            best_cost = np.nan
            energy_matches = True
            cost = 0
            for index, row in groups.iterrows():
                int_periodo = row["qua_hora"]
                matching_det_cab_row = det_cab_date_C04_unidad_bloq.query(
                    "int_periodo == @int_periodo"
                ).iloc[0]
                if matching_det_cab_row.qua_energia < row.qua_energia:
                    energy_matches = False
                    break
                cost += row.qua_energia * matching_det_cab_row.float_bid_price.values[0]
            if energy_matches and cost < best_cost or np.isnan(best_cost):
                best_cost = cost
                best_int_num_bloq_cat = int_num_bloq_cat

        if np.isnan(best_cost):
            raise ValueError(
                f"No suitable C04 order found for C_V_C04 order for unidad {id_unidad}"
            )

        for index, row in groups.iterrows():
            int_periodo = row["qua_hora"]
            matching_det_cab_row = det_cab_date_C04_unidad.query(
                f"@INT_NUM_BLOQ == @best_int_num_bloq_cat and int_periodo == @int_periodo"
            ).iloc[0]
            curva_pbc_uof_C_V_C04.at[index, "qua_precio"] = (
                matching_det_cab_row.float_bid_price.values[0]
            )
    return curva_pbc_uof_C_V_C04


def reconstruct_C01_orders_price_in_curva_pbc_uof_C_V_C01(
    curva_pbc_uof_C_V_C01: pd.DataFrame,
    curva_pbc_uof: pd.DataFrame,
) -> pd.DataFrame:
    curva_pbc_uof_O_V_C01 = curva_pbc_uof.query(
        'cod_ofertada_casada == "O" and cod_tipo_oferta == "V" and cod_simple_block_orders == "C01"'
    )
    curva_pbc_uof_C_V_C01 = curva_pbc_uof_C_V_C01.copy()

    for name, groups in curva_pbc_uof_C_V_C01.groupby(["id_unidad", "qua_hora"]):
        id_unidad, qua_hora = name
        curva_pbc_uof_O_V_C01_unidad_hora = curva_pbc_uof_O_V_C01.query(
            "id_unidad == @id_unidad and qua_hora == @qua_hora"
        )
        for index, row in groups.iterrows():
            best_candidate = (
                curva_pbc_uof_O_V_C01_unidad_hora.query(
                    "qua_energia >= @row.qua_energia"
                )
                .sort_values("qua_precio")
                .head(1)
            )
            if best_candidate.empty:
                raise ValueError(
                    f"No suitable O_V_C01 order found for C_V_C01 order at index {index} (id_unidad={id_unidad}, qua_hora={qua_hora})"
                )
            curva_pbc_uof_C_V_C01.at[index, "qua_precio"] = best_candidate.iloc[0][
                "qua_precio"
            ]
            # Optionally, remove the used O_V_C01 order to avoid reusing it
            curva_pbc_uof_O_V_C01 = curva_pbc_uof_O_V_C01.drop(best_candidate.index)
    return curva_pbc_uof_C_V_C01


def simpliied_complex_orders_price_reconstruction(
    curva_pbc_uof_C_V,
    curva_pbc_uof_O_V,
):
    curva_pbc_uof_C_V_reconstructed = curva_pbc_uof_C_V.copy()

    for name, groups in curva_pbc_uof_C_V.groupby(["id_unidad", "qua_hora"]):
        id_unidad, qua_hora = name
        groups = groups.sort_values("qua_energia", ascending=False)
        curva_pbc_uof_O_V_unidad_hora = curva_pbc_uof_O_V.query(
            "id_unidad == @id_unidad and qua_hora == @qua_hora"
        )
        for index, row in groups.iterrows():
            best_candidate = (
                curva_pbc_uof_O_V_unidad_hora.query("qua_energia >= @row.qua_energia")
                .sort_values("qua_precio")
                .head(1)
            )
            if best_candidate.empty:
                raise ValueError(
                    f"No suitable O_V order found for C_V order at index {index} (id_unidad={id_unidad}, qua_hora={qua_hora})"
                )
            curva_pbc_uof_C_V_reconstructed.at[index, "qua_precio"] = (
                best_candidate.iloc[0]["qua_precio"]
            )
            # Optionally, remove the used O_V order to avoid reusing it
            curva_pbc_uof_O_V_unidad_hora = curva_pbc_uof_O_V_unidad_hora.drop(
                best_candidate.index
            )
    return curva_pbc_uof_C_V_reconstructed


def calculate_welfare_from_curva_pbc_uof(
    curva_pbc_uof: pd.DataFrame,
    det_cab_date: pd.DataFrame,
    omie_clearing_prices: pd.DataFrame,  # columns qua_hora, cod_pais, clearing_price
    france_clearing_prices: pd.Series,  # index qua_hora, values fr_clearing_price
) -> pd.DataFrame:

    omie_clearing_prices = omie_clearing_prices.copy()
    for hora in range(1, 25):
        omie_clearing_prices_hora = omie_clearing_prices.query("qua_hora == @hora")
        if omie_clearing_prices_hora.clearing_price.nunique() == 1:
            omie_clearing_prices.loc[omie_clearing_prices_hora.index, "cod_pais"] = "MI"
    omie_clearing_prices = omie_clearing_prices.drop_duplicates(
        subset=["qua_hora", "cod_pais"]
    )

    curva_pbc_uof = curva_pbc_uof.copy().merge(
        omie_clearing_prices,
        on=["qua_hora", "cod_pais"],
        how="left",
        validate="many_to_one",
        indicator="_merge_cp",
    )
    assert all(
        curva_pbc_uof["_merge_cp"] == "both"
    ), "Some rows in curva_pbc_uof have no matching clearing price"
    curva_pbc_uof = curva_pbc_uof.drop(columns=["_merge_cp"])

    # Define subsets
    # fmt: off
    curva_pbc_uof_C_C_S =       curva_pbc_uof.query('cod_ofertada_casada == "C" and cod_tipo_oferta == "C" and cod_simple_block_orders == "S"').copy()
    curva_pbc_uof_C_C_Exp_FR =  curva_pbc_uof.query('cod_ofertada_casada == "C" and cod_tipo_oferta == "C" and cod_simple_block_orders == "Exp FR"').copy()
    curva_pbc_uof_C_C_Exp_FR.id_unidad = "MIEUExpToFR"

    curva_pbc_uof_C_V_S =       curva_pbc_uof.query('cod_ofertada_casada == "C" and cod_tipo_oferta == "V" and cod_simple_block_orders == "S"').copy()
    curva_pbc_uof_C_V_C01 =     curva_pbc_uof.query('cod_ofertada_casada == "C" and cod_tipo_oferta == "V" and cod_simple_block_orders == "C01"').copy()
    curva_pbc_uof_C_V_C02 =     curva_pbc_uof.query('cod_ofertada_casada == "C" and cod_tipo_oferta == "V" and cod_simple_block_orders == "C02"').copy()
    curva_pbc_uof_C_V_C04 =     curva_pbc_uof.query('cod_ofertada_casada == "C" and cod_tipo_oferta == "V" and cod_simple_block_orders == "C04"').copy()
    curva_pbc_uof_C_V_Imp_FR =  curva_pbc_uof.query('cod_ofertada_casada == "C" and cod_tipo_oferta == "V" and cod_simple_block_orders == "Imp FR"').copy()
    curva_pbc_uof_C_V_Imp_FR.id_unidad = "MIEUImpFromFR"

    curva_pbc_uof_O_V_C01 = curva_pbc_uof.query('cod_ofertada_casada == "O" and cod_tipo_oferta == "V" and cod_simple_block_orders == "C01"')
    curva_pbc_uof_O_V_C02 = curva_pbc_uof.query('cod_ofertada_casada == "O" and cod_tipo_oferta == "V" and cod_simple_block_orders == "C02"')
    curva_pbc_uof_O_V_C04 = curva_pbc_uof.query('cod_ofertada_casada == "O" and cod_tipo_oferta == "V" and cod_simple_block_orders == "C04"')
    # fmt: on

    # Merge France clearing prices
    curva_pbc_uof_C_C_Exp_FR = curva_pbc_uof_C_C_Exp_FR.merge(
        france_clearing_prices,
        on="qua_hora",
        how="left",
        validate="many_to_one",
        indicator="_merge_fr_cp",
    )
    assert all(
        curva_pbc_uof_C_C_Exp_FR["_merge_fr_cp"] == "both"
    ), "Some rows in curva_pbc_uof_C_C_Exp_FR have no matching France clearing price"
    curva_pbc_uof_C_C_Exp_FR["qua_precio"] = curva_pbc_uof_C_C_Exp_FR[
        "fr_clearing_price"
    ]
    curva_pbc_uof_C_C_Exp_FR = curva_pbc_uof_C_C_Exp_FR.drop(
        columns=["_merge_fr_cp", "fr_clearing_price"]
    )

    curva_pbc_uof_C_V_Imp_FR = curva_pbc_uof_C_V_Imp_FR.merge(
        france_clearing_prices,
        on="qua_hora",
        how="left",
        validate="many_to_one",
        indicator="_merge_fr_cp",
    )
    assert all(
        curva_pbc_uof_C_V_Imp_FR["_merge_fr_cp"] == "both"
    ), "Some rows in curva_pbc_uof_C_V_Imp_FR have no matching France clearing price"
    curva_pbc_uof_C_V_Imp_FR["qua_precio"] = curva_pbc_uof_C_V_Imp_FR[
        "fr_clearing_price"
    ]
    curva_pbc_uof_C_V_Imp_FR = curva_pbc_uof_C_V_Imp_FR.drop(
        columns=["_merge_fr_cp", "fr_clearing_price"]
    )

    # calculate price of complex orders
    # fmt: off
    curva_pbc_uof_C_V_C01 = simpliied_complex_orders_price_reconstruction(curva_pbc_uof_C_V_C01,curva_pbc_uof_O_V_C01)
    curva_pbc_uof_C_V_C02 = simpliied_complex_orders_price_reconstruction(curva_pbc_uof_C_V_C02,curva_pbc_uof_O_V_C02)
    curva_pbc_uof_C_V_C04 = simpliied_complex_orders_price_reconstruction(curva_pbc_uof_C_V_C04,curva_pbc_uof_O_V_C04)
    # fmt: on

    # Calculate fix costs
    cleared_SCOs = curva_pbc_uof_C_V_C02.id_unidad.unique().tolist()
    fix_costs = {
        unidad: det_cab_date.query('cat_order_type == "C02" and id_unidad == @unidad')[
            "float_mic"
        ].iloc[0]
        for unidad in cleared_SCOs
    }

    # Calculate variable costs, income, and benefit

    buy_eval_string = """
    qua_value = qua_energia * qua_precio
    qua_paid = qua_energia * clearing_price
    qua_benefit = qua_value - qua_paid
    """
    curva_pbc_uof_C_C_S = curva_pbc_uof_C_C_S.eval(buy_eval_string)
    curva_pbc_uof_C_C_Exp_FR = curva_pbc_uof_C_C_Exp_FR.eval(buy_eval_string)

    sell_eval_string = """
    qua_var_costs = qua_energia * qua_precio
    qua_income = qua_energia * clearing_price
    qua_benefit = qua_income - qua_var_costs
    """
    curva_pbc_uof_C_V_S = curva_pbc_uof_C_V_S.eval(sell_eval_string)
    curva_pbc_uof_C_V_C01 = curva_pbc_uof_C_V_C01.eval(sell_eval_string)
    curva_pbc_uof_C_V_C02 = curva_pbc_uof_C_V_C02.eval(sell_eval_string)
    curva_pbc_uof_C_V_C04 = curva_pbc_uof_C_V_C04.eval(sell_eval_string)
    curva_pbc_uof_C_V_Imp_FR = curva_pbc_uof_C_V_Imp_FR.eval(sell_eval_string)

    # calculate benefit
    total_welfare = (
        curva_pbc_uof_C_C_S["qua_benefit"].sum()
        + curva_pbc_uof_C_C_Exp_FR["qua_benefit"].sum()
        + curva_pbc_uof_C_V_S["qua_benefit"].sum()
        + curva_pbc_uof_C_V_C01["qua_benefit"].sum()
        + curva_pbc_uof_C_V_C02["qua_benefit"].sum()
        + curva_pbc_uof_C_V_C04["qua_benefit"].sum()
        + curva_pbc_uof_C_V_Imp_FR["qua_benefit"].sum()
        - sum(fix_costs.values())
    )
    total_welfare_2 = (
        curva_pbc_uof_C_C_S["qua_value"].sum()
        + curva_pbc_uof_C_C_Exp_FR["qua_value"].sum()
        - curva_pbc_uof_C_V_S["qua_var_costs"].sum()
        - curva_pbc_uof_C_V_C01["qua_var_costs"].sum()
        - curva_pbc_uof_C_V_C02["qua_var_costs"].sum()
        - curva_pbc_uof_C_V_C04["qua_var_costs"].sum()
        - curva_pbc_uof_C_V_Imp_FR["qua_var_costs"].sum()
        - sum(fix_costs.values())
    )

    # fmt: off
    curva_pbc_uof_C_C_S_welfare = curva_pbc_uof_C_C_S.groupby("id_unidad")["qua_benefit"].sum()
    curva_pbc_uof_C_C_Exp_FR_welfare = curva_pbc_uof_C_C_Exp_FR.groupby("id_unidad")["qua_benefit"].sum()
    curva_pbc_uof_C_V_S_welfare = curva_pbc_uof_C_V_S.groupby("id_unidad")["qua_benefit"].sum()
    curva_pbc_uof_C_V_C01_welfare = curva_pbc_uof_C_V_C01.groupby("id_unidad")["qua_benefit"].sum()
    curva_pbc_uof_C_V_C02_welfare = curva_pbc_uof_C_V_C02.groupby("id_unidad")["qua_benefit"].sum()
    for unidad, fix_cost in fix_costs.items():
        if unidad in curva_pbc_uof_C_V_C02_welfare.index:
            curva_pbc_uof_C_V_C02_welfare[unidad] -= fix_cost
    curva_pbc_uof_C_V_C04_welfare = curva_pbc_uof_C_V_C04.groupby("id_unidad")["qua_benefit"].sum()
    curva_pbc_uof_C_V_Imp_FR_welfare = curva_pbc_uof_C_V_Imp_FR.groupby("id_unidad")["qua_benefit"].sum()
    # fmt: on

    welfare_by_unidad = pd.concat(
        [
            curva_pbc_uof_C_C_S_welfare,
            curva_pbc_uof_C_C_Exp_FR_welfare,
            curva_pbc_uof_C_V_S_welfare,
            curva_pbc_uof_C_V_C01_welfare,
            curva_pbc_uof_C_V_C02_welfare,
            curva_pbc_uof_C_V_C04_welfare,
            curva_pbc_uof_C_V_Imp_FR_welfare,
        ],
        axis=0,
    )

    assert np.isclose(welfare_by_unidad.sum(), total_welfare)
    if not np.isclose(welfare_by_unidad.sum(), total_welfare_2):
        print(
            f"Total welfare calculated in two different ways do not match, {total_welfare} vs {total_welfare_2}"
        )
    if any(welfare_by_unidad < -0.1):
        print("Warning: Some unidades have negative welfare:")
        print(welfare_by_unidad[welfare_by_unidad < -0.1])

    return welfare_by_unidad


def calculate_welfare_from_cleared_det_cab_date(
    cleared_det_cab_date: pd.DataFrame,
    omie_clearing_prices: pd.DataFrame,  # columns qua_hora, cod_pais, clearing_price
) -> pd.DataFrame:

    cleared_det_cab_date = cleared_det_cab_date.query("float_cleared_power > 0").copy()
    cleared_det_cab_date = cleared_det_cab_date.merge(
        omie_clearing_prices,
        left_on=["int_periodo", "cat_pais"],
        right_on=["qua_hora", "cod_pais"],
        how="left",
        validate="many_to_one",
        indicator="_merge_cp",
    )
    assert all(
        cleared_det_cab_date["_merge_cp"] == "both"
    ), "Some rows in cleared_det_cab_date have no matching clearing price"
    cleared_det_cab_date = cleared_det_cab_date.drop(columns=["_merge_cp"])

    cleared_det_cab_date_C = cleared_det_cab_date.query('cat_buy_sell == "C"').copy()
    cleared_det_cab_date_V = cleared_det_cab_date.query('cat_buy_sell == "V"').copy()

    cleared_det_cab_date_C = cleared_det_cab_date_C.eval(
        """
    qua_value = float_cleared_power * float_bid_price
    qua_paid = float_cleared_power * clearing_price
    qua_benefit = qua_value - qua_paid
    """
    )
    cleared_det_cab_date_V = cleared_det_cab_date_V.eval(
        """
    qua_var_costs = float_cleared_power * float_bid_price
    qua_income = float_cleared_power * clearing_price
    qua_benefit = qua_income - qua_var_costs
    """
    )

    cleared_det_cab_date_C_welfare = cleared_det_cab_date_C.groupby("id_unidad")[
        "qua_benefit"
    ].sum()
    cleared_det_cab_date_V_grouped = cleared_det_cab_date_V.groupby("id_unidad").agg(
        float_cleared_power_sum=("float_cleared_power", "sum"),
        qua_benefit_sum=("qua_benefit", "sum"),
        fix_cost=("float_mic", "first"),
    )
    # Calculate has_been_cleared outside eval
    cleared_det_cab_date_V_grouped["int_has_been_cleared"] = (
        cleared_det_cab_date_V_grouped["float_cleared_power_sum"] > 0
    ).astype(int)

    # Now use eval for the simple calculation
    cleared_det_cab_date_V_welfare = cleared_det_cab_date_V_grouped.eval(
        "qua_benefit = qua_benefit_sum - (fix_cost * int_has_been_cleared)"
    )["qua_benefit"]

    welfare_by_unidad = pd.concat(
        [
            cleared_det_cab_date_C_welfare,
            cleared_det_cab_date_V_welfare,
        ],
        axis=0,
    )

    total_welfare = welfare_by_unidad.sum()

    if any(welfare_by_unidad < -0.1):
        print("Warning: Some unidades have negative welfare:")
        print(welfare_by_unidad[welfare_by_unidad < -0.1])

    return welfare_by_unidad
