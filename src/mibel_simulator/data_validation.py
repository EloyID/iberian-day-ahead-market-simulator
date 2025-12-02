import pandas as pd
import mibel_simulator.columns as cols
from .pandas_typing import CAB_MAIN_COLUMNS, CAB_TYPING, DET_MAIN_COLUMNS, DET_TYPING
from .tools import get_float_bid_power_cumsum


def det_cab_verifications(det_raw, cab_raw):

    det_raw = det_raw.copy().astype(DET_TYPING, errors="raise")[DET_MAIN_COLUMNS]
    cab_raw = cab_raw.copy().astype(CAB_TYPING, errors="raise")[CAB_MAIN_COLUMNS]

    det = det_raw.astype(DET_TYPING, errors="raise")[DET_MAIN_COLUMNS]
    cab = cab_raw.astype(CAB_TYPING, errors="raise")[CAB_MAIN_COLUMNS]

    det_cab = pd.merge(
        det,
        cab,
        how="outer",
        on=["dat_sesion", cols.ID_ORDER],
        suffixes=("_det", "_cab"),
        validate="many_to_one",
        indicator=True,
    )

    assert det_cab.query(
        "Version_det != Version_cab"
    ).empty, "There are different versions in det and cab for the same CodOferta and dat_sesion"

    assert det_cab._merge.isin(["both"]).all()
    det_cab = det_cab.drop(columns="_merge")

    det_cab = det_cab.query(f"{cols.INT_PERIODO} != 25")

    det_cab["cod_ofertada_casada"] = "O"
    det_cab[cols.FLOAT_BID_POWER_CUMSUM] = get_float_bid_power_cumsum(
        det_cab,
        date_column_name="dat_sesion",
        hour_column_name=cols.INT_PERIODO,
        cod_tipo_oferta_column_name=cols.CAT_BUY_SELL,
        cod_ofertada_casada_column_name="cod_ofertada_casada",
        qua_energia_column_name=cols.FLOAT_BID_POWER,
        qua_precio_column_name=cols.FLOAT_BID_PRICE,
    )

    unique_identifiers = [
        "dat_sesion",
        cols.INT_PERIODO,
        cols.CAT_BUY_SELL,
        cols.ID_ORDER,
        cols.INT_NUM_TRAMO,
        cols.INT_NUM_BLOQ,
    ]
    det_cab[cols.ID_INDIVIDUAL_BID] = (
        det_cab[unique_identifiers].astype(str).agg("_".join, axis=1)
    )
    assert (
        not det_cab[cols.ID_INDIVIDUAL_BID].duplicated().any()
    ), f"There are det_cab rows with the same unique identifiers {unique_identifiers}"

    bloq_unique_identifiers = [
        cols.ID_ORDER,
        cols.INT_PERIODO,
        "dat_sesion",
    ]
    det_cab_aux = (
        det_cab.query(f"{cols.INT_NUM_BLOQ} > 0")
        .groupby(bloq_unique_identifiers, observed=True)[cols.FLOAT_BID_PRICE]
        .diff()
        .fillna(0)
    )
    assert det_cab_aux[
        det_cab_aux != 0
    ].empty, "There are different prices in the same block, not supported"

    det_cab_aux = (
        det_cab.query(f"{cols.INT_NUM_BLOQ} > 0")
        .groupby(bloq_unique_identifiers, observed=True)[cols.FLOAT_MAR]
        .diff()
        .fillna(0)
    )
    assert det_cab_aux[
        det_cab_aux != 0
    ].empty, "There are different MAR in the same block, not supported"

    det_cab_potencia_offered_in_excess = (
        det_cab.groupby(bloq_unique_identifiers, observed=True)
        .agg(
            {
                cols.FLOAT_BID_POWER: "sum",
                cols.FLOAT_MAX_POWER: "first",
                cols.INT_NUM_BLOQ: "first",
            }
        )
        .reset_index()
        .eval(f"{cols.FLOAT_BID_POWER} = {cols.FLOAT_BID_POWER}.round(2)")
        .query("{cols.FLOAT_BID_POWER} > MaxPot")
    )
    det_cab_not_compliant = det_cab.merge(
        det_cab_potencia_offered_in_excess[bloq_unique_identifiers],
        on=bloq_unique_identifiers,
        how="inner",
    )
    for num_bloq in det_cab_not_compliant[cols.INT_NUM_BLOQ].unique():
        det_cab_not_compliant_exclusive_option = det_cab_not_compliant.query(
            f"({cols.INT_NUM_GRUPO_EXCL} > 0 and {cols.INT_NUM_BLOQ} == @num_bloq) or ({cols.INT_NUM_GRUPO_EXCL} == 0)"
        )
        det_cab_exclusive_option_potencia_offered_in_excess = (
            det_cab_not_compliant_exclusive_option.groupby(
                bloq_unique_identifiers, observed=True
            )
            .agg(
                {
                    cols.FLOAT_BID_POWER: "sum",
                    cols.FLOAT_MAX_POWER: "first",
                    cols.INT_NUM_BLOQ: "first",
                }
            )
            .reset_index()
            .eval(f"{cols.FLOAT_BID_POWER} = {cols.FLOAT_BID_POWER}.round(2)")
            .query(f"{cols.FLOAT_BID_POWER} > {cols.FLOAT_MAX_POWER}")
        )

        assert (
            det_cab_exclusive_option_potencia_offered_in_excess.empty
        ), f"There are offers with {cols.INT_NUM_BLOQ} == {num_bloq} that cannot be accepted together due to MaxPot"

    cab_aux = cab[cab.duplicated(subset=[cols.ID_UNIDAD, "dat_sesion"], keep=False)]
    assert len(cab_aux) == 0, "Some Unidades have more than one offer per day"

    assert det_cab.query(
        f"{cols.INT_NUM_GRUPO_EXCL} > 0 and {cols.INT_NUM_BLOQ} == 0"
    ).empty, "An exclusive offer must be a block offer"
    assert det_cab.query(
        f"{cols.INT_NUM_BLOQ} == 0 and {cols.FLOAT_MAR} > 0"
    ).empty, f"{cols.FLOAT_MAR} > 0 only for block offers"
    assert det_cab.query(
        f"{cols.INT_NUM_BLOQ} > 0 and {cols.FLOAT_MAV} > 0"
    ).empty, f"{cols.FLOAT_MAV} > 0 only for SCO or normal offers"
    assert det_cab.query(
        f"{cols.INT_NUM_BLOQ} > 0 and {cols.FLOAT_MIC} > 0"
    ).empty, f"{cols.FLOAT_MIC} > 0 only for SCO offers"
    assert det_cab.query(
        f"{cols.INT_NUM_BLOQ} > 0 and {cols.INT_NUM_TRAMO} > 1"
    ).empty, f"{cols.INT_NUM_TRAMO} > 1 only for SCO and simple offers"
    assert det_cab.query(
        f"{cols.FLOAT_MIC} >= 0"
    ).empty, f"{cols.FLOAT_MIC} must be >= 0"

    det_cab_C = det_cab.query(f"{cols.CAT_BUY_SELL} == 'C'").copy()
    det_cab_V = det_cab.query(f"{cols.CAT_BUY_SELL} == 'V'").copy()

    # Ofertas de compra
    assert det_cab_C.query(
        f"{cols.FLOAT_MIC} > 0"
    ).empty, "No puede haber SCO en ofertas de compra"
    assert det_cab_C.query(
        f"{cols.INT_NUM_BLOQ} > 0"
    ).empty, "No puede haber bloques en ofertas de compra"
    assert det_cab_C.query(
        f"{cols.INT_NUM_GRUPO_EXCL} > 0"
    ).empty, "No puede haber exclusivos en ofertas de compra"
    assert det_cab_C.query(
        f"{cols.FLOAT_MAR} > 0"
    ).empty, "No puede haber MAR en ofertas de compra"
    assert det_cab_C.query(
        f"{cols.FLOAT_MAV} > 0"
    ).empty, "No puede haber MAV en ofertas de compra"

    det_cab_V_aux = (
        det_cab_V.copy()
        .query(f"{cols.INT_NUM_BLOQ} == 0")
        .sort_values(
            ["dat_sesion", cols.ID_ORDER, cols.INT_PERIODO, cols.INT_NUM_TRAMO]
        )
    )
    det_cab_V_aux["Diff Tramo PrecEuro"] = (
        det_cab_V_aux.groupby(["dat_sesion", cols.ID_ORDER, cols.INT_PERIODO])[
            cols.FLOAT_BID_PRICE
        ]
        .diff()
        .fillna(0)
    )
    assert det_cab_V_aux.query(
        "`Diff Tramo PrecEuro` < 0"
    ).empty, "There are some selling prices that decrease with NumTramo"

    det_cab_C_aux = (
        det_cab_C.copy()
        .query(f"{cols.INT_NUM_BLOQ} == 0")
        .sort_values(
            ["dat_sesion", cols.ID_ORDER, cols.INT_PERIODO, cols.INT_NUM_TRAMO]
        )
    )
    det_cab_C_aux["Diff Tramo PrecEuro"] = (
        det_cab_C_aux.groupby(["dat_sesion", cols.ID_ORDER, cols.INT_PERIODO])[
            cols.FLOAT_BID_PRICE
        ]
        .diff()
        .fillna(0)
    )
    assert det_cab_C_aux.query(
        "`Diff Tramo PrecEuro` > 0"
    ).empty, "There are some buying prices that decrease with NumTramo"

    det_cab_aux = (
        det_cab.copy()
        .query(f"{cols.INT_NUM_BLOQ} == 0")
        .sort_values(
            ["dat_sesion", cols.ID_ORDER, cols.INT_PERIODO, cols.INT_NUM_TRAMO]
        )
    )
    det_cab_aux["NumTramo_diff"] = (
        det_cab_aux.groupby(["dat_sesion", cols.ID_ORDER, cols.INT_PERIODO])[
            cols.INT_NUM_TRAMO
        ]
        .diff()
        .fillna(1)
    )
    assert (
        len(det_cab_aux.query("NumTramo_diff != 1")) > 0
    ), "It changed, now all are consecutive"
