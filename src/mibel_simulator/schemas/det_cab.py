import pandas as pd
import pandera as pa
import mibel_simulator.columns as cols
from mibel_simulator.const import (
    BLOCK_UNIQUE_IDENTIFIERS,
    DET_CAB_DATE_UNIQUE_IDENTIFIERS,
)
from .columns_dict import columns_dict
from .cab import CABSchema
from .det import (
    DETSchema,
    exclusive_group_must_be_block_offer,
    mar_only_in_block_offers,
    mav_only_in_scos,
    multiple_tramos_only_scos,
)


same_block_order_price = pa.Check(
    lambda df: df.query(f"{cols.INT_NUM_BLOQ} > 0")
    .groupby([cols.ID_ORDER, cols.INT_NUM_BLOQ], observed=True)[cols.FLOAT_BID_PRICE]
    .nunique()
    .eq(1)
    .all(),
    element_wise=False,
    error="All block (same id_order, same int_num_bloq) must have the same price across all periods",
)

same_block_mar = pa.Check(
    lambda df: df.query(f"{cols.INT_NUM_BLOQ} > 0")
    .groupby([cols.ID_ORDER, cols.INT_NUM_BLOQ], observed=True)[cols.FLOAT_MAR]
    .nunique()
    .eq(1)
    .all(),
    element_wise=False,
    error="All block (same id_order, same int_num_bloq) must have the same MAR across all periods",
)

buy_bids_cannot_have_scos = pa.Check(
    lambda df: df.query(f"{cols.CAT_BUY_SELL} == 'C' and {cols.FLOAT_MIC} > 0").empty,
    element_wise=False,
    error=f"Buy bids {cols.CAT_BUY_SELL} == 'C' cannot be SCO ({cols.FLOAT_MIC} > 0)",
)

buy_bids_cannot_have_mar = pa.Check(
    lambda df: df.query(f"{cols.CAT_BUY_SELL} == 'C' and {cols.FLOAT_MAR} > 0").empty,
    element_wise=False,
    error=f"Buy bids {cols.CAT_BUY_SELL} == 'C' cannot be SCO ({cols.FLOAT_MAR} > 0)",
)

buy_bids_exclusive_block_offers = pa.Check(
    lambda df: df.query(
        f"{cols.CAT_BUY_SELL} == 'C' and {cols.INT_NUM_BLOQ} > 0"
    ).empty,
    element_wise=False,
    error=f"Buy bids {cols.CAT_BUY_SELL} == 'C' cannot be block offers ({cols.INT_NUM_BLOQ} > 0)",
)

buy_bids_exclusive_offers = pa.Check(
    lambda df: df.query(
        f"{cols.CAT_BUY_SELL} == 'C' and {cols.INT_NUM_GRUPO_EXCL} > 0"
    ).empty,
    element_wise=False,
    error=f"Buy bids {cols.CAT_BUY_SELL} == 'C' cannot be exclusive offers ({cols.INT_NUM_GRUPO_EXCL} > 0)",
)

buy_bids_cannot_have_mav = pa.Check(
    lambda df: df.query(f"{cols.CAT_BUY_SELL} == 'C' and {cols.FLOAT_MAV} > 0").empty,
    element_wise=False,
    error=f"Buy bids {cols.CAT_BUY_SELL} == 'C' cannot have MAV ({cols.FLOAT_MAV} > 0)",
)

mic_only_in_scos = pa.Check(
    lambda df: (df[cols.INT_NUM_BLOQ] == 0) | (df[cols.FLOAT_MIC] == 0),
    element_wise=True,
    error=f"{cols.FLOAT_MIC} > 0 block offers cannot have MIC > 0",
)

check_not_exclusive_groups_max_power_not_exceeded = pa.Check(
    lambda df: df.query(f"{cols.INT_NUM_GRUPO_EXCL} == 0")
    .groupby([cols.ID_ORDER, cols.INT_PERIODO], observed=True)
    .agg(
        {
            cols.FLOAT_BID_POWER: "sum",
            cols.FLOAT_MAX_POWER: "first",
        }
    )
    .eval(f"{cols.FLOAT_BID_POWER} = {cols.FLOAT_BID_POWER}.round(2)")
    .query(f"{cols.FLOAT_BID_POWER} > {cols.FLOAT_MAX_POWER}")
    .empty,
    element_wise=False,
    error="There are orders (outside exclusive groups) that exceed MaxPot in some periods",
)

check_exclusive_groups_max_power_not_exceeded = pa.Check(
    lambda df: df.query(f"{cols.INT_NUM_GRUPO_EXCL} > 0")
    .groupby(
        [cols.ID_ORDER, cols.INT_NUM_GRUPO_EXCL, cols.INT_NUM_BLOQ, cols.INT_PERIODO],
        observed=True,
    )
    .agg(
        {
            cols.FLOAT_BID_POWER: "sum",
            cols.FLOAT_MAX_POWER: "first",
        }
    )
    .eval(f"{cols.FLOAT_BID_POWER} = {cols.FLOAT_BID_POWER}.round(2)")
    .query(f"{cols.FLOAT_BID_POWER} > {cols.FLOAT_MAX_POWER}")
    .empty,
    element_wise=False,
    error="There are exclusive groups that exceed MaxPot in some periods",
)


# fmt: off


DETCABSchema = pa.DataFrameSchema(
    {
        **CABSchema.columns,
        **DETSchema.columns,
        cols.ID_INDIVIDUAL_BID:                     columns_dict[cols.ID_INDIVIDUAL_BID],
        cols.ID_BLOCK_ORDER:                        columns_dict[cols.ID_BLOCK_ORDER],
        cols.ID_SCO:                                columns_dict[cols.ID_SCO],
        cols.CAT_ORDER_TYPE:                        columns_dict[cols.CAT_ORDER_TYPE],
        cols.FLOAT_BID_POWER_CUMSUM:                columns_dict[cols.FLOAT_BID_POWER_CUMSUM],
        cols.FLOAT_BID_POWER_CUMSUM_BY_COUNTRY:     columns_dict[cols.FLOAT_BID_POWER_CUMSUM_BY_COUNTRY],
    },
    unique=DET_CAB_DATE_UNIQUE_IDENTIFIERS,
    checks=[
        same_block_order_price,
        exclusive_group_must_be_block_offer,
        mar_only_in_block_offers,
        mav_only_in_scos,
        mic_only_in_scos,
        multiple_tramos_only_scos,
        buy_bids_cannot_have_scos,
        buy_bids_cannot_have_mar,
        buy_bids_exclusive_block_offers,
        buy_bids_exclusive_offers,
        buy_bids_cannot_have_mav,
        check_not_exclusive_groups_max_power_not_exceeded,
        check_exclusive_groups_max_power_not_exceeded,
    ],
).update_columns(
    {
        cols.ID_ORDER: {"unique": False},
        cols.ID_UNIDAD: {"unique": False},
    }
)
