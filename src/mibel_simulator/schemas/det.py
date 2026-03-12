import pandera.pandas as pa

import mibel_simulator.columns as cols

from .columns_dict import columns_dict

exclusive_group_must_be_block_offer = pa.Check(
    lambda df: (df[cols.INT_NUM_EXCL_GROUP] == 0) | (df[cols.INT_NUM_BLOCK] > 0),
    element_wise=True,
    error=f"An exclusive offer include {cols.INT_NUM_BLOCK}",
)

mar_only_in_block_offers = pa.Check(
    lambda df: (df[cols.INT_NUM_BLOCK] > 0) | (df[cols.FLOAT_MAR] == 0),
    element_wise=True,
    error=f"{cols.FLOAT_MAR} > 0 only for block offers",
)

mav_only_in_scos = pa.Check(
    lambda df: (df[cols.INT_NUM_BLOCK] == 0) | (df[cols.FLOAT_MAV] == 0),
    element_wise=True,
    error=f"{cols.FLOAT_MAV} > 0 block offers cannot have MAV > 0",
)


multiple_suborders_only_scos = pa.Check(
    lambda df: (df[cols.INT_NUM_BLOCK] == 0) | (df[cols.INT_NUM_SUBORDER] == 1),
    element_wise=True,
    error=f"{cols.INT_NUM_SUBORDER} > 1 block offers cannot have NumSuborder > 1",
)

mav_only_in_suborder_1 = pa.Check(
    lambda df: (df[cols.INT_NUM_SUBORDER] == 1) | (df[cols.FLOAT_MAV] == 0),
    element_wise=True,
    error=f"{cols.FLOAT_MAV} > 0 only for {cols.INT_NUM_SUBORDER} == 1",
)

# fmt: off

DETSchema = pa.DataFrameSchema(
    {
        cols.DATE_SESION:        columns_dict[cols.DATE_SESION],
        cols.ID_ORDER:           columns_dict[cols.ID_ORDER],
        cols.INT_PERIOD:        columns_dict[cols.INT_PERIOD],
        cols.INT_NUM_BLOCK:       columns_dict[cols.INT_NUM_BLOCK],
        cols.INT_NUM_SUBORDER:      columns_dict[cols.INT_NUM_SUBORDER],
        cols.INT_NUM_EXCL_GROUP: columns_dict[cols.INT_NUM_EXCL_GROUP],
        cols.FLOAT_BID_PRICE:    columns_dict[cols.FLOAT_BID_PRICE],
        cols.FLOAT_BID_POWER:    columns_dict[cols.FLOAT_BID_POWER],
        cols.FLOAT_MAV:          columns_dict[cols.FLOAT_MAV],
        cols.FLOAT_MAR:          columns_dict[cols.FLOAT_MAR],
    },
    checks=[
        exclusive_group_must_be_block_offer,
        mar_only_in_block_offers,
        mav_only_in_scos,
        multiple_suborders_only_scos,
        mav_only_in_suborder_1,

    ],

)
