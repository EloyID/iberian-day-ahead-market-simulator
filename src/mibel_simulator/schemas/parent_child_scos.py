import pandera as pa
import mibel_simulator.columns as cols
from .columns_dict import columns_dict

# fmt: off

ParentChildSCOSSchema = pa.DataFrameSchema(
    {
        cols.ID_ORDER:              columns_dict[cols.ID_ORDER],
        cols.INT_NUM_TRAMO_PARENT:  columns_dict[cols.INT_NUM_TRAMO],
        cols.INT_NUM_TRAMO_CHILD:   columns_dict[cols.INT_NUM_TRAMO],
        cols.ID_SCO_PARENT:         columns_dict[cols.ID_SCO],
        cols.ID_SCO_CHILD:          columns_dict[cols.ID_SCO],
    },
    checks=[
        pa.Check(
            lambda df: df[cols.INT_NUM_TRAMO_PARENT]
            < df[cols.INT_NUM_TRAMO_CHILD],
            error="INT_NUM_TRAMO_parent must be less than INT_NUM_TRAMO_child",
        )
    ],
)
