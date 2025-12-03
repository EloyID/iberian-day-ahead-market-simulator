import pandera as pa
import mibel_simulator.columns as cols
from .columns_dict import columns_dict

# fmt: off

ParentChildBloquesSchema = pa.DataFrameSchema(
    {
        cols.ID_ORDER:                  columns_dict[cols.ID_ORDER],
        cols.INT_NUM_BLOQ_PARENT:       columns_dict[cols.INT_NUM_BLOQ],
        cols.INT_NUM_BLOQ_CHILD:        columns_dict[cols.INT_NUM_BLOQ],
        cols.INT_NUM_GRUPO_EXCL:        columns_dict[cols.INT_NUM_GRUPO_EXCL],
        cols.ID_BLOCK_ORDER_PARENT:     columns_dict[cols.ID_BLOCK_ORDER],
        cols.ID_BLOCK_ORDER_CHILD:      columns_dict[cols.ID_BLOCK_ORDER],
    },
    checks=[
        pa.Check(
            lambda df: df[cols.INT_NUM_BLOQ_PARENT] < df[cols.INT_NUM_BLOQ_CHILD],
            error="INT_NUM_BLOQ_parent must be less than INT_NUM_BLOQ_child",
        )
    ],
)
