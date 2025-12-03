import pandera as pa
import mibel_simulator.columns as cols
from .columns_dict import columns_dict
from .det import DETSchema
from .cab import CABSchema

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
    }
).update_columns(
    {
        cols.ID_ORDER: {"unique": False},
        cols.ID_UNIDAD: {"unique": False},
    }
)
