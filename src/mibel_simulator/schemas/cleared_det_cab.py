import pandera.pandas as pa
import iberian_day_ahead_market_simulator.columns as cols
from .columns_dict import columns_dict
from .det_cab import DETCABSchema

# fmt: off

ClearedDetCabSchema = pa.DataFrameSchema(
    {
        **DETCABSchema.columns,
        cols.FLOAT_CLEARED_POWER: columns_dict[cols.FLOAT_CLEARED_POWER],
    }
)
