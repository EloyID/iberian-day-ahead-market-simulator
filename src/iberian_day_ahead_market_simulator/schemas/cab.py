import pandera.pandas as pa
import iberian_day_ahead_market_simulator.columns as cols
from .columns_dict import columns_dict

# fmt: off

CABSchema = pa.DataFrameSchema(
    {
        cols.DATE_SESION:      columns_dict[cols.DATE_SESION],
        cols.ID_ORDER:         columns_dict[cols.ID_ORDER],
        cols.ID_UNIDAD:        columns_dict[cols.ID_UNIDAD],
        cols.CAT_BUY_SELL:     columns_dict[cols.CAT_BUY_SELL],
        cols.FLOAT_MIC:        columns_dict[cols.FLOAT_MIC],
        cols.FLOAT_MAX_POWER:  columns_dict[cols.FLOAT_MAX_POWER],
    },
    unique=[cols.DATE_SESION, cols.ID_ORDER],
).update_columns(
    {
        cols.ID_ORDER:   {"unique": True},
        cols.ID_UNIDAD:  {"unique": True},
    }
)
