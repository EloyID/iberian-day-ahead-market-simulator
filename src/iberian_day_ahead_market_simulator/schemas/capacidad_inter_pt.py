import pandera.pandas as pa
import iberian_day_ahead_market_simulator.columns as cols
from .columns_dict import columns_dict

# fmt: off

CapacidadInterPTSchema = pa.DataFrameSchema(
    {
        cols.DATE_SESION:           columns_dict[cols.DATE_SESION],
        cols.CAT_FRONTIER:          columns_dict[cols.CAT_FRONTIER],
        cols.INT_PERIOD:           columns_dict[cols.INT_PERIOD],
        cols.FLOAT_IMPORT_CAPACITY: columns_dict[cols.FLOAT_IMPORT_CAPACITY],
        cols.FLOAT_EXPORT_CAPACITY: columns_dict[cols.FLOAT_EXPORT_CAPACITY],
    }
)
