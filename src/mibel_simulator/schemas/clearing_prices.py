import pandera as pa
import mibel_simulator.columns as cols
from .columns_dict import columns_dict

# fmt: off

ClearingPricesSchema = pa.DataFrameSchema(
    {
        cols.INT_PERIODO:           columns_dict[cols.INT_PERIODO],
        cols.FLOAT_CLEARED_PRICE:   columns_dict[cols.FLOAT_CLEARED_PRICE],
        cols.CAT_PAIS:              columns_dict[cols.CAT_PAIS],
    }
)
