import pandera as pa
import mibel_simulator.columns as cols
from .columns_dict import columns_dict

# fmt: off

DETSchema = pa.DataFrameSchema(
    {
        cols.DATE_SESION:        columns_dict[cols.DATE_SESION],
        cols.ID_ORDER:           columns_dict[cols.ID_ORDER],
        cols.INT_PERIODO:        columns_dict[cols.INT_PERIODO],
        cols.INT_NUM_BLOQ:       columns_dict[cols.INT_NUM_BLOQ],
        cols.INT_NUM_TRAMO:      columns_dict[cols.INT_NUM_TRAMO],
        cols.INT_NUM_GRUPO_EXCL: columns_dict[cols.INT_NUM_GRUPO_EXCL],
        cols.FLOAT_BID_PRICE:    columns_dict[cols.FLOAT_BID_PRICE],
        cols.FLOAT_BID_POWER:    columns_dict[cols.FLOAT_BID_POWER],
        cols.FLOAT_MAV:          columns_dict[cols.FLOAT_MAV],
        cols.FLOAT_MAR:          columns_dict[cols.FLOAT_MAR],
    }
)
