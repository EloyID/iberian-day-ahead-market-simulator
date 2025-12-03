import pandera as pa
import mibel_simulator.columns as cols
from .columns_dict import columns_dict

# fmt: off

TrialsSchema = pa.DataFrameSchema(
    {
        cols.MIC_SCOS_COLUMN:                      columns_dict[cols.MIC_SCOS_COLUMN],
        cols.BOOL_IS_MIC_RESPECTED:                columns_dict[cols.BOOL_IS_MIC_RESPECTED],
        cols.SOLVER_RESULTS_COLUMN:                columns_dict[cols.SOLVER_RESULTS_COLUMN],
        cols.INT_MIC_SCOS_COUNT:                   columns_dict[cols.INT_MIC_SCOS_COUNT],
        cols.CLEARED_ENERGY_COLUMN:                columns_dict[cols.CLEARED_ENERGY_COLUMN],
        cols.CLEARING_PRICES_COLUMN:               columns_dict[cols.CLEARING_PRICES_COLUMN],
        cols.SPAIN_PORTUGAL_TRANSMISSIONS_COLUMN:  columns_dict[cols.SPAIN_PORTUGAL_TRANSMISSIONS_COLUMN],
    }
)
