import pandera.pandas as pa
import mibel_simulator.columns as cols
from .columns_dict import columns_dict

# fmt: off

IterationsSchema = pa.DataFrameSchema(
    {
        cols.IDS_MIC_SCOS:                         columns_dict[cols.IDS_MIC_SCOS],
        cols.IDS_BID_BLOCKS:                       columns_dict[cols.IDS_BID_BLOCKS],
        cols.PARADOXAL_ORDERS_COLUMN:                columns_dict[cols.PARADOXAL_ORDERS_COLUMN],
        cols.IDS_PARADOXAL_ORDERS:                   columns_dict[cols.IDS_PARADOXAL_ORDERS],
        cols.FLOAT_OBJECTIVE_VALUE:                columns_dict[cols.FLOAT_OBJECTIVE_VALUE],
        cols.BOOL_IS_EXPECTED_INCOME_RESPECTED:    columns_dict[cols.BOOL_IS_EXPECTED_INCOME_RESPECTED],
        cols.SOLVER_RESULTS_COLUMN:                columns_dict[cols.SOLVER_RESULTS_COLUMN],
        cols.INT_MIC_SCOS_COUNT:                   columns_dict[cols.INT_MIC_SCOS_COUNT],
        cols.INT_BID_BLOCKS_COUNT:                 columns_dict[cols.INT_BID_BLOCKS_COUNT],
        cols.INT_PARADOXAL_ORDERS_COUNT:             columns_dict[cols.INT_PARADOXAL_ORDERS_COUNT],
        cols.CLEARED_ENERGY_COLUMN:                columns_dict[cols.CLEARED_ENERGY_COLUMN],
        cols.CLEARING_PRICES_COLUMN:               columns_dict[cols.CLEARING_PRICES_COLUMN],
        cols.SPAIN_PORTUGAL_TRANSMISSIONS_COLUMN:  columns_dict[cols.SPAIN_PORTUGAL_TRANSMISSIONS_COLUMN],
    }
)
