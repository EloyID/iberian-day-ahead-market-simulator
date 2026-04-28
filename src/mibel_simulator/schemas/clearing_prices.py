import pandera.pandas as pa
import iberian_day_ahead_market_simulator.columns as cols
from .columns_dict import columns_dict

# fmt: off

ClearingPricesSchema = pa.DataFrameSchema(
    {
        cols.INT_PERIOD:           columns_dict[cols.INT_PERIOD],
        cols.FLOAT_CLEARED_PRICE:   columns_dict[cols.FLOAT_CLEARED_PRICE],
        cols.CAT_BIDDING_ZONE:              columns_dict[cols.CAT_BIDDING_ZONE],
    }
)
