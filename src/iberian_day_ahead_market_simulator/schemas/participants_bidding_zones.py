import pandera.pandas as pa
import iberian_day_ahead_market_simulator.columns as cols
from .columns_dict import columns_dict

# fmt: off

ParticipantBiddingZonesSchema = pa.DataFrameSchema(
    {
        cols.ID_UNIDAD:       columns_dict[cols.ID_UNIDAD],
        cols.CAT_BIDDING_ZONE:        columns_dict[cols.CAT_BIDDING_ZONE],
    },
    unique=[cols.ID_UNIDAD],
)
