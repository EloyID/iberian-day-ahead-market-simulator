import pandera.pandas as pa

import iberian_day_ahead_market_simulator.columns as cols

# fmt: off

ExclusiveBlockOrdersGroupedSchema = pa.SeriesSchema(
    name=cols.ID_BLOCK_ORDER,
    index=pa.MultiIndex(
        [
            pa.Index(pa.String, name=cols.ID_ORDER),
            pa.Index(pa.Int, name=cols.INT_NUM_EXCL_GROUP, coerce=True),
        ]
    ),
    dtype=pa.String,
    nullable=False,
)
