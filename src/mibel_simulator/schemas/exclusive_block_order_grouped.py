import pandera.pandas as pa
import mibel_simulator.columns as cols
from .columns_dict import columns_dict

# fmt: off

ExclusiveBlockOrdersGroupedSchema = pa.SeriesSchema(
    name=cols.ID_BLOCK_ORDER,
    index=pa.MultiIndex(
        [
            pa.Index(pa.String, name=cols.ID_ORDER),
            pa.Index(pa.Int, name=cols.INT_NUM_GRUPO_EXCL, coerce=True),
        ]
    ),
    dtype=pa.String,
    nullable=False,
)
