import pandera as pa
import mibel_simulator.columns as cols
from .columns_dict import columns_dict

# fmt: off

SCOBidsTramoGroupedSchema = pa.SeriesSchema(
    name=cols.ID_INDIVIDUAL_BID,
    index=pa.Index(pa.String, name=cols.ID_SCO),
    dtype=pa.Object,
    nullable=False,
)
