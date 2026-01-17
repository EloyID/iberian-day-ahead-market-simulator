import pandera.pandas as pa
import mibel_simulator.columns as cols
from .columns_dict import columns_dict

# fmt: off

UOFZonesSchema = pa.DataFrameSchema(
    {
        cols.ID_UNIDAD:       columns_dict[cols.ID_UNIDAD],
        cols.CAT_PAIS:        columns_dict[cols.CAT_PAIS],
    },
    unique=[cols.ID_UNIDAD],
)
