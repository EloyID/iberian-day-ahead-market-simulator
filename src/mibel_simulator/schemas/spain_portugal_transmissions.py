import pandera as pa
import mibel_simulator.columns as cols
from .columns_dict import columns_dict

# fmt: off

SpainPortugaLTransmissionsSchema = pa.DataFrameSchema(
    {"Transmision_ES_PT": columns_dict["Transmision_ES_PT"]},
    index=pa.Index(
        int, name=cols.ID_SCO, checks=[pa.Check.ge(1), pa.Check.le(24)], coerce=True
    ),
)
