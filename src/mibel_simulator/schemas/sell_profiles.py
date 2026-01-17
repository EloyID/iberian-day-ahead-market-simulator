import pandera.pandas as pa
import mibel_simulator.columns as cols
from .columns_dict import columns_dict

# fmt: off

SellProfilesSchema = pa.DataFrameSchema(
    {
        f'energy_{i}' : pa.Column(float, coerce=True) for i in range(1, 25)
    }
)
