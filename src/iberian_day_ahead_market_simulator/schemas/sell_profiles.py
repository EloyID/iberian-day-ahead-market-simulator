import pandera.pandas as pa

# fmt: off

SellProfilesSchema = pa.DataFrameSchema(
    {
        f'energy_{i}' : pa.Column(float, coerce=True) for i in range(1, 25)
    }
)
