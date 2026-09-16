import pandera.pandas as pa

# fmt: off

SellProfilesSchema = pa.DataFrameSchema(
    {
        **{f'energy_{i}' : pa.Column(float, coerce=True)                 for i in range(1, 24)},
        **{f'energy_{i}' : pa.Column(float, coerce=True, required=False) for i in range(24, 101)}
    }
)
