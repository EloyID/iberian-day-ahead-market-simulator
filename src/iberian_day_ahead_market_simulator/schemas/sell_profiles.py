import pandera.pandas as pa

# fmt: off

SellProfilesSchema = pa.DataFrameSchema(
    {
        **{f'power_{i}' : pa.Column(float, coerce=True)                 for i in range(1, 25)},
        **{f'power_{i}' : pa.Column(float, coerce=True, required=False) for i in range(25, 101)}
    }
)
