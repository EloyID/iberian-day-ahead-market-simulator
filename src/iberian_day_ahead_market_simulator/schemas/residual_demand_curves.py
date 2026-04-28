import pandera.pandas as pa

ResidualDemandCurvesSchema = pa.DataFrameSchema(
    {
        **{f"energy_{i}": pa.Column(float, coerce=True) for i in range(1, 25)},
        **{f"price_{i}": pa.Column(float, coerce=True) for i in range(1, 25)},
    }
)
