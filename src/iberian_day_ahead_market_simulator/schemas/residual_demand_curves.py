import pandera.pandas as pa

from iberian_day_ahead_market_simulator.const import (
    TOTAL_PERIODS_OPTIONS,
    get_rdc_period_numbers,
    spans_whole_market_session,
)

# A session is 23 to 100 periods long, so no single period column can be required
# beyond the 23 that every allowed length has: requiring a power_24 would reject the
# daylight saving spring forward day outright. The session length is pinned by the
# check below instead, which also catches the last period going missing, because the
# power and the price columns then no longer describe the same session.
power_and_price_columns_span_the_same_session = pa.Check(
    lambda df: (
        spans_whole_market_session(get_rdc_period_numbers(df.columns, "power"))
        and get_rdc_period_numbers(df.columns, "power")
        == get_rdc_period_numbers(df.columns, "price")
    ),
    error=(
        "the power_<i> and price_<i> columns must both run from 1 to the same "
        f"session length, which has to be one of {TOTAL_PERIODS_OPTIONS}"
    ),
)

# fmt: off
ResidualDemandCurvesSchema = pa.DataFrameSchema(
    {
        **{f"power_{i}": pa.Column(float, coerce=True)                 for i in range(1, 24)},
        **{f"power_{i}": pa.Column(float, coerce=True, required=False) for i in range(24, 101)},
        **{f"price_{i}":  pa.Column(float, coerce=True)                 for i in range(1, 24)},
        **{f"price_{i}":  pa.Column(float, coerce=True, required=False) for i in range(24, 101)},
    },
    checks=[power_and_price_columns_span_the_same_session],
)
