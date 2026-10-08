import pandera.pandas as pa

from iberian_day_ahead_market_simulator.const import (
    TOTAL_PERIODS_OPTIONS,
    get_rdc_period_numbers,
    spans_whole_market_session,
)

# A session is 23 to 100 periods long, so no single period column can be required
# beyond the 23 that every allowed length has: requiring a power_24 would reject the
# daylight saving spring forward day outright. The session length is pinned by the
# check below instead. Unlike a residual demand curve a sell profile carries no price
# columns to cross check against, so a profile that lost its last period cannot be
# told apart from a genuine 23 period one.
power_columns_span_a_whole_session = pa.Check(
    lambda df: spans_whole_market_session(get_rdc_period_numbers(df.columns, "power")),
    error=(
        "the power_<i> columns must run from 1 to the length of the session, "
        f"which has to be one of {TOTAL_PERIODS_OPTIONS}"
    ),
)

# fmt: off
SellProfilesSchema = pa.DataFrameSchema(
    {
        **{f'power_{i}' : pa.Column(float, coerce=True)                 for i in range(1, 24)},
        **{f'power_{i}' : pa.Column(float, coerce=True, required=False) for i in range(24, 101)}
    },
    checks=[power_columns_span_a_whole_session],
)
