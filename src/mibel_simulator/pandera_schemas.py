import pandera as pa
import mibel_simulator.columns as cols

from mibel_simulator.const import (
    BUY_SELL_OPTIONS,
    CAT_FRONTIER_OPTIONS,
    PAIS_OPTIONS,
    TIPO_OFERTA_OPTIONS,
)

# fmt: off

columns_dict = {
    cols.DATE_SESION: pa.Column(pa.Timestamp, nullable=True, required=False),
    
    cols.ID_ORDER:          pa.Column(str                            ),
    cols.ID_UNIDAD:         pa.Column(str                            ),
    cols.ID_INDIVIDUAL_BID: pa.Column(str, nullable=True, unique=True),
    cols.ID_BLOCK_ORDER:    pa.Column(str, nullable=True             ),
    cols.ID_SCO:            pa.Column(str, nullable=True             ),
    
    cols.CAT_BUY_SELL:   pa.Column(pa.Category, checks=[pa.Check.isin(BUY_SELL_OPTIONS)],                     coerce=True),
    cols.CAT_PAIS:       pa.Column(pa.Category, checks=[pa.Check.isin(PAIS_OPTIONS)],                         coerce=True),
    cols.CAT_ORDER_TYPE: pa.Column(pa.Category, checks=[pa.Check.isin(TIPO_OFERTA_OPTIONS)],                  coerce=True),
    cols.CAT_FRONTIER:   pa.Column(pa.Category, checks=[pa.Check.isin(CAT_FRONTIER_OPTIONS)], required=False, coerce=True),
    
    cols.INT_PERIODO:         pa.Column(int, checks=[pa.Check.ge(1), pa.Check.le(25)], coerce=True),
    cols.INT_NUM_BLOQ:        pa.Column(int, checks=[pa.Check.ge(0)],                  coerce=True),
    cols.INT_NUM_TRAMO:       pa.Column(int, checks=[pa.Check.ge(0)],                  coerce=True),
    cols.INT_NUM_GRUPO_EXCL:  pa.Column(int, checks=[pa.Check.ge(0)],                  coerce=True),
    
    cols.FLOAT_BID_PRICE:         pa.Column(float                                           ),
    cols.FLOAT_BID_POWER:         pa.Column(float, checks=[pa.Check.ge(0)]                  ),
    cols.FLOAT_BID_POWER_CUMSUM:  pa.Column(float, checks=[pa.Check.ge(0)]                  ),
    cols.FLOAT_BID_POWER_CUMSUM_BY_COUNTRY: pa.Column(float, checks=[pa.Check.ge(0)]        ),
    cols.FLOAT_CLEARED_POWER:     pa.Column(float, nullable=True, checks=[pa.Check.ge(0)]   ),
    cols.FLOAT_MAV:               pa.Column(float, checks=[pa.Check.ge(0)]                  ),
    cols.FLOAT_MAR:               pa.Column(float, checks=[pa.Check.ge(0), pa.Check.le(1)]  ),
    cols.FLOAT_MIC:               pa.Column(float                                           ),
    cols.FLOAT_MAX_POWER:         pa.Column(float, checks=[pa.Check.ge(0)]                  ),
    cols.FLOAT_IMPORT_CAPACITY:   pa.Column(float, checks=[pa.Check.le(0)]                  ),
    cols.FLOAT_EXPORT_CAPACITY:   pa.Column(float, checks=[pa.Check.ge(0)]                  ),
    cols.FLOAT_OBJECTIVE_VALUE:   pa.Column(float                                           ),
    cols.FLOAT_CLEARED_PRICE:     pa.Column(float                                           ),
    
    cols.MIC_SCOS_COLUMN:           pa.Column(object                                   ),
    cols.BOOL_IS_MIC_RESPECTED:     pa.Column(bool,                         coerce=True),
    cols.SOLVER_RESULTS_COLUMN:     pa.Column(object                                   ),
    cols.INT_MIC_SCOS_COUNT:        pa.Column(int, checks=[pa.Check.ge(0)], coerce=True),
    cols.CLEARED_ENERGY_COLUMN:     pa.Column(object                                   ),
    cols.CLEARING_PRICES_COLUMN:    pa.Column(object                                   ),
    cols.SPAIN_PORTUGAL_TRANSMISSIONS_COLUMN:  pa.Column(object                        ),
    "Transmision_ES_PT":            pa.Column(float,                        coerce=True),
}

ClearingPricesSchema = pa.DataFrameSchema(
    {
        cols.INT_PERIODO:           columns_dict[cols.INT_PERIODO],
        cols.FLOAT_CLEARED_PRICE:   columns_dict[cols.FLOAT_CLEARED_PRICE],
        cols.CAT_PAIS:              columns_dict[cols.CAT_PAIS],
    }
)

TrialsSchema = pa.DataFrameSchema(
    {
        cols.MIC_SCOS_COLUMN:                      columns_dict[cols.MIC_SCOS_COLUMN],
        cols.BOOL_IS_MIC_RESPECTED:                columns_dict[cols.BOOL_IS_MIC_RESPECTED],
        cols.SOLVER_RESULTS_COLUMN:                columns_dict[cols.SOLVER_RESULTS_COLUMN],
        cols.INT_MIC_SCOS_COUNT:                   columns_dict[cols.INT_MIC_SCOS_COUNT],
        cols.CLEARED_ENERGY_COLUMN:                columns_dict[cols.CLEARED_ENERGY_COLUMN],
        cols.CLEARING_PRICES_COLUMN:               columns_dict[cols.CLEARING_PRICES_COLUMN],
        cols.SPAIN_PORTUGAL_TRANSMISSIONS_COLUMN:  columns_dict[cols.SPAIN_PORTUGAL_TRANSMISSIONS_COLUMN],
    }
)

CapacidadInterPTSchema = pa.DataFrameSchema(
    {
        cols.DATE_SESION:           columns_dict[cols.DATE_SESION],
        cols.CAT_FRONTIER:          columns_dict[cols.CAT_FRONTIER],
        cols.INT_PERIODO:           columns_dict[cols.INT_PERIODO],
        cols.FLOAT_IMPORT_CAPACITY: columns_dict[cols.FLOAT_IMPORT_CAPACITY],
        cols.FLOAT_EXPORT_CAPACITY: columns_dict[cols.FLOAT_EXPORT_CAPACITY],
    }
)

CABSchema = pa.DataFrameSchema(
    {
        cols.DATE_SESION:      columns_dict[cols.DATE_SESION],
        cols.ID_ORDER:         columns_dict[cols.ID_ORDER],
        cols.ID_UNIDAD:        columns_dict[cols.ID_UNIDAD],
        cols.CAT_BUY_SELL:     columns_dict[cols.CAT_BUY_SELL],
        cols.FLOAT_MIC:        columns_dict[cols.FLOAT_MIC],
        cols.FLOAT_MAX_POWER:  columns_dict[cols.FLOAT_MAX_POWER],
    }
).update_columns(
    {
        cols.ID_ORDER:   {"unique": True},
        cols.ID_UNIDAD:  {"unique": True},
    }
)


DETSchema = pa.DataFrameSchema(
    {
        cols.DATE_SESION:        columns_dict[cols.DATE_SESION],
        cols.ID_ORDER:           columns_dict[cols.ID_ORDER],
        cols.INT_PERIODO:        columns_dict[cols.INT_PERIODO],
        cols.INT_NUM_BLOQ:       columns_dict[cols.INT_NUM_BLOQ],
        cols.INT_NUM_TRAMO:      columns_dict[cols.INT_NUM_TRAMO],
        cols.INT_NUM_GRUPO_EXCL: columns_dict[cols.INT_NUM_GRUPO_EXCL],
        cols.FLOAT_BID_PRICE:    columns_dict[cols.FLOAT_BID_PRICE],
        cols.FLOAT_BID_POWER:    columns_dict[cols.FLOAT_BID_POWER],
        cols.FLOAT_MAV:          columns_dict[cols.FLOAT_MAV],
        cols.FLOAT_MAR:          columns_dict[cols.FLOAT_MAR],
    }
)


DETCABSchema = pa.DataFrameSchema(
    {
        **CABSchema.columns,
        **DETSchema.columns,
        cols.ID_INDIVIDUAL_BID:                     columns_dict[cols.ID_INDIVIDUAL_BID],
        cols.ID_BLOCK_ORDER:                        columns_dict[cols.ID_BLOCK_ORDER],
        cols.ID_SCO:                                columns_dict[cols.ID_SCO],
        cols.CAT_ORDER_TYPE:                        columns_dict[cols.CAT_ORDER_TYPE],
        cols.FLOAT_BID_POWER_CUMSUM:                columns_dict[cols.FLOAT_BID_POWER_CUMSUM],
        cols.FLOAT_BID_POWER_CUMSUM_BY_COUNTRY:     columns_dict[cols.FLOAT_BID_POWER_CUMSUM_BY_COUNTRY],
    }
).update_columns(
    {
        cols.ID_ORDER: {"unique": False},
        cols.ID_UNIDAD: {"unique": False},
    }
)

ClearedDetCabSchema = pa.DataFrameSchema(
    {
        **DETCABSchema.columns,
        cols.FLOAT_CLEARED_POWER: columns_dict[cols.FLOAT_CLEARED_POWER],
    }
)

ParentChildBloquesSchema = pa.DataFrameSchema(
    {
        cols.ID_ORDER:                  columns_dict[cols.ID_ORDER],
        cols.INT_NUM_BLOQ_PARENT:       columns_dict[cols.INT_NUM_BLOQ],
        cols.INT_NUM_BLOQ_CHILD:        columns_dict[cols.INT_NUM_BLOQ],
        cols.INT_NUM_GRUPO_EXCL:        columns_dict[cols.INT_NUM_GRUPO_EXCL],
        cols.ID_BLOCK_ORDER_PARENT:     columns_dict[cols.ID_BLOCK_ORDER],
        cols.ID_BLOCK_ORDER_CHILD:      columns_dict[cols.ID_BLOCK_ORDER],
    },
    checks=[
        pa.Check(
            lambda df: df[cols.INT_NUM_BLOQ_PARENT] < df[cols.INT_NUM_BLOQ_CHILD],
            error="INT_NUM_BLOQ_parent must be less than INT_NUM_BLOQ_child",
        )
    ],
)

ParentChildSCOSSchema = pa.DataFrameSchema(
    {
        cols.ID_ORDER:              columns_dict[cols.ID_ORDER],
        cols.INT_NUM_TRAMO_PARENT:  columns_dict[cols.INT_NUM_TRAMO],
        cols.INT_NUM_TRAMO_CHILD:   columns_dict[cols.INT_NUM_TRAMO],
        cols.ID_SCO_PARENT:         columns_dict[cols.ID_SCO],
        cols.ID_SCO_CHILD:          columns_dict[cols.ID_SCO],
    },
    checks=[
        pa.Check(
            lambda df: df[cols.INT_NUM_TRAMO_PARENT]
            < df[cols.INT_NUM_TRAMO_CHILD],
            error="INT_NUM_TRAMO_parent must be less than INT_NUM_TRAMO_child",
        )
    ],
)

SpainPortugaLTransmissionsSchema = pa.DataFrameSchema(
    {"Transmision_ES_PT": columns_dict["Transmision_ES_PT"]},
    index=pa.Index(
        int, name=cols.ID_SCO, checks=[pa.Check.ge(1), pa.Check.le(24)], coerce=True
    ),
)

ExclusiveBlockOrdersGroupedSchema = pa.SeriesSchema(
    name=cols.ID_BLOCK_ORDER,
    index=pa.MultiIndex(
        [
            pa.Index(pa.String, name=cols.ID_ORDER),
            pa.Index(pa.Int, name=cols.INT_NUM_GRUPO_EXCL, coerce=True),
        ]
    ),
    dtype=pa.Object,
    nullable=False,
)

SCOBidsTramoGroupedSchema = pa.SeriesSchema(
    name=cols.ID_INDIVIDUAL_BID,
    index=pa.Index(pa.String, name=cols.ID_SCO),
    dtype=pa.Object,
    nullable=False,
)
