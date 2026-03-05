from mibel_simulator.const import (
    BUY_SELL_OPTIONS,
    CAT_FRONTIER_OPTIONS,
    PAIS_OPTIONS,
    TIPO_OFERTA_OPTIONS,
)
import pandera.pandas as pa
import mibel_simulator.columns as cols

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
    
    cols.FLOAT_BID_PRICE:         pa.Column(float,                                           coerce=True),
    cols.FLOAT_BID_POWER:         pa.Column(float, checks=[pa.Check.ge(0)],                  coerce=True),
    cols.FLOAT_BID_POWER_CUMSUM:  pa.Column(float, checks=[pa.Check.ge(0)],                  coerce=True),
    cols.FLOAT_BID_POWER_CUMSUM_BY_COUNTRY: pa.Column(float, checks=[pa.Check.ge(0)],        coerce=True),
    cols.FLOAT_CLEARED_POWER:     pa.Column(float, nullable=True, checks=[pa.Check.ge(0)],   coerce=True),
    cols.FLOAT_MAV:               pa.Column(float, checks=[pa.Check.ge(0)],                  coerce=True),
    cols.FLOAT_MAR:               pa.Column(float, checks=[pa.Check.ge(0), pa.Check.le(1)],  coerce=True),
    cols.FLOAT_MIC:               pa.Column(float,                                           coerce=True),
    cols.FLOAT_MAX_POWER:         pa.Column(float, checks=[pa.Check.ge(0)],                  coerce=True),
    cols.FLOAT_IMPORT_CAPACITY:   pa.Column(float, checks=[pa.Check.le(0)],                  coerce=True),
    cols.FLOAT_EXPORT_CAPACITY:   pa.Column(float, checks=[pa.Check.ge(0)],                  coerce=True),
    cols.FLOAT_OBJECTIVE_VALUE:   pa.Column(float,                                           coerce=True),
    cols.FLOAT_CLEARED_PRICE:     pa.Column(float,                                           coerce=True),
    
    cols.IDS_MIC_SCOS:              pa.Column(object                                   ),
    cols.IDS_BID_BLOCKS:            pa.Column(object                                   ),
    cols.IDS_PARADOXAL_ORDERS:        pa.Column(object                                   ),
    cols.PARADOXAL_ORDERS_COLUMN:     pa.Column(object                                   ),
    cols.BOOL_IS_EXPECTED_INCOME_RESPECTED:  pa.Column(bool,                coerce=True),
    cols.SOLVER_RESULTS_COLUMN:     pa.Column(object                                   ),
    cols.INT_MIC_SCOS_COUNT:        pa.Column(int, checks=[pa.Check.ge(0)], coerce=True),
    cols.INT_BID_BLOCKS_COUNT:      pa.Column(int, checks=[pa.Check.ge(0)], coerce=True),
    cols.INT_PARADOXAL_ORDERS_COUNT:  pa.Column(int, checks=[pa.Check.ge(0)], coerce=True),
    cols.CLEARED_ENERGY_COLUMN:     pa.Column(object                                   ),
    cols.CLEARING_PRICES_COLUMN:    pa.Column(object                                   ),
    cols.SPAIN_PORTUGAL_TRANSMISSIONS_COLUMN:  pa.Column(object                        ),
    "Transmision_ES_PT":            pa.Column(float,                        coerce=True),
}
