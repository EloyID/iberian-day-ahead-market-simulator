SPAIN_ZONE = "ES"
PORTUGAL_ZONE = "PT"


UNIDADES_SPLITTING_ZONE_COLUMN = "ZONA"
UNIDADES_ZONE_COLUMN = "ZONA/FRONTERA"
FRANCE_CODIGOUNIDAD = "MIEU"

FRONTIER_MAPPING = {
    2: "PT",
    3: "FR",
    4: "MA",  # MOROCCO
    5: "AD",  # ANDORRA
}

FRONTIER_MAPPING_REVERSE = {v: k for k, v in FRONTIER_MAPPING.items()}

##### COLUMNS NAMES FOR DATAFRAMES #####
INT_PERIODO = "int_periodo"
INT_NUM_BLOQ = "int_num_bloq"
INT_NUM_TRAMO = "int_num_tramo"
INT_NUM_GRUPO_EXCL = "int_num_grupo_excl"

CAT_PAIS = "cat_pais"
CAT_TIPO_OFERTA = "cat_tipo_oferta"
CAT_OFERTADA_CASADA = "cat_ofertada_casada"
CAT_ORDER_TYPE = "cat_order_type"
CAT_FRONTIER = "cat_frontier"
CAT_BUY_SELL = "cat_buy_sell"

FLOAT_IMPORT_CAPACITY = "float_import_capacity"
FLOAT_EXPORT_CAPACITY = "float_export_capacity"
FLOAT_BID_POWER = "float_bid_power"
FLOAT_BID_PRICE = "float_bid_price"
FLOAT_PRICE_FR = "float_price_fr"
FLOAT_MIC = "float_mic"
FLOAT_MAX_POWER = "float_max_power"
FLOAT_BID_PRICE = "float_bid_price"
FLOAT_BID_POWER = "float_bid_power"
FLOAT_BID_POWER_CUMSUM = "float_bid_power_cumsum"
FLOAT_BID_POWER_CUMSUM_BY_COUNTRY = "float_bid_power_cumsum_by_country"
FLOAT_MAV = "float_mav"
FLOAT_MAR = "float_mar"

ID_UNIDAD = "id_unidad"
ID_ORDER = "id_order"
ID_INDIVIDUAL_BID = "id_individual_bid"
ID_BLOCK_ORDER = "id_block_order"
ID_SCO = "id_sco"
ID_BLOCK_ORDER_PARENT = "id_block_order_parent"
ID_BLOCK_ORDER_CHILD = "id_block_order_child"
ID_SCO_PARENT = "id_sco_parent"
ID_SCO_CHILD = "id_sco_child"

BOOL_IS_SIMPLE_BID = "bool_is_simple_bid"
BOOL_IS_SCO = "bool_is_sco"
BOOL_IS_NOT_EXCLUSIVE_GROUP = "bool_is_not_exclusive_group"
BOOL_IS_EXCLUSIVE_GROUP = "bool_is_exclusive_group"

DATE_SESION = "date_sesion"
DATETIME_SESION = "datetime_sesion"

##### INTERNATIONAL UOFS #####

FRANCE_UOF = "MIEU"
SPAIN_UOF = "MIE"
PORTUGAL_UOF = "MIP"
INTERCONEXION_UOFS = [FRANCE_UOF, PORTUGAL_UOF, SPAIN_UOF]

FRANCE_ID_ORDER = "12345678901234"
FRANCE_ID_UNIDAD = "MIEU"

### CAT_PAIS_VALUES ###
CAT_PAIS_FRANCE = "FR"
CAT_PAIS_SPAIN = "ES"
CAT_PAIS_PORTUGAL = "PT"
CAT_PAIS_MIBEL = "MI"

CAT_BUY = "C"
CAT_SELL = "V"

CAT_TIPO_OFERTA_SIMPLE = "S"
CAT_TIPO_OFERTA_SIMPLE_BLOCK = "C01"
CAT_TIPO_OFERTA_SCO = "C02"
CAT_TIPO_OFERTA_EXCLUSIVE_GROUP = "C04"
CAT_TIPO_OFERTA_EXPORT_FRANCE = "Exp FR"
CAT_TIPO_OFERTA_IMPORT_FRANCE = "Imp FR"
CAT_TIPO_OFERTA_EXPORT_PORTUGAL = "Exp PT"
CAT_TIPO_OFERTA_IMPORT_PORTUGAL = "Imp PT"
CAT_TIPO_OFERTA_IMPORT_SPAIN = "Imp ES"
CAT_TIPO_OFERTA_EXPORT_SPAIN = "Exp ES"


MIC_SCOS_COLUMN = "mic_scos"
INT_MIC_SCOS_COUNT = "int_mic_scos_count"
BOOL_ARE_MIC_SCOS_TESTED = "bool_are_mic_scos_tested"
BOOL_IS_MIC_RESPECTED = "bool_is_mic_respected"
FLOAT_VARIABLE_COST = "float_variable_cost"
FLOAT_CLEARED_POWER = "float_cleared_power"
FLOAT_CLEARED_POWER_CUMSUM = "float_cleared_power_cumsum"
FLOAT_CLEARED_POWER_CUMSUM_BY_COUNTRY = "float_cleared_power_cumsum_by_country"
FLOAT_CLEARED_PRICE = "float_cleared_price"
FLOAT_COLLECTION_RIGHTS = "float_collection_rights"
FLOAT_OBJECTIVE_VALUE = "float_objective_value"
FLOAT_RATIO_NET_INCOME_CLEARED_POWER = "float_ratio_net_income_cleared_power"
FLOAT_RATIO_NET_INCOME_BID_POWER = "float_ratio_net_income_bid_power"
FLOAT_NET_INCOME = "float_net_income"
SOLVER_RESULTS_COLUMN = "solver_results"
CLEARED_ENERGY_COLUMN = "cleared_energy"
CLEARING_PRICES_COLUMN = "clearing_prices"
SPAIN_PORTUGAL_TRANSMISSION_COLUMN = "spain_portugal_transmission"


##### OTHER CONSTANTS #####

DET_CAB_DATE_UNIQUE_IDENTIFIERS = [
    INT_PERIODO,
    CAT_BUY_SELL,
    ID_ORDER,
    INT_NUM_TRAMO,
    INT_NUM_BLOQ,
    INT_NUM_GRUPO_EXCL,
]

TRIALS_DF_COLUMNS = [
    MIC_SCOS_COLUMN,
    FLOAT_OBJECTIVE_VALUE,
    BOOL_IS_MIC_RESPECTED,
    SOLVER_RESULTS_COLUMN,
    INT_MIC_SCOS_COUNT,
]
