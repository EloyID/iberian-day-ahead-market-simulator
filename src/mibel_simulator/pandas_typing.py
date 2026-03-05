from numpy import dtype
import mibel_simulator.columns as cols


DET_TYPING = {
    "dat_sesion": dtype("<M8[ns]"),
    "CodOferta": "string",
    "Version": dtype("int8"),
    "Period": dtype("int8"),
    "NumBloq": dtype("int8"),
    "NumTramo": dtype("int8"),
    "NumGrupoExcl": dtype("int8"),
    "PrecEuro": dtype("float64"),
    "Potencia": dtype("float64"),
    "MAV": dtype("float64"),
    "MAR": dtype("float64"),
}

DET_CATEGORIES = [
    "CodOferta",
]

DET_CATEGORIES_DICT = {column: "category" for column in DET_CATEGORIES}

DET_MAIN_COLUMNS = [
    "dat_sesion",
    "CodOferta",
    "Period",
    "NumBloq",
    "NumTramo",
    "NumGrupoExcl",
    "PrecEuro",
    "Potencia",
    "MAV",
    "MAR",
]


CAB_TYPING = {
    "dat_sesion": dtype("<M8[ns]"),
    "CodOferta": "string",
    "Version": dtype("int8"),
    "CodigoUnidad": "string",
    "Descripcion": "string",
    "CV": "string",
    "OferPlazo": "string",
    "Fijoeuro": dtype("float64"),
    "MaxPot": dtype("float64"),
}
CAB_MAIN_COLUMNS = [
    "dat_sesion",
    "CodOferta",
    "CodigoUnidad",
    "Descripcion",
    "CV",
    "OferPlazo",
    "Fijoeuro",
    "MaxPot",
]


CAB_CATEGORIES = [
    "CodOferta",
    "CodigoUnidad",
    "CV",
    "OferPlazo",
]

CAB_CATEGORIES_DICT = {column: "category" for column in CAB_CATEGORIES}

CAPACIDAD_INTER_PT_TYPING = {
    "qua_hora": dtype("int64"),
    "dat_sesion": dtype("<M8[ns]"),
    "Frontera": dtype("int64"),
    "Capacidad importación": dtype("float64"),
    "Ocupación Importación": dtype("float64"),
    "Capacidad libre de importación": dtype("float64"),
    "Capacidad exportación": dtype("float64"),
    "Ocupación exportación": dtype("float64"),
    "Capacidad libre de exportación": dtype("float64"),
    # "Unnamed: 9": dtype("float64"),
}
CAPACIDAD_INTER_PT_MAIN_COLUMNS = [
    "qua_hora",
    "dat_sesion",
    "Capacidad importación",
    "Capacidad exportación",
]
CAPACIDAD_INTER_PT_RENAMING = {
    "qua_hora": "Period",
}

UNIDADES_TYPING = {
    "CODIGO": "string",
    "DESCRIPCIÓN": "string",
    "AGENTE PROPIETARIO": "string",
    "PORCENTAJE \nPROPIEDAD": dtype("float64"),
    "TIPO UNIDAD": "category",
    "ZONA/FRONTERA": "category",
    "TECNOLOGÍA": "category",
}

UNIDADES_MAIN_COLUMNS = ["CODIGO", "ZONA/FRONTERA"]
UNIDADES_RENAMING = {
    "CODIGO": "CodigoUnidad",
    "DESCRIPCIÓN": "Descripcion",
}

CURVA_PBC_UOF_TYPING = {
    "qua_hora": dtype("int64"),
    "dat_sesion": dtype("<M8[ns]"),
    "cod_pais": "string",
    "id_unidad": "string",
    "cod_tipo_oferta": "string",
    "qua_energia": dtype("float64"),
    "qua_precio": dtype("float64"),
    "cod_ofertada_casada": "string",
    "cod_simple_block_orders": "string",
    # "Unnamed: 9": dtype("float64"),
}
CURVA_PBC_UOF_MAIN_COLUMNS = [
    "qua_hora",
    "dat_sesion",
    "cod_pais",
    "id_unidad",
    "cod_tipo_oferta",
    "qua_energia",
    "qua_precio",
    "cod_ofertada_casada",
    "cod_simple_block_orders",
]
CURVA_PBC_UOF_RENAMING = {
    "qua_hora": "Period",
    "qua_energia": "Potencia",
    "qua_precio": "PrecEuro",
    "cod_tipo_oferta": "CV",
    "id_unidad": "CodigoUnidad",
}

CURVAS_OFERTAS_AFRR_TYPING = {
    "Cuarto de Hora del dia": dtype("int64"),
    "Sentido": "string",
    "Precio (€/MW)": dtype("float64"),
    "Indicadores": "string",
    "Potencia ofertada (MW)": dtype("float64"),
    "dat_sesion": dtype("<M8[ns]"),
}
CURVAS_OFERTAS_AFRR_CATEGORIES = [
    "Sentido",
    "Indicadores",
]

CURVAS_OFERTAS_AFRR_RENAMING = {
    "Cuarto de Hora del dia": "Period",
    "Precio (€/MW)": "PrecEuro",
    "Potencia ofertada (MW)": "Potencia",
}
CURVAS_OFERTAS_AFRR_CATEGORIES_DICT = {
    column: "category" for column in CURVAS_OFERTAS_AFRR_CATEGORIES
}
CURVAS_OFERTAS_AFRR_COLUMNS = CURVAS_OFERTAS_AFRR_TYPING.keys()


DET_CAB_DAM_SIMULATOR_TYPING = {
    cols.INT_PERIOD: "int8",
    cols.INT_NUM_BLOQ: "int8",
    cols.INT_NUM_TRAMO: "int8",
    cols.INT_NUM_EXCL_GROUP: "int8",
    cols.ID_ORDER: "string",
    cols.ID_UNIDAD: "string",
    cols.CAT_BUY_SELL: "category",
    cols.CAT_ORDER_TYPE: "category",
    cols.ID_BLOCK_ORDER: "string",
    cols.ID_SCO: "string",
    cols.CAT_BIDDING_ZONE: "category",
}
