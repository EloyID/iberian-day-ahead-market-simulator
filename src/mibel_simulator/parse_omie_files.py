import os

import pandas as pd
import logging

from mibel_simulator.const import (
    CAT_BUY_SELL,
    CAT_FRONTIER,
    CAT_OFERTADA_CASADA,
    CAT_PAIS,
    CAT_ORDER_TYPE,
    CAT_TIPO_OFERTA,
    DATE_SESION,
    DATETIME_SESION,
    FLOAT_EXPORT_CAPACITY,
    FLOAT_IMPORT_CAPACITY,
    FLOAT_MAR,
    FLOAT_MAV,
    FLOAT_MAX_POWER,
    FLOAT_MIC,
    FLOAT_BID_POWER,
    FLOAT_BID_PRICE,
    FLOAT_PRICE_FR,
    ID_ORDER,
    ID_UNIDAD,
    INT_NUM_BLOQ,
    INT_NUM_GRUPO_EXCL,
    INT_NUM_TRAMO,
    INT_PERIODO,
    INTERCONEXION_UOFS,
)

logger = logging.getLogger(__name__)

# fmt: off

CURVA_PBC_UOF_RENAMING = {
    "Periodo":                  INT_PERIODO,
    "Pais":                     CAT_PAIS,
    "Fecha":                    DATE_SESION,
    "Tipo Oferta":              CAT_TIPO_OFERTA,
    "Ofertada (O)/Casada (C)":  CAT_OFERTADA_CASADA,
    "Tipología de Oferta":      CAT_ORDER_TYPE,
    "Potencia Compra/Venta":    FLOAT_BID_POWER,
    "Precio Compra/Venta":      FLOAT_BID_PRICE,
    "Unidad":                   ID_UNIDAD,
}

CURVA_PBC_UOF_TYPING = {
    INT_PERIODO:            int,
    CAT_PAIS:               'category',
    CAT_TIPO_OFERTA:        'category',
    CAT_OFERTADA_CASADA:    'category',
    CAT_ORDER_TYPE:         'category',
    FLOAT_BID_POWER:        float,
    FLOAT_BID_PRICE:        float,
}


CAB_FORMAT = [
    {"field": "CodOferta",          "start": 0,     "end": 10,  "type": str},  
    # {"field": "Version",            "start": 10,    "end": 15,  "type": int},  
    {"field": "CodigoUnidad",       "start": 15,    "end": 22,  "type": str},  
    # {"field": "Descripcion",        "start": 22,    "end": 52,  "type": str},  
    {"field": "CV",                 "start": 52,    "end": 53,  "type": str},  
    # {"field": "OferPlazo",          "start": 53,    "end": 54,  "type": str},  
    {"field": "Fijoeuro",           "start": 54,    "end": 71,  "type": float},  
    {"field": "MaxPot",             "start": 71,    "end": 78,  "type": float},  
    # {"field": "CodInt",             "start": 78,   "end": 80,   "type": int},  
    # {"field": "AnoInsercion",       "start": 80,   "end": 84,   "type": int},  
    # {"field": "MesInsercion",       "start": 84,   "end": 86,   "type": int},  
    # {"field": "DiaInsercion",       "start": 86,   "end": 88,   "type": int},  
    # {"field": "HoraInsercion",      "start": 88,   "end": 90,   "type": int},  
    # {"field": "MinutoInsercion",    "start": 90,   "end": 92,   "type": int},  
    # {"field": "SegundoInsercion",   "start": 92,   "end": 94,   "type": int},  

]

CAB_RENAMING = {
    DATE_SESION:    DATE_SESION,
    "CodOferta":    ID_ORDER,
    "CodigoUnidad": ID_UNIDAD,
    "CV":           CAT_BUY_SELL,
    "Fijoeuro":     FLOAT_MIC,
    "MaxPot":       FLOAT_MAX_POWER,
}

DET_FORMAT = [
    {"field": "CodOferta",      "start": 0,    "end": 10,  "type": str},
    # {"field": "Version",        "start": 10,   "end": 15,  "type": int},
    {"field": "Periodo",        "start": 15,   "end": 18,  "type": int},
    {"field": "NumBloq",        "start": 18,   "end": 20,  "type": int},
    {"field": "NumTramo",       "start": 20,   "end": 22,  "type": int},
    {"field": "NumGrupoExcl",   "start": 22,   "end": 24,  "type": int},
    {"field": "PrecEuro",       "start": 24,   "end": 41,  "type": float},
    {"field": "Potencia",       "start": 41,   "end": 48,  "type": float},
    {"field": "MAV",            "start": 48,   "end": 55,  "type": float},
    {"field": "MAR",            "start": 55,   "end": 60,  "type": float},

]

DET_RENAMING = {
    DATE_SESION:        DATE_SESION,
    "CodOferta":        ID_ORDER,
    "Periodo":          INT_PERIODO,
    "NumBloq":          INT_NUM_BLOQ,
    "NumTramo":         INT_NUM_TRAMO,
    "NumGrupoExcl":     INT_NUM_GRUPO_EXCL,
    "PrecEuro":         FLOAT_BID_PRICE,
    "Potencia":         FLOAT_BID_POWER,
    "MAV":              FLOAT_MAV,
    "MAR":              FLOAT_MAR,
}

CAPACIDAD_INTER_RENAMING = {
    "Periodo":                  INT_PERIODO,
    "Fecha":                    DATE_SESION,
    "Frontera":                 CAT_FRONTIER,
    "Capacidad importación":    FLOAT_IMPORT_CAPACITY,
    "Capacidad exportación":    FLOAT_EXPORT_CAPACITY,
}

CAPACIDAD_INTER_TYPING = {
    INT_PERIODO:              int,
    CAT_FRONTIER:             'category',
    FLOAT_IMPORT_CAPACITY:    float,
    FLOAT_EXPORT_CAPACITY:    float,
}

PRICE_FRANCE_RENAMING = {
    "Day-ahead Price (EUR/MWh)":    FLOAT_PRICE_FR,
    "MTU (CET/CEST)":               DATETIME_SESION,
}
PRICE_FRANCE_TYPING = {
    DATE_SESION:      'datetime64[ns]',
    INT_PERIODO:      int,
    FLOAT_PRICE_FR:   float,
}

# fmt: on

CURVA_PBC_UOF_COLUMNS = list(CURVA_PBC_UOF_RENAMING.values())
CAPACIDAD_INTER_COLUMNS = list(CAPACIDAD_INTER_RENAMING.values())
UOF_ZONES_COLUMNS = [CAT_PAIS, ID_UNIDAD]
DET_COLUMNS = list(DET_RENAMING.values())
CAB_COLUMNS = list(CAB_RENAMING.values())
PRICE_FRANCE_COLUMNS = list(PRICE_FRANCE_TYPING.keys())


def curva_pbc_uof_files_to_parquet(
    curva_pbc_uof_folder: str, output_path: str = "curva_pbc_uof.parquet"
):
    """
    Reads all raw curva_pbc_uof CSV files from a folder, cleans and transforms the data, and saves the result as a parquet file.

    Args:
        curva_pbc_uof_folder (str): Path to the folder containing raw curva_pbc_uof CSV files.
        output_path (str, optional): Path to save the consolidated parquet file. Defaults to "curva_pbc_uof.parquet".

    Returns:
        pd.DataFrame: The cleaned and consolidated curva_pbc_uof DataFrame.
    """

    curva_pbc_uof_raw_filepaths = [f for f in os.listdir(curva_pbc_uof_folder)]

    logger.info("Found curva_pbc_uof files: %d", len(curva_pbc_uof_raw_filepaths))
    curva_pbc_uof_raw_files = [
        pd.read_csv(
            curva_pbc_uof_folder + f,
            sep=";",
            skiprows=2,
            encoding="latin1",
            decimal=",",
            thousands=".",
            parse_dates=["Fecha"],
            dayfirst=True,
            skipfooter=1,
            engine="python",
        )
        for f in curva_pbc_uof_raw_filepaths
    ]

    curva_pbc_uof = pd.concat(curva_pbc_uof_raw_files)

    logger.info("Cleaning and transforming curva_pbc_uof data...")

    if "Hora" in curva_pbc_uof.columns:
        curva_pbc_uof["Periodo"] = (
            curva_pbc_uof["Periodo"].fillna(curva_pbc_uof["Hora"]).astype(int)
        )

    if "Energía Compra/Venta" in curva_pbc_uof.columns:
        curva_pbc_uof["Potencia Compra/Venta"] = curva_pbc_uof[
            "Potencia Compra/Venta"
        ].fillna(curva_pbc_uof["Energía Compra/Venta"])

    curva_pbc_uof = curva_pbc_uof.rename(
        columns=CURVA_PBC_UOF_RENAMING, errors="raise"
    )[CURVA_PBC_UOF_COLUMNS].astype(CURVA_PBC_UOF_TYPING)

    logger.info("Saving curva_pbc_uof to parquet at %s", output_path)
    curva_pbc_uof.to_parquet(output_path, index=False)

    return curva_pbc_uof


def cab_files_to_parquet(cab_folder: str, output_path: str = "cab.parquet"):
    """
    Reads all CAB files from a folder, parses the fixed-width format, and saves the result as a parquet file.

    Args:
        cab_folder (str): Path to the folder containing CAB files.
        output_path (str, optional): Path to save the consolidated parquet file. Defaults to "cab.parquet".

    Returns:
        pd.DataFrame: The parsed and consolidated CAB DataFrame.
    """

    cab_files = [f for f in os.listdir(cab_folder)]
    cab_data = []

    logger.info("Found CAB files: %d", len(cab_files))

    for file in cab_files:
        with open(cab_folder + file, "r", encoding="latin-1") as f:
            dat_sesion = pd.to_datetime(
                file.split("_")[1].split(".")[0], format="%Y%m%d"
            )

            for line in f:
                record = {
                    DATE_SESION: dat_sesion,
                    **{
                        entry["field"]: entry["type"](
                            line[entry["start"] : entry["end"]].strip()
                        )
                        for entry in CAB_FORMAT
                    },
                }
                cab_data.append(record)

    cab = (
        pd.DataFrame(cab_data)
        .astype({entry["field"]: entry["type"] for entry in CAB_FORMAT})
        .rename(columns=CAB_RENAMING, errors="raise")[CAB_COLUMNS]
    )

    logger.info("Saving CAB to parquet at %s", output_path)
    cab.to_parquet(output_path, index=False)
    return cab


def det_files_to_parquet(det_folder, output_path="det.parquet"):
    """
    Reads all DET files from a folder, parses the fixed-width format, and saves the result as a parquet file.

    Args:
        det_folder (str): Path to the folder containing DET files.
        output_path (str, optional): Path to save the consolidated parquet file. Defaults to "det.parquet".

    Returns:
        pd.DataFrame: The parsed and consolidated DET DataFrame.
    """

    det_files = [f for f in os.listdir(det_folder)]
    logger.info("Found DET files: %d", len(det_files))
    det_data = []

    for file in det_files:
        with open(det_folder + file, "r", encoding="latin-1") as f:
            dat_sesion = pd.to_datetime(
                file.split("_")[1].split(".")[0], format="%Y%m%d"
            )

            for line in f:
                record = {
                    DATE_SESION: dat_sesion,
                    **{
                        entry["field"]: entry["type"](
                            line[entry["start"] : entry["end"]].strip()
                        )
                        for entry in DET_FORMAT
                    },
                }
                det_data.append(record)

    logger.info("Saving DET to parquet at %s", output_path)
    det = (
        pd.DataFrame(det_data)
        .astype({entry["field"]: entry["type"] for entry in DET_FORMAT})
        .rename(columns=DET_RENAMING, errors="raise")[DET_COLUMNS]
    )

    det.to_parquet(output_path, index=False)

    return det


def capacidad_inter_files_to_parquet(capacidad_inter_folder: str, output_path: str):
    """
    Reads all capacidad_inter CSV files from a folder, cleans and transforms the data, and saves the result as a parquet file.

    Args:
        capacidad_inter_folder (str): Path to the folder containing capacidad_inter CSV files.
        output_path (str): Path to save the consolidated parquet file.

    Returns:
        pd.DataFrame: The cleaned and consolidated capacidad_inter DataFrame.
    """

    capacidad_inter_raw_filepaths = [f for f in os.listdir(capacidad_inter_folder)]
    logger.info("Found capacidad_inter files: %d", len(capacidad_inter_raw_filepaths))

    capacidad_inter_raw_files = [
        pd.read_csv(
            capacidad_inter_folder + f,
            sep=";",
            skiprows=2,
            encoding="latin1",
            decimal=",",
            thousands=".",
            parse_dates=["Fecha"],
            dayfirst=True,
            skipfooter=1,
            engine="python",
        )
        for f in capacidad_inter_raw_filepaths
    ]

    capacidad_inter_data = pd.concat(capacidad_inter_raw_files)
    if "Hora" in capacidad_inter_data.columns:
        capacidad_inter_data["Periodo"] = (
            capacidad_inter_data["Periodo"]
            .fillna(capacidad_inter_data["Hora"])
            .astype(int)
        )
    capacidad_inter_data = capacidad_inter_data.rename(
        columns=CAPACIDAD_INTER_RENAMING, errors="raise"
    )[CAPACIDAD_INTER_COLUMNS].astype(CAPACIDAD_INTER_TYPING)

    # TODO: use when working in quarter hourly
    # is_hourly_data = capacidad_inter_data[INT_PERIODO].max() <= 25
    # if is_hourly_data:
    #     logger.info(
    #         "Detected hourly data in capacidad_inte,r, converting to quarter-hourly..."
    #     )

    #     capacidad_inter_data_quarterly_dfs = []
    #     for i in range(1, 5):
    #         capacidad_inter_data_quarterly = capacidad_inter_data.copy()
    #         capacidad_inter_data_quarterly[INT_PERIODO] = (
    #             capacidad_inter_data_quarterly[INT_PERIODO] - 1
    #         ) * 4 + i
    #         capacidad_inter_data_quarterly_dfs.append(capacidad_inter_data_quarterly)

    #     capacidad_inter_data = pd.concat(capacidad_inter_data_quarterly_dfs)

    capacidad_inter_data = capacidad_inter_data.sort_values(
        by=[DATE_SESION, INT_PERIODO]
    ).reset_index(drop=True)
    logger.info("Saving capacidad_inter to parquet at %s", output_path)
    capacidad_inter_data.to_parquet(output_path, index=False)

    return capacidad_inter_data


def price_france_from_entsoe_file_to_parquet(
    prices_france_filepath: str, output_path: str, use_qh_frequency: bool = False
):
    """
    Reads the France prices CSV file, cleans and transforms the data, and saves the result as a parquet file.

    Args:
        prices_france_filepath (str): Path to the France prices CSV file.
        output_path (str): Path to save the parquet file.
    Returns:
        pd.Series: The cleaned France prices DataFrame.
    """

    price_france = pd.read_csv(prices_france_filepath)
    price_france = price_france.rename(columns=PRICE_FRANCE_RENAMING).query(
        'Sequence == "Without Sequence"'
    )

    price_france[DATETIME_SESION] = (
        price_france[DATETIME_SESION].str.split(" - ").str[0].str.split(" \(").str[0]
    )
    price_france = price_france.drop_duplicates(subset=[DATETIME_SESION])
    price_france[DATETIME_SESION] = pd.to_datetime(
        price_france[DATETIME_SESION], format="%d/%m/%Y %H:%M:%S"
    )

    price_france[DATE_SESION] = pd.to_datetime(price_france[DATETIME_SESION].dt.date)
    if use_qh_frequency:
        logger.info("Detected quarter-hourly data in France prices")
        price_france[INT_PERIODO] = (
            price_france[DATETIME_SESION] - price_france[DATE_SESION]
        ).dt.total_seconds() // (15 * 60) + 1
    else:
        is_QH = price_france[DATETIME_SESION].dt.minute.isin([15, 30, 45]).any()
        price_france["Hour"] = price_france[DATETIME_SESION].dt.hour
        if is_QH:
            logger.warning(
                "Detected quarter-hourly data in France prices, but use_qh_frequency is False. Processing as hourly data."
            )
            price_france = (
                price_france.groupby([DATE_SESION, "Hour"], observed=True)[
                    [FLOAT_PRICE_FR]
                ]
                .mean()
                .reset_index()
            )
        price_france[INT_PERIODO] = price_france["Hour"] + 1
    price_france = price_france[PRICE_FRANCE_COLUMNS].astype(PRICE_FRANCE_TYPING)

    logger.info("Saving France prices to parquet at %s", output_path)
    price_france.to_parquet(output_path)

    return price_france


def generate_uof_zones_parquet_from_uof_files(
    curva_pbc_uof_parquet: str | pd.DataFrame, output_path: str = "uof_zones.parquet"
):
    """
    Generates a parquet file containing unique unit IDs and their corresponding zones from the curva_pbc_uof data.

    Args:
        curva_pbc_uof_parquet (str|pd.DataFrame): Path to the curva_pbc_uof parquet file or a DataFrame.
        output_path (str, optional): Path to save the uof zones parquet file. Defaults to "uof_zones.parquet".

    Returns:
        pd.DataFrame: DataFrame containing unique unit IDs and their zones.
    """

    if isinstance(curva_pbc_uof_parquet, str):
        curva_pbc_uof = pd.read_parquet(curva_pbc_uof_parquet)
    else:
        curva_pbc_uof = curva_pbc_uof_parquet

    curva_pbc_uof_split = curva_pbc_uof.query(
        f"{CAT_PAIS} != 'MI' and {ID_UNIDAD} not in @INTERCONEXION_UOFS"
    )

    curva_pbc_uof_unidad_pais_count = curva_pbc_uof_split.groupby(
        ID_UNIDAD, observed=True
    )[CAT_PAIS].nunique()

    assert (
        curva_pbc_uof_unidad_pais_count.max() == 1
    ), f"These units are in more than one country: {curva_pbc_uof_unidad_pais_count[curva_pbc_uof_unidad_pais_count > 1]}"

    curva_pbc_uof_unidad_pais = curva_pbc_uof_split.drop_duplicates(
        subset=[ID_UNIDAD], keep="last"
    )[UOF_ZONES_COLUMNS]

    logger.info("Saving UOF zones to parquet at %s", output_path)
    curva_pbc_uof_unidad_pais.to_parquet(output_path, index=False)

    return curva_pbc_uof_unidad_pais
