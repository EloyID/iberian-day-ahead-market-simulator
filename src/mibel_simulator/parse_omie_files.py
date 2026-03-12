import logging
import os

import pandas as pd

import mibel_simulator.columns as cols

logger = logging.getLogger(__name__)

# fmt: off

CURVA_PBC_UOF_RENAMING = {
    "Periodo":                   cols.INT_PERIOD,
    "Pais":                     cols.CAT_BIDDING_ZONE,
    "Fecha":                    cols.DATE_SESION,
    "Tipo Oferta":              cols.CAT_BUY_SELL,
    "Ofertada (O)/Casada (C)":  cols.CAT_OFERTADA_CASADA,
    "Tipología de Oferta":      cols.CAT_ORDER_TYPE,
    "Potencia Compra/Venta":    cols.FLOAT_BID_POWER,
    "Precio Compra/Venta":      cols.FLOAT_BID_PRICE,
    "Unidad":                   cols.ID_UNIDAD,
}

CURVA_PBC_UOF_TYPING = {
    cols.INT_PERIOD:            int,
    cols.CAT_BIDDING_ZONE:               'category',
    cols.CAT_BUY_SELL:        'category',
    cols.CAT_OFERTADA_CASADA:    'category',
    cols.CAT_ORDER_TYPE:         'category',
    cols.FLOAT_BID_POWER:        float,
    cols.FLOAT_BID_PRICE:        float,
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
    cols.DATE_SESION:    cols.DATE_SESION,
    "CodOferta":         cols.ID_ORDER,
    "CodigoUnidad":      cols.ID_UNIDAD,
    "CV":                cols.CAT_BUY_SELL,
    "Fijoeuro":          cols.FLOAT_MIC,
    "MaxPot":            cols.FLOAT_MAX_POWER,
}

DET_FORMAT = [
    {"field": "CodOferta",      "start": 0,    "end": 10,  "type": str},
    # {"field": "Version",        "start": 10,   "end": 15,  "type": int},
    {"field": "Periodo",        "start": 15,   "end": 18,  "type": int},
    {"field": "NumBloq",        "start": 18,   "end": 20,  "type": int},
    {"field": "NumSuborder",       "start": 20,   "end": 22,  "type": int},
    {"field": "NumGrupoExcl",   "start": 22,   "end": 24,  "type": int},
    {"field": "PrecEuro",       "start": 24,   "end": 41,  "type": float},
    {"field": "Potencia",       "start": 41,   "end": 48,  "type": float},
    {"field": "MAV",            "start": 48,   "end": 55,  "type": float},
    {"field": "MAR",            "start": 55,   "end": 60,  "type": float},

]

DET_RENAMING = {
    cols.DATE_SESION:   cols.DATE_SESION,
    "CodOferta":        cols.ID_ORDER,
    "Periodo":          cols.INT_PERIOD,
    "NumBloq":          cols.INT_NUM_BLOCK,
    "NumSuborder":         cols.INT_NUM_SUBORDER,
    "NumGrupoExcl":     cols.INT_NUM_EXCL_GROUP,
    "PrecEuro":         cols.FLOAT_BID_PRICE,
    "Potencia":         cols.FLOAT_BID_POWER,
    "MAV":              cols.FLOAT_MAV,
    "MAR":              cols.FLOAT_MAR,
}

CAPACIDAD_INTER_RENAMING = {
    "Periodo":                  cols.INT_PERIOD,
    "Fecha":                    cols.DATE_SESION,
    "Frontera":                 cols.CAT_FRONTIER,
    "Capacidad importación":    cols.FLOAT_IMPORT_CAPACITY,
    "Capacidad exportación":    cols.FLOAT_EXPORT_CAPACITY,
}

CAPACIDAD_INTER_TYPING = {
    cols.INT_PERIOD:              int,
    cols.CAT_FRONTIER:             'category',
    cols.FLOAT_IMPORT_CAPACITY:    float,
    cols.FLOAT_EXPORT_CAPACITY:    float,
}

PRICE_FRANCE_RENAMING = {
    "Day-ahead Price (EUR/MWh)":    cols.FLOAT_PRICE_FR,
    "MTU (CET/CEST)":               cols.DATETIME_SESION,
}
PRICE_FRANCE_TYPING = {
    cols.DATE_SESION:      'datetime64[ns]',
    cols.INT_PERIOD:      int,
    cols.FLOAT_PRICE_FR:   float,
}

# fmt: on

CURVA_PBC_UOF_COLUMNS = list(CURVA_PBC_UOF_RENAMING.values())
CAPACIDAD_INTER_COLUMNS = list(CAPACIDAD_INTER_RENAMING.values())
PARTICIPANTS_BIDDING_ZONES_COLUMNS = [cols.CAT_BIDDING_ZONE, cols.ID_UNIDAD]
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


def parse_cab_file(cab_filepath: str) -> pd.DataFrame:
    """
    Parses a single CAB file in fixed-width format and returns a DataFrame.

    Args:
        cab_filepath (str): Path to the CAB file.

    Returns:
        pd.DataFrame: The parsed CAB DataFrame.
    """

    cab_data = []

    with open(cab_filepath, encoding="latin-1") as f:
        dat_sesion = pd.to_datetime(
            cab_filepath.split("/")[-1].split("_")[-1].split(".")[0], format="%Y%m%d"
        )

        for line in f:
            record = {
                cols.DATE_SESION: dat_sesion,
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

    return cab


def cab_files_to_parquet(cab_folder: str, output_path: str = "cab.parquet"):
    """
    Reads all CAB files from a folder, parses the fixed-width format, and saves the result as a parquet file.

    Args:
        cab_folder (str): Path to the folder containing CAB files.
        output_path (str, optional): Path to save the consolidated parquet file. Defaults to "cab.parquet".

    Returns:
        pd.DataFrame: The parsed and consolidated CAB DataFrame.

    Side Effects:
        Saves the consolidated CAB DataFrame as a parquet file at the specified output path.
    """

    cab_files = [f for f in os.listdir(cab_folder)]
    cab_dfs = []

    logger.info("Found CAB files: %d", len(cab_files))

    for file in cab_files:
        cab_dfs.append(parse_cab_file(cab_folder + file))

    cab = pd.concat(cab_dfs, ignore_index=True)

    logger.info("Saving CAB to parquet at %s", output_path)
    cab.to_parquet(output_path, index=False)
    return cab


def parse_det_file(det_filepath: str) -> pd.DataFrame:
    """
    Parses a single DET file in fixed-width format and returns a DataFrame.

    Args:
        det_filepath (str): Path to the DET file.
    Returns:
        pd.DataFrame: The parsed DET DataFrame.
    """

    det_data = []

    with open(det_filepath, encoding="latin-1") as f:
        dat_sesion = pd.to_datetime(
            det_filepath.split("/")[-1].split("_")[1].split(".")[0], format="%Y%m%d"
        )

        for line in f:
            record = {
                cols.DATE_SESION: dat_sesion,
                **{
                    entry["field"]: entry["type"](
                        line[entry["start"] : entry["end"]].strip()
                    )
                    for entry in DET_FORMAT
                },
            }
            det_data.append(record)
    det = (
        pd.DataFrame(det_data)
        .astype({entry["field"]: entry["type"] for entry in DET_FORMAT})
        .rename(columns=DET_RENAMING, errors="raise")[DET_COLUMNS]
    )
    det_25_hour = det[det[cols.INT_PERIOD] == 25]

    # det files can have 25 periods when they are hourly data, but many times is just
    # false data, so we drop it when it represents less than 1% of the data
    if not det_25_hour.empty:

        print(len(det_25_hour), len(det) / 24)
        if len(det_25_hour) < 0.1 * (len(det) / 24):
            logger.warning(
                "Periodo 25 size is less than 0.1 times the size of the other periods, dropping it. You can ignore this, this is a typical issue with OMIE det files. File: %s",
                det_filepath,
            )
            det = det[det[cols.INT_PERIOD] != 25]
        else:
            logger.warning(
                "Detected period 25 in DET file %s",
                det_filepath,
            )
    return det


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
    det_dfs = []

    for file in det_files:
        det_dfs.append(parse_det_file(det_folder + file))

    logger.info("Saving DET to parquet at %s", output_path)
    det = pd.concat(det_dfs, ignore_index=True)

    det.to_parquet(output_path, index=False)

    return det


def parse_capacidad_inter_file(capacidad_inter_filepath: str) -> pd.DataFrame:
    """
    Parses a single capacidad_inter CSV file and returns a DataFrame.

    Args:
        capacidad_inter_filepath (str): Path to the capacidad_inter CSV file.

    Returns:
        pd.DataFrame: The parsed capacidad_inter DataFrame.
    """

    capacidad_inter_data = pd.read_csv(
        capacidad_inter_filepath,
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

    if "Hora" in capacidad_inter_data.columns:
        capacidad_inter_data["Periodo"] = (
            capacidad_inter_data["Periodo"]
            .fillna(capacidad_inter_data["Hora"])
            .astype(int)
        )

    capacidad_inter_data = capacidad_inter_data.rename(
        columns=CAPACIDAD_INTER_RENAMING, errors="raise"
    )[CAPACIDAD_INTER_COLUMNS].astype(CAPACIDAD_INTER_TYPING)

    return capacidad_inter_data


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

    capacidad_inter_dfs = [
        parse_capacidad_inter_file(capacidad_inter_folder + f)
        for f in capacidad_inter_raw_filepaths
    ]

    capacidad_inter_data = pd.concat(capacidad_inter_dfs, ignore_index=True)

    # TODO: use when working in quarter hourly
    # is_hourly_data = capacidad_inter_data[cols.INT_PERIOD].max() <= 25
    # if is_hourly_data:
    #     logger.info(
    #         "Detected hourly data in capacidad_inte,r, converting to quarter-hourly..."
    #     )

    #     capacidad_inter_data_quarterly_dfs = []
    #     for i in range(1, 5):
    #         capacidad_inter_data_quarterly = capacidad_inter_data.copy()
    #         capacidad_inter_data_quarterly[cols.INT_PERIOD] = (
    #             capacidad_inter_data_quarterly[cols.INT_PERIOD] - 1
    #         ) * 4 + i
    #         capacidad_inter_data_quarterly_dfs.append(capacidad_inter_data_quarterly)

    #     capacidad_inter_data = pd.concat(capacidad_inter_data_quarterly_dfs)

    capacidad_inter_data = capacidad_inter_data.sort_values(
        by=[cols.DATE_SESION, cols.INT_PERIOD]
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

    price_france[cols.DATETIME_SESION] = (
        price_france[cols.DATETIME_SESION]
        .str.split(" - ")
        .str[0]
        .str.split(r" \(")
        .str[0]
    )
    price_france = price_france.drop_duplicates(subset=[cols.DATETIME_SESION])
    price_france[cols.DATETIME_SESION] = pd.to_datetime(
        price_france[cols.DATETIME_SESION], format="%d/%m/%Y %H:%M:%S"
    )

    price_france[cols.DATE_SESION] = pd.to_datetime(
        price_france[cols.DATETIME_SESION].dt.date
    )
    if use_qh_frequency:
        logger.info("Detected quarter-hourly data in France prices")
        price_france[cols.INT_PERIOD] = (
            price_france[cols.DATETIME_SESION] - price_france[cols.DATE_SESION]
        ).dt.total_seconds() // (15 * 60) + 1
    else:
        is_QH = price_france[cols.DATETIME_SESION].dt.minute.isin([15, 30, 45]).any()
        price_france["Hour"] = price_france[cols.DATETIME_SESION].dt.hour
        if is_QH:
            logger.warning(
                "Detected quarter-hourly data in France prices, but use_qh_frequency is False. Processing as hourly data."
            )
            price_france = (
                price_france.groupby([cols.DATE_SESION, "Hour"], observed=True)[
                    [cols.FLOAT_PRICE_FR]
                ]
                .mean()
                .reset_index()
            )
        price_france[cols.INT_PERIOD] = price_france["Hour"] + 1
    price_france = price_france[PRICE_FRANCE_COLUMNS].astype(PRICE_FRANCE_TYPING)

    logger.info("Saving France prices to parquet at %s", output_path)
    price_france.to_parquet(output_path)

    return price_france


def generate_participants_bidding_zones_parquet_from_uof_files(
    curva_pbc_uof_parquet: str | pd.DataFrame,
    output_path: str = "participants_bidding_zones.parquet",
):
    """
    Generates a parquet file containing unique unit IDs and their corresponding zones from the curva_pbc_uof data.

    Args:
        curva_pbc_uof_parquet (str|pd.DataFrame): Path to the curva_pbc_uof parquet file or a DataFrame.
        output_path (str, optional): Path to save the uof zones parquet file. Defaults to "participants_bidding_zones.parquet".

    Returns:
        pd.DataFrame: DataFrame containing unique unit IDs and their zones.
    """

    if isinstance(curva_pbc_uof_parquet, str):
        curva_pbc_uof = pd.read_parquet(curva_pbc_uof_parquet)
    else:
        curva_pbc_uof = curva_pbc_uof_parquet

    curva_pbc_uof_split = curva_pbc_uof.query(
        f"{cols.CAT_BIDDING_ZONE} != 'MI' and {cols.ID_UNIDAD} not in @INTERCONEXION_UOFS"
    )

    curva_pbc_uof_unidad_pais_count = curva_pbc_uof_split.groupby(
        cols.ID_UNIDAD, observed=True
    )[cols.CAT_BIDDING_ZONE].nunique()

    assert (
        curva_pbc_uof_unidad_pais_count.max() == 1
    ), f"These units are in more than one country: {curva_pbc_uof_unidad_pais_count[curva_pbc_uof_unidad_pais_count > 1]}"

    curva_pbc_uof_unidad_pais = curva_pbc_uof_split.drop_duplicates(
        subset=[cols.ID_UNIDAD], keep="last"
    )[PARTICIPANTS_BIDDING_ZONES_COLUMNS]

    logger.info("Saving UOF zones to parquet at %s", output_path)
    curva_pbc_uof_unidad_pais.to_parquet(output_path, index=False)

    return curva_pbc_uof_unidad_pais
