import pandas as pd
import pytest
from mibel_simulator import columns as cols
from mibel_simulator.const import CAT_SELL
from mibel_simulator.data_preprocessor import (
    get_det_cab_date_id_block_order,
    get_det_cab_date_id_individual_bid,
    get_det_cab_date_id_sco,
    get_france_det_cab_date_from_price,
    get_det_cab_date_for_simulation,
)
from mibel_simulator.tools import get_cat_order_type_column


@pytest.fixture
def price_france():
    return pd.DataFrame(
        {
            cols.INT_PERIODO: [1, 2],
            cols.FLOAT_PRICE_FR: [50.0, 60.0],
        }
    )


@pytest.fixture
def capacidad_inter():
    return pd.DataFrame(
        {
            cols.INT_PERIODO: [1, 2],
            cols.FLOAT_IMPORT_CAPACITY: [100.0, 200.0],
            cols.FLOAT_EXPORT_CAPACITY: [110.0, 210.0],
        }
    )


@pytest.fixture
def expected_det_cab():
    return pd.DataFrame(
        {
            "int_periodo": [1, 2, 1, 2],
            "float_bid_price": [50.0, 60.0, 50.0, 60.0],
            "cat_buy_sell": ["V", "V", "C", "C"],
            "float_bid_power": [100.0, 200.0, 110.0, 210.0],
            "id_order": [
                "12345678901234",
                "12345678901234",
                "12345678901234",
                "12345678901234",
            ],
            "id_unidad": ["MIEU", "MIEU", "MIEU", "MIEU"],
            "float_mic": [0, 0, 0, 0],
            "float_max_power": [99999999, 99999999, 99999999, 99999999],
            "int_num_bloq": [0, 0, 0, 0],
            "int_num_tramo": [1, 1, 1, 1],
            "int_num_grupo_excl": [0, 0, 0, 0],
            "float_mav": [0, 0, 0, 0],
            "float_mar": [0, 0, 0, 0],
            "cat_pais": ["ES", "ES", "ES", "ES"],
            "date_sesion": [
                pd.Timestamp("2025-07-01 00:00:00"),
                pd.Timestamp("2025-07-01 00:00:00"),
                pd.Timestamp("2025-07-01 00:00:00"),
                pd.Timestamp("2025-07-01 00:00:00"),
            ],
        }
    ).astype(
        # to match dtypes
        {
            "date_sesion": "datetime64[s]",
        }
    )


@pytest.fixture
def date_basic():
    return "2025-07-01"


# ---------------------------------------------------------------------------
# Fixtures for get_det_cab_date_for_simulation
# ---------------------------------------------------------------------------


@pytest.fixture
def uof_zones_basic():
    return pd.DataFrame(
        {
            cols.ID_UNIDAD: ["UNIT_ES", "UNIT_PT"],
            cols.CAT_PAIS: ["ES", "PT"],
        }
    )


@pytest.fixture
def det_cab_fr_stub():
    date = pd.Timestamp("2025-07-01")
    return pd.DataFrame(
        {
            cols.DATE_SESION: [date],
            cols.ID_ORDER: ["FR_ORD"],
            cols.ID_UNIDAD: ["MIEU"],
            cols.CAT_BUY_SELL: [CAT_SELL],
            cols.INT_PERIODO: [3],
            cols.INT_NUM_BLOQ: [0],
            cols.INT_NUM_TRAMO: [1],
            cols.INT_NUM_GRUPO_EXCL: [0],
            cols.FLOAT_BID_PRICE: [45.0],
            cols.FLOAT_BID_POWER: [120.0],
            cols.FLOAT_MAV: [0.0],
            cols.FLOAT_MAR: [0.0],
            cols.FLOAT_MIC: [0.0],
            cols.FLOAT_MAX_POWER: [99999999.0],
            cols.CAT_PAIS: ["FR"],
        }
    )


class TestGetFranceDetCabDateFromPrice:

    def test_creates_buy_and_sell_bids_per_period(
        self, price_france, capacidad_inter, expected_det_cab, date_basic
    ):
        price_france = price_france.copy()
        capacidad_inter = capacidad_inter.copy()
        expected_det_cab = expected_det_cab.copy()
        det_cab = get_france_det_cab_date_from_price(
            price_france, capacidad_inter, date=date_basic
        )

        pd.testing.assert_frame_equal(
            det_cab.reset_index(drop=True),
            expected_det_cab.reset_index(drop=True),
        )

    def test_filters_by_date_when_multiple_dates_present(
        self, price_france, capacidad_inter, expected_det_cab
    ):
        price_france = price_france.copy()
        price_france[cols.DATE_SESION] = pd.Series(
            [
                pd.Timestamp("2025-07-01"),
                pd.Timestamp("2025-07-02"),
            ]
        )
        capacidad_inter = capacidad_inter.copy()
        capacidad_inter[cols.DATE_SESION] = pd.Series(
            [
                pd.Timestamp("2025-07-01"),
                pd.Timestamp("2025-07-02"),
            ]
        )
        capacidad_inter[cols.CAT_FRONTIER] = pd.Series([3, 3])

        det_cab = get_france_det_cab_date_from_price(
            price_france, capacidad_inter, date="2025-07-01"
        )

        expected_det_cab = expected_det_cab.iloc[[0, 2]].copy()

        pd.testing.assert_frame_equal(
            det_cab.reset_index(drop=True),
            expected_det_cab.reset_index(drop=True),
        )

    def test_filters_cat_frontier_equals_three(
        self, price_france, capacidad_inter, expected_det_cab
    ):
        capacidad_inter = capacidad_inter.copy()
        capacidad_inter[cols.CAT_FRONTIER] = pd.Series(
            [2, 3]
        )  # One row with PT, one with FR
        expected_det_cab = expected_det_cab.iloc[[1, 3]].copy()

        det_cab = get_france_det_cab_date_from_price(
            price_france, capacidad_inter, date="2025-07-01"
        )
        pd.testing.assert_frame_equal(
            det_cab.reset_index(drop=True),
            expected_det_cab.reset_index(drop=True),
        )

    def test_raises_if_multiple_dates_without_date_param(
        self, price_france, capacidad_inter, expected_det_cab
    ):
        price_france = price_france.copy()
        price_france[cols.DATE_SESION] = pd.Series(
            [
                pd.Timestamp("2025-07-01"),
                pd.Timestamp("2025-07-02"),
            ]
        )

        with pytest.raises(ValueError):
            get_france_det_cab_date_from_price(price_france, capacidad_inter)


# ---------------------------------------------------------------------------
# Tests for get_det_cab_date_for_simulation
# ---------------------------------------------------------------------------


class TestGetDetCabDateForSimulation:

    def test_get_det_cab_date_for_simulation(
        self,
        full_simplified_cab_dataframe,
        full_simplified_det_dataframe,
        full_det_cab_uof_zones_dataframe,
        full_simplified_det_cab_fr_dataframe,
        full_simplified_det_cab_dataframe,
    ):
        det_cab = get_det_cab_date_for_simulation(
            full_simplified_det_dataframe,
            full_simplified_cab_dataframe,
            full_det_cab_uof_zones_dataframe,
            det_cab_fr_date=full_simplified_det_cab_fr_dataframe,
        ).sort_index()

        pd.testing.assert_frame_equal(
            det_cab.reset_index(drop=True),
            full_simplified_det_cab_dataframe.reset_index(drop=True),
        )


class TestGetCatOrderTypeColumn:

    def test_get_cat_order_type_column(self, full_simplified_det_cab_dataframe):
        cat_order_type = get_cat_order_type_column(full_simplified_det_cab_dataframe)

        pd.testing.assert_series_equal(
            cat_order_type,
            full_simplified_det_cab_dataframe[cols.CAT_ORDER_TYPE].astype("object"),
        )


class TestGetDetCabDateIdIndividualBid:
    def test_get_det_cab_date_id_individual_bid(
        self, full_simplified_det_cab_dataframe
    ):
        id_individual_bid = get_det_cab_date_id_individual_bid(
            full_simplified_det_cab_dataframe
        )
        pd.testing.assert_series_equal(
            id_individual_bid,
            full_simplified_det_cab_dataframe[cols.ID_INDIVIDUAL_BID].rename(None),
        )


class TestGetDetCabDateIdBlockOrder:

    def test_get_det_cab_date_id_block_order(self, full_simplified_det_cab_dataframe):
        id_block_order = get_det_cab_date_id_block_order(
            full_simplified_det_cab_dataframe
        )
        pd.testing.assert_series_equal(
            pd.Series(id_block_order),
            full_simplified_det_cab_dataframe[cols.ID_BLOCK_ORDER],
            check_names=False,
            check_dtype=False,
        )


class TestGetDetCabDateIdSco:

    def test_get_det_cab_date_id_sco(self, full_simplified_det_cab_dataframe):
        id_sco = get_det_cab_date_id_sco(full_simplified_det_cab_dataframe)
        pd.testing.assert_series_equal(
            pd.Series(id_sco),
            full_simplified_det_cab_dataframe[cols.ID_SCO],
            check_names=False,
            check_dtype=False,
        )
