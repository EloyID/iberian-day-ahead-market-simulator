"""
Shared pytest fixtures and configuration for all tests.

This module provides reusable test fixtures for common data structures
used across the mibel-simulator test suite.
"""

import numpy as np
import pandas as pd
import pytest
from mibel_simulator import columns as cols

from tests.const import STANDARD_TESTING_DATE

# ---------------------------------------------------------------------------
# Full det_cab_date
# ---------------------------------------------------------------------------


@pytest.fixture
def full_simplified_cab_dataframe():
    """Provide a full simplified CAB DataFrame for testing."""
    return pd.DataFrame(
        # fmt: off
        {
            cols.DATE_SESION: [STANDARD_TESTING_DATE] * 6,
            cols.ID_ORDER:        ["ID_SIMPLE",   "ID_SCO_MIC",   "ID_BLOCK",   "ID_EXCL_BLOCK",   "ID_SCO_MAV",   "ID_BUY"],
            cols.ID_UNIDAD:       ["UNIT_SIMPLE", "UNIT_SCO_MIC", "UNIT_BLOCK", "UNIT_EXCL_BLOCK", "UNIT_SCO_MAV", "UNIT_BUY"],
            cols.CAT_BUY_SELL:    ["V",           "V",            "V",          "V",               "V",            "C"],
            cols.FLOAT_MAX_POWER: [100.0,         250.0,          200.0,        150.0,             300.0,          350.0],
            cols.FLOAT_MIC:       [0.0,           10000.0,        0.0,          0.0,               0.0,            0.0],
        }
        # fmt: on
    )


@pytest.fixture
def full_simplified_det_dataframe():
    """Provide a full simplified DET DataFrame for testing."""
    # fmt: off
    simple_order = pd.DataFrame(
        {
            cols.DATE_SESION:      [STANDARD_TESTING_DATE] * 3,
            cols.ID_ORDER:         ["ID_SIMPLE"] * 3,
            cols.INT_PERIODO:      [1, 2, 3],
            cols.INT_NUM_BLOQ:     [0] * 3,
            cols.INT_NUM_TRAMO:    [1] * 3,
            cols.INT_NUM_GRUPO_EXCL: [0] * 3,
            cols.FLOAT_BID_PRICE:  [40.0, 42.0, 44.0],
            cols.FLOAT_BID_POWER:  [50.0, 60.0, 70.0],
            cols.FLOAT_MAV:        [0.0] * 3,
            cols.FLOAT_MAR:        [0.0] * 3,
        }
    )
    sco_mav_order = pd.DataFrame(
        {
            cols.DATE_SESION:      [STANDARD_TESTING_DATE] * 3,
            cols.ID_ORDER:         ["ID_SCO_MAV"] * 3,
            cols.INT_PERIODO:      [1, 2, 3],
            cols.INT_NUM_BLOQ:     [0] * 3,
            cols.INT_NUM_TRAMO:    [1] * 3,
            cols.INT_NUM_GRUPO_EXCL: [0] * 3,
            cols.FLOAT_BID_PRICE:  [38.0, 39.0, 40.0],
            cols.FLOAT_BID_POWER:  [80.0, 90.0, 100.0],
            cols.FLOAT_MAV:        [1.0, 1.0, 0.0],
            cols.FLOAT_MAR:        [0.0] * 3,
        }
    )
    sco_mic_order  = pd.DataFrame(
        {
            cols.DATE_SESION:      [STANDARD_TESTING_DATE] * 3,
            cols.ID_ORDER:         ["ID_SCO_MIC"] * 3,
            cols.INT_PERIODO:      [1, 2, 3],
            cols.INT_NUM_BLOQ:     [0] * 3,
            cols.INT_NUM_TRAMO:    [1] * 3,
            cols.INT_NUM_GRUPO_EXCL: [0] * 3,
            cols.FLOAT_BID_PRICE:  [36.0, 37.0, 38.0],
            cols.FLOAT_BID_POWER:  [90.0, 100.0, 110.0],
            cols.FLOAT_MAV:        [0.0] * 3,
            cols.FLOAT_MAR:        [0.0] * 3,
        }
    )
    exclusive_block_order = pd.DataFrame(
        {
            cols.DATE_SESION:      [STANDARD_TESTING_DATE] * 6,
            cols.ID_ORDER:         ["ID_EXCL_BLOCK"] * 6,
            cols.INT_PERIODO:      [1, 2, 3, 1, 2, 3],
            cols.INT_NUM_BLOQ:     [1] * 3 + [2] * 3,
            cols.INT_NUM_TRAMO:    [1] * 6,
            cols.INT_NUM_GRUPO_EXCL: [1] * 6,
            cols.FLOAT_BID_PRICE:  [35.0, 35.0, 35.0, 37.0, 37.0, 37.0],
            cols.FLOAT_BID_POWER:  [70.0, 80.0, 90.0, 70.0, 80.0, 90.0],
            cols.FLOAT_MAV:        [0.0] * 6,
            cols.FLOAT_MAR:        [0.0] * 6,
        }
    )


    block_order  = pd.DataFrame(
        {
            cols.DATE_SESION:      [STANDARD_TESTING_DATE] * 6,
            cols.ID_ORDER:         ["ID_BLOCK"] * 6,
            cols.INT_PERIODO:      [1, 2, 3, 1, 2, 3],
            cols.INT_NUM_BLOQ:     [1] * 3 + [2] * 3,
            cols.INT_NUM_TRAMO:    [1] * 6,
            cols.INT_NUM_GRUPO_EXCL: [0] * 6,
            cols.FLOAT_BID_PRICE:  [33.0, 33.0, 33.0, 34.0, 34.0, 34.0],
            cols.FLOAT_BID_POWER:  [60.0, 70.0, 80.0, 60.0, 70.0, 80.0],
            cols.FLOAT_MAV:        [0.0] * 6,
            cols.FLOAT_MAR:        [0.1, 0.1, 0.1, 0.2, 0.2, 0.2],
        }
    )
    
    buy_order = pd.DataFrame(
        {
            cols.DATE_SESION:      [STANDARD_TESTING_DATE] * 3,
            cols.ID_ORDER:         ["ID_BUY"] * 3,
            cols.INT_PERIODO:      [1, 2, 3],
            cols.INT_NUM_BLOQ:     [0] * 3,
            cols.INT_NUM_TRAMO:    [1] * 3,
            cols.INT_NUM_GRUPO_EXCL: [0] * 3,
            cols.FLOAT_BID_PRICE:  [55.0, 57.0, 59.0],
            cols.FLOAT_BID_POWER:  [150.0, 160.0, 170.0],
            cols.FLOAT_MAV:        [0.0] * 3,
            cols.FLOAT_MAR:        [0.0] * 3,
        }
    )
    # fmt: on

    return pd.concat(
        [
            simple_order,
            sco_mav_order,
            sco_mic_order,
            exclusive_block_order,
            block_order,
            buy_order,
        ],
        ignore_index=True,
    )


@pytest.fixture
def full_det_cab_uof_zones_dataframe():
    """Provide a full simplified UOF zones DataFrame for testing."""
    return pd.DataFrame(
        {
            cols.ID_UNIDAD: [
                "UNIT_SIMPLE",
                "UNIT_SCO_MIC",
                "UNIT_BLOCK",
                "UNIT_EXCL_BLOCK",
                "UNIT_SCO_MAV",
                "UNIT_BUY",
            ],
            cols.CAT_PAIS: ["ES", "ES", "PT", "PT", "FR", "ES"],
        }
    )


@pytest.fixture
def full_simplified_det_cab_fr_dataframe():
    """Provide a full simplified DET/CAB DataFrame for exchanges with France."""
    return pd.DataFrame(
        {
            cols.INT_PERIODO: [1, 2, 3] * 2,
            cols.DATE_SESION: [STANDARD_TESTING_DATE] * 6,
            cols.ID_ORDER: ["12345678901234"] * 6,
            cols.ID_UNIDAD: ["MIEU"] * 6,
            cols.CAT_BUY_SELL: ["C"] * 3 + ["V"] * 3,
            cols.FLOAT_BID_POWER: [100.0, 150.0, 200.0, 250.0, 300.0, 350.0],
            cols.FLOAT_BID_PRICE: [50.0, 55.0, 60.0] * 2,
            cols.FLOAT_MAV: [0.0] * 6,
            cols.FLOAT_MAR: [0.0] * 6,
            cols.FLOAT_MIC: [0.0] * 6,
            cols.FLOAT_MAX_POWER: [99999999] * 6,
            cols.INT_NUM_BLOQ: [0] * 6,
            cols.INT_NUM_TRAMO: [1] * 6,
            cols.INT_NUM_GRUPO_EXCL: [0] * 6,
            cols.CAT_PAIS: ["ES"] * 6,
        }
    )


@pytest.fixture
def full_simplified_det_cab_dataframe():
    """Provide a full simplified DET/CAB DataFrame for testing."""

    # fmt: off
    return pd.DataFrame(
        {
            'date_sesion': ['2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01', '2025-07-01'],
            'id_order': ['ID_BLOCK', 'ID_BLOCK', 'ID_BLOCK', 'ID_BLOCK', 'ID_BLOCK', 'ID_BLOCK', 'ID_BUY', 'ID_BUY', 'ID_BUY', 'ID_EXCL_BLOCK', 'ID_EXCL_BLOCK', 'ID_EXCL_BLOCK', 'ID_EXCL_BLOCK', 'ID_EXCL_BLOCK', 'ID_EXCL_BLOCK', 'ID_SCO_MAV', 'ID_SCO_MAV', 'ID_SCO_MAV', 'ID_SCO_MIC', 'ID_SCO_MIC', 'ID_SCO_MIC', 'ID_SIMPLE', 'ID_SIMPLE', 'ID_SIMPLE', '12345678901234', '12345678901234', '12345678901234', '12345678901234', '12345678901234', '12345678901234'],
            'int_periodo': [1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3],
            'int_num_bloq': [1, 1, 1, 2, 2, 2, 0, 0, 0, 1, 1, 1, 2, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            'int_num_tramo': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            'int_num_grupo_excl': [0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            'float_bid_price': [33.0, 33.0, 33.0, 34.0, 34.0, 34.0, 55.0, 57.0, 59.0, 35.0, 35.0, 35.0, 37.0, 37.0, 37.0, 38.0, 39.0, 40.0, 36.0, 37.0, 38.0, 40.0, 42.0, 44.0, 50.0, 55.0, 60.0, 50.0, 55.0, 60.0],
            'float_bid_power': [60.0, 70.0, 80.0, 60.0, 70.0, 80.0, 150.0, 160.0, 170.0, 70.0, 80.0, 90.0, 70.0, 80.0, 90.0, 80.0, 90.0, 100.0, 90.0, 100.0, 110.0, 50.0, 60.0, 70.0, 100.0, 150.0, 200.0, 250.0, 300.0, 350.0],
            'float_mav': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            'float_mar': [0.1, 0.1, 0.1, 0.2, 0.2, 0.2, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            'id_unidad': ['UNIT_BLOCK', 'UNIT_BLOCK', 'UNIT_BLOCK', 'UNIT_BLOCK', 'UNIT_BLOCK', 'UNIT_BLOCK', 'UNIT_BUY', 'UNIT_BUY', 'UNIT_BUY', 'UNIT_EXCL_BLOCK', 'UNIT_EXCL_BLOCK', 'UNIT_EXCL_BLOCK', 'UNIT_EXCL_BLOCK', 'UNIT_EXCL_BLOCK', 'UNIT_EXCL_BLOCK', 'UNIT_SCO_MAV', 'UNIT_SCO_MAV', 'UNIT_SCO_MAV', 'UNIT_SCO_MIC', 'UNIT_SCO_MIC', 'UNIT_SCO_MIC', 'UNIT_SIMPLE', 'UNIT_SIMPLE', 'UNIT_SIMPLE', 'MIEU', 'MIEU', 'MIEU', 'MIEU', 'MIEU', 'MIEU'],
            'cat_buy_sell': ['V', 'V', 'V', 'V', 'V', 'V', 'C', 'C', 'C', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'V', 'C', 'C', 'C', 'V', 'V', 'V'],
            'float_max_power': [200.0, 200.0, 200.0, 200.0, 200.0, 200.0, 350.0, 350.0, 350.0, 150.0, 150.0, 150.0, 150.0, 150.0, 150.0, 300.0, 300.0, 300.0, 250.0, 250.0, 250.0, 100.0, 100.0, 100.0, 99999999.0, 99999999.0, 99999999.0, 99999999.0, 99999999.0, 99999999.0],
            'float_mic': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 10000.0, 10000.0, 10000.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            'cat_pais': ['PT', 'PT', 'PT', 'PT', 'PT', 'PT', 'ES', 'ES', 'ES', 'PT', 'PT', 'PT', 'PT', 'PT', 'PT', 'FR', 'FR', 'FR', 'ES', 'ES', 'ES', 'ES', 'ES', 'ES', 'ES', 'ES', 'ES', 'ES', 'ES', 'ES'],
            'cat_order_type': ['C01', 'C01', 'C01', 'C01', 'C01', 'C01', 'S', 'S', 'S', 'C04', 'C04', 'C04', 'C04', 'C04', 'C04', 'C02', 'C02', 'C02', 'C02', 'C02', 'C02', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S', 'S'],
            'float_bid_power_cumsum': [60.0, 70.0, 80.0, 120.0, 140.0, 160.0, 150.0, 160.0, 370.0, 190.0, 220.0, 250.0, 350.0, 300.0, 340.0, 430.0, 490.0, 550.0, 280.0, 400.0, 450.0, 480.0, 550.0, 620.0, 250.0, 310.0, 200.0, 730.0, 850.0, 970.0],
            'id_individual_bid': ['1_V_ID_BLOCK_1_1_0', '2_V_ID_BLOCK_1_1_0', '3_V_ID_BLOCK_1_1_0', '1_V_ID_BLOCK_1_2_0', '2_V_ID_BLOCK_1_2_0', '3_V_ID_BLOCK_1_2_0', '1_C_ID_BUY_1_0_0', '2_C_ID_BUY_1_0_0', '3_C_ID_BUY_1_0_0', '1_V_ID_EXCL_BLOCK_1_1_1', '2_V_ID_EXCL_BLOCK_1_1_1', '3_V_ID_EXCL_BLOCK_1_1_1', '1_V_ID_EXCL_BLOCK_1_2_1', '2_V_ID_EXCL_BLOCK_1_2_1', '3_V_ID_EXCL_BLOCK_1_2_1', '1_V_ID_SCO_MAV_1_0_0', '2_V_ID_SCO_MAV_1_0_0', '3_V_ID_SCO_MAV_1_0_0', '1_V_ID_SCO_MIC_1_0_0', '2_V_ID_SCO_MIC_1_0_0', '3_V_ID_SCO_MIC_1_0_0', '1_V_ID_SIMPLE_1_0_0', '2_V_ID_SIMPLE_1_0_0', '3_V_ID_SIMPLE_1_0_0', '1_C_12345678901234_1_0_0', '2_C_12345678901234_1_0_0', '3_C_12345678901234_1_0_0', '1_V_12345678901234_1_0_0', '2_V_12345678901234_1_0_0', '3_V_12345678901234_1_0_0'],
            'id_block_order': ['ID_BLOCK_B_1_GE_0', 'ID_BLOCK_B_1_GE_0', 'ID_BLOCK_B_1_GE_0', 'ID_BLOCK_B_2_GE_0', 'ID_BLOCK_B_2_GE_0', 'ID_BLOCK_B_2_GE_0', None, None, None, 'ID_EXCL_BLOCK_B_1_GE_1', 'ID_EXCL_BLOCK_B_1_GE_1', 'ID_EXCL_BLOCK_B_1_GE_1', 'ID_EXCL_BLOCK_B_2_GE_1', 'ID_EXCL_BLOCK_B_2_GE_1', 'ID_EXCL_BLOCK_B_2_GE_1', None, None, None, None, None, None, None, None, None, None, None, None, None, None, None],
            'id_sco': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, 'ID_SCO_MAV_SCO', 'ID_SCO_MAV_SCO', 'ID_SCO_MAV_SCO', 'ID_SCO_MIC_SCO', 'ID_SCO_MIC_SCO', 'ID_SCO_MIC_SCO', None, None, None, None, None, None, None, None, None],
            'float_bid_power_cumsum_by_country': [60.0, 70.0, 80.0, 120.0, 140.0, 160.0, 150.0, 160.0, 370.0, 190.0, 220.0, 250.0, 260.0, 300.0, 340.0, np.nan, np.nan, np.nan, 90.0, 100.0, 110.0, 140.0, 160.0, 180.0, 250.0, 310.0, 200.0, 390.0, 460.0, 530.0]}
    ).astype(
        {
            'date_sesion': 'object',
            'id_order': 'string',
            'int_periodo': 'int8',
            'int_num_bloq': 'int8',
            'int_num_tramo': 'int8',
            'int_num_grupo_excl': 'int8',
            'float_bid_price': 'float64',
            'float_bid_power': 'float64',
            'float_mav': 'float64',
            'float_mar': 'float64',
            'id_unidad': 'string',
            'cat_buy_sell': 'category',
            'float_max_power': 'float64',
            'float_mic': 'float64',
            'cat_pais': 'category',
            'cat_order_type': 'category',
            'float_bid_power_cumsum': 'float64',
            'id_individual_bid': 'object',
            'id_block_order': 'string',
            'id_sco': 'string',
            'float_bid_power_cumsum_by_country': 'float64'
        }
    )
    # fmt: on


# date_sesion        id_order  int_periodo  int_num_bloq  int_num_tramo  int_num_grupo_excl  float_bid_price  float_bid_power  float_mav  float_mar        id_unidad cat_buy_sell  float_max_power  float_mic cat_pais cat_order_type  float_bid_power_cumsum         id_individual_bid          id_block_order          id_sco  float_bid_power_cumsum_by_country
# 0   2025-07-01        ID_BLOCK            1             1              1                   0             33.0             60.0        0.0        0.1       UNIT_BLOCK            V            200.0        0.0       PT            C01                    60.0        1_V_ID_BLOCK_1_1_0       ID_BLOCK_B_1_GE_0            <NA>                               60.0
# 1   2025-07-01        ID_BLOCK            2             1              1                   0             33.0             70.0        0.0        0.1       UNIT_BLOCK            V            200.0        0.0       PT            C01                    70.0        2_V_ID_BLOCK_1_1_0       ID_BLOCK_B_1_GE_0            <NA>                               70.0
# 2   2025-07-01        ID_BLOCK            3             1              1                   0             33.0             80.0        0.0        0.1       UNIT_BLOCK            V            200.0        0.0       PT            C01                    80.0        3_V_ID_BLOCK_1_1_0       ID_BLOCK_B_1_GE_0            <NA>                               80.0
# 3   2025-07-01        ID_BLOCK            1             2              1                   0             34.0             60.0        0.0        0.2       UNIT_BLOCK            V            200.0        0.0       PT            C01                   120.0        1_V_ID_BLOCK_1_2_0       ID_BLOCK_B_2_GE_0            <NA>                              120.0
# 4   2025-07-01        ID_BLOCK            2             2              1                   0             34.0             70.0        0.0        0.2       UNIT_BLOCK            V            200.0        0.0       PT            C01                   140.0        2_V_ID_BLOCK_1_2_0       ID_BLOCK_B_2_GE_0            <NA>                              140.0
# 5   2025-07-01        ID_BLOCK            3             2              1                   0             34.0             80.0        0.0        0.2       UNIT_BLOCK            V            200.0        0.0       PT            C01                   160.0        3_V_ID_BLOCK_1_2_0       ID_BLOCK_B_2_GE_0            <NA>                              160.0
# 6   2025-07-01          ID_BUY            1             0              1                   0             55.0            150.0        0.0        0.0         UNIT_BUY            C            350.0        0.0       ES              S                   150.0          1_C_ID_BUY_1_0_0                    <NA>            <NA>                              150.0
# 7   2025-07-01          ID_BUY            2             0              1                   0             57.0            160.0        0.0        0.0         UNIT_BUY            C            350.0        0.0       ES              S                   160.0          2_C_ID_BUY_1_0_0                    <NA>            <NA>                              160.0
# 8   2025-07-01          ID_BUY            3             0              1                   0             59.0            170.0        0.0        0.0         UNIT_BUY            C            350.0        0.0       ES              S                   370.0          3_C_ID_BUY_1_0_0                    <NA>            <NA>                              370.0
# 9   2025-07-01   ID_EXCL_BLOCK            1             1              1                   1             35.0             70.0        0.0        0.0  UNIT_EXCL_BLOCK            V            150.0        0.0       PT            C04                   190.0   1_V_ID_EXCL_BLOCK_1_1_1  ID_EXCL_BLOCK_B_1_GE_1            <NA>                              190.0
# 10  2025-07-01   ID_EXCL_BLOCK            2             1              1                   1             35.0             80.0        0.0        0.0  UNIT_EXCL_BLOCK            V            150.0        0.0       PT            C04                   220.0   2_V_ID_EXCL_BLOCK_1_1_1  ID_EXCL_BLOCK_B_1_GE_1            <NA>                              220.0
# 11  2025-07-01   ID_EXCL_BLOCK            3             1              1                   1             35.0             90.0        0.0        0.0  UNIT_EXCL_BLOCK            V            150.0        0.0       PT            C04                   250.0   3_V_ID_EXCL_BLOCK_1_1_1  ID_EXCL_BLOCK_B_1_GE_1            <NA>                              250.0
# 12  2025-07-01   ID_EXCL_BLOCK            1             2              1                   1             37.0             70.0        0.0        0.0  UNIT_EXCL_BLOCK            V            150.0        0.0       PT            C04                   350.0   1_V_ID_EXCL_BLOCK_1_2_1  ID_EXCL_BLOCK_B_2_GE_1            <NA>                              260.0
# 13  2025-07-01   ID_EXCL_BLOCK            2             2              1                   1             37.0             80.0        0.0        0.0  UNIT_EXCL_BLOCK            V            150.0        0.0       PT            C04                   300.0   2_V_ID_EXCL_BLOCK_1_2_1  ID_EXCL_BLOCK_B_2_GE_1            <NA>                              300.0
# 14  2025-07-01   ID_EXCL_BLOCK            3             2              1                   1             37.0             90.0        0.0        0.0  UNIT_EXCL_BLOCK            V            150.0        0.0       PT            C04                   340.0   3_V_ID_EXCL_BLOCK_1_2_1  ID_EXCL_BLOCK_B_2_GE_1            <NA>                              340.0
# 15  2025-07-01      ID_SCO_MAV            1             0              1                   0             38.0             80.0        1.0        0.0     UNIT_SCO_MAV            V            300.0        0.0       FR            C02                   430.0      1_V_ID_SCO_MAV_1_0_0                    <NA>  ID_SCO_MAV_SCO                                NaN
# 16  2025-07-01      ID_SCO_MAV            2             0              1                   0             39.0             90.0        1.0        0.0     UNIT_SCO_MAV            V            300.0        0.0       FR            C02                   490.0      2_V_ID_SCO_MAV_1_0_0                    <NA>  ID_SCO_MAV_SCO                                NaN
# 17  2025-07-01      ID_SCO_MAV            3             0              1                   0             40.0            100.0        0.0        0.0     UNIT_SCO_MAV            V            300.0        0.0       FR            C02                   550.0      3_V_ID_SCO_MAV_1_0_0                    <NA>  ID_SCO_MAV_SCO                                NaN
# 18  2025-07-01      ID_SCO_MIC            1             0              1                   0             36.0             90.0        0.0        0.0     UNIT_SCO_MIC            V            250.0    10000.0       ES            C02                   280.0      1_V_ID_SCO_MIC_1_0_0                    <NA>  ID_SCO_MIC_SCO                               90.0
# 19  2025-07-01      ID_SCO_MIC            2             0              1                   0             37.0            100.0        0.0        0.0     UNIT_SCO_MIC            V            250.0    10000.0       ES            C02                   400.0      2_V_ID_SCO_MIC_1_0_0                    <NA>  ID_SCO_MIC_SCO                              100.0
# 20  2025-07-01      ID_SCO_MIC            3             0              1                   0             38.0            110.0        0.0        0.0     UNIT_SCO_MIC            V            250.0    10000.0       ES            C02                   450.0      3_V_ID_SCO_MIC_1_0_0                    <NA>  ID_SCO_MIC_SCO                              110.0
# 21  2025-07-01       ID_SIMPLE            1             0              1                   0             40.0             50.0        0.0        0.0      UNIT_SIMPLE            V            100.0        0.0       ES              S                   480.0       1_V_ID_SIMPLE_1_0_0                    <NA>            <NA>                              140.0
# 22  2025-07-01       ID_SIMPLE            2             0              1                   0             42.0             60.0        0.0        0.0      UNIT_SIMPLE            V            100.0        0.0       ES              S                   550.0       2_V_ID_SIMPLE_1_0_0                    <NA>            <NA>                              160.0
# 23  2025-07-01       ID_SIMPLE            3             0              1                   0             44.0             70.0        0.0        0.0      UNIT_SIMPLE            V            100.0        0.0       ES              S                   620.0       3_V_ID_SIMPLE_1_0_0                    <NA>            <NA>                              180.0
# 24  2025-07-01  12345678901234            1             0              1                   0             50.0            100.0        0.0        0.0             MIEU            C       99999999.0        0.0       ES              S                   250.0  1_C_12345678901234_1_0_0                    <NA>            <NA>                              250.0
# 25  2025-07-01  12345678901234            2             0              1                   0             55.0            150.0        0.0        0.0             MIEU            C       99999999.0        0.0       ES              S                   310.0  2_C_12345678901234_1_0_0                    <NA>            <NA>                              310.0
# 26  2025-07-01  12345678901234            3             0              1                   0             60.0            200.0        0.0        0.0             MIEU            C       99999999.0        0.0       ES              S                   200.0  3_C_12345678901234_1_0_0                    <NA>            <NA>                              200.0
# 27  2025-07-01  12345678901234            1             0              1                   0             50.0            250.0        0.0        0.0             MIEU            V       99999999.0        0.0       ES              S                   730.0  1_V_12345678901234_1_0_0                    <NA>            <NA>                              390.0
# 28  2025-07-01  12345678901234            2             0              1                   0             55.0            300.0        0.0        0.0             MIEU            V       99999999.0        0.0       ES              S                   850.0  2_V_12345678901234_1_0_0                    <NA>            <NA>                              460.0
# 29  2025-07-01  12345678901234            3             0              1                   0             60.0            350.0        0.0        0.0             MIEU            V       99999999.0        0.0       ES              S                   970.0  3_V_12345678901234_1_0_0                    <NA>            <NA>                              530.0
