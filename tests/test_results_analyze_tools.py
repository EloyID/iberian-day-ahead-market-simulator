"""
Tests for iberian_day_ahead_market_simulator.results_analyze_tools module.

Regression test for the fixed bug: summary_det_cab_and_curva_pbc_uof's backward
-compat rename for legacy curva_pbc_uof data was a no-op
(rename(columns={"qua_potencia": "qua_potencia"})) instead of the intended
{"qua_energia": "qua_potencia"}, so a legacy DataFrame using the old
"qua_energia" column name raised a bare KeyError instead of being transparently
upgraded.
"""

import pandas as pd
import pytest

import iberian_day_ahead_market_simulator.columns as cols
from iberian_day_ahead_market_simulator.const import FRANCE_ID_UNIDAD
from iberian_day_ahead_market_simulator.results_analyze_tools import (
    summary_det_cab_and_curva_pbc_uof,
)


def _det_cab() -> pd.DataFrame:
    return pd.DataFrame(
        {
            cols.FLOAT_CLEARED_POWER: [100.0],
            cols.CAT_BUY_SELL: ["V"],
            cols.CAT_ORDER_TYPE: ["S"],
            cols.ID_UNIDAD: ["UNIT1"],
        }
    )


def _curva_pbc_uof(power_column_name: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            power_column_name: [100.0],
            "cod_simple_block_orders": ["S"],
            "cod_pais": ["ES"],
            "cod_ofertada_casada": ["C"],
            "cod_tipo_oferta": ["V"],
        }
    )


class TestSummaryDetCabAndCurvaPbcUof:
    def test_current_qua_potencia_column_works(self):
        result = summary_det_cab_and_curva_pbc_uof(
            _det_cab(), _curva_pbc_uof("qua_potencia")
        )
        assert result["reference_cleared_power_simple"] == 100.0

    def test_legacy_qua_energia_column_is_upgraded(self):
        """A curva_pbc_uof DataFrame still using the pre-rename "qua_energia"
        column name must be transparently upgraded, not raise a KeyError."""
        result = summary_det_cab_and_curva_pbc_uof(
            _det_cab(), _curva_pbc_uof("qua_energia")
        )
        assert result["reference_cleared_power_simple"] == 100.0

    def test_missing_both_column_names_raises_key_error(self):
        """Sanity check: without either column name, the function should still
        fail (loudly), confirming the rename isn't silently swallowing errors."""
        curva_pbc_uof = _curva_pbc_uof("qua_potencia").rename(
            columns={"qua_potencia": "some_other_name"}
        )
        with pytest.raises(KeyError):
            summary_det_cab_and_curva_pbc_uof(_det_cab(), curva_pbc_uof)
