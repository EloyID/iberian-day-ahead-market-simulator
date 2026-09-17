"""
Tests for iberian_day_ahead_market_simulator.parse_omie_files module.

Focuses on capacidad_inter_files_to_parquet()'s hourly-vs-QH consistency check.
"""

import pandas as pd
import pytest

import iberian_day_ahead_market_simulator.columns as cols
from iberian_day_ahead_market_simulator.parse_omie_files import (
    capacidad_inter_files_to_parquet,
)

CAPACIDAD_INTER_HEADER = (
    "OMIE - Mercado de electricidad;Fecha Emision :12/11/2025 - 14:05;;13/11/2025;"
    "Capacidad y ocupacion de impor/expor casacion (MW B.C.);;;;\n"
    "\n"
    "Periodo;Fecha;Frontera;Capacidad importación;Ocupación Importación;"
    "Capacidad libre de importación;Capacidad exportación;Ocupación exportación;"
    "Capacidad libre de exportación;\n"
)
CAPACIDAD_INTER_FOOTER = ";;;;;;;;;\n"


def _write_capacidad_inter_csv(folder, filename: str, period_rows: list[str]) -> None:
    content = (
        CAPACIDAD_INTER_HEADER + "\n".join(period_rows) + "\n" + CAPACIDAD_INTER_FOOTER
    )
    (folder / filename).write_text(content, encoding="latin1")


def _row(period: str, date: str = "13/11/2025", frontier: str = "2") -> str:
    # Periodo;Fecha;Frontera;Cap.imp;Ocup.imp;Cap.libre.imp;Cap.exp;Ocup.exp;Cap.libre.exp
    return f"{period};{date};{frontier};-100,0;0;-100,0;100,0;0;100,0;"


@pytest.fixture
def qh_capacidad_inter_folder(tmp_path):
    """A folder with a single quarter-hourly capacidad_inter file covering a full
    7 hours (periods formatted as 'HxQx', transforming to periods 1..28), i.e.
    comfortably more than the 25-period hourly-market boundary."""
    folder = tmp_path / "qh_capacidad_inter"
    folder.mkdir()
    rows = [_row(f"H{h}Q{q}") for h in range(1, 8) for q in [1, 2, 3, 4]]
    _write_capacidad_inter_csv(folder, "capacidad_inter_pbc_20251113.1", rows)
    return folder


@pytest.fixture
def sparse_qh_capacidad_inter_folder(tmp_path):
    """A folder with a QH-formatted capacidad_inter file that only covers 2 hours
    (periods formatted as 'HxQx', transforming to periods 1..8) - too few periods
    to be distinguished from genuinely hourly data by the max-period check."""
    folder = tmp_path / "sparse_qh_capacidad_inter"
    folder.mkdir()
    rows = [_row(f"H{h}Q{q}") for h in [1, 2] for q in [1, 2, 3, 4]]
    _write_capacidad_inter_csv(folder, "capacidad_inter_pbc_20251113.1", rows)
    return folder


@pytest.fixture
def hourly_capacidad_inter_folder(tmp_path):
    """A folder with a single genuinely hourly capacidad_inter file (periods
    formatted as plain integers 1..3)."""
    folder = tmp_path / "hourly_capacidad_inter"
    folder.mkdir()
    rows = [_row(str(period)) for period in [1, 2, 3]]
    _write_capacidad_inter_csv(folder, "capacidad_inter_pbc_20250810.1", rows)
    return folder


class TestCapacidadInterFilesToParquet:
    """Test suite for capacidad_inter_files_to_parquet's is-this-really-QH check.

    Regression tests for the fix: the old check compared the max INT_PERIOD
    against a fixed threshold of 100, which is always true for both hourly (<=25)
    and QH (<=96) data, so it always concluded the data was "hourly" and raised
    whenever qh_output=True - even for genuinely quarter-hourly input files with
    enough periods in a day to tell them apart from hourly data.
    """

    def test_qh_file_with_qh_output_succeeds(self, qh_capacidad_inter_folder, tmp_path):
        """Genuinely QH-formatted data (>25 periods that day) with qh_output=True
        must not raise. Before the fix, this always raised regardless of the
        actual period count."""
        output_path = tmp_path / "out.parquet"

        result = capacidad_inter_files_to_parquet(
            str(qh_capacidad_inter_folder), str(output_path), qh_output=True
        )

        # Testing only 29 but in reality would have 96 periods
        assert sorted(result[cols.INT_PERIOD].tolist()) == list(range(1, 29))
        assert output_path.exists()

    def test_sparse_qh_file_with_qh_output_raises(
        self, sparse_qh_capacidad_inter_folder, tmp_path
    ):
        """QH-formatted data that only reaches period 8 that day is still below
        the 25-period hourly-market boundary, so it is (correctly) flagged as
        indistinguishable from hourly data and rejected."""
        output_path = tmp_path / "out.parquet"

        with pytest.raises(ValueError, match="hourly format"):
            capacidad_inter_files_to_parquet(
                str(sparse_qh_capacidad_inter_folder),
                str(output_path),
                qh_output=True,
            )

    def test_hourly_file_with_hourly_output_succeeds(
        self, hourly_capacidad_inter_folder, tmp_path
    ):
        """Genuinely hourly-formatted data with qh_output=False (the default)
        succeeds, as before."""
        output_path = tmp_path / "out.parquet"

        result = capacidad_inter_files_to_parquet(
            str(hourly_capacidad_inter_folder),
            str(output_path),
            qh_output=False,
        )

        assert sorted(result[cols.INT_PERIOD].tolist()) == [1, 2, 3]
        assert output_path.exists()
