"""
Tests for iberian_day_ahead_market_simulator.plot_helpers module.

Regression tests for two fixed bugs in plot_residual_demand_curves:
- The subplot grid was hardcoded to 4x6=24 panels, silently dropping periods
  25-100 for QH (96/100-period) residual demand curves with no warning.
- The x-axis label read "Power (MWh)" (an energy unit) after the plotted
  quantity was renamed from energy to power.
"""

import matplotlib

matplotlib.use("Agg")

import pandas as pd

from iberian_day_ahead_market_simulator.plot_helpers import (
    plot_residual_demand_curves,
)


def _rdc_dataframe(market_periods_count: int) -> pd.DataFrame:
    data = {}
    for period in range(1, market_periods_count + 1):
        data[f"power_{period}"] = [0.0, 100.0]
        data[f"price_{period}"] = [10.0, 20.0]
    return pd.DataFrame(data)


class TestPlotResidualDemandCurvesGridSize:
    def test_hourly_24_periods_grid_covers_all_periods(self):
        fig_axes = plot_residual_demand_curves(
            _rdc_dataframe(24), axs=None, colorbar=False
        )
        n_axes = len(matplotlib.pyplot.gcf().axes)
        assert n_axes >= 24
        matplotlib.pyplot.close("all")

    def test_qh_96_periods_grid_covers_all_periods(self):
        """Regression test: before the fix, only the first 24 (of 96) periods
        were plotted, with the rest silently dropped."""
        plot_residual_demand_curves(_rdc_dataframe(96), axs=None, colorbar=False)
        titled_periods = {
            ax.get_title() for ax in matplotlib.pyplot.gcf().axes if ax.get_title()
        }
        assert "Period 96" in titled_periods
        matplotlib.pyplot.close("all")

    def test_qh_100_periods_grid_covers_all_periods(self):
        plot_residual_demand_curves(_rdc_dataframe(100), axs=None, colorbar=False)
        titled_periods = {
            ax.get_title() for ax in matplotlib.pyplot.gcf().axes if ax.get_title()
        }
        assert "Period 100" in titled_periods
        matplotlib.pyplot.close("all")


class TestPlotResidualDemandCurvesAxisLabels:
    def test_xlabel_uses_power_unit_not_energy(self):
        """Regression test: the x-axis used to be mislabeled "Power (MWh)"
        (an energy unit) after the energy->power rename."""
        plot_residual_demand_curves(_rdc_dataframe(24), axs=None, colorbar=False)
        xlabels = {ax.get_xlabel() for ax in matplotlib.pyplot.gcf().axes}
        assert "Power (MW)" in xlabels
        assert "Power (MWh)" not in xlabels
        matplotlib.pyplot.close("all")
