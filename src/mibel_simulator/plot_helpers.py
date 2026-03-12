import matplotlib.pyplot as plt
import numpy as np

import mibel_simulator.columns as cols


def plot_sell_profiles(
    sell_profiles,
    ax=None,
    figsize=(12, 6),
    title="Homothetic Sell Profiles",
    ylabel="MW",
    xlabel="Hour of the day",
    colorbar=True,
    cmap="viridis",
    legend=True,
    linewidth=2,
    alpha=1,
    **plot_kwargs,
):
    """
    Plot sell profiles with detailed customization.
    If the index is of the form 'scale_{value}', use a colormap and colorbar.
    All other kwargs are passed to plt.plot.
    """
    created_fig = False
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)
        created_fig = True

    transposed = sell_profiles.T

    # Detect if index is scale_{value}
    if all(str(idx).startswith("scale_") for idx in sell_profiles.index):
        color_values = np.array(
            [float(str(idx).split("_")[1]) for idx in sell_profiles.index]
        )
        norm = plt.Normalize(color_values.min(), color_values.max())
        sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
        colors = sm.to_rgba(color_values)
        if colorbar and created_fig:
            cbar = plt.colorbar(sm, ax=ax, orientation="vertical", label="Scale")
    else:
        colors = None

    transposed.plot(
        ax=ax,
        linewidth=linewidth,
        alpha=alpha,
        color=colors,
        legend=False,
        **plot_kwargs,
    )

    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.set_xlabel(xlabel)
    ax.grid(True, which="both", linestyle="--", alpha=0.5)
    if legend:
        ax.legend(title="Profile", bbox_to_anchor=(1.05, 1), loc="upper left")
    if created_fig:
        plt.tight_layout()
        plt.show()


def plot_residual_demand_curves(
    rdc_dfs,
    label=None,
    axs=None,
    figsize=(22, 15),
    suptitle="Residual Demand Curves",
    colorbar=True,
    cmap="viridis",
    linestyle="--",
    linewidth=2,
    alpha=0.9,
    **plot_kwargs,
):
    """
    Plot one or several residual demand curves (with ResidualDemandCurvesSchema structure).
    - rdc_dfs: a DataFrame or a list of DataFrames.
    - labels: list of labels for each DataFrame (optional).
    - axs: optional array of axes to plot on.
    - If only one DataFrame and index is scale_, use a colorbar.
    - plot_kwargs: passed to ax.plot.
    """
    import pandas as pd

    # Accept single df or list
    if isinstance(rdc_dfs, (pd.DataFrame, np.ndarray)):
        rdc_dfs = [rdc_dfs]

    created_fig = False
    if axs is None:
        fig, axs = plt.subplots(4, 6, figsize=figsize, sharex=True, sharey=True)
        axs_flat = axs.flatten()
        created_fig = True
    else:
        axs_flat = axs.flatten()

    # If only one df and index is scale_, use colorbar
    use_colorbar = (
        len(rdc_dfs) == 1
        and hasattr(rdc_dfs[0], "index")
        and all(str(idx).startswith("scale_") for idx in rdc_dfs[0].index)
        and colorbar
        and created_fig
    )

    if use_colorbar:
        color_values = np.array(
            [float(str(idx).split("_")[1]) for idx in rdc_dfs[0].index]
        )
        norm = plt.Normalize(color_values.min(), color_values.max())
        sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
        colors = sm.to_rgba(color_values)
    else:
        colors = None

    for idx, ax in enumerate(axs_flat):
        hour = idx + 1
        for i, df in enumerate(rdc_dfs):
            if use_colorbar:
                for j, (ix, row) in enumerate(df.iterrows()):
                    ax.scatter(
                        row[f"energy_{hour}"],
                        row[f"price_{hour}"],
                        color=colors[j],
                        alpha=alpha,
                        **plot_kwargs,
                    )
            df_aux = df.sort_values(by=f"energy_{hour}")
            ax.plot(
                df_aux[f"energy_{hour}"],
                df_aux[f"price_{hour}"],
                linewidth=linewidth,
                alpha=alpha,
                label=label if idx == 0 else None,
                linestyle=linestyle,
                **plot_kwargs,
            )
        if idx == 0:
            ax.legend()
        ax.set_title(f"Hour {hour}")
        ax.set_xlabel("Energy (MWh)")
        ax.set_ylabel("Price (€/MWh)")

    if use_colorbar:
        fig.colorbar(sm, ax=axs_flat, orientation="vertical", label="Scale")

    if created_fig:
        plt.suptitle(suptitle)
        plt.tight_layout()
        plt.show()


def plot_clearing_prices(
    clearing_prices,
    ax=None,
    hue=cols.CAT_BIDDING_ZONE,
    title="Clearing Prices by Period",
    ylabel="Cleared Price (€/MWh)",
    xlabel="Hour of the day",
    legend=True,
    marker="o",
    linewidth=2,
    alpha=0.9,
    **plot_kwargs,
):
    """
    Plot clearing prices DataFrame following ClearingPricesSchema.
    By default, splits by country (CAT_BIDDING_ZONE).
    """
    created_fig = False
    if ax is None:
        fig, ax = plt.subplots(figsize=plot_kwargs.pop("figsize", (10, 5)))
        created_fig = True

    if hue and hue in clearing_prices.columns:
        for key, grp in clearing_prices.groupby(hue):
            ax.plot(
                grp[cols.INT_PERIOD],
                grp[cols.FLOAT_CLEARED_PRICE],
                label=str(key),
                marker=marker,
                linewidth=linewidth,
                alpha=alpha,
                **plot_kwargs,
            )
    else:
        ax.plot(
            clearing_prices["period"],
            clearing_prices["cleared_price"],
            marker=marker,
            linewidth=linewidth,
            alpha=alpha,
            **plot_kwargs,
        )

    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.set_xlabel(xlabel)
    ax.grid(True, which="both", linestyle="--", alpha=0.5)
    if legend:
        ax.legend(title=hue)
    if created_fig:
        plt.tight_layout()
        plt.show()


def plot_spain_portugal_transmissions(
    transmissions_df,
    ax=None,
    title="Spain-Portugal Hourly Transmission",
    ylabel="Transmission ES→PT (MW)",
    xlabel="Hour of the day",
    legend=False,
    marker="o",
    linewidth=2,
    alpha=0.9,
    **plot_kwargs,
):
    """
    Plot a DataFrame following SpainPortugaLTransmissionsSchema.
    """
    created_fig = False
    if ax is None:
        fig, ax = plt.subplots(figsize=plot_kwargs.pop("figsize", (10, 5)))
        created_fig = True

    ax.plot(
        transmissions_df.index,
        transmissions_df["Transmision_ES_PT"],
        marker=marker,
        linewidth=linewidth,
        alpha=alpha,
        label="ES→PT",
        **plot_kwargs,
    )

    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.set_xlabel(xlabel)
    ax.grid(True, which="both", linestyle="--", alpha=0.5)
    if legend:
        ax.legend()
    if created_fig:
        plt.tight_layout()
        plt.show()
