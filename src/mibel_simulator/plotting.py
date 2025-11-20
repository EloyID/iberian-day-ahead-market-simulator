from matplotlib import pyplot as plt

from mibel_simulator.const import (
    CAT_BUY_SELL,
    FLOAT_BID_POWER_CUMSUM,
    FLOAT_BID_PRICE,
    FLOAT_CLEARED_POWER,
)


def plot_period_curves(
    det_cab_period_results,
    clearing_price,
    potencia_cumsum_column=FLOAT_BID_POWER_CUMSUM,
    potencia_casada_cumsum_column="Potencia_casada cumsum",
    ax=None,
):
    if ax is None:
        fig, ax = plt.subplots(figsize=(10, 6))

    det_cab_period_results_C = det_cab_period_results.query(
        f'{CAT_BUY_SELL} == "C"'
    ).sort_values(potencia_cumsum_column)
    det_cab_period_results_V = det_cab_period_results.query(
        f'{CAT_BUY_SELL} == "V"'
    ).sort_values(potencia_cumsum_column)
    det_cab_period_results_C_casada = det_cab_period_results_C.query(
        f"{FLOAT_CLEARED_POWER} > 0"
    ).sort_values(potencia_casada_cumsum_column)
    det_cab_period_results_V_casada = det_cab_period_results_V.query(
        f"{FLOAT_CLEARED_POWER} > 0"
    ).sort_values(potencia_casada_cumsum_column)

    cleared_energy = det_cab_period_results_C_casada[
        potencia_casada_cumsum_column
    ].max()

    line_properties = {
        "xlabel": "Cumulative Power (MW)",
        "ylabel": "Price (€/MWh)",
        "drawstyle": "steps",
        "grid": True,
        "ax": ax,
        "linewidth": 2,
    }

    det_cab_period_results_V.query(f'Descripcion != "INTERCAMBIO ES-PT"').plot(
        x=potencia_cumsum_column,
        y=FLOAT_BID_PRICE,
        label="Demand curve",
        **line_properties,
    )

    det_cab_period_results_C.query(f'Descripcion != "INTERCAMBIO ES-PT"').plot(
        x=potencia_cumsum_column,
        y=FLOAT_BID_PRICE,
        label="Supply curve",
        **line_properties,
    )

    det_cab_period_results_V_casada.plot(
        x=potencia_casada_cumsum_column,
        y=FLOAT_BID_PRICE,
        label="Cleared demand curve",
        **line_properties,
    )

    det_cab_period_results_C_casada.plot(
        x=potencia_casada_cumsum_column,
        y=FLOAT_BID_PRICE,
        label="Cleared supply curve",
        **line_properties,
    )
    ax.axhline(
        y=clearing_price,
        color="r",
        linestyle="--",
        label=f"Clearing Price: {clearing_price} €/MWh",
    )
    ax.axvline(
        x=cleared_energy,
        color="g",
        linestyle="--",
        label=f"Cleared Quantity: {cleared_energy} MW",
    )
    ax.legend()

    return ax
