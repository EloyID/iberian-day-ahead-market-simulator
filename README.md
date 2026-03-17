# mibel-simulator

A Python simulator for the **MIBEL** (Mercado Ibérico de Electricidad) Iberian day-ahead electricity market clearing process.

The package replicates the OMIE/MIBEL market-clearing algorithm, including:
- Simple and complex bid orders
- Paradoxical order inclusion and removal
- Spain–Portugal interconnection capacity constraints
- France exchange integration

## Requirements

### Solver support

The simulator uses Pyomo `SolverFactory`, so you can choose the solver with `solver_factory_type`.

- Recommended/default: **Gurobi** (`solver_factory_type="gurobi"`)
- Also possible: **CBC**, **GLPK**, or any solver plugin available in your Pyomo environment

> Note: model performance and feasibility behavior can vary by solver. Gurobi is the most tested option in this project.

If you use Gurobi, install `gurobipy` and configure your licence:
```bash
pip install gurobipy
```

If you use CBC/GLPK, install the corresponding solver binaries in your system and use the appropriate `solver_factory_type`.

## Installation

```bash
pip install mibel-simulator
```

Or install from source:
```bash
git clone https://github.com/EloyID/mibel-simulator.git
cd mibel-simulator
pip install -e .
```

## Quick Start

```python
from mibel_simulator.clearing_process import run_mibel_simulator
from mibel_simulator.plot_helpers import (
    plot_clearing_prices,
    plot_spain_portugal_transmissions,
)

# These files are available in OMIE
det_date = "path/to/det_file.1"
cab_date = "path/to/cab_file.1"
capacidad_inter_date = "path/to/capacidad_inter_file.1"

# Data available in ENTSO-E Transparency Platform
# The df must have date_sesion, int_periodo and 
# float_price_fr columns
price_france_date = "path/to/price_france_file.parquet"

results = run_mibel_simulator(
    det_date=det_date,
    cab_date=cab_date,
    capacidad_inter_date=capacidad_inter_date,
    price_france_date=price_france_date,
)

# Now you can analyze the results, for example:
plot_clearing_prices(results["clearing_prices"])
plot_spain_portugal_transmissions(
    results["spain_portugal_transmissions"]
)
```

## Project Structure

```
src/mibel_simulator/
├── clearing_process.py        # Main iterative clearing loop
├── make_model.py              # Pyomo MILP model builder
├── run_model.py               # Pyomo solver wrapper (configurable solver)
├── data_preprocessor.py       # Input data processing
├── parse_omie_files.py        # OMIE flat-file parsers
├── paradoxal_orders_tools.py  # Paradoxical order utilities
├── residual_demand_curve.py   # RDC computation & interpolation
├── schemas/                   # Pandera data-validation schemas
└── data/                      # Bundled reference data
```

## Development

```bash
pip install -e ".[dev]"
pytest
```

## Licence

[MIT](LICENSE)
