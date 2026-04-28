# Data Book — iberian-day-ahead-market-simulator

This document describes the main datasets used and produced by `iberian-day-ahead-market-simulator`, including field meanings, data types, allowed values, and key validation rules.

## 1) Scope and conventions

- **Business scope:** MIBEL day-ahead market clearing (Spain/Portugal), with explicit handling of France exchange modeling.
- **Time resolution:** hourly by default (`int_period` in `1..24`, occasionally `25` in DST/raw OMIE files).
- **Units:**
  - Price: **€/MWh**
  - Power/capacity/energy terms in model inputs: **MWh**
- **Naming convention:** typed prefixes in column names:
  - `int_` integer
  - `float_` numeric continuous
  - `cat_` categorical
  - `id_` identifier
  - `bool_` boolean
  - `date_` date 
  - `datetime_` timestamp
- **Paradoxical orders:** orders that can be subject to paradoxical rejection/acceptance by the optimization problem. These include block orders and SCOs that include a fixed MIC term (`float_mic > 0`).

Canonical names are defined in [src/mibel_simulator/columns.py](../src/mibel_simulator/columns.py).

---

## 2) Source datasets and parser outputs

A thorough description of original OMIE files can be found [here](https://www.omeldiversificacion.es/sites/default/files/2025-09/formato_ficheros_inf_pub_137.pdf)

### 2.1 CAB (header of bids)

**Original OMIE file:** `CAB_YYYYMMDD.1` from *Cabecera de las ofertas al mercado diario* [link](https://www.omie.es/es/file-access-list?parents=/Mercado%20Diario/4.%20Ofertas&dir=Cabecera%20de%20las%20ofertas%20al%20mercado%20diario&realdir=cab).

**OMIE file parser:** `parse_cab_file()` in [src/mibel_simulator/parse_omie_files.py](../src/mibel_simulator/parse_omie_files.py)

| Column            | Type               | Meaning                                     | Corresponding OMIE field name |
| ----------------- | ------------------ | ------------------------------------------- | ----------------------------- |
| `date_sesion`     | datetime           | Trading session date inferred from filename |                               |
| `id_order`        | str                | OMIE offer/order identifier                 | CodOferta                     |
| `id_unidad`       | str                | Unit identifier                             | Código                        |
| `cat_buy_sell`    | category (`C`/`V`) | Buy/Sell side                               | CV                            |
| `float_mic`       | float              | Fixed MIC term (used for SCO)               | Fijoeuro                      |
| `float_max_power` | float              | Maximum power of the unit                   | MaxPot                        |

**Schema reference:** [src/mibel_simulator/schemas/cab.py](../src/mibel_simulator/schemas/cab.py)

---

### 2.2 DET (detail of bids)

**Original OMIE file:** `DET_YYYYMMDD.1` from *Detalle de las ofertas al mercado diario* [link](https://www.omie.es/es/file-access-list?parents=/Mercado%20Diario/4.%20Ofertas&dir=Detalle%20de%20las%20ofertas%20al%20mercado%20diario&realdir=det).

**OMIE file parser:** `parse_det_file()` in [src/mibel_simulator/parse_omie_files.py](../src/mibel_simulator/parse_omie_files.py)

| Column               | Type     | Meaning                                     | Corresponding OMIE field name |
| -------------------- | -------- | ------------------------------------------- | ----------------------------- |
| `date_sesion`        | datetime | Trading session date inferred from filename |                               |
| `id_order`           | str      | OMIE offer/order identifier                 | CodOferta                     |
| `int_period`         | int      | Session period index                        | Periodo                       |
| `int_num_block`      | int      | Block number (`0` if not block)             | NumBlock                      |
| `int_num_suborder`   | int      | Suborder number (SCO-related)               | NumTramo                      |
| `int_num_excl_group` | int      | Exclusive group id (`0` if none)            | NumGrupoExcl                  |
| `float_bid_price`    | float    | Offered price                               | PrecEuro                      |
| `float_bid_power`    | float    | Offered quantity/power                      | Cantidad                      |
| `float_mav`          | float    | Minimum acceptance volume                   | MAV                           |
| `float_mar`          | float    | Minimum acceptance ratio                    | MAR                           |

**Special handling:** if some bids include period `25` appears, but the quantity of bids is very small, it will be dropped. 

**Schema reference:** [src/mibel_simulator/schemas/det.py](../src/mibel_simulator/schemas/det.py)

---

### 2.3 Interconnection capacities (`capacidad_inter_pbc`)

**Original OMIE file:** `capacidad_inter_pbc_YYYYMMDD.1` from *Capacidad y ocupación de las interconexiones tras la casación del mercado diario* [link](https://www.omie.es/es/file-access-list?parents=/Mercado%20Diario/6.%20Capacidades&dir=Capacidad%20y%20ocupaci%C3%B3n%20de%20las%20interconexiones%20tras%20la%20casaci%C3%B3n%20del%20mercado%20diario&realdir=capacidad_inter_pbc).

**OMIE file parser:** `parse_capacidad_inter_file()` in [src/mibel_simulator/parse_omie_files.py](../src/mibel_simulator/parse_omie_files.py)

Supports:
- optional `bidding_zone` filter (`"ES"`, `"PT"`, `"FR"`, `"MA"`)
- optional `only_capacity_columns`

#### Base capacity output (`only_capacity_columns=True`)

| Column                  | Type                | Meaning                              | Corresponding OMIE field name |
| ----------------------- | ------------------- | ------------------------------------ | ----------------------------- |
| `date_sesion`           | datetime            | Session date                         |                               |
| `cat_frontier`          | category (int code) | Frontier code (mapped in `const.py`) | Frontera                      |
| `int_period`            | int                 | Session period                       | Periodo                       |
| `float_import_capacity` | float               | Import limit                         | Capacidad importación         |
| `float_export_capacity` | float               | Export limit                         | Capacidad exportación         |

Frontier coding is detailed in section *Enumerations and code lists* below. 

#### Extended capacity output (`only_capacity_columns=False`)

Adds:
- `float_import_capacity_occupation`: corresponds to *Ocupación Importación* in OMIE
- `float_import_capacity_free`: corresponds to *Capacidad libre de importación* in OMIE
- `float_export_capacity_occupation`: corresponds to *Ocupación Exportación* in OMIE
- `float_export_capacity_free`: corresponds to *Capacidad libre de exportación* in OMIE

**Schema reference (base):** [src/mibel_simulator/schemas/capacidad_inter_pt.py](../src/mibel_simulator/schemas/capacidad_inter_pt.py)

---

### 2.4 France day-ahead prices (ENTSO-E export)

**Original ENTSO-E file:** CSV file from [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/) > FR > Market > Energy Prices > Export

**Parser:** `parse_price_france_from_entsoe_file()` in [src/mibel_simulator/parse_omie_files.py](../src/mibel_simulator/parse_omie_files.py)

| Column           | Type     | Meaning                                 |
| ---------------- | -------- | --------------------------------------- |
| `date_sesion`    | datetime | Session day                             |
| `int_period`     | int      | Period index (hourly or quarter-hourly) |
| `float_price_fr` | float    | France day-ahead price                  |
**Notes:**
- Filters `Sequence == "Without Sequence"`.
- If quarter-hourly is detected and hourly mode is requested, values are aggregated by hour.

---

### 2.5 OMIE marginal prices (`marginalpdbc`)

**Original OMIE file:** `marginalpdbc_YYYYMMDD.1` from *Precios horarios del mercado diario en España* [link](https://www.omie.es/es/file-access-list?parents=/Mercado%20Diario/1.%20Precios&dir=Precios%20horarios%20del%20mercado%20diario%20en%20Espa%C3%B1a&realdir=marginalpdbc).

**OMIE file parser:** `parse_marginalpdbc_file()` in [src/mibel_simulator/parse_omie_files.py](../src/mibel_simulator/parse_omie_files.py)

Input has PT and ES values per period; parser returns long format:

| Column                | Type                 | Meaning                | Corresponding OMIE field name |
| --------------------- | -------------------- | ---------------------- | ----------------------------- |
| `int_period`          | int                  | Session period         | Periodo                       |
| `cat_bidding_zone`    | category (`ES`/`PT`) | Zone                   |                               |
| `float_cleared_price` | float                | Observed cleared price | MarginalPT and MarginalES     |

---

### 2.6 Participants bidding zones

| Column             | Type     | Meaning           |
| ------------------ | -------- | ----------------- |
| `id_unidad`        | str      | Unit id           |
| `cat_bidding_zone` | category | Unit bidding zone |

**Schema reference:** [src/mibel_simulator/schemas/participants_bidding_zones.py](../src/mibel_simulator/schemas/participants_bidding_zones.py)

---

## 3) Core simulation dataset: `det_cab`

Built by `get_det_cab_for_simulation()` in [src/mibel_simulator/data_preprocessor.py](../src/mibel_simulator/data_preprocessor.py).

This table merges DET + CAB + zone mapping (and optionally synthetic France bids), then enriches with derived identifiers and cumulative metrics.

### 3.1 Key columns

**From CAB/DET:**
- `date_sesion`, `id_order`, `id_unidad`, `cat_buy_sell`, `float_mic`, `float_max_power`
- `int_period`, `int_num_block`, `int_num_suborder`, `int_num_excl_group`
- `float_bid_price`, `float_bid_power`, `float_mav`, `float_mar`

**Derived:**

| Column                              | Type                 | Meaning                                           |
| ----------------------------------- | -------------------- | ------------------------------------------------- |
| `cat_order_type`                    | category             | Order type (simple/block/SCO/exclusive/synthetic) |
| `id_individual_bid`                 | str                  | Unique bid row id                                 |
| `id_block_order`                    | str                  | Block-order identifier                            |
| `id_sco`                            | str                  | SCO identifier                                    |
| `float_bid_power_cumsum`            | float                | Global merit-order cumulative power               |
| `float_bid_power_cumsum_by_country` | float                | Zone-wise cumulative power                        |
| `cat_bidding_zone`                  | category (`ES`/`PT`) | Unit bidding zone                                 |

**Schema reference:** [src/mibel_simulator/schemas/det_cab.py](../src/mibel_simulator/schemas/det_cab.py)

### 3.2 Important modeling rules validated

From schema checks in [src/mibel_simulator/schemas/det.py](../src/mibel_simulator/schemas/det.py) and [src/mibel_simulator/schemas/det_cab.py](../src/mibel_simulator/schemas/det_cab.py):

- Exclusive group implies block order.
- `float_mar > 0` only for block offers.
- `float_mav > 0` only for SCO-compatible rows.
- Buy-side (`cat_buy_sell == "C"`) cannot be SCO/block/exclusive in current assumptions.
- Same block (`id_order`, `int_num_block`) must keep same price and MAR across periods.
- Max power consistency checks for both non-exclusive and exclusive structures.

---

## 4) Main outputs from `run_mibel_simulator`

Returned keys are assembled in [src/mibel_simulator/clearing_process.py](../src/mibel_simulator/clearing_process.py).

### 4.1 `cleared_det_cab`

`det_cab` plus:
- `float_cleared_power`

Schema: [src/mibel_simulator/schemas/cleared_det_cab.py](../src/mibel_simulator/schemas/cleared_det_cab.py)

### 4.2 `clearing_prices`

| Column                | Type     | Meaning                         |
| --------------------- | -------- | ------------------------------- |
| `int_period`          | int      | Session period                  |
| `cat_bidding_zone`    | category | Zone (`ES`/`PT`)                |
| `float_cleared_price` | float    | Simulated market clearing price |

Schema: [src/mibel_simulator/schemas/clearing_prices.py](../src/mibel_simulator/schemas/clearing_prices.py)

### 4.3 `spain_portugal_transmissions`

| Column / Index      | Type          | Meaning                                         |
| ------------------- | ------------- | ----------------------------------------------- |
| index               | int (`1..24`) | Period                                          |
| `Transmision_ES_PT` | float         | Net ES→PT transmission convention used by model |

Schema: [src/mibel_simulator/schemas/spain_portugal_transmissions.py](../src/mibel_simulator/schemas/spain_portugal_transmissions.py)

### 4.4 `iterations_df`

Iteration diagnostics and optimization outcomes:
- paradoxical order ids/lists,
- objective values,
- expected-income feasibility flags,
- solver artifacts,
- snapshots of cleared energy / prices / transmission by iteration.

Schema: [src/mibel_simulator/schemas/iterations.py](../src/mibel_simulator/schemas/iterations.py)

---

## 5) Enumerations and code lists

Defined in [src/mibel_simulator/const.py](../src/mibel_simulator/const.py):

- `cat_buy_sell`: `C` (buy), `V` (sell)
- bidding zones: `ES`, `PT` (plus `FR`, `MI` in specific contexts)
- frontiers (numeric code mapping):
  - `2 -> PT`
  - `3 -> FR`
  - `4 -> AD`
  - `5 -> MA`
- order types include:
  - `S` simple
  - `C01` simple block
  - `C02` SCO
  - `C04` exclusive group
  - synthetic interconnection tags (`Imp/Exp FR/PT/ES`)

---

## 6) Lineage (high-level)

1. Parse raw OMIE/ENTSO-E files (`parse_omie_files.py`)
2. Build `det_cab` (`data_preprocessor.py`)
3. Run iterative clearing (`clearing_process.py` + model build/solve)
4. Extract outputs and validate (`schemas/*`)

---

## 7) Quality checks checklist (recommended operational runbook)

Before publishing a run:

- Validate all input schemas (CAB, DET, capacities).
- Confirm single session day for day-level runs.
- Check missing unit-zone mappings (`id_unidad`).
- Confirm no duplicate `id_individual_bid`.
- Track paradoxical-order counts and objective evolution in `iterations_df`.

---

## 8) Known caveats

- Some OMIE files may include 25 or 23 periods due to hour changes. Current implementation does not support these cases.
- France prices may be quarter-hourly; conversion behavior depends on parser mode.
- Sign conventions in capacities/transmissions should be interpreted consistently with model constraints.

---

## 9) Maintenance

When adding/changing fields:

1. Update constants in [src/mibel_simulator/columns.py](../src/mibel_simulator/columns.py)
2. Update parser renaming/typing in [src/mibel_simulator/parse_omie_files.py](../src/mibel_simulator/parse_omie_files.py)
3. Update schema checks in [src/mibel_simulator/schemas/](../src/mibel_simulator/schemas/)
4. Update this document

This file is intended to be the canonical data dictionary for users and contributors.
