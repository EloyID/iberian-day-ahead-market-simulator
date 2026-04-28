# Data folder for Day-ahead market inputs

This folder contains input data used to build and validate the day-ahead market simulation, including OMIE bid files, interconnection capacities, and clearing prices for Spain, Portugal, and France.

Only data from the day 21 June 2025 is included in this folder, as it is used in application_examples.ipynb. You can download data for other days from the sources listed below. For capacidad_inter_pbc and marginalpdbc you can use the provided scripts in [download_omie_data.ipynb](../download_omie_data.ipynb)


## Folder structure

- CAB: Cabecera de las ofertas (Header of bids for Day-ahead Market). 
- DET: Detalle de las ofertas (Day-ahead market bid details).
- capacidad_inter_pbc: Capacidad y ocupación de las interconexiones tras la casación del mercado diario (Capacity and occupation of the interconnectors after Day-ahead matching process).
- marginalpdbc: Hourly clearing prices for the Spanish and Portuguese day-ahead markets.
- ENTSOE_FR_Clearing_prices.csv: Clearing prices for France. 
- price_france.parquet: Cleaned parquet version of the above CSV file for easier loading.
- LISTA_UNIDADES.XLS: List of generation units in the Iberian Peninsula.
- marginalpdbc.parquet: Cleaned parquet version of the marginalpdbc files.

## Data sources

General OMIE access:
- https://www.omie.es/en/file-access-list

OMIE specific pages:
- Capacity and occupation of the interconnectors after Day-ahead matching process (capacidad_inter_pbc): https://www.omie.es/en/file-access-list?parents=/Day-ahead%20Market/6.%20Capacities&dir=%20Capacity%20and%20occupation%20of%20the%20interconnectors%20after%20Day-ahead%20matching%20process&realdir=capacidad_inter_pbc
- Header of bids for Day-ahead Market (CAB): https://www.omie.es/en/file-access-list?parents=/Day-ahead%20Market/4.%20Bids&dir=Header%20of%20bids%20for%20Day-ahead%20Market&realdir=cab
- Day-ahead market bids detail (DET): https://www.omie.es/en/file-access-list?parents=/Day-ahead%20Market/4.%20Bids&dir=Day-ahead%20market%20bids%20detail&realdir=det
- Day-ahead hourly prices in Spain (marginalpdbc): https://www.omie.es/es/file-access-list?parents=/Mercado%20Diario/1.%20Precios&dir=Precios%20horarios%20del%20mercado%20diario%20en%20Espa%C3%B1a&realdir=marginalpdbc

- OMIE list of generation units in the Iberian Peninsula: https://www.omie.es/sites/default/files/dados/listados/LISTA_UNIDADES.XLS

ENTSO-E page for energy prices (France source):
- https://transparency.entsoe.eu/market/energyPrices

