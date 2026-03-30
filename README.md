# MeteoSwiss

[![CI](https://github.com/saijithendr/MeteoSwiss/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/saijithendr/MeteoSwiss/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/meteoschweiz.svg)](https://pypi.org/project/meteoschweiz/)
[![Python Versions](https://img.shields.io/pypi/pyversions/meteoschweiz.svg)](https://pypi.org/project/meteoschweiz/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A comprehensive Python library for accessing, managing, and analyzing Swiss weather data from MeteoSwiss (Federal Office of Meteorology and Climatology). This toolkit provides high-level interfaces for working with weather stations, parameters, historic data, and forecasts.

---

## Table of Contents
- [Installation](#installation)
- [Usage](#usage)
  - [1. Metadata](#1-metadata)
  - [2. Historic Weather Data](#2-historic-weather-data)
  - [3. Local Forecast Data](#3-local-forecast-data)
- [API Reference](#api-reference)
- [Developer Guide](#developer-guide)
- [Bugs / Issues](#bugs--issues)
- [Roadmap](#roadmap)
- [License](#license)

---

## Installation

```bash
pip install meteoschweiz
```

**Requirements:** Python 3.9+ · pandas ≥ 1.5.0 · requests ≥ 2.28.0

---

## Usage

### 1. Metadata

#### 1.1 Stations

```python
from meteoschweiz.metadata.stations import SwissWeatherStations

stations = SwissWeatherStations()
stations_df = stations.load()  # returns all available weather stations in Switzerland

# Get a specific station by abbreviation
gre = stations.get_by_abbr('GRE')
print(f"{gre.point_name}: {gre.elevation:.0f}m")

# Find stations near a coordinate (lat, lon, radius in km)
nearby = stations.find_nearby(lat=47.0, lon=7.4, radius_km=30)
```

#### 1.2 Parameter Metadata

`MetaParametersLoader` loads parameter definitions from any MeteoSwiss CSV source URL.

```python
from meteoschweiz.metadata.parameters import MetaParametersLoader

loader = MetaParametersLoader()

# Register sources
loader.add_source(
    name="Forecast Parameters",
    url="https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
    description="MeteoSwiss forecast parameters",
    encoding="latin-1",
    delimiter=";",
    key_column="parameter_shortname",
)

loader.add_source(
    name="Historic Parameters",
    url="https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/ogd-smn_meta_parameters.csv",
    description="MeteoSwiss historic/SMN parameters",
    encoding="latin-1",
    delimiter=";",
    key_column="parameter_shortname",
)

# Load all sources
loader.load_all()
print(f"Loaded {len(loader.data)} sources")

# Get all parameters for a source as a DataFrame
df_params = loader.get_all_params(source="Forecast Parameters")
print(f"{len(df_params)} parameters, columns: {list(df_params.columns)[:5]}")

# Search by keyword
temp_params = loader.search("temperature", source="Forecast Parameters")
print(f"Found {len(temp_params)} temperature-related parameters")

# Get a specific parameter by key
param = loader.get("tre200h0", source="Historic Parameters")
if param:
    print(f"Source: {param.source}, keys: {list(param.data.keys())}")

# Print summary
print(loader.summary())
```

---

### 2. Historic Weather Data

```python
from meteoschweiz.historic.historic_handler import HistoricWeatherHandler, MeteoSwissClient

client = MeteoSwissClient()
historic_handler = HistoricWeatherHandler(
    stations_handler=stations,   # from section 1.1
    params_loader=loader,        # from section 1.2
    meteoswiss_client=client,
    language="en",               # "en" or "de"
)

# Query by station abbreviation
result = historic_handler.get_historic_by_station_id(
    station_id="GRE",
    start_date="2023-06-01",
    end_date="2023-06-30",
    aggregation="daily",
    parameters=["tre200d0", "tre200dn", "tre200dx", "rre150d0"],
    rename_columns=True,
    include_units=True,
)

# result is a HistoricQueryResult dataclass:
# .station_id, .station_name, .canton, .latitude, .longitude,
# .altitude, .aggregation, .parameters, .start_date, .end_date, .data (DataFrame)
print(result.data.head())

# Convenience methods
temp = historic_handler.get_temperature_history("GRE", "2023-01-01", "2023-12-31")
precip = historic_handler.get_precipitation_history("GRE", "2023-01-01", "2023-12-31")

# Export
historic_handler.export_to_csv(result, "grenchen_june_2023.csv")
```

---

### 3. Local Forecast Data

```python
from meteoschweiz.forecasts.forecast_handler import LocalForecastHandler

forecast_handler = LocalForecastHandler(
    stations_handler=stations,   # from section 1.1
    params_loader=loader,        # from section 1.2
)

# By MeteoSwiss point ID
result = forecast_handler.get_forecast_for_point_id(
    point_id=774,
    parameters=["tre200h0", "rre150h0"],
)
print(result.summary())

# By station name
result = forecast_handler.get_forecast_for_station_name("Grenchen")

# By coordinates (uses nearest station)
result = forecast_handler.get_forecast_for_coordinates(lat=47.18, lon=7.41)

# Export
forecast_handler.export_to_csv(result, "forecast_grenchen.csv")
```

---

## API Reference

### `SwissWeatherStations`

| Method | Description |
|--------|-------------|
| `load()` | Load all stations from MeteoSwiss |
| `get_by_abbr(abbr)` | Get station by abbreviation (e.g. `'GRE'`) |
| `get_by_name(name)` | Search stations by name |
| `get_by_id(id)` | Get station by point ID |
| `find_nearby(lat, lon, radius_km)` | Find stations within a radius |
| `find_nearest(lat, lon, n)` | Find the *n* nearest stations |
| `filter_by_bbox(lat_min, lat_max, lon_min, lon_max)` | Filter by bounding box |
| `filter_by_elevation(min, max)` | Filter by elevation range |
| `get_highest_stations(n)` | Get the *n* highest stations |
| `get_statistics()` | Summary statistics for the station network |

### `MetaParametersLoader`

| Method | Description |
|--------|-------------|
| `add_source(name, url, ...)` | Register a CSV source |
| `load_source(name)` | Load data from a named source |
| `load_all()` | Load all registered sources |
| `get_all_params(source)` | Return all parameters as a DataFrame |
| `search(keyword, source)` | Search by keyword |
| `filter(conditions)` | Filter by field conditions |
| `get(key, source)` | Get a single parameter by key |
| `export_to_csv(source, filepath)` | Export to CSV |
| `export_to_json(source, filepath)` | Export to JSON |
| `summary()` | Print loader summary |
| `list_sources()` | List registered source names |
| `clear_cache(source)` | Clear cached data |

### `HistoricWeatherHandler`

| Method | Description |
|--------|-------------|
| `get_historic_by_station_id(station_id, ...)` | Query by SMN station ID |
| `get_historic_by_name(station_name, ...)` | Query by station name |
| `get_historic_by_coords(lat, lon, ...)` | Query nearest station |
| `get_temperature_history(station, start, end)` | Temperature convenience method |
| `get_precipitation_history(station, start, end)` | Precipitation convenience method |
| `list_available_parameters()` | List all available parameters |
| `export_to_csv(result, filepath)` | Export to CSV |
| `export_to_json(result, filepath)` | Export to JSON |

### `LocalForecastHandler`

| Method | Description |
|--------|-------------|
| `get_forecast_for_point_id(point_id, ...)` | Get forecast by MeteoSwiss point ID |
| `get_forecast_for_station_name(name, ...)` | Get forecast by station name |
| `get_forecast_for_coordinates(lat, lon, ...)` | Get forecast for nearest station |
| `get_all_parameters()` | List available forecast parameters |
| `export_to_csv(result, filepath)` | Export to CSV |
| `export_to_json(result, filepath)` | Export to JSON |

---

## Developer Guide

> Full details are in [CONTRIBUTING.md](CONTRIBUTING.md).

### Quick setup

```bash
# 1. Fork the repository on GitHub, then clone your fork
git clone https://github.com/<your-username>/MeteoSwiss.git
cd MeteoSwiss

# 2. Create a virtual environment and install dev dependencies
python -m venv .venv
source .venv/bin/activate        # Linux/macOS
# .venv\Scripts\activate         # Windows

pip install -e ".[dev]"
```

### Branch workflow

```bash
# Always branch from master
git checkout -b feature/your-feature-name
```

| Branch prefix | Purpose |
|---------------|---------|
| `feature/` | New functionality |
| `fix/` | Bug fixes |
| `docs/` | Documentation only |
| `refactor/` | Code quality, no behavior change |

### Run tests

```bash
pytest tests/ -v                              # all tests
pytest tests/ -m "not integration"            # skip network tests
pytest tests/ --cov=src/meteoswiss           # with coverage
tox                                           # all Python versions (3.8–3.13)
```

### Lint and format

```bash
ruff check src/ tests/           # check for issues
ruff check --fix src/ tests/     # auto-fix where possible
ruff format src/ tests/          # format code
```

### Branch protection on `master`

The `master` branch is protected:
- All CI checks (lint, tests on Python 3.8–3.13, type-check) must pass.
- At least **1 approving review** is required before merging.
- Direct pushes are disabled — all changes go through Pull Requests.

### Submitting a Pull Request

1. Push your branch: `git push origin feature/your-feature-name`
2. Open a PR against `master` and fill in the [PR template](.github/PULL_REQUEST_TEMPLATE.md).
3. Address review feedback; a maintainer will merge once approved and CI passes.

---

## Bugs / Issues

Open an issue on GitHub: [New Bug Report](https://github.com/saijithendr/MeteoSwiss/issues/new?template=bug_report.md)

Please include:
- Python version and OS
- `meteoschweiz` version (`pip show meteoschweiz`)
- A minimal code snippet that reproduces the problem

---

## Roadmap

1. Better visualizations (station geo coordinates, districts, cantons)
2. Data preprocessing pipelines
3. Individual weather feature handlers

---

## Data Source & Attribution

This library accesses data from **MeteoSwiss** (Federal Office of Meteorology and Climatology, Switzerland).

**Source: MeteoSwiss** — data is made available under the [Creative Commons CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) licence. When you use or redistribute data obtained through this library, you must include the attribution **"Source: MeteoSwiss"**.

This project is an independent open-source tool and is not endorsed by or affiliated with MeteoSwiss.

Users are responsible for complying with MeteoSwiss [Terms of Use](https://opendatadocs.meteoswiss.ch/general/terms-of-use), including avoiding excessive or high-frequency downloads.

---

## License

[MIT](LICENSE) — Copyright (c) 2026 Sai Jithendra Gangireddy
