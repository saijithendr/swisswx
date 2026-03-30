"""
Local Forecast Handler
======================

Fetch all weather data for specific point_id(s) from MeteoSwiss local forecasts.
Combines STAC API access with SwissWeatherStations and MetaParametersLoader.

Features:
  - Get all forecast data for a point_id
  - Multiple parameter retrieval
  - Geographic metadata enrichment
  - Export capabilities
  - Data validation and caching
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from io import StringIO
from typing import Any

import pandas as pd
import requests

logger = logging.getLogger(__name__)


@dataclass
class ForecastQueryResult:
    """Container for forecast data + metadata."""

    point_id: int
    station_name: str
    latitude: float
    longitude: float
    elevation: float
    station_type: str
    forecast_reference_time: datetime | None
    parameters: list[str]
    data: pd.DataFrame

    def summary(self) -> str:
        """Get result summary."""
        lines = [
            f"Forecast for {self.station_name} (Point ID: {self.point_id})",
            f"  Location: {self.latitude:.6f}°N, {self.longitude:.6f}°E",
            f"  Elevation: {self.elevation:.0f}m",
            f"  Type: {self.station_type}",
            f"  Reference Time: {self.forecast_reference_time}",
            f"  Parameters: {len(self.parameters)}",
            f"  Data Points: {len(self.data)}",
        ]
        if "Date" in self.data.columns and len(self.data) > 0:
            lines.append(
                f"  Date Range: {self.data['Date'].min()} to {self.data['Date'].max()}"
            )
        return "\n".join(lines)


class LocalForecastHandler:
    """
    Handler for MeteoSwiss local forecast data.

    Provides high-level interface to fetch all weather data for specific point_ids.

    """

    STAC_BASE_URL = "https://data.geo.admin.ch/api/stac/v1"
    COLLECTION_ID = "ch.meteoschweiz.ogd-local-forecasting"

    def __init__(
        self,
        stations_handler=None,
        params_loader=None,
        language: str = "en",
        timeout: int = 60,
        auto_load_metadata: bool = True,
    ):
        """
        Initialize the forecast handler.

        Args:
            stations_handler: SwissWeatherStations instance (or None to create new)
            params_loader: MetaParametersLoader instance (or None to create new)
            language: Language for parameter descriptions ('en', 'de', 'fr', 'it')
            timeout: HTTP timeout in seconds
            auto_load_metadata: If True, load parameter and station metadata on init
        """
        self.language = language
        self.timeout = timeout

        # Initialize core components
        if stations_handler is None:
            from metadata.stations import SwissWeatherStations

            self.stations = SwissWeatherStations()
        else:
            self.stations = stations_handler

        if params_loader is None:
            from metadata.parameters import MetaParametersLoader

            self.params_loader = MetaParametersLoader()
        else:
            self.params_loader = params_loader

        # Cache
        self._forecast_items_cache = None
        self._parameters_cache = None

        if auto_load_metadata:
            self._init_metadata()

    def _init_metadata(self) -> None:
        """Load parameter and station metadata."""
        try:
            # Load parameter metadata
            logger.info("Loading parameter metadata...")
            self.params_loader.add_source(
                name="forecast_parameters",
                url="https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
                description="MeteoSwiss local forecast parameters",
                encoding="latin-1",
                delimiter=";",
                key_column="parameter_shortname",
            )
            self.params_loader.load_source("forecast_parameters")
            logger.info("✓ Parameter metadata loaded")
        except Exception as e:
            logger.warning(f"Could not load parameter metadata: {e}")

        try:
            logger.info("Loading Swiss weather stations...")
            self.stations.load()
            logger.info("✓ Loaded stations")
        except Exception as e:
            logger.warning(f"Could not load stations: {e}")

    def _get_stac_items(
        self,
        force_reload: bool = False,
        limit: int = 10,
    ) -> list[dict]:
        """
        Get forecast items from STAC API.

        Args:
            force_reload: Force reload even if cached
            limit: Maximum number of items to return

        Returns:
            list of STAC features (forecast items)
        """
        if self._forecast_items_cache and not force_reload:
            return self._forecast_items_cache

        try:
            stac_url = f"{self.STAC_BASE_URL}/collections/{self.COLLECTION_ID}/items"
            logger.info(f"Querying STAC API: {stac_url}")

            response = requests.get(
                stac_url, params={"limit": limit}, timeout=self.timeout
            )
            response.raise_for_status()

            data = response.json()
            items = data.get("features", [])

            logger.info(f"✓ Found {len(items)} forecast items")
            self._forecast_items_cache = items

            return items
        except Exception as e:
            logger.error(f"Failed to query STAC API: {e}")
            return []

    def _find_asset_for_parameter(
        self,
        items: list[dict],
        parameter: str,
    ) -> tuple[str, dict] | None:
        """
        Find CSV asset URL for a parameter.

        Args:
            items: STAC items
            parameter: Parameter code (e.g., 'tre200h0')

        Returns:
            tuple of (asset_name, asset_info) or None
        """
        for item in items:
            assets = item.get("assets", {})

            for asset_name, asset_info in assets.items():
                if asset_info.get("type") == "text/csv" and parameter in asset_name:
                    return (asset_name, asset_info)

        return None

    def _download_parameter_csv(
        self,
        asset_url: str,
        parameter: str,
    ) -> pd.DataFrame | None:
        """
        Download and parse parameter CSV from STAC asset.

        Args:
            asset_url: URL to CSV file
            parameter: Parameter code (for logging)

        Returns:
            DataFrame or None if download fails
        """
        try:
            logger.info(f"Downloading {parameter} from {asset_url[:60]}...")

            response = requests.get(asset_url, timeout=self.timeout)
            response.raise_for_status()

            content = response.content.decode("latin-1")
            df = pd.read_csv(StringIO(content), sep=";")

            logger.info(
                f"✓ Downloaded {parameter}: {len(df)} rows, {len(df.columns)} columns"
            )

            return df
        except Exception as e:
            logger.error(f"Failed to download {parameter}: {e}")
            return None

    def _extract_forecast_reference_time(
        self,
        items: list[dict],
    ) -> datetime | None:
        """
        Extract forecast reference time from STAC item.

        Args:
            items: STAC items

        Returns:
            datetime or None
        """
        try:
            if not items:
                return None

            # Get asset name from first item
            first_item = items[0]
            assets = first_item.get("assets", {})

            if not assets:
                return None

            asset_name = next(iter(assets.keys()))

            # Extract timestamp: vnut12.lssw.202512162300.parameter.csv
            # Pattern: vnut12.lssw.YYYYMMDDHHMM.parameter.csv
            match = re.search(r"\.(\d{12})\.", asset_name)
            if match:
                timestamp_str = match.group(1)
                ref_time = pd.to_datetime(timestamp_str, format="%Y%m%d%H%M")
                return ref_time
        except Exception as e:
            logger.warning(f"Could not extract forecast reference time: {e}")

        return None

    def _get_station_metadata(self, point_id: int) -> dict[str, Any]:
        """
        Get station metadata for a point_id.

        Args:
            point_id: Location ID

        Returns:
            dictionary with station metadata
        """
        try:
            station = self.stations.get_by_id(point_id)
            if station:
                return {
                    "station_name": station.point_name,
                    "latitude": station.coordinates_wgs84_lat,
                    "longitude": station.coordinates_wgs84_lon,
                    "elevation": station.point_height_masl,
                    "station_type": station.point_type,
                }
        except Exception as e:
            logger.warning(f"Could not get metadata for point_id {point_id}: {e}")

        # Fallback
        return {
            "station_name": f"Point {point_id}",
            "latitude": float("nan"),
            "longitude": float("nan"),
            "elevation": float("nan"),
            "station_type": "Unknown",
        }

    def get_all_parameters(self) -> list[str]:
        """
        Get list of all available forecast parameters.

        Returns:
            list of parameter codes
        """
        try:
            if self._parameters_cache:
                return self._parameters_cache

            df_params = self.params_loader.get_all_params(source="forecast_parameters")

            if df_params is not None and not df_params.empty:
                params = (
                    df_params["parameter_shortname"]
                    .dropna()
                    .astype(str)
                    .unique()
                    .tolist()
                )
                self._parameters_cache = params
                return params
        except Exception as e:
            logger.warning(f"Could not get parameters list: {e}")

        return []

    def get_forecast_for_point_id(
        self,
        point_id: int,
        parameters: list[str] | None = None,
        latest_only: bool = True,
        filter_on: str = "measured",
        force_reload: bool = False,
    ) -> ForecastQueryResult | None:
        """
        Get all forecast data for a specific point_id.

        Args:
            point_id: Location ID (1-965800)
            parameters: list of parameter codes to fetch. If None, fetch all available.
            force_reload: Force reload even if cached

        Returns:
            ForecastQueryResult with data and metadata

        Example:
            handler = LocalForecastHandler()

            result = handler.get_forecast_for_point_id(
                point_id=1,
                parameters=['tre200h0', 'rre150h0', 'fu3010h0']
            )

            if result:
                print(result.summary())
                print(result.data.head())
        """
        logger.info(f"Fetching forecast for point_id {point_id}")

        # Get STAC items
        items = self._get_stac_items(force_reload=force_reload)
        if not items:
            logger.error("No STAC items found")
            return None

        if latest_only:
            total_items_date = [
                pd.to_datetime(
                    int(prop.get("id").split("-")[0]), format="%Y%m%d", errors="coerce"
                )
                for prop in items
            ]
            today = pd.Timestamp.today().normalize()

            today_index = [
                i for i, date in enumerate(total_items_date) if date == today
            ]

            items = [items[i] for i in today_index]

        # Determine which parameters to fetch
        if parameters is None:
            parameters = self.get_all_parameters()
            logger.info(f"Fetching all {len(parameters)} available parameters")
        else:
            logger.info(f"Fetching {len(parameters)} specified parameters")

        # Download data for each parameter
        all_dfs = []
        for param in parameters:
            # Find asset
            asset_info = self._find_asset_for_parameter(items, param)
            if not asset_info:
                logger.warning(f"Asset not found for parameter {param}")
                continue

            _, asset_data = asset_info
            asset_url = asset_data.get("href")

            # Download CSV
            df = self._download_parameter_csv(asset_url, param)
            if df is None:
                continue

            # Filter for point_id
            df_filtered = df[df["point_id"] == point_id]

            if df_filtered.empty:
                logger.warning(f"No data for point_id {point_id} in parameter {param}")
                continue

            all_dfs.append(df_filtered)

        if not all_dfs:
            logger.error(f"No data found for point_id {point_id}")
            return None

        # Combine data
        combined_df = pd.concat(all_dfs, ignore_index=True)

        if filter_on == "measured":
            combined_df = combined_df[combined_df["point_type_id"] == 3]
        elif filter_on == "observed":
            combined_df = combined_df[combined_df["point_type_id"] == 1]
        else:
            combined_df = combined_df

        combined_df = combined_df.groupby(
            ["point_id", "point_type_id", "Date"], as_index=False
        ).max()

        if "Date" in combined_df.columns:
            try:
                combined_df["Date"] = pd.to_datetime(
                    combined_df["Date"], format="%Y%m%d%H%M", errors="coerce"
                )
                combined_df = combined_df.sort_values("Date").reset_index(drop=True)
            except Exception as e:
                logger.warning(f"Could not parse dates: {e}")

        # Get station metadata
        station_meta = self._get_station_metadata(point_id)

        # Extract forecast reference time
        ref_time = self._extract_forecast_reference_time(items)

        # Create result
        result = ForecastQueryResult(
            point_id=point_id,
            station_name=station_meta["station_name"],
            latitude=station_meta["latitude"],
            longitude=station_meta["longitude"],
            elevation=station_meta["elevation"],
            station_type=station_meta["station_type"],
            forecast_reference_time=ref_time,
            parameters=parameters,
            data=combined_df,
        )

        logger.info(f"✓ Retrieved forecast for {result.station_name}")

        return result

    def get_forecast_for_station_name(
        self,
        station_name: str,
        parameters: list[str] | None = None,
        exact: bool = True,
    ) -> ForecastQueryResult | None:
        """
        Get forecast for a station by name.

        Args:
            station_name: Swiss station name (e.g., 'Grenchen')
            parameters: list of parameter codes
            exact: If True, require exact name match

        Returns:
            ForecastQueryResult or None
        """
        try:
            # Find station
            candidates = self.stations.get_by_name(station_name, exact=exact)

            if not candidates:
                logger.error(f"Station '{station_name}' not found")
                return None

            if len(candidates) > 1:
                logger.warning(
                    f"Found {len(candidates)} stations with name '{station_name}'"
                )
                logger.info(f"Using first: {candidates[0].point_name}")

            station = candidates[0]

            # Fetch forecast
            return self.get_forecast_for_point_id(
                point_id=station.point_id,
                parameters=parameters,
            )
        except Exception as e:
            logger.error(f"Failed to get forecast for station '{station_name}': {e}")
            return None

    def get_forecast_for_coordinates(
        self,
        lat: float,
        lon: float,
        parameters: list[str] | None = None,
    ) -> ForecastQueryResult | None:
        """
        Get forecast for the nearest station to given coordinates.

        Args:
            lat: Latitude
            lon: Longitude
            parameters: list of parameter codes

        Returns:
            ForecastQueryResult for nearest station
        """
        try:
            # Find nearest station
            nearest = self.stations.find_nearest(lat, lon, n=1)
            if not nearest:
                logger.error("No stations found")
                return None

            station, distance = nearest[0]
            logger.info(
                f"Nearest station: {station.point_name} ({distance:.1f}km away)"
            )

            # Fetch forecast
            return self.get_forecast_for_point_id(
                point_id=station.point_id,
                parameters=parameters,
            )
        except Exception as e:
            logger.error(f"Failed to get forecast for coordinates: {e}")
            return None

    def export_to_csv(
        self,
        result: ForecastQueryResult,
        filepath: str,
        encoding: str = "utf-8",
    ) -> bool:
        """Export forecast data to CSV."""
        try:
            result.data.to_csv(filepath, index=False, encoding=encoding, sep=",")
            logger.info(f"✓ Exported to {filepath}")
            return True
        except Exception as e:
            logger.error(f"Failed to export to CSV: {e}")
            return False

    def export_to_json(
        self,
        result: ForecastQueryResult,
        filepath: str,
    ) -> bool:
        """Export forecast data to JSON."""
        try:
            result.data.to_json(filepath, orient="records", indent=2)
            logger.info(f"✓ Exported to {filepath}")
            return True
        except Exception as e:
            logger.error(f"Failed to export to JSON: {e}")
            return False
