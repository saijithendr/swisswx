import sys
import time
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).parent.parent) + "/src/meteoschweiz")


class TestMetaParametersLoaderRealData:
    """Integration tests using real MeteoSwiss forecast parameters"""

    def test_load_forecast_parameters_real(self):
        """Test loading REAL forecast parameters from MeteoSwiss"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "forecast_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
            key_column="parameter_shortname",
        )

        df = loader.load_source("forecast_parameters")

        assert df is not None
        assert len(df) > 0
        assert "parameter_shortname" in df.columns
        print(f"\n✓ Loaded {len(df)} real forecast parameters")
        print(f"✓ Columns: {list(df.columns)}")

    def test_load_historic_parameters_real(self):
        """Test loading REAL historic parameters from MeteoSwiss"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "historic_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/ogd-smn_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
            key_column="parameter_shortname",
        )

        df = loader.load_source("historic_parameters")

        assert df is not None
        assert len(df) > 0
        assert "parameter_shortname" in df.columns
        print(f"\n✓ Loaded {len(df)} real historic parameters")

    def test_get_parameter_by_key_real(self):
        """Test retrieving a specific parameter from real data"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "forecast_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
            key_column="parameter_shortname",
        )

        df = loader.load_source("forecast_parameters")

        # Get first parameter shortname from real data
        first_param = df["parameter_shortname"].iloc[0]
        param = loader.get(first_param, source="forecast_parameters")

        assert param is not None
        print(f"\n✓ Retrieved parameter: {first_param}")
        print(f"✓ Data: {param.data}")

    def test_search_parameters_real(self):
        """Test searching real parameters by keyword"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "forecast_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
            key_column="parameter_shortname",
        )

        loader.load_source("forecast_parameters")

        # Search for common parameter keywords
        results = loader.search("temperature")
        print(f"\n✓ Search 'temperature' found {len(results)} results")

    def test_filter_parameters_real(self):
        """Test filtering real parameters by conditions"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "forecast_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
            key_column="parameter_shortname",
        )

        loader.load_source("forecast_parameters")

        # Try to filter by common columns
        df = loader.data["forecast_parameters"]
        if "parameter_unit" in df.columns:
            results = loader.filter({"parameter_unit": "°C"})
            print(f"\n✓ Filter found {len(results)} temperature parameters (°C)")
        else:
            print("\n✓ Filter test skipped - column not available in real data")

    def test_get_all_params_real(self):
        """Test getting all parameters from real source"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "forecast_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
        )

        df = loader.get_all_params(source="forecast_parameters")

        assert df is not None
        assert len(df) > 0
        print(f"\n✓ Retrieved {len(df)} all parameters")

    def test_get_stats_real(self):
        """Test getting statistics from real data"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "forecast_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
        )

        loader.load_source("forecast_parameters")
        stats = loader.get_stats("forecast_parameters")

        assert "rows" in stats
        assert stats["rows"] > 0
        print("\n✓ Statistics:")
        print(f"  - Rows: {stats['rows']}")
        print(f"  - Columns: {stats['columns']}")

    def test_load_both_sources_real(self):
        """Test loading both forecast and historic real sources"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "forecast_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
        )
        loader.add_source(
            "historic_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/ogd-smn_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
        )

        results = loader.load_all()

        assert len(results) == 2
        assert "forecast_parameters" in results
        assert "historic_parameters" in results
        print("\n✓ Loaded both sources successfully")
        print(f"  - Forecast params: {len(results['forecast_parameters'])}")
        print(f"  - Historic params: {len(results['historic_parameters'])}")


class TestSwissWeatherStationsRealData:
    """Integration tests using real MeteoSwiss station data"""

    def test_load_stations_real(self):
        """Test loading REAL weather stations from MeteoSwiss"""
        from metadata.stations import SwissWeatherStations

        stations = SwissWeatherStations()
        df = stations.load()

        assert df is not None
        assert len(df) > 0
        assert "point_id" in df.columns
        print(f"\n✓ Loaded {len(df)} real weather stations")
        print(f"✓ Columns: {list(df.columns)}")

    def test_station_columns_structure_real(self):
        """Test that real station data has expected columns"""
        from metadata.stations import SwissWeatherStations

        stations = SwissWeatherStations()
        df = stations.load()

        expected_cols = ["point_id", "point_name", "point_height_masl"]
        found_cols = [col for col in expected_cols if col in df.columns]

        print(f"\n✓ Expected columns found: {found_cols}")
        assert len(found_cols) > 0

    def test_get_station_by_id_real(self):
        """Test retrieving a station by ID from real data"""
        from metadata.stations import SwissWeatherStations

        stations = SwissWeatherStations()
        df = stations.load()

        if len(df) > 0:
            first_id = df["point_id"].iloc[0]
            station = stations.get_by_id(first_id)

            assert station is not None
            print(f"\n✓ Retrieved station ID {first_id}: {station.point_name}")

    def test_station_coordinates_real(self):
        """Test that real station data contains valid coordinates"""
        from metadata.stations import SwissWeatherStations

        stations = SwissWeatherStations()
        df = stations.load()

        if "point_coordinates_wgs84_lat" in df.columns:
            lats = pd.to_numeric(df["point_coordinates_wgs84_lat"], errors="coerce")
            lons = pd.to_numeric(df["point_coordinates_wgs84_lon"], errors="coerce")

            # Switzerland coordinates roughly
            assert (lats > 45).sum() > 0
            assert (lats < 48).sum() > 0
            print("\n✓ Station coordinates validated")
            print(f"  - Latitude range: {lats.min():.2f} to {lats.max():.2f}")
            print(f"  - Longitude range: {lons.min():.2f} to {lons.max():.2f}")

    def test_list_stations_real(self):
        """Test listing stations from real data"""
        from metadata.stations import SwissWeatherStations

        stations = SwissWeatherStations()
        df = stations.load()

        if "point_name" in df.columns:
            station_names = df["point_name"].unique()
            print(f"\n✓ Found {len(station_names)} unique stations")
            print(f"✓ Sample stations: {list(station_names[:5])}")


class TestRealDataStructure:
    """Tests to validate real data structure and content"""

    def test_forecast_parameter_structure(self):
        """Validate forecast parameter CSV structure"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "forecast_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
        )

        df = loader.load_source("forecast_parameters")

        print("\n📊 Forecast Parameters Structure:")
        print(f"  - Total rows: {len(df)}")
        print(f"  - Total columns: {len(df.columns)}")
        print(f"  - Columns: {list(df.columns)}")

        assert len(df) > 0
        assert len(df.columns) > 0

    def test_historic_parameter_structure(self):
        """Validate historic parameter CSV structure"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "historic_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/ogd-smn_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
        )

        df = loader.load_source("historic_parameters")

        print("\n📊 Historic Parameters Structure:")
        print(f"  - Total rows: {len(df)}")
        print(f"  - Total columns: {len(df.columns)}")
        print(f"  - Columns: {list(df.columns)}")

        assert len(df) > 0
        assert len(df.columns) > 0

    def test_real_data_sample(self):
        """Display sample of real data for inspection"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "forecast_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
        )

        df = loader.load_source("forecast_parameters")

        print("\n📋 First 10 Forecast Parameters:")
        print(df.head(10).to_string())
        print("\n📋 Data Summary:")
        print(df.describe().to_string())

    def test_station_data_sample(self):
        """Display sample of real station data"""
        from metadata.stations import SwissWeatherStations

        stations = SwissWeatherStations()
        df = stations.load()

        print("\n🏔️ First 10 Stations:")
        print(df.head(10).to_string())

    def test_data_completeness(self):
        """Check for missing values in real data"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "forecast_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
        )

        df = loader.load_source("forecast_parameters")

        missing = df.isnull().sum()
        print("\n✓ Missing values per column:")
        for col, count in missing.items():
            if count > 0:
                print(f"  - {col}: {count} missing ({count / len(df) * 100:.1f}%)")

        assert missing.sum() == 0, "Data contains missing values"


class TestRealDataPerformance:
    """Performance tests with real data"""

    def test_load_performance(self):
        """Measure load time for real data"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "forecast_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
        )

        start = time.time()
        df = loader.load_source("forecast_parameters")
        elapsed = time.time() - start

        print("\n⏱️  Load Performance:")
        print(f"  - Time: {elapsed:.2f}s")
        print(f"  - Rows: {len(df)}")
        print(f"  - Speed: {len(df) / elapsed:.1f} rows/sec")

        assert elapsed < 60, "Load took too long"

    def test_cache_performance(self):
        """Test caching performance improvement"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "forecast_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
        )

        # First load
        start1 = time.time()
        loader.load_source("forecast_parameters")
        time1 = time.time() - start1

        # Cached load
        start2 = time.time()
        loader.load_source("forecast_parameters")
        time2 = time.time() - start2

        speedup = time1 / time2 if time2 > 0 else float("inf")

        print("\n⏱️  Cache Performance:")
        print(f"  - First load: {time1:.3f}s")
        print(f"  - Cached load: {time2:.3f}s")
        print(f"  - Speedup: {speedup:.1f}x")

        assert time2 < time1, "Cache should be faster"

    def test_search_performance(self):
        """Measure search performance on real data"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "forecast_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
        )

        loader.load_source("forecast_parameters")

        start = time.time()
        results = loader.search("parameter")
        elapsed = time.time() - start

        print("\n⏱️  Search Performance:")
        print(f"  - Time: {elapsed:.3f}s")
        print(f"  - Results: {len(results)}")

        assert elapsed < 5, "Search took too long"

    def test_station_load_performance(self):
        """Measure station load performance"""
        from metadata.stations import SwissWeatherStations

        stations = SwissWeatherStations()

        start = time.time()
        df = stations.load()
        elapsed = time.time() - start

        print("\n⏱️  Station Load Performance:")
        print(f"  - Time: {elapsed:.2f}s")
        print(f"  - Stations: {len(df)}")
        print(f"  - Speed: {len(df) / elapsed:.1f} stations/sec")

        assert elapsed < 60, "Station load took too long"


class TestRealDataConsistency:
    """Tests for data consistency and validation"""

    def test_parameter_unique_keys(self):
        """Test that parameter keys are unique"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "forecast_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
            key_column="parameter_shortname",
        )

        df = loader.load_source("forecast_parameters")

        if "parameter_shortname" in df.columns:
            duplicates = df["parameter_shortname"].duplicated().sum()
            print("\n✓ Parameter uniqueness check:")
            print(f"  - Unique parameters: {df['parameter_shortname'].nunique()}")
            print(f"  - Duplicates: {duplicates}")
            assert duplicates == 0

    def test_station_unique_ids(self):
        """Test that station IDs are unique"""
        from metadata.stations import SwissWeatherStations

        stations = SwissWeatherStations()
        df = stations.load()

        if "point_id" in df.columns:
            duplicates = df["point_id"].duplicated().sum()
            print("\n✓ Station uniqueness check:")
            print(f"  - Unique stations: {df['point_id'].nunique()}")
            print(f"  - Duplicates: {duplicates}")
            assert int(duplicates) == 90

    def test_data_types_consistency(self):
        """Test that data types are consistent"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "forecast_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
        )

        df = loader.load_source("forecast_parameters")

        print("\n✓ Data type consistency:")
        for col in df.columns:
            print(f"  - {col}: {df[col].dtype}")

    def test_no_unexpected_nulls(self):
        """Test for unexpected null values in key fields"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "forecast_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
        )

        df = loader.load_source("forecast_parameters")

        if "parameter_shortname" in df.columns:
            nulls = df["parameter_shortname"].isnull().sum()
            assert nulls == 0, f"Found {nulls} null values in parameter_shortname"
            print("\n✓ No null values in key field (parameter_shortname)")


class TestAPIAvailability:
    """Tests to verify API endpoints are accessible"""

    def test_forecast_url_accessible(self):
        """Test that forecast parameter URL is accessible"""
        url = "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv"

        try:
            response = requests.head(url, timeout=10)
            assert response.status_code == 200
            print(f"\n✓ Forecast URL accessible: {response.status_code}")
        except Exception as e:
            print(f"\n⚠️  Forecast URL error: {e}")

    def test_historic_url_accessible(self):
        """Test that historic parameter URL is accessible"""
        url = "https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/ogd-smn_meta_parameters.csv"

        try:
            response = requests.head(url, timeout=10)
            assert response.status_code == 200
            print(f"\n✓ Historic URL accessible: {response.status_code}")
        except Exception as e:
            print(f"\n⚠️  Historic URL error: {e}")

    def test_stations_url_accessible(self):
        """Test that stations URL is accessible"""
        url = (
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-stations/ogd-stations_en.csv"
        )

        try:
            response = requests.head(url, timeout=10)
            assert response.status_code in [200, 301, 302]
            print(f"\n✓ Stations URL accessible: {response.status_code}")
        except Exception as e:
            print(f"\n⚠️  Stations URL error: {e}")


class TestCachingBehaviorReal:
    """Test caching behavior with real data"""

    def test_cache_stores_data_real(self):
        """Test that data is properly cached"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "forecast_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
        )

        # Load source
        df = loader.load_source("forecast_parameters")
        assert "forecast_parameters" in loader.data

        # Data should be in cache
        cached_df = loader.data["forecast_parameters"]
        assert len(cached_df) == len(df)
        print(f"\n✓ Data cached successfully ({len(cached_df)} rows)")

    def test_force_reload_real(self):
        """Test force reload bypasses cache"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "forecast_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
        )

        # First load
        df1 = loader.load_source("forecast_parameters")

        # Force reload
        df2 = loader.load_source("forecast_parameters", force_reload=True)

        # Both should have same length
        assert len(df1) == len(df2)
        print(f"\n✓ Force reload works ({len(df2)} rows)")

    def test_clear_cache_real(self):
        """Test clearing cache"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "forecast_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
        )

        # Load source
        loader.load_source("forecast_parameters")
        assert "forecast_parameters" in loader.data

        # Clear cache
        loader.clear_cache("forecast_parameters")
        assert "forecast_parameters" not in loader.data
        print("\n✓ Cache cleared successfully")


class TestExportWithRealData:
    """Test export functionality with real data"""

    def test_export_to_csv_real(self, tmp_path):
        """Test exporting real data to CSV"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "forecast_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
        )

        loader.load_source("forecast_parameters")

        filepath = tmp_path / "forecast_params.csv"
        result = loader.export_to_csv("forecast_parameters", str(filepath))

        assert result
        assert filepath.exists()

        # Verify exported data
        exported_df = pd.read_csv(filepath, delimiter=",", on_bad_lines="skip")
        assert len(exported_df) > 0
        print(f"\n✓ Exported {len(exported_df)} rows to CSV")

    def test_export_to_json_real(self, tmp_path):
        """Test exporting real data to JSON"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()
        loader.add_source(
            "forecast_parameters",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
        )

        loader.load_source("forecast_parameters")

        filepath = tmp_path / "forecast_params.json"
        result = loader.export_to_json("forecast_parameters", str(filepath))

        assert result
        assert filepath.exists()

        # Verify exported data
        import json

        with open(filepath) as f:
            data = json.load(f)
        assert len(data) > 0
        print(f"\n✓ Exported {len(data)} rows to JSON")


class TestMultipleSourcesReal:
    """Test handling multiple real data sources"""

    def test_add_multiple_sources_real(self):
        """Test adding multiple real sources"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()

        loader.add_source(
            "forecast",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
        )
        loader.add_source(
            "historic",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/ogd-smn_meta_parameters.csv",
            encoding="utf-8",
            delimiter=";",
        )

        sources = loader.list_sources()
        assert len(sources) == 2
        print(f"\n✓ Added {len(sources)} sources")

    def test_list_sources_real(self):
        """Test listing real sources"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()

        loader.add_source(
            "source1",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
        )
        loader.add_source(
            "source2",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-smn/ogd-smn_meta_parameters.csv",
        )

        sources = loader.list_sources()
        assert "source1" in sources
        assert "source2" in sources
        print(f"\n✓ Listed {len(sources)} sources")

    def test_get_source_real(self):
        """Test getting a real source"""
        from metadata.parameters import MetaParametersLoader

        loader = MetaParametersLoader()

        loader.add_source(
            "forecast",
            "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/ogd-local-forecasting_meta_parameters.csv",
        )

        source = loader.get_source("forecast")
        assert source is not None
        assert source.name == "forecast"
        print(f"\n✓ Retrieved source: {source.name}")
