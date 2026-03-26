"""Shared pytest fixtures for local TNS tests."""

from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import Polygon


FIXTURE_ROOT = Path(__file__).resolve().parent
GEOMS_PATH = FIXTURE_ROOT / "geoms.json"


def feature_to_polygon(feature: dict[str, object]) -> Polygon:
    """Convert a feature from the repository fixture JSON into a polygon."""
    return Polygon(feature["geometry"]["rings"][0])


def load_states_geodataframe() -> gpd.GeoDataFrame:
    """Load the repository's state boundary fixture as a GeoDataFrame."""
    with GEOMS_PATH.open("r", encoding="utf-8") as stream:
        states_json = json.load(stream)

    rows = [
        {
            "pk_and_model": feature["attributes"]["STATE_NAME"],
            "geometry": feature_to_polygon(feature),
        }
        for feature in states_json["features"]
    ]
    return gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")


@pytest.fixture(scope="session")
def states_gdf() -> gpd.GeoDataFrame:
    """Provide the canonical state geometry fixture for tests."""
    return load_states_geodataframe()


@pytest.fixture(scope="function")
def aois_gdf(states_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Return AOI geometries for a test case."""
    return states_gdf.copy()


@pytest.fixture(scope="function")
def tiles_gdf(states_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Return tile geometries that align one-for-one with the state fixture."""
    tiles = states_gdf.copy()
    tiles["pk_and_model"] = [
        f"raster_{index}" for index in range(len(tiles.index))
    ]
    return tiles


@pytest.fixture(scope="function")
def split_tiles_gdf(states_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Return a smaller tile fixture split across multiple parquet files."""
    subset = states_gdf.iloc[:10].copy()
    subset["pk_and_model"] = [f"split_{index}" for index in range(len(subset))]
    return subset


@pytest.fixture(scope="function")
def parquet_dir(tmp_path: Path) -> Path:
    """Create a temporary directory for parquet test artifacts."""
    path = tmp_path / "parquet"
    path.mkdir()
    return path
