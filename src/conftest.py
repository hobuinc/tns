"""Shared pytest fixtures for local TNS tests."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tns_core import connect_duckdb, quote_sql_string


FIXTURE_ROOT = Path(__file__).resolve().parent
GEOMS_PATH = FIXTURE_ROOT / "geoms.json"


def feature_to_wkt(feature: dict[str, object]) -> str:
    """Convert a feature from the repository fixture JSON into polygon WKT."""
    coordinates = ", ".join(
        f"{x} {y}" for x, y in feature["geometry"]["rings"][0]
    )
    return f"POLYGON (({coordinates}))"


def load_state_rows() -> list[dict[str, str]]:
    """Load the repository's state boundary fixture as simple parquet rows."""
    with GEOMS_PATH.open("r", encoding="utf-8") as stream:
        states_json = json.load(stream)

    return [
        {
            "pk_and_model": feature["attributes"]["STATE_NAME"],
            "geometry_wkt": feature_to_wkt(feature),
        }
        for feature in states_json["features"]
    ]


def write_geometry_parquet(path: Path, rows: list[dict[str, str]]) -> None:
    """Write WKT geometry rows as a parquet file with WKB geometry values."""
    raw_path = path.with_suffix(".raw.parquet")
    raw_table = pa.table(
        {
            "pk_and_model": [row["pk_and_model"] for row in rows],
            "geometry_wkt": [row["geometry_wkt"] for row in rows],
        }
    )
    pq.write_table(raw_table, raw_path)

    connection = connect_duckdb()
    connection.execute(
        f"""
        COPY (
            SELECT
                pk_and_model,
                ST_AsWKB(ST_GeomFromText(geometry_wkt)) AS geometry
            FROM read_parquet('{quote_sql_string(str(raw_path))}')
        ) TO '{quote_sql_string(str(path))}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    raw_path.unlink()


@pytest.fixture(scope="session", autouse=True)
def duckdb_extension_dir() -> str:
    """Point DuckDB extension downloads at a stable writable directory."""
    extension_dir = FIXTURE_ROOT.parent / ".duckdb_extensions"
    extension_dir.mkdir(parents=True, exist_ok=True)
    os.environ["TNS_DUCKDB_EXTENSION_DIR"] = str(extension_dir)
    return str(extension_dir)


@pytest.fixture(scope="session")
def state_rows() -> list[dict[str, str]]:
    """Provide the canonical state geometry fixture for tests."""
    return load_state_rows()


@pytest.fixture(scope="function")
def aoi_rows(state_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Return AOI rows for a test case."""
    return list(state_rows)


@pytest.fixture(scope="function")
def tile_rows(state_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Return tile rows that align one-for-one with the state fixture."""
    return [
        {"pk_and_model": f"raster_{index}", "geometry_wkt": row["geometry_wkt"]}
        for index, row in enumerate(state_rows)
    ]


@pytest.fixture(scope="function")
def split_tile_rows(state_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    """Return a smaller tile fixture split across multiple parquet files."""
    return [
        {"pk_and_model": f"split_{index}", "geometry_wkt": row["geometry_wkt"]}
        for index, row in enumerate(state_rows[:10])
    ]


@pytest.fixture(scope="function")
def parquet_dir(tmp_path: Path) -> Path:
    """Create a temporary directory for parquet test artifacts."""
    path = tmp_path / "parquet"
    path.mkdir()
    return path
