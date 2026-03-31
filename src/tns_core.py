"""Core DuckDB-based GeoParquet comparison utilities for TNS.

This module keeps the spatial comparison path free of AWS concerns so it can
be tested locally with repository fixtures and reused by different runtime
adapters.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Protocol

import duckdb


DEFAULT_EXTENSION_DIR = Path("/tmp/tns_duckdb_extensions")
REQUIRED_COLUMNS = {"pk_and_model", "geometry"}
_DUCKDB_CONNECTION: duckdb.DuckDBPyConnection | None = None


class GeoParquetStore(Protocol):
    """Storage adapter for staging input parquet files and persisting outputs."""

    def materialize_input(self, uri: str) -> str:
        """Return a local filesystem path for the requested parquet dataset."""

    def persist_output(self, local_path: str, destination_uri: str) -> str:
        """Persist a local parquet file to its final destination and return the URI."""


@dataclass(frozen=True)
class CompareArtifacts:
    """Structured output from a completed AOI/tile comparison run."""

    matched_aois: list[str]
    output_uri: str
    row_count: int


class FilesystemGeoParquetStore:
    """GeoParquet store implementation backed by the local filesystem."""

    def materialize_input(self, uri: str) -> str:
        """Return a local parquet path unchanged."""
        return uri

    def persist_output(self, local_path: str, destination_uri: str) -> str:
        """Move a local parquet output into place on the filesystem."""
        source = Path(local_path)
        destination = Path(destination_uri)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(source.read_bytes())
        return str(destination)


class S3GeoParquetStore:
    """GeoParquet store implementation that reads and writes through S3."""

    def __init__(self, s3_client, working_directory: str = "/tmp/tns_store"):
        """Create a store with a boto3-compatible S3 client."""
        self.s3 = s3_client
        self.working_directory = Path(working_directory)
        self.working_directory.mkdir(parents=True, exist_ok=True)
        self._cached_objects: dict[str, tuple[str, str | None]] = {}

    def materialize_input(self, uri: str) -> str:
        """Download an input parquet object from S3 into the working directory.

        Warm Lambda containers often process many batches back to back. We keep
        a local copy keyed by URI and ETag so the AOI parquet and any repeated
        inputs can be reused safely across invocations.
        """
        bucket, key = parse_s3_uri(uri)
        filename = key.replace("/", "_")
        local_path = self.working_directory / filename
        metadata = self.s3.head_object(Bucket=bucket, Key=key)
        etag = metadata.get("ETag")
        cached = self._cached_objects.get(uri)
        if cached and cached[0] == etag and Path(cached[1]).exists():
            return cached[1]

        body = self.s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        local_path.write_bytes(body)
        self._cached_objects[uri] = (etag, str(local_path))
        return str(local_path)

    def persist_output(self, local_path: str, destination_uri: str) -> str:
        """Upload a local parquet file into S3 and return the destination URI."""
        bucket, key = parse_s3_uri(destination_uri)
        self.s3.put_object(Bucket=bucket, Key=key, Body=Path(local_path).read_bytes())
        return destination_uri


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split an S3 URI into bucket and key components."""
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected S3 URI, got: {uri}")
    without_scheme = uri[5:]
    bucket, _, key = without_scheme.partition("/")
    if not bucket or not key:
        raise ValueError(f"Malformed S3 URI: {uri}")
    return bucket, key


def quote_sql_string(value: str) -> str:
    """Escape a string for interpolation into a DuckDB SQL literal."""
    return value.replace("'", "''")


def get_extension_directory() -> Path:
    """Return the writable directory DuckDB should use for extensions."""
    return Path(os.environ.get("TNS_DUCKDB_EXTENSION_DIR", str(DEFAULT_EXTENSION_DIR)))


def connect_duckdb(extension_directory: Path | None = None) -> duckdb.DuckDBPyConnection:
    """Create or reuse a DuckDB connection with the spatial extension loaded."""
    global _DUCKDB_CONNECTION
    if _DUCKDB_CONNECTION is not None:
        try:
            _DUCKDB_CONNECTION.execute("SELECT 1")
            return _DUCKDB_CONNECTION
        except duckdb.Error:
            _DUCKDB_CONNECTION = None

    ext_dir = extension_directory or get_extension_directory()
    ext_dir.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect()
    connection.execute(f"SET extension_directory = '{quote_sql_string(str(ext_dir))}'")
    try:
        connection.execute("LOAD spatial")
    except duckdb.Error:
        connection.execute("INSTALL spatial")
        connection.execute("LOAD spatial")
    _DUCKDB_CONNECTION = connection
    return _DUCKDB_CONNECTION


def validate_parquet_schema(connection: duckdb.DuckDBPyConnection, path: str) -> None:
    """Ensure the parquet file contains the columns TNS expects."""
    columns = {
        row[0]
        for row in connection.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{quote_sql_string(path)}')"
        ).fetchall()
    }
    missing = REQUIRED_COLUMNS.difference(columns)
    if missing:
        raise ValueError(f"{path} is missing required columns: {sorted(missing)}")


def build_intersection_query(aoi_path: str, tile_paths: list[str]) -> str:
    """Build the DuckDB spatial join query for the given parquet inputs."""
    if not tile_paths:
        raise ValueError("At least one tile GeoParquet path is required")

    quoted_tiles = ", ".join(f"'{quote_sql_string(path)}'" for path in tile_paths)
    quoted_aoi = quote_sql_string(aoi_path)

    return f"""
        WITH aois AS (
            SELECT
                pk_and_model,
                ST_GeomFromWKB(geometry) AS geometry
            FROM read_parquet('{quoted_aoi}')
        ),
        tiles AS (
            SELECT
                pk_and_model,
                ST_GeomFromWKB(geometry) AS geometry
            FROM read_parquet([{quoted_tiles}], union_by_name=true)
        )
        SELECT
            aois.pk_and_model AS aois,
            list(DISTINCT tiles.pk_and_model ORDER BY tiles.pk_and_model) AS tiles
        FROM aois
        JOIN tiles
          ON ST_Intersects(aois.geometry, tiles.geometry)
        GROUP BY aois.pk_and_model
        ORDER BY aois.pk_and_model
    """


def compare_geoparquets(
    aoi_uri: str,
    tile_uris: list[str],
    output_uri: str,
    store: GeoParquetStore,
) -> CompareArtifacts:
    """Compare AOIs and tiles with DuckDB, persist the result, and summarize it."""
    local_aoi_path = store.materialize_input(aoi_uri)
    local_tile_paths = [store.materialize_input(uri) for uri in tile_uris]

    with TemporaryDirectory(prefix="tns-duckdb-") as temp_dir:
        local_output = str(Path(temp_dir) / "intersects.parquet")
        connection = connect_duckdb()
        validate_parquet_schema(connection, local_aoi_path)
        for path in local_tile_paths:
            validate_parquet_schema(connection, path)

        query = build_intersection_query(local_aoi_path, local_tile_paths)
        rows = connection.execute(query).fetchall()
        connection.execute(
            f"COPY ({query}) TO '{quote_sql_string(local_output)}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )

        resolved_output_uri = store.persist_output(local_output, output_uri)
        matched_aois = [row[0] for row in rows]
        return CompareArtifacts(
            matched_aois=matched_aois,
            output_uri=resolved_output_uri,
            row_count=len(rows),
        )
