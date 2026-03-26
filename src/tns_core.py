"""Core GeoParquet comparison utilities for TNS.

This module keeps the spatial comparison path free of AWS concerns so it can
be tested locally with repository fixtures and reused by different runtime
adapters.
"""

from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Protocol

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from shapely.strtree import STRtree


GEO_CRS = "EPSG:4326"
REQUIRED_COLUMNS = {"pk_and_model", "geometry"}


class GeoParquetStore(Protocol):
    def read_geodataframe(self, uri: str) -> gpd.GeoDataFrame:
        """Read a GeoParquet file into a GeoDataFrame."""

    def write_parquet_table(self, uri: str, table: pa.Table) -> str:
        """Write a Parquet table and return the resolved URI."""


@dataclass(frozen=True)
class CompareArtifacts:
    """Structured output from a completed AOI/tile comparison run."""

    matched_aois: list[str]
    output_uri: str
    row_count: int


class FilesystemGeoParquetStore:
    """GeoParquet store implementation backed by the local filesystem."""

    def read_geodataframe(self, uri: str) -> gpd.GeoDataFrame:
        """Read a local GeoParquet file into a GeoDataFrame."""
        return gpd.read_parquet(BytesIO(Path(uri).read_bytes()))

    def write_parquet_table(self, uri: str, table: pa.Table) -> str:
        """Write a Parquet table to a local path and return that path."""
        path = Path(uri)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path)
        return str(path)


class S3GeoParquetStore:
    """GeoParquet store implementation that reads and writes through S3."""

    def __init__(self, s3_client):
        """Create a store with a boto3-compatible S3 client."""
        self.s3 = s3_client

    def read_geodataframe(self, uri: str) -> gpd.GeoDataFrame:
        """Read a GeoParquet object from S3 into a GeoDataFrame."""
        bucket, key = parse_s3_uri(uri)
        body = self.s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        return gpd.read_parquet(BytesIO(body))

    def write_parquet_table(self, uri: str, table: pa.Table) -> str:
        """Serialize a Parquet table and upload it to S3."""
        bucket, key = parse_s3_uri(uri)
        buffer = BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        self.s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
        return uri


def parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split an S3 URI into bucket and key components."""
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected S3 URI, got: {uri}")
    without_scheme = uri[5:]
    bucket, _, key = without_scheme.partition("/")
    if not bucket or not key:
        raise ValueError(f"Malformed S3 URI: {uri}")
    return bucket, key


def ensure_required_columns(frame: gpd.GeoDataFrame, dataset_name: str) -> None:
    """Validate that a GeoDataFrame includes the columns TNS expects."""
    missing = REQUIRED_COLUMNS.difference(frame.columns)
    if missing:
        raise ValueError(
            f"{dataset_name} is missing required columns: {sorted(missing)}"
        )


def normalize_geometries(frame: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Normalize a GeoDataFrame into the project CRS."""
    normalized = frame.copy()
    if normalized.crs is None:
        normalized = normalized.set_crs(GEO_CRS)
    elif normalized.crs.to_string() != GEO_CRS:
        normalized = normalized.to_crs(GEO_CRS)
    return normalized


def load_geodataframe(
    uri: str, store: GeoParquetStore, dataset_name: str
) -> gpd.GeoDataFrame:
    """Load, validate, and normalize a GeoParquet dataset."""
    frame = store.read_geodataframe(uri)
    ensure_required_columns(frame, dataset_name)
    return normalize_geometries(frame)


def combine_tile_frames(
    tile_uris: list[str], store: GeoParquetStore
) -> gpd.GeoDataFrame:
    """Read multiple tile GeoParquet files into one GeoDataFrame."""
    if not tile_uris:
        raise ValueError("At least one tile GeoParquet path is required")

    frames = [
        load_geodataframe(uri, store, f"tile dataset {uri}") for uri in tile_uris
    ]
    combined = pd.concat(frames, ignore_index=True)
    return gpd.GeoDataFrame(combined, geometry="geometry", crs=frames[0].crs)


def intersect_geodataframes(
    aois: gpd.GeoDataFrame, tiles: gpd.GeoDataFrame
) -> pd.DataFrame:
    """Compute AOI-to-tile intersections using a spatial index."""
    if aois.empty or tiles.empty:
        return pd.DataFrame(columns=["aois", "tiles"])

    tile_geometries = list(tiles.geometry)
    tile_names = tiles["pk_and_model"].tolist()
    tree = STRtree(tile_geometries)
    rows: list[dict[str, object]] = []

    for aoi_name, geometry in zip(aois["pk_and_model"], aois.geometry):
        candidate_indexes = tree.query(geometry, predicate="intersects")
        matches = sorted({tile_names[index] for index in candidate_indexes})
        if matches:
            rows.append({"aois": aoi_name, "tiles": matches})

    return pd.DataFrame(rows, columns=["aois", "tiles"])


def compare_geoparquets(
    aoi_uri: str,
    tile_uris: list[str],
    output_uri: str,
    store: GeoParquetStore,
) -> CompareArtifacts:
    """Compare AOIs and tiles, persist the result parquet, and summarize it."""
    aois = load_geodataframe(aoi_uri, store, "AOI dataset")
    tiles = combine_tile_frames(tile_uris, store)
    result_frame = intersect_geodataframes(aois, tiles)
    output_table = pa.Table.from_pandas(result_frame, preserve_index=False)
    resolved_output_uri = store.write_parquet_table(output_uri, output_table)

    return CompareArtifacts(
        matched_aois=result_frame["aois"].tolist(),
        output_uri=resolved_output_uri,
        row_count=len(result_frame.index),
    )
