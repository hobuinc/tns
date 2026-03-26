"""Unit tests for the local DuckDB GeoParquet intersection workflow."""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

from conftest import write_geometry_parquet
from tns_core import (
    FilesystemGeoParquetStore,
    build_intersection_query,
    compare_geoparquets,
    connect_duckdb,
)


def test_build_intersection_query_matches_all_states(
    aoi_rows: list[dict[str, str]], tile_rows: list[dict[str, str]], parquet_dir: Path
):
    """Every AOI should intersect the tile derived from the same geometry."""
    aoi_path = parquet_dir / "subscriptions.parquet"
    tile_path = parquet_dir / "tiles.parquet"
    write_geometry_parquet(aoi_path, aoi_rows)
    write_geometry_parquet(tile_path, tile_rows)

    with connect_duckdb() as connection:
        query = build_intersection_query(str(aoi_path), [str(tile_path)])
        rows = connection.execute(query).fetchall()

    assert len(rows) == len(aoi_rows)
    lookup = {row[0]: row[1] for row in rows}
    for index, row in enumerate(aoi_rows):
        assert f"raster_{index}" in lookup[row["pk_and_model"]]


def test_compare_geoparquets_writes_output_parquet(
    aoi_rows: list[dict[str, str]], tile_rows: list[dict[str, str]], parquet_dir: Path
):
    """Full parquet comparison should write a result table with all AOIs."""
    store = FilesystemGeoParquetStore()
    aoi_path = parquet_dir / "subscriptions.parquet"
    tile_path = parquet_dir / "tiles.parquet"
    output_path = parquet_dir / "intersects" / "result.parquet"

    write_geometry_parquet(aoi_path, aoi_rows)
    write_geometry_parquet(tile_path, tile_rows)

    artifacts = compare_geoparquets(
        aoi_uri=str(aoi_path),
        tile_uris=[str(tile_path)],
        output_uri=str(output_path),
        store=store,
    )

    assert artifacts.row_count == 50
    assert output_path.exists()

    result_table = pq.read_table(output_path)
    assert result_table.num_rows == 50
    assert set(result_table.column("aois").to_pylist()) == {
        row["pk_and_model"] for row in aoi_rows
    }


def test_multiple_tile_parquets_are_joined_together(
    split_tile_rows: list[dict[str, str]], parquet_dir: Path
):
    """Tile inputs from multiple parquet files should join in one query."""
    first = parquet_dir / "tiles-1.parquet"
    second = parquet_dir / "tiles-2.parquet"
    aoi_path = parquet_dir / "subscriptions.parquet"

    write_geometry_parquet(aoi_path, split_tile_rows)
    write_geometry_parquet(first, split_tile_rows[:5])
    write_geometry_parquet(second, split_tile_rows[5:])

    with connect_duckdb() as connection:
        query = build_intersection_query(str(aoi_path), [str(first), str(second)])
        rows = connection.execute(query).fetchall()

    assert len(rows) == len(split_tile_rows)
