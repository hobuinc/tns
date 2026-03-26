"""Unit tests for the local GeoParquet intersection workflow."""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import pyarrow.parquet as pq

from tns_core import (
    FilesystemGeoParquetStore,
    combine_tile_frames,
    compare_geoparquets,
    intersect_geodataframes,
)


def test_intersect_geodataframes_matches_all_states(
    aois_gdf: gpd.GeoDataFrame, tiles_gdf: gpd.GeoDataFrame
):
    """Every AOI should intersect the tile derived from the same geometry."""
    result = intersect_geodataframes(aois_gdf, tiles_gdf)
    assert len(result.index) == len(aois_gdf.index)
    assert set(result["aois"]) == set(aois_gdf["pk_and_model"])
    tile_lookup = dict(zip(result["aois"], result["tiles"]))
    for index, aoi_name in enumerate(aois_gdf["pk_and_model"]):
        assert f"raster_{index}" in tile_lookup[aoi_name]


def test_compare_geoparquets_writes_output_parquet(
    aois_gdf: gpd.GeoDataFrame, tiles_gdf: gpd.GeoDataFrame, parquet_dir: Path
):
    """Full parquet comparison should write a result table with all AOIs."""
    store = FilesystemGeoParquetStore()
    aoi_path = parquet_dir / "subscriptions.parquet"
    tile_path = parquet_dir / "tiles.parquet"
    output_path = parquet_dir / "intersects" / "result.parquet"

    aois_gdf.to_parquet(aoi_path)
    tiles_gdf.to_parquet(tile_path)

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
    assert set(result_table.column("aois").to_pylist()) == set(
        aois_gdf["pk_and_model"]
    )


def test_combine_tile_frames_accepts_multiple_geoparquets(
    split_tiles_gdf: gpd.GeoDataFrame, parquet_dir: Path
):
    """Tile inputs from multiple parquet files should merge into one frame."""
    store = FilesystemGeoParquetStore()
    first = parquet_dir / "tiles-1.parquet"
    second = parquet_dir / "tiles-2.parquet"

    split_tiles_gdf.iloc[:5].to_parquet(first)
    split_tiles_gdf.iloc[5:].to_parquet(second)

    combined = combine_tile_frames([str(first), str(second)], store)
    assert len(combined.index) == len(split_tiles_gdf.index)
    assert set(combined["pk_and_model"]) == set(split_tiles_gdf["pk_and_model"])
