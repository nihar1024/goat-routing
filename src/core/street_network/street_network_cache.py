import os
from uuid import UUID

import polars as pl
from polars import DataFrame

from src.core.config import settings
from src.utils import print_warning


class StreetNetworkCache:
    def __init__(self):
        """Initialize the cache directory if it does not exist."""

        if not os.path.exists(settings.CACHE_DIR):
            os.makedirs(settings.CACHE_DIR)

    def _get_edge_cache_file_name(
        self,
        edge_layer_id: UUID,
        h3_short: int,
    ):
        """Get edge cache file path for the specified H3_3 cell."""

        return os.path.join(
            settings.CACHE_DIR,
            f"{str(edge_layer_id)}_{str(h3_short)}_edge.parquet",
        )

    def _get_node_cache_file_name(
        self,
        node_layer_id: UUID,
        h3_short: int,
    ):
        """Get node cache file path for the specified H3_3 cell."""

        return os.path.join(
            settings.CACHE_DIR,
            f"{node_layer_id}_{str(h3_short)}_node.parquet",
        )

    def edge_cache_exists(self, edge_layer_id: UUID, h3_short: int):
        """Check if edge data for the specified H3_3 cell is cached."""

        edge_cache_file = self._get_edge_cache_file_name(edge_layer_id, h3_short)
        return os.path.exists(edge_cache_file)

    def node_cache_exists(self, node_layer_id: UUID, h3_short: int):
        """Check if node data for the specified H3_3 cell is cached."""

        node_cache_file = self._get_node_cache_file_name(node_layer_id, h3_short)
        return os.path.exists(node_cache_file)

    def read_edge_cache(
        self,
        edge_layer_id: UUID,
        h3_short: int,
    ):
        """Read edge data for the specified H3_3 cell from cache."""

        edge_df: DataFrame | None = None

        edge_cache_file = self._get_edge_cache_file_name(edge_layer_id, h3_short)

        try:
            with open(edge_cache_file, "rb") as file:
                edge_df = pl.read_parquet(file)
        except Exception:
            error_msg = f"Failed to read edge data for H3_3 cell {h3_short} from cache."
            raise ValueError(error_msg)

        return edge_df

    def read_node_cache(
        self,
        node_layer_id: UUID,
        h3_short: int,
    ):
        """Read node data for the specified H3_3 cell from cache."""

        node_df: DataFrame | None = None

        node_cache_file = self._get_node_cache_file_name(node_layer_id, h3_short)

        try:
            with open(node_cache_file, "rb") as file:
                node_df = pl.read_parquet(file)
        except Exception:
            error_msg = f"Failed to read node data for H3_3 cell {h3_short} from cache."
            raise ValueError(error_msg)

        return node_df

    def write_edge_cache(
        self,
        edge_layer_id: UUID,
        h3_short: int,
        edge_df: DataFrame,
    ):
        """Write edge data for the specified H3_3 cell into cache."""

        edge_cache_file = self._get_edge_cache_file_name(edge_layer_id, h3_short)

        try:
            # Only write non-empty edge data into cache
            if not edge_df.is_empty():
                with open(edge_cache_file, "wb") as file:
                    edge_df.write_parquet(file)
            else:
                if settings.ENVIRONMENT == "dev":
                    print_warning(
                        f"Skipping H3_3 cell {h3_short}, street network is empty or unavailable."
                    )
        except Exception:
            # Clean up cache file if writing fails
            if os.path.exists(edge_cache_file):
                os.remove(edge_cache_file)
            error_msg = (
                f"Failed to write edge data for H3_3 cell {h3_short} into cache."
            )
            raise RuntimeError(error_msg)

    def write_node_cache(
        self,
        node_layer_id: UUID,
        h3_short: int,
        node_df: DataFrame,
    ):
        """Write node data for the specified H3_3 cell into cache."""

        node_cache_file = self._get_node_cache_file_name(node_layer_id, h3_short)

        try:
            with open(node_cache_file, "wb") as file:
                node_df.write_parquet(file)
        except Exception:
            # Clean up cache file if writing fails
            if os.path.exists(node_cache_file):
                os.remove(node_cache_file)
            error_msg = (
                f"Failed to write node data for H3_3 cell {h3_short} into cache."
            )
            raise RuntimeError(error_msg)
