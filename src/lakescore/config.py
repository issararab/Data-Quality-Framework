"""Runtime configuration for LakeScore notebooks and jobs.

Centralizes the values that previously appeared as hardcoded literals scattered
across notebooks and generator modules (catalog name, LLM/vector-search endpoint
names, default thresholds), so a single workspace-specific `LakeScoreConfig` can be
constructed once and passed through the pipeline.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class LakeScoreConfig:
    """Workspace-specific settings for a LakeScore run.

    Parameters:
        catalog_name (str): The Unity Catalog catalog LakeScore monitors and writes its
            `data_quality` schema into.
        schema_name (str): The schema name for LakeScore's own tables. Defaults to
            `"data_quality"`.
        llm_model_endpoint (str): Databricks Model Serving endpoint name used for GenAI
            description/check generation.
        vector_search_endpoint (str): Databricks Vector Search endpoint name hosting the
            SodaCL knowledge-base index.
        vector_search_index (str): Fully qualified vector-search index name
            (`<catalog>.<schema>.knowledge_base_index`).
        low_cardinality_threshold (int): Default max distinct values to classify a column
            as low-cardinality.
        freshness_window (str): Default freshness evaluation window (e.g. `"1d"`).
        validity_window (str): Default validity evaluation window (e.g. `"1d"`).
    """

    catalog_name: str
    schema_name: str = "data_quality"
    llm_model_endpoint: str = "databricks-meta-llama-3-1-70b-instruct"
    vector_search_endpoint: str = "sodacl_indexer"
    vector_search_index: str = ""
    low_cardinality_threshold: int = 10
    freshness_window: str = "1d"
    validity_window: str = "1d"

    def __post_init__(self) -> None:
        if not self.vector_search_index:
            object.__setattr__(
                self,
                "vector_search_index",
                f"{self.catalog_name}.{self.schema_name}.knowledge_base_index",
            )

    @property
    def knowledge_base_table(self) -> str:
        """Fully qualified path to the SodaCL knowledge-base Delta table."""
        return f"{self.catalog_name}.{self.schema_name}.knowledge_base"

    @classmethod
    def from_widgets(cls, dbutils: object, **overrides: str) -> LakeScoreConfig:
        """Builds a `LakeScoreConfig` from Databricks notebook/job widgets.

        Reads `catalog_name`, `llm_model_endpoint`, and `vector_search_endpoint` widgets
        via `dbutils.widgets.get(...)` where available, falling back to this class's
        defaults (or an explicit override) when a widget is unset. Intended for use from
        a Databricks notebook, where `dbutils` is already available in scope.

        Parameters:
            dbutils (object): The notebook's `dbutils` instance (accepts `object` since
                `dbutils` has no importable type outside a Databricks runtime).
            **overrides (str): Explicit values that take precedence over widgets and
                environment variables, e.g. for tests.

        Returns:
            LakeScoreConfig: The resolved configuration.
        """

        def _get(name: str, default: str | None = None) -> str | None:
            if name in overrides:
                return overrides[name]
            try:
                return dbutils.widgets.get(name)  # type: ignore[attr-defined]
            except Exception:
                return os.environ.get(f"LAKESCORE_{name.upper()}", default)

        catalog_name = _get("catalog_name")
        if not catalog_name:
            raise ValueError("catalog_name widget/env var is required to build LakeScoreConfig")

        field_defaults = {
            f: getattr(cls, f, None) for f in ("llm_model_endpoint", "vector_search_endpoint")
        }
        kwargs: dict[str, Any] = {"catalog_name": catalog_name}
        for field, default in field_defaults.items():
            value = _get(field, default)
            if value:
                kwargs[field] = value
        return cls(**kwargs)

    @classmethod
    def from_env(cls, catalog_name: str | None = None, **overrides: str) -> LakeScoreConfig:
        """Builds a `LakeScoreConfig` from environment variables (`LAKESCORE_*`), for local
        development and tests where no `dbutils` is available.

        Parameters:
            catalog_name (str | None): Overrides the `LAKESCORE_CATALOG_NAME` env var.
            **overrides (str): Additional field overrides.

        Returns:
            LakeScoreConfig: The resolved configuration.
        """
        resolved_catalog = catalog_name or os.environ.get("LAKESCORE_CATALOG_NAME")
        if not resolved_catalog:
            raise ValueError("catalog_name must be provided or set via LAKESCORE_CATALOG_NAME")

        kwargs: dict[str, Any] = {"catalog_name": resolved_catalog}
        for field in ("llm_model_endpoint", "vector_search_endpoint", "schema_name"):
            env_value = os.environ.get(f"LAKESCORE_{field.upper()}")
            if env_value:
                kwargs[field] = env_value
        kwargs.update(overrides)
        return cls(**kwargs)
