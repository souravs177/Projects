from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class DatasetConfig:
    name: str
    path: str
    format: str = "csv"
    schema: str = ""
    options: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class StorageConfig:
    raw_path: str
    bronze_path: str
    silver_path: str
    gold_path: str
    metadata_path: str = "data/system"
    bronze_format: str = "delta"
    silver_format: str = "delta"
    gold_format: str = "delta"

    def by_layer(self, layer: str) -> str:
        layer_paths = {
            "raw": self.raw_path,
            "bronze": self.bronze_path,
            "silver": self.silver_path,
            "gold": self.gold_path,
        }
        try:
            return layer_paths[layer]
        except KeyError as error:
            raise ValueError(f"Unsupported layer '{layer}'.") from error

    def format_by_layer(self, layer: str) -> str:
        layer_formats = {
            "bronze": self.bronze_format,
            "silver": self.silver_format,
            "gold": self.gold_format,
        }
        try:
            return layer_formats[layer].lower()
        except KeyError as error:
            raise ValueError(f"Unsupported layer '{layer}'.") from error


@dataclass(frozen=True)
class SparkConfig:
    app_name: str
    master: str = "local[*]"
    shuffle_partitions: int = 4


@dataclass(frozen=True)
class IncrementalConfig:
    enabled: bool = True
    bronze_merge_keys: dict[str, list[str]] = field(default_factory=dict)
    silver_merge_keys: dict[str, list[str]] = field(default_factory=dict)

    def merge_keys(self, layer: str, dataset_name: str) -> list[str]:
        layer_keys = {
            "bronze": self.bronze_merge_keys,
            "silver": self.silver_merge_keys,
        }
        try:
            keys = layer_keys[layer].get(dataset_name, [])
        except KeyError as error:
            raise ValueError(f"Unsupported incremental layer '{layer}'.") from error
        return [str(key) for key in keys]


@dataclass(frozen=True)
class PipelineConfig:
    project_name: str
    root_dir: Path
    storage: StorageConfig
    spark: SparkConfig
    incremental: IncrementalConfig
    datasets: dict[str, DatasetConfig]
    write_mode: str = "overwrite"

    def resolve_path(self, relative_path: str | Path) -> Path:
        return (self.root_dir / Path(relative_path)).resolve()

    def layer_path(self, layer: str, *parts: str) -> Path:
        return self.resolve_path(Path(self.storage.by_layer(layer), *parts))

    def raw_dataset_path(self, dataset_name: str) -> Path:
        dataset = self.datasets[dataset_name]
        return self.layer_path("raw", dataset.path)

    def output_path(self, layer: str, dataset_name: str) -> Path:
        return self.layer_path(layer, dataset_name)

    def output_format(self, layer: str) -> str:
        return self.storage.format_by_layer(layer)

    def metadata_path(self, *parts: str) -> Path:
        return self.resolve_path(Path(self.storage.metadata_path, *parts))

    def merge_keys(self, layer: str, dataset_name: str) -> list[str]:
        return self.incremental.merge_keys(layer, dataset_name)

    def uses_delta(self) -> bool:
        return any(
            self.output_format(layer) == "delta"
            for layer in ("bronze", "silver", "gold")
        )


def load_config(config_path: str | Path) -> PipelineConfig:
    resolved_path = Path(config_path).resolve()
    raw_config = yaml.safe_load(resolved_path.read_text(encoding="utf-8"))

    storage = StorageConfig(**raw_config["storage"])
    spark = SparkConfig(**raw_config["spark"])
    incremental = IncrementalConfig(
        enabled=bool(raw_config.get("incremental", {}).get("enabled", True)),
        bronze_merge_keys={
            str(name): [str(key) for key in keys]
            for name, keys in raw_config.get("incremental", {}).get("bronze_merge_keys", {}).items()
        },
        silver_merge_keys={
            str(name): [str(key) for key in keys]
            for name, keys in raw_config.get("incremental", {}).get("silver_merge_keys", {}).items()
        },
    )
    datasets = {
        name: DatasetConfig(
            name=name,
            path=dataset_config["path"],
            format=dataset_config.get("format", "csv"),
            schema=dataset_config.get("schema", name),
            options={str(key): str(value) for key, value in dataset_config.get("options", {}).items()},
        )
        for name, dataset_config in raw_config["datasets"].items()
    }

    return PipelineConfig(
        project_name=raw_config["project_name"],
        root_dir=resolved_path.parent.parent.resolve(),
        storage=storage,
        spark=spark,
        incremental=incremental,
        datasets=datasets,
        write_mode=raw_config.get("write_mode", "overwrite"),
    )
