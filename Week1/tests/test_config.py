from pathlib import Path

from urban_data.config import load_datasets_config, resolve_dataset
from urban_data.paths import CONFIG, PROJECT_ROOT


def test_config_paths_exist():
    assert CONFIG.is_file()
    datasets = load_datasets_config()
    assert "taxi_2024_01" in datasets
    cfg = resolve_dataset("taxi_2024_01")
    assert cfg["source_path"] == PROJECT_ROOT / "datasets/yellow_tripdata_2024-01.parquet"
    assert cfg["source_path"].is_file()


def test_all_dataset_sources_exist():
    for key in load_datasets_config():
        cfg = resolve_dataset(key)
        assert cfg["source_path"].is_file(), key
