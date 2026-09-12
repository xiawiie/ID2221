from pathlib import Path

import yaml

from urban_data.paths import CONFIG, PROJECT_ROOT


def load_project_config() -> dict:
    return yaml.safe_load(CONFIG.read_text(encoding="utf-8"))


def load_datasets_config() -> dict:
    return load_project_config()["datasets"]


def resolve_dataset(key: str) -> dict:
    project = load_project_config()
    datasets = project["datasets"]
    if key not in datasets:
        known = ", ".join(sorted(datasets))
        raise KeyError(f"Unknown dataset {key!r}; known: {known}")
    cfg = dict(datasets[key])
    cfg["key"] = key
    cfg["schema_version"] = str(project["schema_version"])
    cfg["source_path"] = PROJECT_ROOT / cfg["path"]
    return cfg
