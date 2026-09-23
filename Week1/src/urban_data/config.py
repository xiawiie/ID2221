from pathlib import Path

import yaml

from urban_data.paths import CONFIG, DATASETS, PROJECT_ROOT

UPDATES_DIR = DATASETS / "updates"
UPDATE_MANIFEST = UPDATES_DIR / "manifest.json"
VALIDATION_RULES_CONFIG = PROJECT_ROOT / "config" / "validation_rules.yml"


def load_project_config() -> dict:
    return yaml.safe_load(CONFIG.read_text(encoding="utf-8"))


def load_datasets_config() -> dict:
    return load_project_config()["datasets"]


def load_validation_rules_config() -> dict:
    return yaml.safe_load(VALIDATION_RULES_CONFIG.read_text(encoding="utf-8"))


def resolve_dataset(key: str) -> dict:
    project = load_project_config()
    datasets = project["datasets"]
    if key not in datasets:
        known = ", ".join(sorted(datasets))
        raise KeyError(f"Unknown dataset {key!r}; known: {known}")
    cfg = dict(datasets[key])
    cfg["key"] = key
    cfg["schema_version"] = str(cfg.get("schema_version", project["schema_version"]))
    cfg["source_path"] = PROJECT_ROOT / cfg["path"]
    return cfg


def incremental_update_keys() -> list[str]:
    project = load_project_config()
    return sorted(
        key
        for key, cfg in project["datasets"].items()
        if cfg.get("mode") == "incremental"
    )
