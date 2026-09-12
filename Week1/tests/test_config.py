from pathlib import Path
import unittest

from urban_data.config import load_datasets_config, resolve_dataset
from urban_data.paths import CONFIG, PROJECT_ROOT


class ConfigTest(unittest.TestCase):
    def test_config_paths_exist(self):
        self.assertTrue(CONFIG.is_file())
        datasets = load_datasets_config()
        self.assertIn("taxi_2024_01", datasets)
        cfg = resolve_dataset("taxi_2024_01")
        self.assertEqual(
            cfg["source_path"],
            PROJECT_ROOT / "datasets/yellow_tripdata_2024-01.parquet",
        )
        self.assertTrue(cfg["source_path"].is_file())


    def test_all_dataset_sources_exist(self):
        for key in load_datasets_config():
            cfg = resolve_dataset(key)
            self.assertTrue(cfg["source_path"].is_file(), key)


if __name__ == "__main__":
    unittest.main()
