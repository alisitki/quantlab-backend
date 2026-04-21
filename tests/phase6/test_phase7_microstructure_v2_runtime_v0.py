from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[2] / "tools" / "phase7_microstructure_v2_runtime_v0.py"


def load_module():
    spec = importlib.util.spec_from_file_location("phase7_microstructure_v2_runtime_v0", MODULE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable_to_load_module:{MODULE_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Phase7MicrostructureV2RuntimeV0Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = load_module()

    def test_resolve_processed_event_count_prefers_summary_fallback(self):
        item = {"processed_event_count": None}
        summary = {"processed_event_count": 10458}
        self.assertEqual(self.module.resolve_processed_event_count(item, summary), 10458)

    def test_resolve_processed_event_count_ignores_unknown(self):
        item = {"processed_event_count": "unknown"}
        summary = {"processed_event_count": "17"}
        self.assertEqual(self.module.resolve_processed_event_count(item, summary), 17)


if __name__ == "__main__":
    unittest.main()
