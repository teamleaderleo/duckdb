from __future__ import annotations

import copy
import unittest

from tools.fieldwork.validate_art_wal_shutdown_fix import classify_lifecycle


ROW_COUNT = 2


def common_bind() -> dict[str, object]:
    return {
        "full_count": ROW_COUNT,
        "first_values": [1, 2],
        "sequential_filtered_count": 1,
        "sequential_terminal_count": 1,
        "explicit_checkpoint_executed": True,
        "wal_after_explicit_checkpoint_while_open": {"present": False},
        "wal_after_connection_close": {"present": False},
    }


def common_inspection() -> dict[str, object]:
    return {
        "full_count": ROW_COUNT,
        "first_values": [1, 2],
        "sequential_filtered_count": 1,
        "sequential_terminal_count": 1,
        "wal_before_read_only_open": {"present": False},
        "wal_after_read_only_close": {"present": False},
    }


class ArtWalShutdownLifecycleClassifierTest(unittest.TestCase):
    def test_parent_corrupt_shape(self) -> None:
        shutdown = {
            "wal_before_shutdown": {"present": True},
            "wal_after_shutdown": {"present": False},
        }
        bind = common_bind()
        bind.update(
            {
                "wal_before_bind": {"present": False},
                "enabled_filtered_count": 0,
                "enabled_terminal_count": 0,
            }
        )
        inspection = common_inspection()
        inspection.update(
            {
                "enabled_filtered_count": 0,
                "enabled_terminal_count": 0,
            }
        )

        result = classify_lifecycle(
            "parent", shutdown, bind, inspection, ROW_COUNT
        )
        self.assertEqual(
            result["classification"], "parent-corrupt-shutdown-checkpoint"
        )
        self.assertIs(
            result["wal_preserved_after_context_free_shutdown"], False
        )

    def test_candidate_preserve_bind_checkpoint_shape(self) -> None:
        shutdown = {
            "wal_before_shutdown": {"present": True},
            "wal_after_shutdown": {"present": True},
        }
        bind = common_bind()
        bind.update(
            {
                "wal_before_bind": {"present": True},
                "enabled_filtered_count": 1,
                "enabled_terminal_count": 1,
            }
        )
        inspection = common_inspection()
        inspection.update(
            {
                "enabled_filtered_count": 1,
                "enabled_terminal_count": 1,
            }
        )

        result = classify_lifecycle(
            "candidate", shutdown, bind, inspection, ROW_COUNT
        )
        self.assertEqual(
            result["classification"],
            "candidate-preserves-wal-and-heals-after-bind",
        )
        self.assertIs(
            result["wal_preserved_after_context_free_shutdown"], True
        )

    def test_ambiguous_or_broken_controls_fail_closed(self) -> None:
        shutdown = {
            "wal_before_shutdown": {"present": True},
            "wal_after_shutdown": {"present": True},
        }
        bind = common_bind()
        bind.update(
            {
                "wal_before_bind": {"present": True},
                "enabled_filtered_count": 1,
                "enabled_terminal_count": 1,
            }
        )
        inspection = common_inspection()
        inspection.update(
            {
                "enabled_filtered_count": 1,
                "enabled_terminal_count": 1,
            }
        )

        broken = copy.deepcopy(bind)
        broken["sequential_filtered_count"] = 0
        with self.assertRaisesRegex(ValueError, "common lifecycle controls"):
            classify_lifecycle(
                "broken-control", shutdown, broken, inspection, ROW_COUNT
            )

        ambiguous = copy.deepcopy(bind)
        ambiguous["enabled_filtered_count"] = 0
        with self.assertRaisesRegex(ValueError, "neither uniquely"):
            classify_lifecycle(
                "ambiguous", shutdown, ambiguous, inspection, ROW_COUNT
            )


if __name__ == "__main__":
    unittest.main()
