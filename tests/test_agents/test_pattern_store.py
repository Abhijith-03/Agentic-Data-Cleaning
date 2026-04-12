"""Tests for the auto-learning pattern store."""

import tempfile
from pathlib import Path

from src.knowledge.pattern_store import LearnedPattern, PatternStore


class TestPatternStore:
    def _get_store(self):
        tmp = tempfile.mktemp(suffix=".db")
        return PatternStore(db_path=tmp), tmp

    def test_add_and_find(self):
        store, _ = self._get_store()
        pattern = LearnedPattern(
            column_pattern=".*email.*",
            value_pattern=".*@.*",
            fix_template="{value}",
            domain="generic",
            success_count=5,
            fail_count=0,
        )
        pid = store.add_pattern(pattern)
        assert pid > 0

        match = store.find_match("user_email", "test@bad", domain="generic")
        assert match is not None
        assert match.id == pid
        store.close()

    def test_no_match(self):
        store, _ = self._get_store()
        match = store.find_match("address", "123 Main St", domain="generic")
        assert match is None
        store.close()

    def test_success_failure_tracking(self):
        store, _ = self._get_store()
        pattern = LearnedPattern(
            column_pattern=".*",
            value_pattern=".*",
            fix_template="{value}",
            success_count=10,
            fail_count=0,
        )
        pid = store.add_pattern(pattern)

        store.record_failure(pid)
        patterns = store.list_patterns()
        assert patterns[0].fail_count == 1
        assert patterns[0].confidence < 1.0
        store.close()

    def test_apply_template(self):
        store, _ = self._get_store()
        pattern = LearnedPattern(
            column_pattern=".*",
            value_pattern=".*",
            fix_template="{value}",
        )
        result = store.apply_template(pattern, "hello")
        assert result == "hello"
        store.close()

    def test_list_by_domain(self):
        store, _ = self._get_store()
        store.add_pattern(LearnedPattern(
            column_pattern=".*", value_pattern=".*", fix_template="{value}",
            domain="healthcare",
        ))
        store.add_pattern(LearnedPattern(
            column_pattern=".*", value_pattern=".*", fix_template="{value}",
            domain="finance",
        ))

        hc = store.list_patterns(domain="healthcare")
        assert len(hc) == 1
        all_p = store.list_patterns()
        assert len(all_p) == 2
        store.close()
