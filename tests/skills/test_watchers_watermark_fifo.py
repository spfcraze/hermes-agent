"""Regression tests for the watchers watermark FIFO eviction.

The shared watermark helper (optional-skills/devops/watchers/scripts/_watermark.py)
bounds stored seen-IDs at max_seen. It previously rebuilt the retained list from
``list(set(...))``, which orders by string hash — randomized per process by
PYTHONHASHSEED. Once a watcher saturated the cap, trimming evicted an arbitrary
and rerun-variable slice, so an evicted ID still present in the source feed was
re-delivered as new (silent duplicate notifications).

Contract under test (from the issue): trimming evicts the OLDEST IDs first, and
a given stored state plus a given batch always produces the same retained set.

These tests import the real skill helper with no Hermes runtime, mirroring the
minimal repro in the bug report.
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

import pytest

_SCRIPTS = (
    Path(__file__).resolve().parents[2]
    / "optional-skills" / "devops" / "watchers" / "scripts"
)
sys.path.insert(0, str(_SCRIPTS))

from _watermark import Watermark  # noqa: E402


@pytest.fixture(autouse=True)
def _isolated_state_dir(monkeypatch: pytest.MonkeyPatch) -> None:
    """Point WATCHER_STATE_DIR at a tempdir so tests never touch real state."""
    monkeypatch.setenv("WATCHER_STATE_DIR", tempfile.mkdtemp())


def _saturated(max_seen: int = 10, n: int = 10) -> Watermark:
    """A watermark already at the cap with ids id-0..id-(n-1) (oldest first)."""
    wm = Watermark("dbg", max_seen=max_seen)
    wm._data = {"seen_ids": [f"id-{i}" for i in range(n)], "first_run": False}
    return wm


def test_evicts_oldest_first() -> None:
    """At the cap, adding new IDs evicts the OLDEST stored IDs, not an arbitrary set."""
    wm = _saturated()
    before = wm.seen
    wm.filter_new([{"id": f"id-{i}"} for i in range(8, 12)])  # 2 new -> 2 evicted
    evicted = [x for x in before if x not in wm.seen]
    assert evicted == ["id-0", "id-1"]
    # newest 10 of 12 retained, in insertion order
    assert wm.seen == [f"id-{i}" for i in range(2, 12)]


def test_deterministic_retained_set_across_instances() -> None:
    """Same stored state + same batch must always produce the same retained set."""
    results = []
    for _ in range(20):
        wm = _saturated()
        wm.filter_new([{"id": f"id-{i}"} for i in range(8, 12)])
        results.append(tuple(wm.seen))
    assert len(set(results)) == 1


def test_recent_id_within_cap_stays_suppressed() -> None:
    """An ID still inside the retained window must not be re-emitted."""
    wm = _saturated()
    wm.filter_new([{"id": f"id-{i}"} for i in range(8, 12)])
    again = wm.filter_new([{"id": "id-5"}])  # id-5 is still stored
    assert again == []


def test_retained_set_exactly_capped() -> None:
    """After trimming the retained list never exceeds max_seen."""
    wm = _saturated(max_seen=10, n=10)
    wm.filter_new([{"id": f"id-{i}"} for i in range(10, 30)])  # 20 new
    assert len(wm.seen) == 10
    # all 20 new id-10..id-29 are recorded; oldest 10 of the 30 dropped
    assert wm.seen == [f"id-{i}" for i in range(20, 30)]


def test_no_eviction_below_cap() -> None:
    """Below max_seen nothing is dropped; order is preserved."""
    wm = Watermark("dbg", max_seen=500)
    wm._data = {"seen_ids": [f"id-{i}" for i in range(3)], "first_run": False}
    wm.filter_new([{"id": "id-9"}, {"id": "id-10"}])
    assert wm.seen == ["id-0", "id-1", "id-2", "id-9", "id-10"]
