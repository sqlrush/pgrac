#!/usr/bin/env python3
"""Deterministic preflight for the two-node 1s-tail counter probe.

This is an offline fixture, not a native cluster run.  It fixes the 20-round
alternating-update shape and the counter/wait-event fields that a later
two-node harness must read from running backends.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable


WAIT_EVENT_NAMES = (
    "GES_TX_ENQUEUE_WAIT",
    "ClusterPcmCompatibleStateWait",
    "SpinDelay",
)

COUNTER_SYMBOLS = {
    "retransmit_attempts": (
        "src/backend/cluster/cluster_gcs_block.c",
        "cluster_gcs_get_block_retransmit_attempt_count",
    ),
    "txw_wakeups": (
        "src/backend/cluster/cluster_tx_enqueue.c",
        "cluster_txw_get_wakeup_count",
    ),
    "txw_timeouts": (
        "src/backend/cluster/cluster_tx_enqueue.c",
        "cluster_txw_get_timeout_count",
    ),
    "pcm_install_waits": (
        "src/backend/cluster/cluster_pcm_lock.c",
        "WAIT_EVENT_PCM_COMPATIBLE_STATE_WAIT",
    ),
}


@dataclass(frozen=True)
class CounterSnapshot:
    retransmit_attempts: int = 0
    txw_wakeups: int = 0
    txw_timeouts: int = 0
    pcm_install_waits: int = 0


@dataclass(frozen=True)
class WaitSample:
    round_no: int
    requester: int
    holder: int
    wait_event: str


class FixtureCounterReader:
    """Deterministic injected snapshots for the offline fixture."""

    def __init__(self, before: CounterSnapshot, after: CounterSnapshot):
        self._snapshots = {"before": before, "after": after}

    def __call__(self, phase: str) -> CounterSnapshot:
        return self._snapshots[phase]


def counter_delta(before: CounterSnapshot, after: CounterSnapshot) -> CounterSnapshot:
    return CounterSnapshot(
        retransmit_attempts=after.retransmit_attempts - before.retransmit_attempts,
        txw_wakeups=after.txw_wakeups - before.txw_wakeups,
        txw_timeouts=after.txw_timeouts - before.txw_timeouts,
        pcm_install_waits=after.pcm_install_waits - before.pcm_install_waits,
    )


def source_counter_inventory(root: Path) -> dict[str, bool]:
    inventory: dict[str, bool] = {}
    for name, (relative_path, symbol) in COUNTER_SYMBOLS.items():
        path = root / relative_path
        inventory[name] = path.exists() and symbol in path.read_text(encoding="utf-8")
    return inventory


def run_alternating_updates(
    rounds: int = 20,
    snapshot_reader: Callable[[str], CounterSnapshot] | None = None,
) -> tuple[CounterSnapshot, CounterSnapshot, list[WaitSample]]:
    """Return the fixed request shape and counter snapshots.

    ``snapshot_reader`` is deliberately injected: a native harness can read
    production accessors before and after the updates, while this offline
    fixture uses zero-valued snapshots and never claims runtime evidence.
    """

    if rounds != 20:
        raise ValueError("diagnostic fixture requires exactly 20 alternating updates")
    reader = snapshot_reader or (lambda _phase: CounterSnapshot())
    before = reader("before")
    samples = [
        WaitSample(
            round_no=round_no,
            requester=(round_no - 1) % 2,
            holder=round_no % 2,
            wait_event=WAIT_EVENT_NAMES[(round_no - 1) % len(WAIT_EVENT_NAMES)],
        )
        for round_no in range(1, rounds + 1)
    ]
    after = reader("after")
    return before, after, samples


def _assert_fixture(samples: Iterable[WaitSample]) -> None:
    samples = list(samples)
    assert len(samples) == 20
    assert [sample.requester for sample in samples] == [i % 2 for i in range(20)]
    assert [sample.holder for sample in samples] == [1 - (i % 2) for i in range(20)]
    assert {sample.wait_event for sample in samples} == set(WAIT_EVENT_NAMES)


def main() -> int:
    root = Path(__file__).resolve().parents[3]
    inventory = source_counter_inventory(root)
    assert all(inventory.values()), inventory
    wait_candidates = source_wait_candidate_inventory(root)
    before, after, samples = run_alternating_updates()
    _assert_fixture(samples)
    delta = counter_delta(before, after)
    assert delta == CounterSnapshot()
    injected_after = CounterSnapshot(
        retransmit_attempts=3,
        txw_wakeups=17,
        txw_timeouts=2,
        pcm_install_waits=5,
    )
    injected_before, injected_final, injected_samples = run_alternating_updates(
        snapshot_reader=FixtureCounterReader(CounterSnapshot(), injected_after)
    )
    _assert_fixture(injected_samples)
    assert counter_delta(injected_before, injected_final) == injected_after
    print("offline two-node alternating-update fixture: 20 rounds")
    print("counter accessors: " + ", ".join(sorted(inventory)))
    print("wait candidates: " + ", ".join(sorted(wait_candidates)))
    for name in sorted(wait_candidates):
        candidate = wait_candidates[name]
        line_numbers = candidate["line_numbers"]
        markers = "; ".join(
            f"{needle}={line_numbers[needle]}"
            for needle in candidate["needles"]
        )
        print(f"{name} source lines: {markers}")
    print("wait events: " + ", ".join(WAIT_EVENT_NAMES))
    print("runtime evidence: NOT_CAPTURED (offline fixture)")
    return 0


def source_wait_candidate_inventory(root: Path) -> dict[str, dict[str, object]]:
    """Return source-only markers for the fixed-delay wait candidates."""
    candidates = {
        "tx_enqueue_tick": {
            "path": "src/backend/cluster/cluster_tx_enqueue.c",
            "needles": ("CLUSTER_TXW_TICK_MS 1000", "GES_TX_ENQUEUE_WAIT"),
        },
        "gcs_retransmit_backoff": {
            "path": "src/backend/cluster/cluster_gcs_block.c",
            "needles": ("gcs_block_backoff_ms_for_retry",),
        },
        "pcm_install_wait": {
            "path": "src/backend/cluster/cluster_pcm_lock.c",
            "needles": (
                "WAIT_EVENT_PCM_COMPATIBLE_STATE_WAIT",
                "ConditionVariableTimedSleep",
            ),
        },
    }
    for candidate in candidates.values():
        source = (root / candidate["path"]).read_text(encoding="utf-8")
        missing = [needle for needle in candidate["needles"] if needle not in source]
        if missing:
            raise AssertionError(f"missing wait candidate markers: {candidate['path']}: {missing}")
        lines = source.splitlines()
        candidate["line_numbers"] = {
            needle: [line_no for line_no, line in enumerate(lines, start=1) if needle in line]
            for needle in candidate["needles"]
        }
    return candidates


if __name__ == "__main__":
    raise SystemExit(main())
