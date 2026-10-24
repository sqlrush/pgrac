#!/usr/bin/env python3
"""Source-level guard for deferred service-side CONTENT_LOCK BUSY handling.

The native service path must release/yield its revoke owner on BUSY and let the
event loop make progress; it must not synchronously wait while the retained
owner is still held.  Keep this fixture deterministic until a native cluster
fixture can exercise the same path.
"""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "src/backend/cluster/cluster_gcs_block.c"


def _region(text: str, start: str, end: str) -> str:
    begin = text.index(start)
    finish = text.index(end, begin)
    return text[begin:finish]


def main() -> None:
    text = SOURCE.read_text(encoding="utf-8")
    assert "CLUSTER_PCM_OWN_FINISH_REFUSAL_CONTENT_LOCK" in text

    finish = _region(
        text,
        "gcs_block_resource_x_source_finish_owned(",
        "source_finish_refusal_done:",
    )
    drain = _region(
        text,
        'failure_stage = "terminal-content-drain"',
        'if (tagless_target_x && target_x_drop && !semantic_retained)',
    )

    assert "CLUSTER_PCM_OWN_BUSY" in finish
    assert "CLUSTER_PCM_OWN_FINISH_REFUSAL_CONTENT_LOCK" in finish
    assert "cluster_pcm_lock_resource_x_source_finish_defer_exact" in finish
    assert "WaitLatch" not in finish
    assert "cluster_lmon_wait" not in finish

    assert "CLUSTER_PCM_OWN_BUSY" in drain
    assert "cluster_bufmgr_pcm_own_abort_held_x_revoke" in drain
    assert "cluster_pcm_lock_resource_x_terminal_x_revoke_yield_exact" in drain
    assert "WaitLatch" not in drain
    assert "cluster_lmon_wait" not in drain

    trace = "gcs_block_pcm_trace_guard_capture(&retained_drain_trace"
    assert trace in drain
    assert drain.index(trace) < drain.index("if (own_result == CLUSTER_PCM_OWN_BUSY)")


if __name__ == "__main__":
    main()
