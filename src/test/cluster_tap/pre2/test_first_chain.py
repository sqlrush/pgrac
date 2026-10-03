"""Harness-only negative checks, not evidence of a running shared cluster."""
import copy
import unittest

from first_chain import check_recovery, check_scache, check_shutdown


def writer(node):
    return dict(node=node, thread=node + 1, incarnation=1, wal_generation=2,
                boot=f"boot-{node}")


def identities():
    return dict(system_identifier="123", victim_writer=writer(3),
                recoverer_writers=[writer(0), writer(1)])


def recovery():
    kinds = [("walr_s_acquired", 0), ("walr_s_acquired", 1),
             ("data_write", 0), ("ir_released", 0), ("data_write", 1),
             ("completion_deferred", 0), ("ir_released", 1),
             ("walr_s_released", 1), ("complete_x_acquired", 0),
             ("recovery_complete", 0), ("complete_x_released", 0),
             ("walr_s_released", 0),
             ("stale_write_rejected", 1), ("worker_retired", 0),
             ("worker_retired", 1), ("observation_closed", 0)]
    events = [dict(seq=i + 1, kind=k, node=n, duty="duty1", token="token1", boot=f"boot-{n}")
              for i, (k, n) in enumerate(kinds)]
    events[9]["durable_readback"] = True
    events[12]["reason"] = "RECOVERY_COMPLETE"
    return dict(duty="duty1", token="token1", recoverers=[0, 1], victim=3,
                terminal_proof="proof1", trace_complete=True, overflow=False,
                events=events, identities=identities())


class FirstChainTest(unittest.TestCase):
    def test_recovery_interleave(self):
        check_recovery(recovery(), "duty1", "token1", "proof1", [0, 1], 3, identities())

    def test_recovery_rejects_wrong_or_incomplete_proof(self):
        for field, value in [("duty", "other"), ("token", "old"),
                             ("terminal_proof", "other"), ("overflow", True),
                             ("trace_complete", False), ("recoverers", [0, 0])]:
            with self.subTest(field=field):
                trace = recovery()
                trace[field] = value
                with self.assertRaises(ValueError):
                    check_recovery(trace, "duty1", "token1", "proof1", [0, 1], 3, identities())

    def test_recovery_rejects_late_write(self):
        trace = recovery()
        trace["events"][11]["kind"] = "data_write"
        with self.assertRaisesRegex(ValueError, "after RECOVERY_COMPLETE"):
            check_recovery(trace, "duty1", "token1", "proof1", [0, 1], 3, identities())

    def test_recovery_rejects_completion_with_peer_s(self):
        trace = recovery()
        trace["events"][7]["kind"] = "ir_released"
        with self.assertRaises(ValueError):
            check_recovery(trace, "duty1", "token1", "proof1", [0, 1], 3, identities())

    def test_recovery_requires_actual_interleave_and_retirement(self):
        for index in (4, 5, 11, 13, 14):
            trace = recovery()
            del trace["events"][index]
            with self.subTest(index=index), self.assertRaises(ValueError):
                check_recovery(trace, "duty1", "token1", "proof1", [0, 1], 3, identities())

    def test_recovery_rejects_a_previous_survivor_boot(self):
        trace = recovery()
        trace["events"][4]["boot"] = "old-boot"
        with self.assertRaisesRegex(ValueError, "different survivor boot"):
            check_recovery(trace, "duty1", "token1", "proof1", [0, 1], 3, identities())

    def test_shutdown_all_exact_writers(self):
        before = [writer(n) for n in range(4)]
        stopped = dict(system_identifier="123", writers=[dict(w, state="CLOSED") for w in before],
                       final_checkpoint=True, all_processes_exited=True)
        check_shutdown(stopped, "123", before)
        for mutate in (lambda r: r["writers"].pop(),
                       lambda r: r["writers"][0].update(boot="previous"),
                       lambda r: r.update(all_processes_exited=False),
                       lambda r: r.update(final_checkpoint=False)):
            bad = copy.deepcopy(stopped)
            mutate(bad)
            with self.assertRaises(ValueError):
                check_shutdown(bad, "123", before)

    def test_scache_exact_tag_and_no_reship(self):
        target = dict(path="base/5/42", fork="main", block=0, space="space1")
        before = dict(target=target, x_holder=0, s_holders=[], ships=0)
        first = dict(target=target, x_holder=None, s_holders=[0, 1], ships=1)
        repeat = dict(first)
        check_scache(before, first, repeat, target, 0, 1)
        for bad in (dict(repeat, ships=2), dict(repeat, s_holders=[]),
                    dict(repeat, target=dict(target, space="reused")),
                    dict(repeat, x_holder=0)):
            with self.assertRaises(ValueError):
                check_scache(before, first, bad, target, 0, 1)


if __name__ == "__main__":
    unittest.main()
