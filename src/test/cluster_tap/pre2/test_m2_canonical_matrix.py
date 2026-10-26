"""Evidence checks do not replace a native scheduling run."""
import unittest
from m2_canonical_matrix import check_a1, check_a2

KEY = dict(xid=101, origin=0, segment=7, slot=4, wrap=2, generation=3,
           root=51, root_generation=9)


def row(phase):
    return dict(KEY, caller="physical", phase=phase, pid=123, tt_status=1,
                generation_known=1, tt_xid=KEY["xid"], tt_wrap=KEY["wrap"],
                tt_flags=0, tt_scn=0, undo_invalid=0, native_known=1, native_status=0,
                outcome=0, authoritative=0)


def a1():
    rows = [row("COPY"), row("NATIVE"), row("RECOPY"), row("DECIDE")]
    rows[1].update(native_status=1)
    rows[2].update(tt_status=2, tt_scn=80)
    rows[3].update(tt_status=2, tt_scn=80, native_status=1, outcome=3)
    return dict(key=KEY, caller="physical", traces=rows, result=dict(outcome=3, scn=80),
                producer_scn=80, reached=True, guard_clear=True, values_correct=True,
                producer_complete=True)


def a2():
    rows = [row("COPY"), row("NATIVE")]
    for r in rows:
        r.update(tt_status=3, undo_invalid=1)
    rows[1].update(outcome=1)
    return dict(key=KEY, caller="physical", traces=rows, result=dict(outcome=1, scn=0),
                fresh_result=dict(outcome=4, scn=0), reached=True, guard_clear=True,
                values_correct=True, producer_complete=True)


class MatrixEvidence(unittest.TestCase):
    def test_a1_accepts_only_complete_same_key_exact_successor(self):
        self.assertTrue(check_a1(a1())[0])

    def test_a2_accepts_precise_live_preabort(self):
        self.assertTrue(check_a2(a2())[0])

    def test_all_identity_dimensions_and_pid_are_required(self):
        for field in [*KEY, "pid", "caller"]:
            case = a1()
            case["traces"][-1][field] = "different"
            self.assertFalse(check_a1(case)[0], field)

    def test_a1_unknown_or_fabricated_scn_never_passes(self):
        for field, value in [("outcome", 0), ("scn", 0), ("scn", 81)]:
            case = a1()
            case["result"][field] = value
            self.assertFalse(check_a1(case)[0])

    def test_unreached_or_no_cleanup_or_wrong_data_is_not_pass(self):
        for field in ["reached", "guard_clear", "values_correct"]:
            for make, check in [(a1, check_a1), (a2, check_a2)]:
                case = make()
                case[field] = False
                self.assertFalse(check(case)[0])

    def test_a1_requires_real_initial_active_and_native_commit(self):
        for index, field, value in [(0, "tt_status", 2), (1, "native_status", 0),
                                    (1, "native_known", 0), (2, "tt_scn", 0)]:
            case = a1()
            case["traces"][index][field] = value
            self.assertFalse(check_a1(case)[0])

    def test_no_missing_extra_or_reordered_recopy(self):
        for rows in [a1()["traces"][:2], a1()["traces"] * 2,
                     list(reversed(a1()["traces"]))]:
            case = a1()
            case["traces"] = rows
            self.assertFalse(check_a1(case)[0])

    def test_a2_rejects_bad_preabort_and_terminal_publication(self):
        for field, value in [("tt_flags", 1), ("tt_scn", 80), ("undo_invalid", 0),
                            ("native_status", 2), ("outcome", 4)]:
            case = a2()
            case["traces"][-1][field] = value
            self.assertFalse(check_a2(case)[0])

    def test_owner_requires_refusal_then_fresh_capture(self):
        case = a1()
        case["caller"] = "owner"
        for r in case["traces"]:
            r["caller"] = "owner"
        case["result"] = dict(outcome=0, scn=0)
        case["fresh_result"] = dict(outcome=3, scn=80)
        self.assertTrue(check_a1(case)[0])
        case["result"] = dict(outcome=3, scn=80)
        self.assertFalse(check_a1(case)[0])

    def test_diagnostics_never_count_as_published_authority(self):
        case = a2()
        case["traces"][-1]["authoritative"] = 1
        self.assertFalse(check_a2(case)[0])

    def test_unknown_generation_or_missing_copied_identity_is_not_evidence(self):
        for field in ("generation_known", "tt_xid", "tt_wrap"):
            case = a1()
            del case["traces"][-1][field]
            self.assertFalse(check_a1(case)[0], field)

    def test_incomplete_producer_export_or_missing_pid_cannot_pass(self):
        case = a2()
        case["producer_complete"] = False
        self.assertFalse(check_a2(case)[0])

        case = a2()
        for r in case["traces"]:
            del r["pid"]
        self.assertFalse(check_a2(case)[0])

    def test_a2_requires_fresh_exact_abort_after_native_rollback(self):
        for fresh in (None, dict(outcome=0, scn=0), dict(outcome=1, scn=0),
                      dict(outcome=3, scn=80), dict(outcome=4, scn=80)):
            case = a2()
            case["fresh_result"] = fresh
            self.assertFalse(check_a2(case)[0], fresh)


if __name__ == "__main__":
    unittest.main()
