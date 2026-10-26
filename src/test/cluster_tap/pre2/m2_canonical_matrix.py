"""Validate finite same-key observations; diagnostic rows are never authority."""
import json
import re
import sys

KEY_FIELDS = ("xid", "origin", "segment", "slot", "wrap", "generation", "root", "root_generation")


def traces(text, xid, caller=None, pid=None):
    rows = []
    for line in text.splitlines():
        if "TX_CANONICAL " not in line:
            continue
        row = {k: int(v) if v.isdigit() else v for k, v in
               re.findall(r"([a-z_]+)=([^ \n]+)", line.split("TX_CANONICAL ", 1)[1])}
        if row.get("xid") == xid and (caller is None or row.get("caller") == caller) and (
                pid is None or row.get("pid") == pid):
            rows.append(row)
    return rows


def common(case, phases):
    if not all(case.get(k) is True for k in
               ("reached", "guard_clear", "values_correct", "producer_complete")):
        return False, "unreached, cleanup missing or data mismatch"
    rows = case.get("traces", [])
    if [r.get("phase") for r in rows] != phases:
        return False, "missing, repeated or reordered samples"
    key = case.get("key", {})
    if any(k not in key for k in KEY_FIELDS) or not key.get("root"):
        return False, "incomplete physical identity"
    pid = rows[0].get("pid")
    if not isinstance(pid, int) or pid <= 0:
        return False, "missing native process identity"
    for r in rows:
        if (any(r.get(k) != key[k] for k in KEY_FIELDS) or r.get("pid") != pid
                or r.get("caller") != case.get("caller") or r.get("authoritative") != 0):
            return False, "sample identity drift or diagnostic authority"
        if (r.get("generation_known") != 1 or r.get("tt_xid") != key["xid"]
                or r.get("tt_wrap") != key["wrap"]):
            return False, "copied slot identity drift"
    return True, "same physical identity"


def check_a1(case):
    ok, reason = common(case, ["COPY", "NATIVE", "RECOPY", "DECIDE"])
    if not ok:
        return ok, reason
    a, n, b, d = case["traces"]
    scn = case.get("producer_scn", 0)
    if (a.get("tt_status") != 1 or a.get("tt_scn") != 0 or n.get("native_known") != 1
            or n.get("native_status") != 1 or not scn
            or any(r.get("tt_status") != 2 or r.get("tt_scn") != scn for r in (b, d))
            or d.get("native_status") != 1 or d.get("outcome") != 3):
        return False, "no exact native commit recopy"
    expected = dict(outcome=3, scn=scn)
    if case["caller"] == "owner":
        if case.get("result") != dict(outcome=0, scn=0) or case.get("fresh_result") != expected:
            return False, "old ACTIVE allocator snapshot must refuse before a fresh capture"
    elif case.get("result") != expected:
        return False, "no exact final committed result"
    return True, "one exact commit recopy with all final fences"


def check_a2(case):
    ok, reason = common(case, ["COPY", "NATIVE"])
    if not ok:
        return ok, reason
    for r in case["traces"]:
        if (r.get("tt_status") != 3 or r.get("tt_flags") != 0 or r.get("tt_scn") != 0
                or r.get("undo_invalid") != 1):
            return False, "not precise preabort"
    n = case["traces"][-1]
    if (n.get("native_known") != 1 or n.get("native_status") != 0 or n.get("outcome") != 1
            or case.get("result") != dict(outcome=1, scn=0)):
        return False, "preabort was promoted or native was no longer live"
    if case.get("fresh_result") != dict(outcome=4, scn=0):
        return False, "fresh native abort proof is missing after rollback"
    return True, "precise preabort remains conservative IN_PROGRESS"


if __name__ == "__main__":
    case = json.load(sys.stdin)
    ok, reason = (check_a1 if case["kind"] == "A1" else check_a2)(case)
    print(json.dumps(dict(passed=ok, reason=reason)))
    sys.exit(0 if ok else 1)
