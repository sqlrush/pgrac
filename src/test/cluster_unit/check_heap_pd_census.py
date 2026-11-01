#!/usr/bin/env python3
"""Closed PD publisher census paired with the original VM setter audit.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
import collections
import pathlib
import re
import sys

from check_vm_setter_census import HEAP, VAC, VM, audit as vm_audit, bodies, clean, load

HEADER = "src/include/storage/bufpage.h"
APPLY = "src/backend/cluster/cluster_block_apply_heap.c"
EXPECTED = collections.Counter({
    (VM, "visibilitymap_set_locked"): 1,
    (HEAP, "heap_multi_insert"): 1,
    (HEAP, "heap_xlog_visible"): 1,
    (HEAP, "heap_xlog_multi_insert"): 1,
    (VAC, "lazy_scan_heap"): 2,
    (VAC, "lazy_scan_new_or_empty"): 1,
    (VAC, "lazy_vacuum_heap_page"): 1,
    (APPLY, "apply_heap_insert"): 1,
    (APPLY, "apply_heap_visible"): 1,
})
CALL = re.compile(r"\bPageSetAllVisible\s*\(")


def audit(source):
    errors = vm_audit(source)
    edges, table = collections.Counter(), {}
    for path, text in source.items():
        if not CALL.search(text) and "PD_ALL_VISIBLE" not in text:
            continue
        functions = bodies(text)
        table[path] = functions
        for owner, (body, _, _) in functions.items():
            edges[path, owner] += len(CALL.findall(body))
            if "PD_ALL_VISIBLE" in body and (path != HEADER or owner not in {
                    "PageIsAllVisible", "PageSetAllVisible", "PageClearAllVisible"}):
                errors.append("unregistered direct PD access: " + path + ":" + owner)
        for call in CALL.finditer(clean(text)):
            if not any(start <= call.start() < end for _, start, end in functions.values()):
                errors.append("unowned PD publisher: " + path)
        for token in re.finditer(r"\bPD_ALL_VISIBLE\b", clean(text)):
            if any(start <= token.start() < end for _, start, end in functions.values()):
                continue
            prefix = text[text.rfind("\n", 0, token.start()) + 1:token.start()]
            if path != HEADER or not re.fullmatch(r"\s*#define\s+", prefix):
                errors.append("unowned direct PD access: " + path)
    edges += collections.Counter()  # Remove zero-count observer functions.
    if edges != EXPECTED:
        errors.append("unregistered PD publisher edges: " + repr(dict(edges - EXPECTED))
                      + "; missing: " + repr(dict(EXPECTED - edges)))
    required = {
        (VAC, "heap_vacuum_rel"): [
            "vacrel->shared_maintenance_restricted = cluster_heap_maintenance_restricted(rel)"],
        (VAC, "lazy_scan_heap"): [
            "if (!vacrel->shared_maintenance_restricted && !all_visible_according_to_vm && prunestate.all_visible)",
            "if (!vacrel->shared_maintenance_restricted && all_visible_according_to_vm && prunestate.all_visible && prunestate.all_frozen"],
        (VAC, "lazy_scan_new_or_empty"): [
            "if (!vacrel->shared_maintenance_restricted && !PageIsAllVisible(page))"],
        (VAC, "lazy_vacuum_heap_page"): [
            "if (!vacrel->shared_maintenance_restricted && heap_page_is_all_visible("],
        (HEAP, "heap_multi_insert"): [
            "if (starting_with_empty_page && (options & HEAP_INSERT_FROZEN)) all_frozen_set = true"],
        # These are replay codecs, not independent positive publishers.
        # The original trusted-WAL/whole-record install boundary is retained.
        (APPLY, "apply_heap_insert"): [
            "if (flags & XLH_INSERT_ALL_FROZEN_SET) PageSetAllVisible(scratch.data)",
            "flags & XLH_INSERT_ALL_VISIBLE_CLEARED", "heap_maintenance_page_valid(page)"],
        (APPLY, "apply_heap_visible"): [
            "block_id != 1", "heap->forknum != MAIN_FORKNUM", "SizeOfHeapVisible",
            "vm->forknum != VISIBILITYMAP_FORKNUM", "RelFileLocatorEquals(heap->rlocator, vm->rlocator)",
            "!(rec.flags & VISIBILITYMAP_ALL_VISIBLE)"],
    }
    for (path, owner), fragments in required.items():
        body = re.sub(r"\s+", " ", table.get(path, {}).get(owner, ("",))[0])
        if any(fragment not in body for fragment in fragments):
            errors.append("missing PD source gate: " + owner)
    return errors


def run(root):
    source = load(root)
    tests = []
    found = audit(source)
    tests.append(("all PD publishers belong to the guarded VM/freeze/redo family", not found, found))
    for owner in ["lazy_scan_heap", "lazy_scan_new_or_empty", "lazy_vacuum_heap_page"]:
        changed = dict(source)
        _, start, end = bodies(source[VAC])[owner]
        changed[VAC] = source[VAC][:start] + source[VAC][start:end].replace(
            "!vacrel->shared_maintenance_restricted", "true") + source[VAC][end:]
        found = audit(changed)
        tests.append(("reject removed PD source gate in " + owner,
                      "missing PD source gate: " + owner in found, found))
    for text, label in [
        ("void\nnew_pd_writer(void)\n{\nPageSetAllVisible(page);\n}\n", "extension publisher"),
        ("void new_pd_writer(void) { PageSetAllVisible(page); }\n", "single-line publisher"),
        ("#define NEW_PD() \\\nPageSetAllVisible(page)\n", "continued macro"),
        ("void\nnew_pd_writer(void)\n{\np->pd_flags |= PD_ALL_VISIBLE;\n}\n", "raw bit writer"),
        ("#define NEW_PD() (p->pd_flags |= PD_ALL_VISIBLE)\n", "raw bit macro"),
    ]:
        changed = dict(source)
        changed["contrib/new_pd/new_pd.c"] = text
        found = audit(changed)
        tests.append(("reject unregistered " + label,
                      any(e.startswith(("unowned PD publisher:", "unregistered PD publisher edges:",
                                        "unregistered direct PD access:", "unowned direct PD access:")) for e in found), found))
    changed = dict(source)
    changed[HEAP] = source[HEAP].replace("all_frozen_set = true", "all_frozen_set = arbitrary")
    found = audit(changed)
    tests.append(("reject unqualified COPY FREEZE PD source",
                  "missing PD source gate: heap_multi_insert" in found, found))
    for owner, token in [("apply_heap_insert", "XLH_INSERT_ALL_FROZEN_SET"),
                         ("apply_heap_visible", "VISIBILITYMAP_ALL_VISIBLE")]:
        changed = dict(source)
        _, start, end = bodies(source[APPLY])[owner]
        changed[APPLY] = source[APPLY][:start] + source[APPLY][start:end].replace(
            token, "missing_record_flag") + source[APPLY][end:]
        found = audit(changed)
        tests.append(("reject missing native replay qualification in " + owner,
                      "missing PD source gate: " + owner in found, found))
    print("1.." + str(len(tests)))
    failed = 0
    for n, (name, passed, details) in enumerate(tests, 1):
        print(("ok " if passed else "not ok ") + str(n) + " - " + name)
        if not passed:
            failed += 1
            for detail in details:
                print("# " + detail)
    return failed != 0


if __name__ == "__main__":
    raise SystemExit(run(pathlib.Path(sys.argv[1]) if len(sys.argv) > 1
                         else pathlib.Path(__file__).resolve().parents[3]))
