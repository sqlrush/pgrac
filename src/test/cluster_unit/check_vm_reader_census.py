#!/usr/bin/env python3
"""Closed VM/PD read-call census and shared shortcut boundary checks.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
import collections
import pathlib
import re
import sys

from check_vm_setter_census import bodies, clean, load

VM = "src/backend/access/heap/visibilitymap.c"
HEAP = "src/backend/access/heap/heapam.c"
VAC = "src/backend/access/heap/vacuumlazy.c"
IOS = "src/backend/executor/nodeIndexonlyscan.c"
BITMAP = "src/backend/executor/nodeBitmapHeapscan.c"
PLANNER = "src/backend/utils/adt/selfuncs.c"
APPROX = "contrib/pgstattuple/pgstatapprox.c"
HANDLER = "src/backend/access/heap/heapam_handler.c"


# Raw diagnostics and write-side observations remain registered readers,
# not positive shortcut permissions. New calls need an explicit owner.
EXPECTED = collections.Counter({
    ('contrib/amcheck/verify_heapam.c', 'verify_heapam', 'visibilitymap_get_status'): 1,
    ('contrib/pg_surgery/heap_surgery.c', 'heap_force_common', 'PageIsAllVisible'): 3,
    ('contrib/pg_visibility/pg_visibility.c', 'visibilitymap_get_status_for_report', 'visibilitymap_get_status'): 1,
    ('contrib/pg_visibility/pg_visibility.c', 'pg_visibility_map', 'visibilitymap_get_status_for_report'): 1,
    ('contrib/pg_visibility/pg_visibility.c', 'pg_visibility', 'PageIsAllVisible'): 1,
    ('contrib/pg_visibility/pg_visibility.c', 'pg_visibility', 'visibilitymap_get_status_for_report'): 1,
    ('contrib/pg_visibility/pg_visibility.c', 'pg_visibility_map_summary', 'visibilitymap_get_status_for_report'): 1,
    ('contrib/pg_visibility/pg_visibility.c', 'collect_visibility_data', 'PageIsAllVisible'): 1,
    ('contrib/pg_visibility/pg_visibility.c', 'collect_visibility_data', 'visibilitymap_get_status_for_report'): 1,
    ('contrib/pg_visibility/pg_visibility.c', 'collect_corrupt_items', 'VM_ALL_FROZEN'): 2,
    ('contrib/pg_visibility/pg_visibility.c', 'collect_corrupt_items', 'VM_ALL_VISIBLE'): 2,
    ('contrib/pgstattuple/pgstatapprox.c', 'statapprox_vm_all_visible', 'VM_ALL_VISIBLE'): 1,
    ('contrib/pgstattuple/pgstatapprox.c', 'statapprox_vm_all_visible', 'visibilitymap_read_end'): 1,
    ('contrib/pgstattuple/pgstatapprox.c', 'statapprox_vm_all_visible', 'visibilitymap_read_qualified_begin'): 1,
    ('src/backend/access/heap/heapam.c', 'cluster_heap_itl_ensure_capacity_with_terminal_census', 'PageIsAllVisible'): 1,
    ('src/backend/access/heap/heapam.c', 'cluster_heap_page_all_visible', 'PageIsAllVisible'): 1,
    ('src/backend/access/heap/heapam.c', 'heapgetpage', 'PageIsAllVisible'): 1,
    ('src/backend/access/heap/heapam.c', 'heapgetpage', 'cluster_heap_page_all_visible'): 1,
    ('src/backend/access/heap/heapam.c', 'heapgetpage', 'cluster_heap_page_visibility_prepare'): 1,
    ('src/backend/access/heap/heapam.c', 'cluster_heap_hot_search_buffer', 'cluster_heap_page_all_visible'): 1,
    ('src/backend/access/heap/heapam.c', 'heap_insert', 'PageIsAllVisible'): 2,
    ('src/backend/access/heap/heapam.c', 'heap_multi_insert', 'PageIsAllVisible'): 3,
    ('src/backend/access/heap/heapam.c', 'cluster_current_mx_relock_with_vm', 'PageIsAllVisible'): 1,
    ('src/backend/access/heap/heapam.c', 'cluster_heap_lock_with_vm_repin', 'PageIsAllVisible'): 2,
    ('src/backend/access/heap/heapam.c', 'cluster_heap_lock_vm_needs_clear', 'visibilitymap_get_status'): 1,
    ('src/backend/access/heap/heapam.c', 'cluster_heap_lock_vm_needs_clear', 'visibilitymap_get_status_pinned'): 1,
    ('src/backend/access/heap/heapam.c', 'heap_delete', 'PageIsAllVisible'): 7,
    ('src/backend/access/heap/heapam.c', 'heap_update', 'PageIsAllVisible'): 10,
    ('src/backend/access/heap/heapam.c', 'heap_update', 'visibilitymap_get_status'): 2,
    ('src/backend/access/heap/heapam.c', 'heap_lock_tuple_internal', 'PageIsAllVisible'): 4,
    ('src/backend/access/heap/heapam.c', 'heap_lock_tuple_internal', 'visibilitymap_get_status'): 1,
    ('src/backend/access/heap/heapam.c', 'heap_lock_updated_tuple_rec', 'PageIsAllVisible'): 7,
    ('src/backend/access/heap/heapam.c', 'heap_lock_updated_tuple_rec', 'visibilitymap_get_status'): 2,
    ('src/backend/access/heap/heapam.c', 'heap_abort_speculative', 'PageIsAllVisible'): 1,
    ('src/backend/access/heap/heapam_handler.c', 'heapam_scan_bitmap_next_block', 'cluster_heap_page_all_visible'): 1,
    ('src/backend/access/heap/heapam_handler.c', 'heapam_scan_bitmap_next_block', 'cluster_heap_page_visibility_prepare'): 1,
    ('src/backend/access/heap/heapam_handler.c', 'heapam_scan_sample_next_tuple', 'PageIsAllVisible'): 1,
    ('src/backend/access/heap/heapam_handler.c', 'heapam_scan_sample_next_tuple', 'cluster_heap_page_all_visible'): 1,
    ('src/backend/access/heap/heapam_handler.c', 'heapam_scan_sample_next_tuple', 'cluster_heap_page_visibility_prepare'): 1,
    ('src/backend/access/heap/hio.c', 'GetVisibilityMapPins', 'PageIsAllVisible'): 6,
    ('src/backend/access/heap/hio.c', 'RelationGetBufferForTuple', 'PageIsAllVisible'): 4,
    ('src/backend/access/heap/vacuumlazy.c', 'lazy_clear_vm_after_heap', 'visibilitymap_get_status'): 1,
    ('src/backend/access/heap/vacuumlazy.c', 'lazy_scan_heap', 'PageIsAllVisible'): 5,
    ('src/backend/access/heap/vacuumlazy.c', 'lazy_scan_heap', 'VM_ALL_FROZEN'): 1,
    ('src/backend/access/heap/vacuumlazy.c', 'lazy_scan_heap', 'visibilitymap_get_status'): 1,
    ('src/backend/access/heap/vacuumlazy.c', 'lazy_scan_heap', 'visibilitymap_get_status_coherent'): 1,
    ('src/backend/access/heap/vacuumlazy.c', 'lazy_scan_skip_shared_page', 'visibilitymap_read_end'): 1,
    ('src/backend/access/heap/vacuumlazy.c', 'lazy_scan_skip_shared_page', 'visibilitymap_read_qualified_begin'): 1,
    ('src/backend/access/heap/vacuumlazy.c', 'lazy_scan_skip', 'visibilitymap_get_status'): 1,
    ('src/backend/access/heap/vacuumlazy.c', 'lazy_scan_new_or_empty', 'PageIsAllVisible'): 1,
    ('src/backend/access/heap/vacuumlazy.c', 'lazy_vacuum_heap_page', 'PageIsAllVisible'): 1,
    ('src/backend/access/heap/visibilitymap.c', 'visibilitymap_set_empty', 'PageIsAllVisible'): 1,
    ('src/backend/access/heap/visibilitymap.c', 'visibilitymap_set_locked', 'PageIsAllVisible'): 2,
    ('src/backend/access/heap/visibilitymap.c', 'visibilitymap_read_begin', 'visibilitymap_get_status'): 1,
    ('src/backend/access/heap/visibilitymap.c', 'visibilitymap_read_begin', 'visibilitymap_get_status_pinned'): 1,
    ('src/backend/access/heap/visibilitymap.c', 'visibilitymap_read_snapshot_begin', 'visibilitymap_read_begin'): 2,
    ('src/backend/access/heap/visibilitymap.c', 'visibilitymap_read_snapshot_begin', 'visibilitymap_read_end'): 1,
    ('src/backend/access/heap/visibilitymap.c', 'visibilitymap_read_qualified_begin', 'visibilitymap_read_begin'): 1,
    ('src/backend/access/heap/visibilitymap.c', 'visibilitymap_read_qualified_begin', 'visibilitymap_read_snapshot_begin'): 1,
    ('src/backend/access/heap/visibilitymap.c', 'visibilitymap_get_status_coherent', 'visibilitymap_read_begin'): 1,
    ('src/backend/access/heap/visibilitymap.c', 'visibilitymap_get_status_coherent', 'visibilitymap_read_end'): 1,
    ('src/backend/executor/nodeBitmapHeapscan.c', 'BitmapHeapNext', 'VM_ALL_VISIBLE'): 1,
    ('src/backend/executor/nodeBitmapHeapscan.c', 'BitmapPrefetch', 'VM_ALL_VISIBLE'): 2,
    ('src/backend/executor/nodeIndexonlyscan.c', 'IndexOnlyNext', 'VM_ALL_VISIBLE'): 1,
    ('src/backend/executor/nodeIndexonlyscan.c', 'IndexOnlyNext', 'visibilitymap_read_end'): 2,
    ('src/backend/executor/nodeIndexonlyscan.c', 'IndexOnlyNext', 'visibilitymap_read_qualified_begin'): 1,
    ('src/backend/utils/adt/selfuncs.c', 'get_shared_variable_endpoint', 'visibilitymap_read_end'): 1,
    ('src/backend/utils/adt/selfuncs.c', 'get_shared_variable_endpoint', 'visibilitymap_read_qualified_begin'): 1,
    ('src/backend/utils/adt/selfuncs.c', 'get_actual_variable_endpoint', 'VM_ALL_VISIBLE'): 1,
})

APIS = {key[2] for key in EXPECTED}
TOKEN = re.compile(r"\b(" + "|".join(sorted(APIS)) + r")\b")
CALL = re.compile(r"\b(" + "|".join(sorted(APIS)) + r")\s*\(")
DECLARATION = re.compile(r"(?m)^(?:extern|static)\s+(?:void|bool|uint8)\s+(?:"
                         + "|".join(sorted(APIS)) + r")\s*\([^;{}]*\);\s*$")
HEADER = "src/include/access/visibilitymap.h"
MACROS = {
    "VM_ALL_VISIBLE": "((visibilitymap_get_status((r), (b), (v)) & VISIBILITYMAP_ALL_VISIBLE) != 0)",
    "VM_ALL_FROZEN": "((visibilitymap_get_status((r), (b), (v)) & VISIBILITYMAP_ALL_FROZEN) != 0)",
}
# These checks detect removal of existing source boundaries. They do not
# replace the behavioral fixtures for grant, snapshot or cleanup semantics.
BOUNDARIES = {
    (VM, "visibilitymap_read_begin"): [
        ("if (!cluster_pcm_is_active()) return false;", 1),
        ("ClusterLockBufferShareBarrierAware(*vmbuf)", 1)],
    (VM, "visibilitymap_read_snapshot_begin"): [
        ("cluster_undo_horizon_read_admission_current(&admission)", 1),
        ("snapshot->curcid == original_cid", 1),
        ("cluster_space_relation_identity_current_v1(rel, &identity)", 1)],
    (VM, "visibilitymap_read_qualified_begin"): [
        ("minimum_catalog == 0 || GetControlFileCatalogVersion() < minimum_catalog", 1)],
    (IOS, "IndexOnlyNext"): [
        ("cluster_heap_maintenance_restricted(scandesc->heapRelation) ?", 1)],
    (BITMAP, "BitmapHeapNext"): [
        ("skip_fetch = (!cluster_heap_maintenance_restricted(node->ss.ss_currentRelation) &&", 1)],
    (BITMAP, "BitmapPrefetch"): [
        ("skip_fetch = (!cluster_heap_maintenance_restricted(node->ss.ss_currentRelation) &&", 2)],
    (PLANNER, "get_actual_variable_endpoint"): [
        ("if (cluster_heap_maintenance_restricted(heapRel) || !VM_ALL_VISIBLE(", 1)],
    (APPROX, "statapprox_vm_all_visible"): [
        ("if (!cluster_heap_maintenance_restricted(rel)) return VM_ALL_VISIBLE(", 1)],
    (VAC, "lazy_scan_skip"): [
        ("if (vacrel->shared_maintenance_restricted)", 1),
        ("lazy_scan_skip_shared_page(", 1)],
    (HEAP, "cluster_heap_page_visibility_prepare"): [
        ("minimum_catalog == 0 || GetControlFileCatalogVersion() < minimum_catalog", 1)],
    (HEAP, "cluster_heap_page_all_visible"): [
        ("cluster_heap_pcm_read_authority_valid(&own)", 1),
        ("snapshot->curcid == observation->command_id", 1)],
    (HEAP, "heapgetpage"): [
        ("all_visible = !cluster_heap_maintenance_restricted(scan->rs_base.rs_rd) && PageIsAllVisible(page)", 1)],
    (HANDLER, "heapam_scan_sample_next_tuple"): [
        ("all_visible = !cluster_heap_maintenance_restricted(scan->rs_rd) && PageIsAllVisible(page)", 1)],
}


def audit(source):
    errors, edges, table, macros = [], collections.Counter(), {}, collections.Counter()
    for path, original in source.items():
        if not TOKEN.search(original):
            continue
        text = clean(original)
        for token in TOKEN.finditer(text):
            if not re.match(r"\s*\(", text[token.end():]):
                errors.append("indirect VM/PD read: " + path + ":" + token[1])
        functions = bodies(original)
        table[path] = functions
        for owner, (body, _, _) in functions.items():
            for call in CALL.finditer(body):
                edges[path, owner, call[1]] += 1
        allowed = [(m.start(), m.end()) for m in DECLARATION.finditer(text)]
        for macro in re.finditer(r"(?m)^[ \t]*#[ \t]*define[ \t]+(\w+)((?:[^\n]*\\\n)*[^\n]*)", text):
            if path == HEADER and macro[1] in MACROS:
                macros[macro[1]] += 1
                body = re.sub(r"\s+", "", macro[2].replace("\\\n", " "))
                expected = re.sub(r"\s+", "", "(r, b, v) " + MACROS[macro[1]])
                if body != expected:
                    errors.append("changed raw VM macro: " + macro[1])
                else:
                    allowed.append((macro.start(), macro.end()))
        for call in CALL.finditer(text):
            if any(start <= call.start() < end for _, start, end in functions.values()) \
                    or any(start <= call.start() < end for start, end in allowed):
                continue
            errors.append("unowned VM/PD read: " + path + ":" + call[1])
    if edges != EXPECTED:
        errors.append("unregistered VM/PD read edges: " + repr(dict(edges - EXPECTED))
                      + "; missing: " + repr(dict(EXPECTED - edges)))
    for name in MACROS:
        if macros[name] != 1:
            errors.append("changed raw VM macro: " + name)
    for (path, owner), rules in BOUNDARIES.items():
        body = re.sub(r"\s+", " ", table.get(path, {}).get(owner, ("",))[0])
        for fragment, count in rules:
            if body.count(fragment) != count:
                errors.append("missing read boundary: " + owner + ":" + fragment)
    return errors


def run(root):
    source = load(root)
    tests = []
    found = audit(source)
    tests.append(("production VM/PD read entries retain their original boundaries", not found, found))
    for api in ["visibilitymap_get_status", "visibilitymap_get_status_pinned",
                "visibilitymap_get_status_coherent", "visibilitymap_read_begin",
                "visibilitymap_read_snapshot_begin", "visibilitymap_read_qualified_begin",
                "VM_ALL_VISIBLE", "VM_ALL_FROZEN", "PageIsAllVisible",
                "cluster_heap_page_all_visible"]:
        changed = dict(source)
        changed["contrib/new_reader/new_reader.c"] = "bool\nnew_reader(void)\n{\nreturn " + api + "(0);\n}\n"
        bad = audit(changed)
        tests.append(("reject an unregistered extension reader of " + api, bool(bad), bad))
    for label, text in [
        ("single-line function", "bool new_reader(void) { return VM_ALL_VISIBLE(0); }\n"),
        ("continued macro", "#define NEW_READ() \\\nVM_ALL_VISIBLE(0)\n"),
        ("inline function", "static inline bool new_reader(void) { return PageIsAllVisible(0); }\n"),
    ]:
        changed = dict(source)
        changed["contrib/new_reader/new_reader.h"] = text
        bad = audit(changed)
        tests.append(("reject an unowned read in a " + label, bool(bad), bad))
    for label, text in [
        ("function alias", "#define READ_ALIAS visibilitymap_get_status\n"),
        ("raw-bit macro alias", "#define READ_ALIAS VM_ALL_VISIBLE\n"),
        ("function pointer", "uint8 (*read_alias)(Relation, BlockNumber, Buffer *) = visibilitymap_get_status;\n"),
        ("parenthesized call", "uint8\nnew_reader(Relation r, BlockNumber b, Buffer *v)\n{\nreturn (visibilitymap_get_status)(r, b, v);\n}\n"),
    ]:
        changed = dict(source)
        changed["contrib/new_reader/new_reader.c"] = text
        bad = audit(changed)
        tests.append(("reject an indirect read through a " + label, bool(bad), bad))
    changed = dict(source)
    changed[HEADER] += "\n#define EXTRA_READ(r, b, v) \\\n((visibilitymap_get_status((r), (b), (v)) & VISIBILITYMAP_ALL_VISIBLE) != 0)\n"
    bad = audit(changed)
    tests.append(("reject a new raw-bit macro in the original header", bool(bad), bad))
    changed = dict(source)
    changed[HEADER] = source[HEADER].replace("!= 0)", "== 0)", 1)
    bad = audit(changed)
    tests.append(("reject a rewritten original raw-bit macro", bool(bad), bad))
    mutations = [
        (VM, "visibilitymap_read_begin", "cluster_pcm_is_active()", "true", "current authority gate"),
        (VM, "visibilitymap_read_begin", "ClusterLockBufferShareBarrierAware", "read_without_current", "current S acquisition"),
        (VM, "visibilitymap_read_snapshot_begin", "cluster_undo_horizon_read_admission_current", "skip_admission_check", "retained admission check"),
        (VM, "visibilitymap_read_snapshot_begin", "snapshot->curcid == original_cid", "true", "retained command ID"),
        (VM, "visibilitymap_read_qualified_begin", "minimum_catalog == 0", "false", "unassigned catalog refusal"),
        (VM, "visibilitymap_read_qualified_begin", "GetControlFileCatalogVersion", "compiled_catalog_version", "loaded catalog gate"),
        (IOS, "IndexOnlyNext", "visibilitymap_read_qualified_begin", "visibilitymap_read_snapshot_begin", "qualified IOS entry"),
        (IOS, "IndexOnlyNext", "cluster_heap_maintenance_restricted(scandesc->heapRelation) ?", "false ?", "native IOS branch gate"),
        (BITMAP, "BitmapHeapNext", "!cluster_heap_maintenance_restricted(node->ss.ss_currentRelation)", "true", "bitmap count-only gate"),
        (BITMAP, "BitmapPrefetch", "!cluster_heap_maintenance_restricted(node->ss.ss_currentRelation)", "true", "serial prefetch gate"),
        (PLANNER, "get_actual_variable_endpoint", "cluster_heap_maintenance_restricted(heapRel) ||", "false ||", "planner heap fallback gate"),
        (APPROX, "statapprox_vm_all_visible", "!cluster_heap_maintenance_restricted(rel)", "true", "native statistics branch gate"),
        (VAC, "lazy_scan_skip", "if (vacrel->shared_maintenance_restricted)", "if (false)", "qualified VACUUM dispatch"),
        (HEAP, "cluster_heap_page_visibility_prepare", "GetControlFileCatalogVersion", "compiled_catalog_version", "PD loaded catalog gate"),
        (HEAP, "cluster_heap_page_all_visible", "cluster_heap_pcm_read_authority_valid", "skip_authority_check", "PD current authority"),
        (HEAP, "heapgetpage", "!cluster_heap_maintenance_restricted(scan->rs_base.rs_rd)", "true", "native pagemode branch gate"),
        (HANDLER, "heapam_scan_sample_next_tuple", "!cluster_heap_maintenance_restricted(scan->rs_rd)", "true", "native TABLESAMPLE branch gate"),
    ]
    for path, owner, old, new, label in mutations:
        changed = dict(source)
        _, start, end = bodies(source[path])[owner]
        original = source[path][start:end]
        if old not in original:
            raise ValueError("missing mutation input: " + owner + ":" + old)
        changed[path] = source[path][:start] + original.replace(old, new, 1) + source[path][end:]
        bad = audit(changed)
        tests.append(("reject removed " + label, bool(bad), bad))
    changed = dict(source)
    _, start, end = bodies(source[BITMAP])["BitmapPrefetch"]
    original = source[BITMAP][start:end]
    gate = "!cluster_heap_maintenance_restricted(node->ss.ss_currentRelation)"
    offset = original.rindex(gate)
    assert original.index(gate) != offset
    changed[BITMAP] = source[BITMAP][:start] + original[:offset] + "true" + original[offset + len(gate):] + source[BITMAP][end:]
    bad = audit(changed)
    tests.append(("reject removed parallel prefetch gate", bool(bad), bad))
    for path, owner in [(IOS, "IndexOnlyNext"), (PLANNER, "get_shared_variable_endpoint"),
                        (APPROX, "statapprox_vm_all_visible"), (VAC, "lazy_scan_skip_shared_page")]:
        changed = dict(source)
        _, start, end = bodies(source[path])[owner]
        original = source[path][start:end]
        changed[path] = source[path][:start] + original.replace("visibilitymap_read_end", "forgot_scope_end") + source[path][end:]
        bad = audit(changed)
        tests.append(("reject missing scope retirement in " + owner, bool(bad), bad))
    changed = dict(source)
    changed["contrib/new_reader/new_reader.c"] = '/* VM_ALL_VISIBLE(0) */\nconst char *read_note = "PageIsAllVisible(0)";\n'
    found = audit(changed)
    tests.append(("comments and strings do not create read entries", not found, found))
    print("1.." + str(len(tests)))
    failures = 0
    for index, (name, passed, details) in enumerate(tests, 1):
        print(("ok " if passed else "not ok ") + str(index) + " - " + name)
        if not passed:
            failures += 1
            for detail in details:
                print("# " + detail)
    return failures != 0


if __name__ == "__main__":
    raise SystemExit(run(pathlib.Path(sys.argv[1]) if len(sys.argv) > 1
                         else pathlib.Path(__file__).resolve().parents[3]))
