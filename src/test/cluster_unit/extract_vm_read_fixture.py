#!/usr/bin/env python3
"""Extract the raw VM reader and real index-only consumer without rewriting.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""

import pathlib
import re
import sys

from extract_vacuum_mx_fixture import function

root = pathlib.Path(sys.argv[1])
vm = (root / "access/heap/visibilitymap.c").read_text()
if "\ncluster_heap_maintenance_restricted(" in vm:
    print(function(vm, "cluster_heap_maintenance_restricted", "bool"))
print(function(vm, "vm_readbuf", "static Buffer"))
print(function(vm, "visibilitymap_get_status", "uint8"))
if "\nvisibilitymap_get_status_coherent(" in vm:
    print("#define FIXTURE_HAS_COHERENT_VM_READER 1")
    print(function(vm, "vm_redo_page_valid", "static bool"))
    print(function(vm, "visibilitymap_get_status_coherent", "bool"))
print(function((root / "executor/nodeIndexonlyscan.c").read_text(),
               "IndexOnlyNext", "static TupleTableSlot *"))

# The remaining consumers use these exact branch expressions. Keep their
# surrounding page/index scan types so the test cannot replace relation policy.
def assignment(path, owner, variable):
    body = function(path.read_text(), owner, "unused")
    return re.findall(r"\b" + variable + r"\s*=\s*[^;]+;", body)


def wrapper(name, args, expression):
    print("static bool\n" + name + "(" + args + ")\n{\nreturn " + expression + ";\n}")


for name, path, owner, args in [
    ("fixture_pagemode", root / "access/heap/heapam.c", "heapgetpage",
     "HeapScanDesc scan, Page page, Snapshot snapshot"),
    ("fixture_sample", root / "access/heap/heapam_handler.c", "heapam_scan_sample_next_tuple",
     "TableScanDesc scan, Page page")]:
    statements = assignment(path, owner, "all_visible")
    assert len(statements) == 1
    wrapper(name, args, statements[0].split("=", 1)[1].strip().removesuffix(";"))

path = root / "executor/nodeBitmapHeapscan.c"
for owner, expected in [("BitmapHeapNext", 1), ("BitmapPrefetch", 2)]:
    statements = assignment(path, owner, "skip_fetch")
    assert len(statements) == expected
    for index, statement in enumerate(statements):
        wrapper("fixture_" + owner + str(index),
                "BitmapHeapScanState *node, TBMIterateResult *tbmres pg_attribute_unused(), "
                "TBMIterateResult *tbmpre pg_attribute_unused()",
                statement.split("=", 1)[1].strip().removesuffix(";"))


def vm_condition(path, owner):
    body = function(path.read_text(), owner, "unused")
    at = body.index("VM_ALL_VISIBLE(")
    start = body.rfind("if (", 0, at) + len("if (")
    depth = 1
    end = start
    while depth:
        if body[end] == "(": depth += 1
        if body[end] == ")": depth -= 1
        end += 1
    return body[start:end - 1]

wrapper("fixture_planner_fetch", "Relation heapRel, BlockNumber block, Buffer vmbuffer",
        vm_condition(root / "utils/adt/selfuncs.c", "get_actual_variable_endpoint"))
wrapper("fixture_statapprox_skip", "Relation rel, BlockNumber blkno, Buffer vmbuffer",
        vm_condition(root.parent.parent / "contrib/pgstattuple/pgstatapprox.c", "statapprox_heap"))
