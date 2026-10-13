#!/usr/bin/env python3
"""Extract unchanged heap maintenance predicates for standalone execution.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""

import pathlib
import sys

from extract_vacuum_mx_fixture import function


root = pathlib.Path(sys.argv[1])
vacuum = (root / "access/heap/vacuumlazy.c").read_text()
visibility = (root / "access/heap/heapam_visibility.c").read_text()
verdict = (root / "cluster/cluster_visibility_verdict.c").read_text()
vm = (root / "access/heap/visibilitymap.c").read_text()
if "\ncluster_heap_maintenance_restricted(" in vm:
    print(function(vm, "cluster_heap_maintenance_restricted", "bool"))
print(function(verdict, "cluster_vis_prune_must_defer", "bool"))
print(function(visibility, "HeapTupleSatisfiesVacuumHorizon", "HTSV_Result"))
print(function(visibility, "HeapTupleSatisfiesVacuum", "HTSV_Result"))
print(function(vacuum, "heap_page_is_all_visible", "static bool"))
print(function(vacuum, "lazy_scan_prune", "static void"))
print(function(vacuum, "lazy_scan_noprune", "static bool"))
print(function(vacuum, "lazy_scan_skip", "static BlockNumber"))
