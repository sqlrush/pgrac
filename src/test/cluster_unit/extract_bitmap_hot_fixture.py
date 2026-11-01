#!/usr/bin/env python3
"""Extract original bool HOT traversal and optional qualified bitmap wrapper.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
import pathlib
import sys

from extract_vacuum_mx_fixture import function

source = pathlib.Path(sys.argv[1]).read_text()
visibility = pathlib.Path(sys.argv[2]).read_text()
print('#include "cluster/cluster_multixact_current.h"')
print(function(visibility, "cluster_vis_prune_must_defer", "static bool"))
start = source.index("typedef struct ClusterCurrentMxHeapFingerprint")
end = source.index("} ClusterCurrentMxFailurePhase;", start)
print(source[start:end + len("} ClusterCurrentMxFailurePhase;")])
print('#include "cluster_bitmap_hot_test_stubs.h"')
if "\nheap_hot_search_buffer_impl(" in source:
    print(function(source, "heap_hot_search_buffer_impl", "static bool"))
print(function(source, "heap_hot_search_buffer", "static bool"))
if "\ncluster_heap_hot_search_buffer(" in source:
    print(function(source, "cluster_heap_hot_search_buffer", "static bool"))
