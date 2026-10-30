#!/usr/bin/env python3
"""Extract whole heap PD consumers and their original selected-page helpers.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
import pathlib
import sys

from extract_vacuum_mx_fixture import function

root = pathlib.Path(sys.argv[1])
heap = (root / "heapam.c").read_text()
private = (root / "heapam_r4_private.h").read_text()
handler = (root / "heapam_handler.c").read_text()
if "\ncluster_heap_page_visibility_prepare(" in heap:
    print("#define FIXTURE_HAS_HEAP_PD_QUALIFICATION 1")
    start = private.index("typedef struct ClusterHeapPageVisibility")
    end = private.index("} ClusterHeapPageVisibility;", start)
    print(private[start:end + len("} ClusterHeapPageVisibility;")])
    start = private.index("extern void cluster_heap_page_visibility_prepare(")
    end = private.index(";", private.index("extern bool cluster_heap_page_all_visible(", start))
    print(private[start:end + 1])
    print(function(heap, "cluster_heap_pcm_read_authority_valid", "static bool"))
    print(function(heap, "cluster_heap_page_visibility_prepare", "void"))
    print(function(heap, "cluster_heap_page_all_visible", "bool"))
for name, result in [("cluster_heap_read_needs_copy", "static inline bool"),
                     ("cluster_heap_copy_read_tuple", "static inline void"),
                     ("cluster_heap_scan_capture_page", "static inline void"),
                     ("cluster_heap_scan_capture_tuple", "static inline void"),
                     ("cluster_heap_scan_page", "static inline Page"),
                     ("cluster_heap_scan_store", "static inline void")]:
    print(function(private, name, result))
print(function(heap, "heapgetpage", "void"))
print(function(handler, "SampleHeapTupleVisible", "static bool"))
print(function(handler, "heapam_scan_sample_next_tuple", "static bool"))
