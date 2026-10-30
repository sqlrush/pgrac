#!/usr/bin/env python3
"""Extract the real AF decision, decoder and local buffer-read brackets.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""

import pathlib
import re
import sys

from extract_vacuum_mx_fixture import function

root = pathlib.Path(sys.argv[1])
vm = (root / "access/heap/visibilitymap.c").read_text()
heap = (root / "access/heap/heapam.c").read_text()
buf = (root / "storage/buffer/bufmgr.c").read_text()

for line in vm.splitlines():
    if re.match(r"#define\s+(MAPSIZE|HEAPBLOCKS_PER_\w+|HEAPBLK_TO_\w+)\b", line):
        print(line)

pin = re.search(r"typedef struct PrivateRefCountEntry\n\{.*?\n\} PrivateRefCountEntry;", buf, re.S)
if pin is None:
    raise SystemExit("missing original private pin structure")
print(pin.group())
print("static PrivateRefCountEntry *GetPrivateRefCountEntry(Buffer, bool) pg_attribute_unused();")

if "\nClusterConditionalLockBufferShare(" in buf:
    print("#define FIXTURE_HAS_LOCAL_SHARE 1")
    print(function(buf, "ClusterBufferHasSharedPin", "bool"))
    print(function(buf, "ClusterConditionalLockBufferShare", "bool"))
    print(function(buf, "ClusterUnlockBufferAfterConditionalRead", "void"))
print(function(buf, "ClusterUnlockReleaseBufferAfterConditionalRead", "void"))
print(function(vm, "vm_buffer_matches", "static bool"))
print(function(vm, "vm_redo_page_valid", "static bool"))
print(function(vm, "visibilitymap_pin_ok", "bool"))
print(function(vm, "visibilitymap_get_status", "uint8"))
print(function(vm, "visibilitymap_get_status_pinned", "bool"))
print(function(heap, "cluster_heap_lock_vm_needs_clear", "static bool"))
