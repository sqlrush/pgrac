#!/usr/bin/env python3
"""Extract the actual operation-local INSERT VM writer bracket.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
import pathlib
import re
import sys
from extract_vacuum_mx_fixture import function

source = pathlib.Path(sys.argv[1]).read_text()
match = re.search(r"^typedef struct ClusterHeapVmInsertState\n\{.*?^\} ClusterHeapVmInsertState;", source, re.M | re.S)
if match is not None:
    print("#define FIXTURE_HAS_INSERT_VM_STATE 1")
    print(match.group())
    print(function(source, "cluster_heap_vm_insert_lock", "static bool"))
    print(function(source, "cluster_heap_vm_insert_prepare", "static void"))
