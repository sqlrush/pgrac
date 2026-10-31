#!/usr/bin/env python3
"""Extract the actual operation-local VM writer brackets.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
import pathlib
import re
import sys
from extract_vacuum_mx_fixture import function

source = pathlib.Path(sys.argv[1]).read_text()
if len(sys.argv) > 2 and sys.argv[2] == "--dml-writer":
    if "\ncluster_heap_vm_dml_writer_lock(" in source:
        print(function(source, "cluster_heap_vm_dml_writer_lock", "static bool"))
    sys.exit(0)

if len(sys.argv) > 2 and sys.argv[2] == "--tuple-writer":
    if "\ncluster_heap_vm_tuple_writer_lock(" in source:
        print(function(source, "cluster_heap_vm_tuple_writer_lock", "static bool"))
    sys.exit(0)

match = re.search(r"^typedef struct ClusterHeapVmMutationState\n\{.*?^\} ClusterHeapVmMutationState;", source, re.M | re.S)
if match is not None:
    print("#define FIXTURE_HAS_INSERT_VM_STATE 1")
    print(match.group())
    print(function(source, "cluster_heap_vm_mutation_lock", "static bool"))
    print(function(source, "cluster_heap_vm_mutation_prepare", "static void"))
