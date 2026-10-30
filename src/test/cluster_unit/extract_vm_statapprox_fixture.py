#!/usr/bin/env python3
"""Extract the original statistics scan and native tuple estimator.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
from pathlib import Path
import sys

from extract_vacuum_mx_fixture import function

source = Path(sys.argv[1]).read_text()
start = source.index("typedef struct output_type")
end = source.index("} output_type;", start) + len("} output_type;")
print(source[start:end])
if "\nstatapprox_vm_all_visible(" in source:
    print(function(source, "statapprox_vm_all_visible", "static bool"))
print(function(source, "statapprox_heap", "static void"))
print(function(Path(sys.argv[2]).read_text(), "vac_estimate_reltuples", "double"))
