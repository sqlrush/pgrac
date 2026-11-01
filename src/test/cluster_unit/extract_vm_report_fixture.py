#!/usr/bin/env python3
"""Extract the original visibility metadata reports.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
from pathlib import Path
import sys

from extract_vacuum_mx_fixture import function

source = Path(sys.argv[1]).read_text()
mode = sys.argv[2] if len(sys.argv) > 2 else "--report"
if mode == "--corrupt":
    start = source.index("typedef struct corrupt_items")
    end = source.index("} corrupt_items;", start) + len("} corrupt_items;")
    print(source[start:end])
    print(function(source, "check_relation_relkind", "static void"))
    print(function(source, "record_corrupt_item", "static void"))
    print(function(source, "collect_corrupt_items", "static corrupt_items *"))
elif mode == "--report":
    start = source.index("typedef struct vbits")
    end = source.index("} vbits;", start) + len("} vbits;")
    print(source[start:end])
    print(function(source, "check_relation_relkind", "static void"))
    if "\nvisibilitymap_get_status_for_report(" in source:
        print(function(source, "visibilitymap_get_status_for_report", "static int32"))
    for name in ("pg_visibility_map", "pg_visibility", "pg_visibility_map_summary"):
        print(function(source, name, "Datum"))
    print(function(source, "collect_visibility_data", "static vbits *"))
else:
    raise ValueError("unknown visibility report extraction mode")
