#!/usr/bin/env python3
"""Extract the original visibility metadata reports.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
from pathlib import Path
import sys

from extract_vacuum_mx_fixture import function

source = Path(sys.argv[1]).read_text()
start = source.index("typedef struct vbits")
end = source.index("} vbits;", start) + len("} vbits;")
print(source[start:end])
print(function(source, "check_relation_relkind", "static void"))
if "\nvisibilitymap_get_status_for_report(" in source:
    print(function(source, "visibilitymap_get_status_for_report", "static int32"))
for name in ("pg_visibility_map", "pg_visibility", "pg_visibility_map_summary"):
    print(function(source, name, "Datum"))
print(function(source, "collect_visibility_data", "static vbits *"))
