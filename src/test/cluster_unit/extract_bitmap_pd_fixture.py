#!/usr/bin/env python3
"""Extract complete original bitmap heap callbacks without rewriting bodies.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
import pathlib
import sys

from extract_vacuum_mx_fixture import function

source = pathlib.Path(sys.argv[1]).read_text()
for name in ("heapam_scan_bitmap_next_block", "heapam_scan_bitmap_next_tuple"):
    print(function(source, name, "static bool"))
