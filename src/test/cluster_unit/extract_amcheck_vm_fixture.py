#!/usr/bin/env python3
"""Extract the original heap checker and its context types.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
from pathlib import Path
import sys

from extract_vacuum_mx_fixture import function


source = Path(sys.argv[1]).read_text()
if sys.argv[2] == "--types":
    start = source.index("typedef enum XidBoundsViolation")
    end = source.index("} HeapCheckContext;", start) + len("} HeapCheckContext;")
    print(source[start:end])
elif sys.argv[2] == "--entry":
    print(function(source, "verify_heapam", "Datum"))
else:
    raise ValueError("unknown heap checker extraction mode")
