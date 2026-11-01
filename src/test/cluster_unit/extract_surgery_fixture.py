#!/usr/bin/env python3
"""Extract the original repair entry for controlled admission tests.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
import pathlib
import re
import sys

from extract_vacuum_mx_fixture import function

source = pathlib.Path(sys.argv[1]).read_text()
option = re.search(r"^typedef enum HeapTupleForceOption\n\{.*?^\} HeapTupleForceOption;",
                   source, re.M | re.S)
if option is None:
    raise ValueError("missing production surgery options")
print(option.group())
print(function(source, "heap_force_common", "static Datum"))
