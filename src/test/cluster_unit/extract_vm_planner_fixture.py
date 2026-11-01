#!/usr/bin/env python3
"""Extract the complete planner endpoint and its original-data consumers.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
from pathlib import Path
import sys

from extract_vacuum_mx_fixture import function

source = Path(sys.argv[1]).read_text()
for name, result in [("get_variable_endpoint_from_index", "static void"),
                     ("get_shared_variable_endpoint", "static bool"),
                     ("get_actual_variable_endpoint", "static bool")]:
    if "\n" + name + "(" in source:
        print(function(source, name, result))
