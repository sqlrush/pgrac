#!/usr/bin/env python3
"""Extract the native VACUUM range selector and qualified-page helper.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
from pathlib import Path
import re
import sys

from extract_vacuum_mx_fixture import function

source = Path(sys.argv[1]).read_text()
print(re.search(r"(?m)^#define SKIP_PAGES_THRESHOLD[^\n]+", source)[0])
if "\nlazy_scan_skip_shared_page(" in source:
    print(function(source, "lazy_scan_skip_shared_page", "static bool"))
print(function(source, "lazy_scan_skip", "static BlockNumber"))
