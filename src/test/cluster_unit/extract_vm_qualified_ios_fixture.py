#!/usr/bin/env python3
"""Keep original VM publication/read and IOS bodies in one behavioral fixture.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
import pathlib
import sys

from extract_vacuum_mx_fixture import function

root = pathlib.Path(sys.argv[1])
kind = sys.argv[2]
if kind == "format":
    text = (root / "access/transam/xlog.c").read_text()
    if "\nGetControlFileCatalogVersion(" in text:
        print("#define FIXTURE_HAS_CONTROL_CATALOG_GETTER 1")
        print(function(text, "GetControlFileCatalogVersion", "uint32"))
elif kind == "read":
    text = (root / "access/heap/visibilitymap.c").read_text()
    for name, result in [("visibilitymap_set_retry_aware", "bool"),
                         ("visibilitymap_read_begin", "bool"),
                         ("visibilitymap_read_snapshot_begin", "bool"),
                         ("visibilitymap_read_qualified_begin", "bool"),
                         ("visibilitymap_read_end", "void")]:
        if "\n" + name + "(" in text:
            if name == "visibilitymap_read_qualified_begin":
                print("#define FIXTURE_HAS_QUALIFIED_VM_READER 1")
            print(function(text, name, result))
elif kind == "ios":
    print(function((root / "executor/nodeIndexonlyscan.c").read_text(),
                   "IndexOnlyNext", "static TupleTableSlot *"))
else:
    raise SystemExit("expected format, read, or ios")
