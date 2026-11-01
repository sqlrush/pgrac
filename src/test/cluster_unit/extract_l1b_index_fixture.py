#!/usr/bin/env python3
"""Extract unchanged native VACUUM/index consumers for ordering regression tests.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
import pathlib
import re
import sys


def function(source, name, result):
    matches = list(re.finditer(r"^" + re.escape(name) + r"\(", source, re.M))
    if len(matches) != 1:
        raise ValueError("expected one function: " + name)
    start = matches[0].start()
    end = re.search(r"^\}", source[start:], re.M)
    if end is None:
        raise ValueError("missing function end: " + name)
    return result + "\n" + source[start:start + end.end()] + "\n"


def extract(lazy, vacuum, index):
    start = lazy.index("typedef enum\n{")
    end = lazy.index("} LVRelState;", start) + len("} LVRelState;")
    print(lazy[start:end])
    start = lazy.index("typedef struct LVSavedErrInfo")
    end = lazy.index("} LVSavedErrInfo;", start) + len("} LVSavedErrInfo;")
    print(lazy[start:end])
    print('#include "cluster_l1b_index_test_leaves.h"')
    for name, result in [
        ("update_vacuum_error_info", "static void"),
        ("restore_vacuum_error_info", "static void"),
        ("lazy_check_wraparound_failsafe", "static bool"),
        ("lazy_vacuum_heap_page", "static int"),
        ("lazy_vacuum_heap_rel", "static void"),
        ("lazy_vacuum_one_index", "static IndexBulkDeleteResult *"),
        ("lazy_vacuum_all_indexes", "static bool"),
        ("lazy_vacuum", "static void"),
    ]:
        print(function(lazy, name, result))
    print(function(vacuum, "vac_cmp_itemptr", "static int"))
    print(function(vacuum, "vac_tid_reaped", "static bool"))
    print(function(vacuum, "vac_bulkdel_one_index", "IndexBulkDeleteResult *"))
    for name, following in [("RELATION_CHECKS", "SCAN_CHECKS"),
                            ("CHECK_REL_PROCEDURE(pname)", "CHECK_SCAN_PROCEDURE")]:
        start = index.index("#define " + name + " \\")
        end = index.index("#define " + following, start)
        print(index[start:end])
    print(function(index, "index_bulk_delete", "IndexBulkDeleteResult *"))
    print("#undef RELATION_CHECKS\n#undef CHECK_REL_PROCEDURE")


if __name__ == "__main__":
    extract(*(pathlib.Path(arg).read_text() for arg in sys.argv[1:]))
