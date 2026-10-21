#!/usr/bin/env python3
"""Extract real VACUUM consumers for the standalone shared-MX test.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""

import pathlib
import re
import sys


def function(source, name, return_type):
    match = re.search(r"^" + re.escape(name) + r"\([^\n]*", source, re.M)
    if match is None:
        raise ValueError("missing consumer: " + name)
    end = re.search(r"^\}", source[match.start():], re.M)
    if end is None:
        raise ValueError("missing consumer end: " + name)
    return return_type + "\n" + source[match.start():match.start() + end.end()] + "\n"


def extract(source):
    start = source.index("typedef enum\n{")
    end = source.index("} LVPagePruneState;", start) + len("} LVPagePruneState;")
    state = source[start:end]
    print(state)
    if "shared_mx_deferred_pages" in state:
        print("#define FIXTURE_HAS_MX_DEFER 1")
    if "shared_freeze_input_valid" in state:
        print("#define FIXTURE_HAS_FREEZE_INPUT 1")
    print('#include "cluster_vacuum_mx_test_stubs.h"')
    if re.search(r"^lazy_scan_shared_mx\(", source, re.M):
        print(function(source, "lazy_scan_shared_mx", "static bool"))
    print(function(source, "lazy_scan_heap", "static void"))

    # Keep both native assertions and the real publication decision intact.
    start = source.index("\tAssert(", source.index("* Prepare to update rel's pg_class entry."))
    end = source.index("\n\t/*\n\t * For safety, clamp relallvisible", start)
    print("static void\nfixture_publish_cutoffs(LVRelState *vacrel)\n{")
    print(source[start:end])
    print("}")

    # Execute the existing SPACE-identity refusal before any preservation.
    start = source.index("\tvacrel->rel = rel;")
    end = source.index("\tvac_open_indexes(", start)
    print("static void\nfixture_bind_relation(LVRelState *vacrel, Relation rel)\n{")
    print("\tconst VacuumParams fixture_params = {0};")
    print("\tconst VacuumParams *params = &fixture_params;")
    print(source[start:end])
    print("}")


if __name__ == "__main__":
    extract(pathlib.Path(sys.argv[1]).read_text())
