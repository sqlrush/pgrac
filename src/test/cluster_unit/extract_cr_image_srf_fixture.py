#!/usr/bin/env python3
"""Copy the actual diagnostic SRFs, including their shared implementation.

For an older source without the target entry, the fixture invokes its existing
own-instance entry. This exposes the old acquisition behavior against the same
target-only expectations, without inventing an implementation for the parent.
All function bodies are copied verbatim; PG/transport boundaries live in C.

Author: SqlRush <sqlrush@gmail.com>
"""
import pathlib
import re
import sys


def function(source, name, declaration):
    start = re.search(r"^" + name + r"\(", source, re.M)
    if start is None:
        raise ValueError("missing function: " + name)
    end = re.search(r"^\}", source[start.start():], re.M)
    if end is None:
        raise ValueError("missing function end: " + name)
    return declaration + "\n" + source[start.start():start.start() + end.end()]


source = pathlib.Path(sys.argv[1]).read_text()
enabled, disabled = source.split("#else /* !USE_PGRAC_CLUSTER */", 1)
if re.search(r"^cr_test_image_rows\(", enabled, re.M):
    print(function(enabled, "cr_test_image_rows", "static Datum"))
print(function(enabled, "cluster_cr_test_image", "Datum"))
if re.search(r"^cluster_cr_test_target_image\(", enabled, re.M):
    print(function(enabled, "cluster_cr_test_target_image", "Datum"))
    print("#define fixture_target cluster_cr_test_target_image")
    stub = function(disabled, "cluster_cr_test_target_image", "Datum")
else:
    print("#define fixture_target cluster_cr_test_image")
    stub = function(disabled, "cluster_cr_test_image", "Datum")
print(stub.replace("cluster_cr_test_target_image(", "fixture_disabled_target(")
      .replace("cluster_cr_test_image(", "fixture_disabled_target("))
