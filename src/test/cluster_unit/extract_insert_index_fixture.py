#!/usr/bin/env python3
"""Extract the original ordinary INSERT publication consumers.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
import pathlib
import sys

from check_vm_setter_census import clean
from extract_vacuum_mx_fixture import function


def ordinary_insert(source):
    owner = function(source, "ExecInsert", "static TupleTableSlot *")
    marker = "/* insert the tuple normally */"
    if owner.count(marker) != 1:
        raise ValueError("ordinary INSERT block is not unique")
    start = clean(owner).rfind("{", 0, owner.index(marker))
    if start < 0:
        raise ValueError("missing ordinary INSERT block")
    text = clean(owner)
    level, end = 1, start + 1
    while level and end < len(text):
        level += (text[end] == "{") - (text[end] == "}")
        end += 1
    if level:
        raise ValueError("unterminated ordinary INSERT block")
    return owner[start + 1:end - 1]


if __name__ == "__main__":
    source = pathlib.Path(sys.argv[1]).read_text()
    if sys.argv[2] == "--am":
        print(function(source, "heapam_tuple_insert", "static void"))
    elif sys.argv[2] == "--ordinary":
        print(ordinary_insert(source))
    else:
        raise ValueError("expected --am or --ordinary")
