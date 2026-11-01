#!/usr/bin/env python3
"""Extract the original INSERT publication consumers.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
import pathlib
import sys

from check_vm_setter_census import clean
from extract_vacuum_mx_fixture import function


def insert_block(source, marker):
    owner = function(source, "ExecInsert", "static TupleTableSlot *")
    if owner.count(marker) != 1:
        raise ValueError("INSERT block is not unique: " + marker)
    start = clean(owner).rfind("{", 0, owner.index(marker))
    if start < 0:
        raise ValueError("missing INSERT block: " + marker)
    text = clean(owner)
    level, end = 1, start + 1
    while level and end < len(text):
        level += (text[end] == "{") - (text[end] == "}")
        end += 1
    if level:
        raise ValueError("unterminated INSERT block: " + marker)
    return owner[start + 1:end - 1]


def ordinary_insert(source):
    return insert_block(source, "/* insert the tuple normally */")


if __name__ == "__main__":
    source = pathlib.Path(sys.argv[1]).read_text()
    if sys.argv[2] == "--am":
        print(function(source, "heapam_tuple_insert", "static void"))
    elif sys.argv[2] == "--ordinary":
        print(ordinary_insert(source))
    elif sys.argv[2] == "--speculative-am":
        for name in ("heapam_tuple_insert_speculative",
                     "heapam_tuple_complete_speculative"):
            print(function(source, name, "static void"))
    elif sys.argv[2] == "--speculative":
        print(insert_block(source, "/* Perform a speculative insertion. */"))
    else:
        raise ValueError("unknown INSERT extraction mode")
