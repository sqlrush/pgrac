#!/usr/bin/env python3
"""Extract the original UPDATE/DELETE executor and heap AM consumers.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
import pathlib
import sys

from extract_vacuum_mx_fixture import function


if __name__ == "__main__":
    source = pathlib.Path(sys.argv[1]).read_text()
    if sys.argv[2] == "--types":
        for name in ("ModifyTableContext", "UpdateContext"):
            start = source.index("typedef struct " + name + "\n{")
            end = source.index("} " + name + ";", start) + len("} " + name + ";")
            print(source[start:end])
    elif sys.argv[2] == "--am":
        print(function(source, "heapam_tuple_update", "static TM_Result"))
    elif sys.argv[2] == "--delete-am":
        print(function(source, "heapam_tuple_delete", "static TM_Result"))
    elif sys.argv[2] == "--delete-executor":
        for name, result in (("ExecDeletePrologue", "static bool"),
                             ("ExecDeleteAct", "static TM_Result"),
                             ("ExecDeleteEpilogue", "static void"),
                             ("ExecDelete", "static TupleTableSlot *")):
            print(function(source, name, result))
    elif sys.argv[2] == "--executor":
        for name, result in (("ExecUpdatePrepareSlot", "static void"),
                             ("ExecUpdateAct", "static TM_Result"),
                             ("ExecUpdateEpilogue", "static void"),
                             ("ExecUpdate", "static TupleTableSlot *")):
            print(function(source, name, result))
    else:
        raise ValueError("unknown UPDATE extraction mode")
