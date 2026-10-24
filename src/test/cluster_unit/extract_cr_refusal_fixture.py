#!/usr/bin/env python3
"""Extract the real CR refusal routine and its four consumer cuts.

Only the enclosing test functions are synthetic. The error routine, injection
function, origin guard, retained-history guard and dispatch/refusal prefix are
copied verbatim. This does not execute a whole CR construction or transport.

Author: SqlRush <sqlrush@gmail.com>
"""

import pathlib
import re
import sys


def function(source, name, declaration):
    match = re.search(r"^(?:pg_attribute_noreturn\(\) )?" + name + r"\(", source, re.M)
    if match is None:
        raise ValueError("missing function: " + name)
    end = re.search(r"^\}", source[match.start():], re.M)
    if end is None:
        raise ValueError("missing function end: " + name)
    return declaration + "\n" + source[match.start():match.start() + end.end()]


def extract(source):
    print(function(source, "cr_coordinator_refuse_runtime_remote", "static void"))
    print(function(source, "cr_check_error_injections", "static void"))

    walk = function(source, "cr_walk_chain", "static void")
    start = walk.index("\t\t{\n\t\t\tNodeId cr_origin =")
    end = walk.index("\n\t\tlen = cluster_undo_get_record", start)
    print("static void\nfixture_walk_origin(UBA uba)\n{")
    print(walk[start:end])
    print("}")

    history = function(source, "cr_construct_retained_history", "static bool")
    start = history.index("\t\t\tif (cluster_cr_coordinator_classify_origin(origin)")
    end = history.index("\n\t\t\tif (*steps >=", start)
    print("static void\nfixture_history_origin(NodeId origin, bool selected, "
          "bool server_mode, bool *out_partial)\n{\n\tdo {")
    print(history[start:end])
    print("\t} while (false);\n}")

    construct = function(source, "cr_construct_from_copy", "static void")
    start = construct.index("\t\tif (cluster_cr_coordinator_classify_origin(head_origin)")
    end = construct.index("\n\t\t\tif (!source_result.partial)", start)
    print("static void\nfixture_source_dispatch(NodeId head_origin, "
          "RelFileLocator cur_locator, ForkNumber cur_fork, "
          "BlockNumber cur_block, SCN read_scn, char *dst_page)\n{")
    print(construct[start:end])
    print("\t\t}\n}")


if __name__ == "__main__":
    extract(pathlib.Path(sys.argv[1]).read_text())
