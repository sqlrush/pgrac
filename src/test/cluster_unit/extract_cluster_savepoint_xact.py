#!/usr/bin/env python3
"""Extract actual xact frame/name functions and restart branches for tests."""
import re
import sys
from pathlib import Path

source = Path(sys.argv[1]).read_text()
output = Path(sys.argv[2])
types = re.search(
    r"typedef enum TransState\b.*?typedef TransactionStateData \*TransactionState;",
    source, re.S,
)
if types is None:
    raise SystemExit("native transaction types not found")

names = ("PushTransaction", "PopTransaction", "DefineSavepoint", "ReleaseSavepoint",
         "RollbackToSavepoint", "BeginInternalSubTransaction",
         "ReleaseCurrentSubTransaction", "RollbackAndReleaseCurrentSubTransaction",
         "IsTransactionState", "GetCurrentFullTransactionId",
         "GetCurrentFullTransactionIdIfAny", "GetTopFullTransactionIdIfAny",
         "GetCurrentSubTransactionId", "GetCurrentCommandId")
if "GetFullTransactionIdForSubTransactionIfAny(" in source:
    names += ("GetFullTransactionIdForSubTransactionIfAny",)
bodies = []
for name in names:
    start = re.search(r"^(?:static )?(?:void|bool|FullTransactionId|SubTransactionId|CommandId)\n" + name + r"\([^)]*\)\n\{", source, re.M)
    if start is None:
        raise SystemExit("native function not found: " + name)
    end = re.search(r"^}", source[start.end():], re.M)
    if end is None:
        raise SystemExit("native function end not found: " + name)
    bodies.append(source[start.start():start.end() + end.end()] + "\n")

# Execute the native state transition with its real cluster hook, if present.
# The rest of abort's unchanged resource machinery is outside this fixture.
abort = source.index("static void\nAbortSubTransaction(void)\n{")
start = source.index('\tShowTransactionState("AbortSubTransaction");', abort)
end = source.index("\ts->state = TRANS_ABORT;", start) + len("\ts->state = TRANS_ABORT;")
bodies.append("static void\ntest_native_subabort_transition(void)\n{\n"
              "\tTransactionState s = CurrentTransactionState;\n"
              + source[start:end] + "\n}\n")

command = source.index("void\nCommitTransactionCommand(void)")
for state, name in (("TBLOCK_SUBRESTART", "test_native_restart_live"),
                    ("TBLOCK_SUBABORT_RESTART", "test_native_restart_aborted")):
    start = source.index("\t\tcase " + state + ":", command)
    block = source.index("\n\t\t\t{", start)
    end = source.index("\n\t\t\tbreak;", block)
    bodies.append("static void\n" + name + "(void)\n{\n"
                  "\tTransactionState s = CurrentTransactionState;\n"
                  + source[block:end] + "\n}\n")

text = ("/* Generated from actual xact.c; do not edit. */\n"
        "#ifdef CLUSTER_SAVEPOINT_TEST_TYPES\n" + types.group() + "\n#else\n"
        + "\n".join(bodies) + "#endif\n")
temp = output.with_suffix(output.suffix + ".tmp")
temp.write_text(text)
temp.replace(output)
