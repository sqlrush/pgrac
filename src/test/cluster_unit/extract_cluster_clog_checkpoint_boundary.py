"""Extract the original checkpoint prelude, CLOG call and shutdown check."""
import pathlib
import sys

source = pathlib.Path(sys.argv[1]).read_text()
owner = source[source.index("\nCreateCheckPoint(int flags)"):]
start = owner.index("\tSyncPreCheckpoint();")
end = owner.index("\n#ifdef USE_PGRAC_CLUSTER\n\n\t/*\n\t * RF-ROOT P6", start)
prelude = owner[start:end]
start = owner.index("\tif (shutdown && checkPoint.redo != ProcLastRecPtr)")
end = owner.index("\n\n", start)
check = owner[start:end]
guts = source[source.index("\nCheckPointGuts("):]
guts = guts[:guts.index("\n}")]
if guts.count("\tCheckPointCLOG();") != 1:
    raise SystemExit("missing/duplicate original CLOG checkpoint call")
for name, text in (("prelude", prelude), ("check", check), ("guts", "\tCheckPointCLOG();")):
    out = pathlib.Path(sys.argv[2] + "_" + name + ".inc")
    temporary = out.with_suffix(".tmp")
    temporary.write_text(text + "\n")
    temporary.replace(out)
