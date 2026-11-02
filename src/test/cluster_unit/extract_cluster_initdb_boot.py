"""Extract complete original BOOT dispatch bodies without rewriting."""
import pathlib
import re
import sys

parts = []
for name, path in [("BootStrapCLOGWithIdentity", sys.argv[1]),
                   ("pgrac_native_create_boot_undo", sys.argv[2]),
                   ("popen_check", sys.argv[2]),
                   ("initdb_boot_recovery_accept", sys.argv[3]),
                   ("cluster_wal_thread_initdb_accept", sys.argv[3])]:
    source = pathlib.Path(path).read_text()
    matches = re.findall(r"(?:static )?(?:bool|void|FILE\s*\*)\n" + name + r"\(.*?^\}", source, re.M | re.S)
    if len(matches) != 1:
        raise SystemExit("missing/duplicate " + name)
    parts += matches
out = pathlib.Path(sys.argv[4])
tmp = out.with_suffix(".tmp")
tmp.write_text("\n\n".join(parts) + "\n")
tmp.replace(out)
