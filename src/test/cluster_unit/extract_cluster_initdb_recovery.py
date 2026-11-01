"""Extract complete original creator ingress/current bodies without rewriting."""
import pathlib
import re
import sys
s = pathlib.Path(sys.argv[1]).read_text()
parts = []
for name in ["InitdbDirectory", "InitdbOrigin"]:
    found = re.findall(r"typedef struct " + name + r" \{.*?\} " + name + r";", s, re.S)
    if len(found) != 1:
        raise SystemExit("missing/duplicate " + name)
    parts += found
parts.append("""
static void request_current(const PgracInitdbCohortContext *, const ClusterSharedConfigRef *);
static void run_child_registered(char **, uint64, const PgracInitdbRecoveryContext *);
static void run_child(char **, uint64);
static void control_read(InitdbOrigin *, uint32, uint64, bool);
static void directory_current(const InitdbDirectory *);
static void creation_side_current(const InitdbDirectory *, const InitdbOrigin *, unsigned);
static void creation_tree_recheck(int, bool, const ClusterInitdbTree *);
static void creation_storage_current(const InitdbOrigin *);
""")
for name in ["run_origin", "creation_sources_current", "creation_relmap_registry_valid"]:
    found = re.findall(r"static (?:void|bool)\n" + name + r"\(.*?^\}", s, re.M | re.S)
    if len(found) != 1:
        raise SystemExit("missing/duplicate " + name)
    parts += found
out = pathlib.Path(sys.argv[2])
tmp = out.with_name(out.name + ".tmp")
tmp.write_text("\n\n".join(parts) + "\n")
tmp.replace(out)
