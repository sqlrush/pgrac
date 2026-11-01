"""Extract complete original creator bodies; refuse missing/duplicate entries."""
import pathlib
import re
import sys
source = pathlib.Path(sys.argv[1]).read_text()
parts = []
for name in ["InitdbDirectory", "InitdbOrigin", "InitdbStartupObject"]:
    matches = re.findall(r"typedef struct " + name + r" \{.*?\} " + name + r";", source, re.S)
    if len(matches) != 1:
        raise SystemExit("missing/duplicate type " + name)
    parts += matches
parts.append("""static void creation_sources_current(const PgracInitdbCohortContext *,
 const ClusterSharedConfigRef *, InitdbDirectory [4], InitdbOrigin *,
 const ClusterInitdbTree *, const ClusterInitdbTree *, bool);""")
for name in ["owned_directory", "same_directory", "parent_current", "directory_current",
             "creation_derived_current", "creation_legacy_format_current",
             "creation_relmap_registry_valid", "creation_registry_valid", "creation_registry_current", "creation_root_publish"]:
    matches = re.findall(r"static (?:bool|void)\n" + name + r"\(.*?^\}", source, re.M | re.S)
    if len(matches) != 1:
        raise SystemExit("missing/duplicate function " + name)
    parts += matches
output = pathlib.Path(sys.argv[2])
temporary = output.with_name(output.name + ".tmp")
temporary.write_text("\n\n".join(parts) + "\n")
temporary.replace(output)
