"""Compile complete original initdb path/custody functions in the fixture."""
import pathlib
import re
import sys

source = pathlib.Path(sys.argv[1]).read_text()
required = [
    "pgrac_hw_resolve_target", "pgrac_hw_path_contains",
    "pgrac_native_config_check", "pgrac_native_check_directory",
    "pgrac_native_check_directories", "pgrac_native_prepare_paths",
    "pgrac_native_bind_creation",
]
optional = ["pgrac_native_read_registration"]
parts = []
for name in required + optional:
    bodies = re.findall(
        r"static (?:void|bool|char \*)\n" + name + r"\(.*?^\}", source, re.M | re.S
    )
    if len(bodies) != 1:
        if name in optional and not bodies:
            continue  # Parent has the complete ingress inline in bind.
        raise SystemExit("missing/duplicate complete body: " + name)
    if name in optional:
        parts.insert(0, "static void " + name + "(void);")
    parts.append(bodies[0])
out = pathlib.Path(sys.argv[2])
temporary = out.with_name(out.name + ".tmp")
temporary.write_text("\n\n".join(parts) + "\n")
temporary.replace(out)
