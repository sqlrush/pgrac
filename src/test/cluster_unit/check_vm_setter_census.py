#!/usr/bin/env python3
"""Closed production VM setter census, including unregistered extension calls.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
import collections
import pathlib
import re
import sys

VM = "src/backend/access/heap/visibilitymap.c"
HEAP = "src/backend/access/heap/heapam.c"
VAC = "src/backend/access/heap/vacuumlazy.c"
APIS = ("visibilitymap_set", "visibilitymap_set_retry_aware", "visibilitymap_set_empty",
        "visibilitymap_set_locked", "visibilitymap_set_versioned_redo", "vm_image_versioned_redo",
        "log_heap_visible")
CALL = re.compile(r"\b(" + "|".join(APIS) + r")\s*\(")
DECLARATION = re.compile(r"(?m)^(?:extern|static)\s+(?:void|bool|XLogRecPtr)\s+(?:"
                         + "|".join(APIS) + r")\([^;{}]*\);\s*$")
EXPECTED = collections.Counter({
    (VM, "visibilitymap_set", "visibilitymap_set_locked"): 1,
    (VM, "visibilitymap_set_retry_aware", "visibilitymap_set_locked"): 1,
    (VM, "visibilitymap_set_empty", "visibilitymap_set_locked"): 1,
    (VM, "visibilitymap_set_locked", "log_heap_visible"): 1,
    (VM, "visibilitymap_set_versioned_redo", "vm_image_versioned_redo"): 1,
    (VM, "visibilitymap_clear_redo", "vm_image_versioned_redo"): 1,
    (HEAP, "heap_multi_insert", "visibilitymap_set"): 1,
    (HEAP, "heap_xlog_visible", "visibilitymap_set_versioned_redo"): 1,
    (HEAP, "heap_xlog_visible", "visibilitymap_set"): 1,
    (VAC, "lazy_scan_heap", "visibilitymap_set"): 2,
    (VAC, "lazy_scan_new_or_empty", "visibilitymap_set"): 2,
    (VAC, "lazy_vacuum_heap_page", "visibilitymap_set"): 1,
    (VAC, "lazy_publish_shared_empty", "visibilitymap_set_empty"): 1,
})


def clean(text):
    return re.sub(r'/\*.*?\*/|//[^\n]*|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'',
                  lambda m: re.sub(r"[^\n]", " ", m[0]), text, flags=re.S)


def bodies(text):
    text = clean(text)
    result = {}
    for match in re.finditer(r"(?m)^([a-zA-Z_]\w*)\([^;{}]*\)\s*\{", text):
        start = match.end() - 1
        level, end = 1, start + 1
        while level and end < len(text):
            level += (text[end] == "{") - (text[end] == "}")
            end += 1
        if level:
            raise ValueError("unterminated function " + match[1])
        result[match[1]] = (text[start:end], match.start(), end)
    return result


def audit(sources):
    errors, edges, table = [], collections.Counter(), {}
    for path, source in sources.items():
        if not CALL.search(source) and path != VM:
            continue
        functions = bodies(source)
        table[path] = functions
        for owner, (body, _, _) in functions.items():
            for call in CALL.finditer(body):
                edges[path, owner, call[1]] += 1
        # Anything outside a parsed body must be an ordinary declaration.
        # This also rejects a new inline/single-line body or continued macro
        # rather than silently omitting it from the closed owner list.
        text = clean(source)
        declarations = [(m.start(), m.end()) for m in DECLARATION.finditer(text)]
        for call in CALL.finditer(text):
            if not any(start <= call.start() < end for _, start, end in functions.values()) \
                    and not any(start <= call.start() < end for start, end in declarations):
                errors.append("unowned setter: " + path + ":" + call[1])
        # A macro must not hide a new setter from the function-owner census.
        for line in clean(source).splitlines():
            if line.lstrip().startswith("#define") and CALL.search(line):
                errors.append("unowned setter macro: " + path)
    if edges != EXPECTED:
        errors.append("unregistered setter edges: " + repr(dict(edges - EXPECTED))
                      + "; missing: " + repr(dict(EXPECTED - edges)))
    vm = table.get(VM, {})
    for name, terminal in [("visibilitymap_set", r"return\s*;"),
                           ("visibilitymap_set_retry_aware", r"return\s+false\s*;")]:
        body = vm.get(name, ("",))[0]
        gate = re.search(r"if\s*\(cluster_heap_maintenance_restricted\(rel\)\s*&&"
                         r"\s*!\(InRecovery\s*&&\s*!XLogRecPtrIsInvalid\(recptr\)\)\)"
                         r"\s*" + terminal, body)
        if not gate or body.find("LockBuffer(") in range(gate.start()):
            errors.append("unguarded setter: " + name)
    required = {
        "visibilitymap_set_empty": ["ClusterBufferCaptureMutationContext", "visibilitymap_heap_page_is_empty",
                                    "cluster_space_relation_identity_current_v1", "vm_redo_page_valid",
                                    "cluster_space_buffer_version_component"],
        "visibilitymap_set_versioned_redo": ["XLOG_HEAP2_VISIBLE", "SizeOfHeapVisible",
                                             "XLogRecGetRmid", "vm_image_versioned_redo"],
        "visibilitymap_read_qualified_begin": ["PGRAC_VM_QUALIFIED_CATALOG_VERSION",
                                              "GetControlFileCatalogVersion", "visibilitymap_read_snapshot_begin"],
    }
    for name, tokens in required.items():
        body = vm.get(name, ("",))[0]
        if any(token not in body for token in tokens):
            errors.append("missing qualification: " + name)
    # The only runtime bitmap OR is private and every incoming edge above is
    # registered. Recorded full images have their own exact WAL/edge validator.
    writers = [name for name, (body, _, _) in vm.items()
               if re.search(r"\bmap\s*\[[^\]]+\]\s*(?:\|=|=(?!=))", body)]
    if writers != ["visibilitymap_set_locked"]:
        errors.append("unregistered bitmap writer: " + repr(writers))
    for name in ["heap_insert", "heap_multi_insert"]:
        body = table.get(HEAP, {}).get(name, ("",))[0]
        if not re.search(r"if\s*\(\(options\s*&\s*HEAP_INSERT_FROZEN\)\s*&&"
                         r"\s*cluster_heap_maintenance_restricted\(relation\)\)", body):
            errors.append("missing direct freeze gate: " + name)
    return errors


def load(root):
    paths = []
    for folder in [root / "src/backend", root / "src/include", root / "contrib"]:
        paths.extend(p for p in folder.rglob("*") if p.suffix in {".c", ".h"})
    return {str(p.relative_to(root)): p.read_text() for p in paths}


def run(root):
    source = load(root)
    tests = []
    errors = audit(source)
    tests.append(("all production VM publishers have a registered qualification path", not errors, errors))
    for name in ["visibilitymap_set", "visibilitymap_set_retry_aware"]:
        changed = dict(source)
        body, start, end = bodies(source[VM])[name]
        assert "cluster_heap_maintenance_restricted(rel)" in body
        original = source[VM][start:end]
        changed[VM] = source[VM][:start] + original.replace("cluster_heap_maintenance_restricted(rel)", "false") + source[VM][end:]
        found = audit(changed)
        tests.append(("reject removed shared gate in " + name,
                      "unguarded setter: " + name in found, found))
    for owner, token in [("visibilitymap_set_empty", "ClusterBufferCaptureMutationContext"),
                         ("visibilitymap_set_versioned_redo", "XLOG_HEAP2_VISIBLE"),
                         ("visibilitymap_read_qualified_begin", "GetControlFileCatalogVersion")]:
        changed = dict(source)
        found = bodies(source[VM]).get(owner)
        if found is not None:
            _, start, end = found
            changed[VM] = source[VM][:start] + source[VM][start:end].replace(token, "missing_qualification") + source[VM][end:]
        bad = audit(changed)
        tests.append(("reject missing original qualification in " + owner,
                      "missing qualification: " + owner in bad, bad))
    for api in ["visibilitymap_set", "visibilitymap_set_locked", "visibilitymap_set_empty"]:
        changed = dict(source)
        changed["contrib/new_publisher/new_publisher.c"] = "void\nnew_publisher(void)\n{\n" + api + "(0);\n}\n"
        bad = audit(changed)
        tests.append(("reject unregistered extension call to " + api,
                      any(e.startswith("unregistered setter edges:") and "new_publisher" in e for e in bad), bad))
    for label, call in [
        ("single-line function", "void new_publisher(void) { visibilitymap_set(0); }"),
        ("continued macro", "#define NEW_PUBLISHER() \\\nvisibilitymap_set(0)"),
    ]:
        changed = dict(source)
        changed["contrib/new_publisher/new_publisher.c"] = call + "\n"
        bad = audit(changed)
        tests.append(("reject an unowned setter in a " + label,
                      any(e.startswith("unowned setter:") for e in bad), bad))
    changed = dict(source)
    changed[VM] += "\nvoid\nunregistered_vm_write(void)\n{\nmap[byte] |= flags;\n}\n"
    bad = audit(changed)
    tests.append(("reject a second direct bitmap writer",
                  any(e.startswith("unregistered bitmap writer:") for e in bad), bad))
    print("1.." + str(len(tests)))
    failures = 0
    for index, (name, passed, details) in enumerate(tests, 1):
        print(("ok " if passed else "not ok ") + str(index) + " - " + name)
        if not passed:
            failures += 1
            for detail in details:
                print("# " + detail)
    return failures != 0


if __name__ == "__main__":
    raise SystemExit(run(pathlib.Path(sys.argv[1]) if len(sys.argv) > 1 else pathlib.Path(__file__).resolve().parents[3]))
