#!/usr/bin/env python3
# PGRAC: regression census of undo final-file creation and publication owners.
"""Keep every undo final-file publisher behind inventory tracking or disable.

This is a source regression check, not the runtime completeness certificate.
New file-creation sites require an explicit classification and dynamic tests.
"""

from __future__ import annotations

import collections
import pathlib
import re
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[3]
SMGR = "src/backend/cluster/storage/cluster_undo_smgr.c"
XLOG = "src/backend/cluster/storage/cluster_undo_xlog.c"
TOKEN = re.compile(
    r"\bO_CREAT\b|\b(?:link|linkat|rename|renameat|durable_rename|creat|fopen)\s*\("
)
LEXICAL = re.compile(r'/\*.*?\*/|//[^\n]*|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'', re.S)
FUNCTION = re.compile(r"^(\w+)\s*\([^;{}]*\)\s*\{", re.M)

# Temporary provision files and the root descriptor are not segment members.
# Their exact owners are classified too; they are not filename exemptions.
SITES = {
    (SMGR, "cluster_undo_smgr_recovery_materialize_v1"): {"O_CREAT": 1},
    (SMGR, "cluster_undo_smgr_provision_temp_create"): {"O_CREAT": 1},
    (SMGR, "cluster_undo_smgr_root_descriptor_publish"): {"O_CREAT": 1, "link": 1},
    (SMGR, "provision_temp_publish_impl"): {"link": 1},
    (XLOG, "cluster_undo_redo_open_segment"): {"O_CREAT": 1},
    (XLOG, "cluster_undo_redo_segment_init"): {"O_CREAT": 1},
    (XLOG, "cluster_undo_redo_segment_reuse"): {"O_CREAT": 1},
}
DISABLED = {
    (SMGR, "cluster_undo_smgr_recovery_materialize_v1"),
    (XLOG, "cluster_undo_redo_open_segment"),
    (XLOG, "cluster_undo_redo_segment_init"),
    (XLOG, "cluster_undo_redo_segment_reuse"),
}


def code_only(source: str) -> str:
    return LEXICAL.sub(lambda match: re.sub(r"[^\n]", " ", match.group()), source)


def function_bodies(source: str) -> dict[str, str]:
    code = code_only(source)
    result = {}
    for match in FUNCTION.finditer(code):
        start = match.end() - 1
        depth = 1
        end = start + 1
        while end < len(code) and depth:
            depth += (code[end] == "{") - (code[end] == "}")
            end += 1
        if depth:
            raise ValueError("unclosed function: " + match.group(1))
        result[match.group(1)] = code[start:end]
    return result


def sources() -> dict[str, str]:
    result = {}
    for path in (ROOT / "src/backend").rglob("*.c"):
        text = path.read_text(encoding="utf-8")
        # Include new undo files and any new direct namespace consumer,
        # even when it is placed outside the existing storage directory.
        if ("undo" in path.name or "cluster_undo_path_resolve" in text
                or "provision_temp_publish_impl" in text or '"pg_undo' in text):
            result[str(path.relative_to(ROOT))] = text
    return result


def audit(files: dict[str, str]) -> list[str]:
    bodies = {(path, name): body for path, text in files.items()
              for name, body in function_bodies(text).items()}
    observed = {}
    for key, body in bodies.items():
        tokens = collections.Counter(re.sub(r"\s*\($", "", m.group())
                                     for m in TOKEN.finditer(body))
        if tokens:
            observed[key] = dict(tokens)
    errors = []
    all_tokens = sum(len(TOKEN.findall(code_only(text))) for text in files.values())
    owned_tokens = sum(sum(tokens.values()) for tokens in observed.values())
    if all_tokens != owned_tokens:
        errors.append("creation token outside classified function bodies")
    if observed != SITES:
        errors.append("creation sites differ: " + repr(observed))
    for key in DISABLED:
        body = bodies.get(key, "")
        disable = body.find("cluster_undo_inventory_disable(")
        opening = body.rfind("BasicOpenFile(")
        if disable < 0 or opening < disable:
            errors.append("unprotected creation: " + repr(key))
    wrapper = bodies.get((SMGR, "cluster_undo_smgr_provision_temp_publish"), "")
    order = [wrapper.find(token) for token in (
        "cluster_undo_inventory_publish_begin(", "provision_temp_publish_impl(",
        "cluster_undo_smgr_probe_segment(", "PG_FINALLY()",
        "cluster_undo_inventory_publish_end(")]
    if min(order) < 0 or order != sorted(order):
        errors.append("runtime publisher lost tracking/readability/cleanup order")
    callers = {key: len(re.findall(r"\bprovision_temp_publish_impl\s*\(", body))
               for key, body in bodies.items()
               if re.search(r"\bprovision_temp_publish_impl\s*\(", body)}
    if callers != {(SMGR, "cluster_undo_smgr_provision_temp_publish"): 1}:
        errors.append("runtime final link has an untracked caller")
    begin = bodies.get((SMGR, "cluster_undo_inventory_publish_begin"), "")
    if not re.search(r"if\s*\(!inventory_scope\(intent, owner\)\)\s*\{\s*"
                     r"cluster_undo_inventory_disable\(owner\);\s*return false;", begin):
        errors.append("own-owner scope refusal does not disable before publication")
    return errors


class UndoInventoryPublicationCensus(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.files = sources()

    def test_current_publishers_are_classified_and_guarded(self) -> None:
        self.assertEqual(audit(self.files), [])

    def test_removed_recovery_disable_is_rejected(self) -> None:
        for path, function in DISABLED:
            with self.subTest(function=function):
                files = dict(self.files)
                body = function_bodies(files[path])[function]
                damaged = body.replace("cluster_undo_inventory_disable(", "ignored_disable(")
                # Use the same offsets in a comment/string-free source.
                files[path] = code_only(files[path]).replace(body, damaged, 1)
                self.assertTrue(any("unprotected creation" in e for e in audit(files)))

    def test_new_undo_creation_and_namespace_consumer_are_rejected(self) -> None:
        for path in ("src/backend/cluster/storage/cluster_undo_new.c",
                     "src/backend/cluster/cluster_new_consumer.c"):
            files = dict(self.files)
            files[path] = "int\nuntracked(void)\n{ return BasicOpenFile(path, O_CREAT); }"
            self.assertTrue(any("creation sites differ" in e for e in audit(files)))

    def test_untracked_runtime_caller_is_rejected(self) -> None:
        files = dict(self.files)
        files[SMGR] += "\nint\nbypass(void)\n{ return provision_temp_publish_impl(); }\n"
        self.assertTrue(any("untracked caller" in e for e in audit(files)))

    def test_creation_flag_hidden_in_global_or_macro_is_rejected(self) -> None:
        files = dict(self.files)
        files[SMGR] += "\n#define UNTRACKED_FLAGS (O_RDWR | O_CREAT)\n"
        self.assertTrue(any("outside classified" in e for e in audit(files)))

    def test_moving_disable_after_open_is_rejected(self) -> None:
        files = dict(self.files)
        files[XLOG] = files[XLOG].replace(
            "cluster_undo_inventory_disable(instance);\n\tfd = BasicOpenFile(path, O_CREAT | O_RDWR | PG_BINARY);",
            "fd = BasicOpenFile(path, O_CREAT | O_RDWR | PG_BINARY);\n\tcluster_undo_inventory_disable(instance);")
        self.assertTrue(any("unprotected creation" in e for e in audit(files)))

    def test_lost_scope_disable_or_finally_is_rejected(self) -> None:
        for old, new in (("cluster_undo_inventory_disable(owner);", "ignored_disable(owner);"),
                         ("PG_FINALLY();", "ignored_finally();")):
            files = dict(self.files)
            files[SMGR] = files[SMGR].replace(old, new)
            self.assertTrue(audit(files))

    def test_comments_and_string_literals_do_not_create_sites(self) -> None:
        files = dict(self.files)
        files[SMGR] += '\n/* link(a,b); O_CREAT */\nconst char *note = "O_CREAT";\n'
        self.assertEqual(audit(files), [])


if __name__ == "__main__":
    unittest.main()
