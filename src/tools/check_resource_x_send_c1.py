#!/usr/bin/env python3
# PGRAC: validate real cluster send-edge evidence in source and build trees.
"""Validate and freshness-check the Resource-X SEND-C1 9+2 gate."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import pathlib
import re
import shlex
import subprocess
import sys
import tempfile
from typing import Iterable, NamedTuple


REQUIRED_IDS = [f"C1-{number}" for number in range(1, 12)]
REQUIRED_COLUMNS = [
    "id",
    "name",
    "root",
    "producer_chain",
    "consumer_chain",
    "observable",
    "positive_witness",
    "negative_witness",
    "forbidden_symbol",
    "mutation_edge",
    "mutation_witness",
    "mutation_result",
    "positive_assertion",
    "negative_assertion",
]
BOUND_ONLY_PROBE = "cluster_pcm_lock_resource_x_grant_intent_probe_exact"
PARSER_ONLY_CONSUMERS = {
    "cluster_resource_x_wire_decode",
    "gcs_block_resource_x_payload_candidate",
}


class GateError(RuntimeError):
    """The closed SEND-C1 gate is incomplete or stale."""


class FunctionDefinition(NamedTuple):
    source: str
    body: str


def _strip_comments_and_literals(text: str) -> str:
    """Blank comments and literals while preserving offsets and newlines."""

    pattern = re.compile(
        r"//[^\n]*|/\*.*?\*/|\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'",
        re.DOTALL,
    )

    def blank(match: re.Match[str]) -> str:
        return "".join("\n" if char == "\n" else " " for char in match.group(0))

    return pattern.sub(blank, text)


def _matching(text: str, start: int, opener: str, closer: str) -> int:
    depth = 0
    for offset in range(start, len(text)):
        if text[offset] == opener:
            depth += 1
        elif text[offset] == closer:
            depth -= 1
            if depth == 0:
                return offset
    raise GateError(f"unbalanced {opener}{closer} near byte {start}")


def _definitions(source_root: pathlib.Path, symbols: set[str]) -> dict[str, FunctionDefinition]:
    found: dict[str, list[FunctionDefinition]] = {symbol: [] for symbol in symbols}
    patterns = {
        symbol: re.compile(rf"\b{re.escape(symbol)}\s*\(") for symbol in symbols
    }
    backend = source_root / "src" / "backend"
    if not backend.is_dir():
        raise GateError(f"production source root is absent: {backend}")
    for source in sorted(backend.rglob("*.c")):
        raw = source.read_text(encoding="utf-8")
        candidates = [symbol for symbol in symbols if symbol in raw]
        if not candidates:
            continue
        text = _strip_comments_and_literals(raw)
        relative = source.relative_to(source_root).as_posix()
        for symbol in candidates:
            for match in patterns[symbol].finditer(text):
                open_paren = text.find("(", match.start())
                close_paren = _matching(text, open_paren, "(", ")")
                cursor = close_paren + 1
                while cursor < len(text) and text[cursor].isspace():
                    cursor += 1
                if cursor >= len(text) or text[cursor] != "{":
                    continue
                close_brace = _matching(text, cursor, "{", "}")
                found[symbol].append(
                    FunctionDefinition(relative, text[cursor : close_brace + 1])
                )
    definitions: dict[str, FunctionDefinition] = {}
    for symbol, matches in found.items():
        if not matches:
            raise GateError(f"production root/function is absent: {symbol}")
        if len(matches) != 1:
            locations = ", ".join(item.source for item in matches)
            raise GateError(f"ambiguous production function {symbol}: {locations}")
        definitions[symbol] = matches[0]
    return definitions


def _parse_chain(value: str, row_id: str, field: str) -> list[str]:
    chain = [item.strip() for item in value.split(">")]
    if len(chain) < 2 or any(not item for item in chain):
        raise GateError(f"{row_id}: {field} must contain at least one direct edge")
    return chain


def _parse_witness(value: str, row_id: str, field: str) -> tuple[str, str]:
    parts = value.split("::")
    if len(parts) != 2 or not all(parts):
        raise GateError(f"{row_id}: malformed {field}")
    path, symbol = parts
    if not path.startswith("src/test/") or not symbol.startswith("test_"):
        raise GateError(f"{row_id}: {field} is not a dynamic test witness")
    return path, symbol


def load_manifest(path: pathlib.Path) -> list[dict[str, str]]:
    try:
        with path.open(encoding="utf-8", newline="") as stream:
            reader = csv.DictReader(stream, delimiter="\t")
            if reader.fieldnames != REQUIRED_COLUMNS:
                raise GateError("SEND-C1 manifest columns are stale or incomplete")
            rows = [dict(row) for row in reader]
    except OSError as error:
        raise GateError(f"cannot read SEND-C1 manifest: {error}") from error
    ids = [row["id"] for row in rows]
    if len(ids) != len(set(ids)):
        raise GateError("duplicate row id in SEND-C1 manifest")
    if ids != REQUIRED_IDS:
        raise GateError(
            "SEND-C1 manifest must contain ordered exact ids " + ",".join(REQUIRED_IDS)
        )
    for row in rows:
        if any(not row[column].strip() for column in REQUIRED_COLUMNS):
            raise GateError(f"{row['id']}: empty manifest field")
        if row["mutation_witness"] not in {"positive", "negative"}:
            raise GateError(f"{row['id']}: mutation_witness must be positive or negative")
    if rows[9]["forbidden_symbol"] != BOUND_ONLY_PROBE:
        raise GateError(f"C1-10 must forbid {BOUND_ONLY_PROBE}")
    if any(row["forbidden_symbol"] != "-" for row in rows[:9] + rows[10:]):
        raise GateError("only C1-10 may name a forbidden symbol")
    return rows


def _direct_edge(
    definitions: dict[str, FunctionDefinition], caller: str, callee: str, row_id: str
) -> None:
    if re.search(rf"\b{re.escape(callee)}\s*\(", definitions[caller].body) is None:
        raise GateError(f"{row_id}: missing direct edge {caller}>{callee}")


def _witness_evidence(
    source_root: pathlib.Path, value: str, row_id: str, field: str,
    assertion: str,
) -> tuple[str, str]:
    relative, symbol = _parse_witness(value, row_id, field)
    path = source_root / relative
    try:
        raw = path.read_text(encoding="utf-8")
        text = _strip_comments_and_literals(raw)
    except OSError as error:
        raise GateError(f"{row_id}: cannot read witness {relative}: {error}") from error
    declaration = re.search(
        rf"\bUT_TEST\s*\(\s*{re.escape(symbol)}\s*\)\s*\{{", text
    )
    if declaration is None:
        raise GateError(f"{row_id}: witness function is absent: {symbol}")
    if re.search(rf"\bUT_RUN\s*\(\s*{re.escape(symbol)}\s*\)", text) is None:
        raise GateError(f"{row_id}: witness is not registered: {symbol}")
    open_brace = text.find("{", declaration.start())
    close_brace = _matching(text, open_brace, "{", "}")
    body = raw[open_brace : close_brace + 1]
    if "UT_ASSERT" not in body or assertion not in body:
        raise GateError(
            f"{row_id}: {field} lacks named behavioral assertion {assertion!r}"
        )
    return hashlib.sha256(body.encode("utf-8")).hexdigest(), pathlib.Path(relative).stem


def audit_product(source_root: pathlib.Path, manifest_path: pathlib.Path) -> dict[str, object]:
    source_root = source_root.resolve()
    manifest_path = manifest_path.resolve()
    rows = load_manifest(manifest_path)
    parsed_rows = []
    all_symbols: set[str] = set()
    for row in rows:
        producer = _parse_chain(row["producer_chain"], row["id"], "producer_chain")
        consumer = _parse_chain(row["consumer_chain"], row["id"], "consumer_chain")
        if producer[0] != row["root"] or consumer[0] != row["root"]:
            raise GateError(f"{row['id']}: production root does not anchor both chains")
        if consumer[-1] in PARSER_ONLY_CONSUMERS:
            raise GateError(f"{row['id']}: parser-only consumer {consumer[-1]}")
        mutation = [item.strip() for item in row["mutation_edge"].split(">")]
        edges = set(zip(producer, producer[1:])) | set(zip(consumer, consumer[1:]))
        if len(mutation) != 2 or tuple(mutation) not in edges:
            raise GateError(f"{row['id']}: mutation edge is not in the row call graph")
        all_symbols.update(producer)
        all_symbols.update(consumer)
        parsed_rows.append((row, producer, consumer, mutation))
    definitions = _definitions(source_root, all_symbols)

    product_files = sorted((source_root / "src" / "backend").rglob("*.[ch]"))
    product_files += sorted((source_root / "src" / "include").rglob("*.h"))
    product_text = {
        path.relative_to(source_root).as_posix(): _strip_comments_and_literals(
            path.read_text(encoding="utf-8")
        )
        for path in product_files
    }
    normalized_rows = []
    aggregate = hashlib.sha256()
    for row, producer, consumer, mutation in parsed_rows:
        for chain in (producer, consumer):
            for caller, callee in zip(chain, chain[1:]):
                _direct_edge(definitions, caller, callee, row["id"])
        row_sources = {
            definitions[symbol].source for symbol in set(producer + consumer)
        }
        observable_hits = sorted(
            relative for relative in row_sources
            if re.search(
                rf"\b{re.escape(row['observable'])}\b", product_text[relative]
            )
        )
        if not observable_hits:
            raise GateError(f"{row['id']}: production observable is absent: {row['observable']}")
        forbidden = row["forbidden_symbol"]
        if forbidden != "-":
            hits = sorted(
                relative
                for relative, text in product_text.items()
                if re.search(rf"\b{re.escape(forbidden)}\b", text)
            )
            if hits:
                raise GateError(
                    f"{row['id']}: forbidden symbol {forbidden} remains in "
                    + ", ".join(hits)
                )
        positive_digest, positive_binary = _witness_evidence(
            source_root, row["positive_witness"], row["id"], "positive_witness",
            row["positive_assertion"],
        )
        negative_digest, negative_binary = _witness_evidence(
            source_root, row["negative_witness"], row["id"], "negative_witness",
            row["negative_assertion"],
        )
        if row["positive_witness"] == row["negative_witness"]:
            raise GateError(f"{row['id']}: positive and negative witnesses must differ")
        mutation_caller, mutation_callee = mutation
        mutated_body = re.sub(
            rf"\b{re.escape(mutation_callee)}\s*\(",
            "send_c1_removed_edge(",
            definitions[mutation_caller].body,
        )
        if re.search(rf"\b{re.escape(mutation_callee)}\s*\(", mutated_body):
            raise GateError(f"{row['id']}: exact edge-removal mutation did not apply")
        mutation_digest = hashlib.sha256(
            (mutation_caller + ">" + mutation_callee + "\0" + mutated_body).encode("utf-8")
        ).hexdigest()
        evidence = hashlib.sha256()
        for symbol in sorted(set(producer + consumer)):
            definition = definitions[symbol]
            evidence.update(symbol.encode("utf-8"))
            evidence.update(definition.source.encode("utf-8"))
            evidence.update(definition.body.encode("utf-8"))
        evidence.update(row["observable"].encode("utf-8"))
        evidence.update("\0".join(observable_hits).encode("utf-8"))
        evidence.update(positive_digest.encode("ascii"))
        evidence.update(negative_digest.encode("ascii"))
        evidence.update(mutation_digest.encode("ascii"))
        evidence_digest = evidence.hexdigest()
        aggregate.update(row["id"].encode("ascii"))
        aggregate.update(evidence_digest.encode("ascii"))
        normalized_rows.append(
            {
                "id": row["id"],
                "name": row["name"],
                "root": row["root"],
                "producer_chain": producer,
                "consumer_chain": consumer,
                "production_sources": {
                    symbol: definitions[symbol].source
                    for symbol in sorted(set(producer + consumer))
                },
                "observable": row["observable"],
                "observable_sources": observable_hits,
                "positive_witness": row["positive_witness"],
                "negative_witness": row["negative_witness"],
                "positive_binary": positive_binary,
                "negative_binary": negative_binary,
                "positive_assertion": row["positive_assertion"],
                "negative_assertion": row["negative_assertion"],
                "mutation_edge": mutation,
                "mutation_witness": row["mutation_witness"],
                "mutation_result": row["mutation_result"],
                "mutation_sha256": mutation_digest,
                "forbidden_symbol": None if forbidden == "-" else forbidden,
                "evidence_sha256": evidence_digest,
            }
        )
    return {
        "schema": "pgrac-resource-x-send-c1-v1",
        "gate": "SEND-C1-9+2",
        "row_count": len(normalized_rows),
        "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "evidence_sha256": aggregate.hexdigest(),
        "rows": normalized_rows,
    }


def canonical_json(report: dict[str, object]) -> str:
    row_keys = (
        "id", "name", "root", "producer_chain", "consumer_chain",
        "observable", "positive_witness", "negative_witness",
        "positive_assertion", "negative_assertion", "forbidden_symbol",
        "mutation_edge", "mutation_witness", "mutation_result", "mutation_sha256",
        "evidence_sha256",
    )
    artifact = {
        "schema": report["schema"],
        "gate": report["gate"],
        "row_count": report["row_count"],
        "manifest_sha256": report["manifest_sha256"],
        "evidence_sha256": report["evidence_sha256"],
        "rows": [
            {key: row[key] for key in row_keys}
            for row in report["rows"]
        ],
    }
    return json.dumps(artifact, sort_keys=True, separators=(",", ":")) + "\n"


def verify_artifact(report: dict[str, object], artifact_path: pathlib.Path) -> None:
    expected = canonical_json(report)
    try:
        actual = artifact_path.read_text(encoding="utf-8")
    except OSError as error:
        raise GateError(f"cannot read generated artifact: {error}") from error
    if actual != expected:
        raise GateError("stale generated artifact; regenerate send-c1-9-plus-2.json")


def verify_witness_outputs(
    report: dict[str, object], outputs: dict[str, str]
) -> None:
    for row in report["rows"]:
        for polarity in ("positive", "negative"):
            binary = row[f"{polarity}_binary"]
            witness = row[f"{polarity}_witness"].split("::", 1)[1]
            output = outputs.get(binary)
            if output is None or re.search(
                rf"^ok\s+\d+\s+-\s+{re.escape(witness)}\s*$",
                output,
                re.MULTILINE,
            ) is None:
                raise GateError(
                    f"{row['id']}: {polarity} dynamic witness did not pass: "
                    f"{binary}:{witness}"
                )


def verify_mutation_outputs(
    row: dict[str, object], outputs: dict[str, str]
) -> None:
    target = row["mutation_witness"]
    for polarity in ("positive", "negative"):
        binary = str(row[f"{polarity}_binary"])
        witness = str(row[f"{polarity}_witness"]).split("::", 1)[1]
        output = outputs.get(binary)
        if output is None or re.search(
            rf"^(?:not )?ok\s+\d+\s+-\s+{re.escape(witness)}\s*$",
            output,
            re.MULTILINE,
        ) is None:
            raise GateError(
                f"{row['id']}: {polarity} witness did not execute under mutation: "
                f"{binary}:{witness}"
            )
        if polarity == target and re.search(
            rf"^not ok\s+\d+\s+-\s+{re.escape(witness)}\s*$",
            output,
            re.MULTILINE,
        ) is None:
            raise GateError(
                f"{row['id']}: mutation survived bound {polarity} witness: "
                f"{binary}:{witness}"
            )


def run_dynamic_witnesses(
    report: dict[str, object], unit_directory: pathlib.Path
) -> dict[str, str]:
    binaries = sorted(
        {
            row[key]
            for row in report["rows"]
            for key in ("positive_binary", "negative_binary")
        }
    )
    outputs: dict[str, str] = {}
    for binary in binaries:
        executable = unit_directory / binary
        try:
            completed = subprocess.run(
                [str(executable)],
                cwd=unit_directory,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=180,
                check=False,
            )
        except (OSError, subprocess.SubprocessError) as error:
            raise GateError(f"cannot execute dynamic witness binary {binary}: {error}") from error
        if completed.returncode != 0:
            raise GateError(
                f"dynamic witness binary {binary} failed rc={completed.returncode}:\n"
                + completed.stdout[-2000:]
            )
        outputs[binary] = completed.stdout
    verify_witness_outputs(report, outputs)
    return outputs


def _mutated_source(
    source_root: pathlib.Path, source_relative: str, caller: str, callee: str,
    mutation_result: str,
) -> str:
    path = source_root / source_relative
    raw = path.read_text(encoding="utf-8")
    text = _strip_comments_and_literals(raw)
    open_brace = -1
    for declaration in re.finditer(rf"\b{re.escape(caller)}\s*\(", text):
        open_paren = text.find("(", declaration.start())
        close_paren = _matching(text, open_paren, "(", ")")
        candidate = close_paren + 1
        while candidate < len(text) and text[candidate].isspace():
            candidate += 1
        if candidate < len(text) and text[candidate] == "{":
            if open_brace >= 0:
                raise GateError(f"mutation caller has multiple definitions: {caller}")
            open_brace = candidate
    if open_brace < 0:
        raise GateError(f"mutation caller definition is absent: {caller}")
    close_brace = _matching(text, open_brace, "{", "}")
    matches = list(re.finditer(
        rf"\b{re.escape(callee)}(?=\s*\()",
        text[open_brace : close_brace + 1],
    ))
    if not matches:
        raise GateError(f"exact edge-removal mutation did not find {caller}>{callee}")
    mutated = raw
    for match in reversed(matches):
        start = open_brace + match.start()
        end = open_brace + match.end()
        mutated = mutated[:start] + "send_c1_removed_edge" + mutated[end:]
    return f"#define send_c1_removed_edge(...) ({mutation_result})\n" + mutated


def _run_compile(command: list[str], cwd: pathlib.Path, label: str) -> None:
    completed = subprocess.run(
        command,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=180,
        check=False,
    )
    if completed.returncode != 0:
        raise GateError(f"{label} failed rc={completed.returncode}:\n{completed.stdout[-4000:]}")


def _compile_mutation_binary(
    row: dict[str, object], source_root: pathlib.Path, unit_directory: pathlib.Path,
    temporary: pathlib.Path, cc: str, cflags: str, cppflags: str,
    pthread_cflags: str, pthread_libs: str,
) -> tuple[pathlib.Path, str, str]:
    caller, callee = row["mutation_edge"]
    source_relative = row["production_sources"][caller]
    binary_by_source = {
        "src/backend/cluster/cluster_pcm_lock.c": "test_cluster_pcm_lock",
        "src/backend/cluster/cluster_lms_outbound.c": "test_cluster_lms_outbound",
        "src/backend/cluster/cluster_gcs_block.c": "test_cluster_gcs_block",
    }
    binary_name = binary_by_source.get(source_relative)
    if binary_name is None:
        raise GateError(f"{row['id']}: no mutation build recipe for {source_relative}")
    target_binary = row[f"{row['mutation_witness']}_binary"]
    if target_binary != binary_name:
        raise GateError(
            f"{row['id']}: bound mutation witness {target_binary} does not link/read "
            f"{source_relative}"
        )

    mutated_text = _mutated_source(
        source_root, source_relative, caller, callee, str(row["mutation_result"])
    )
    mutated_path = temporary / f"{row['id'].lower()}-{pathlib.Path(source_relative).name}"
    mutated_path.write_text(mutated_text, encoding="utf-8")
    mutation_sha256 = hashlib.sha256(mutated_text.encode("utf-8")).hexdigest()
    output_binary = temporary / f"{row['id'].lower()}-{binary_name}"
    compiler = shlex.split(cc)
    common_flags = shlex.split(cflags) + shlex.split(cppflags)
    build_root = unit_directory.parents[2]
    unit_source = source_root / "src" / "test" / "cluster_unit"
    include_flags = ["-I", str(unit_directory), "-I", str(unit_source),
                     "-I", str(build_root / "src" / "include"),
                     "-I", str(source_root / "src" / "include")]
    backend = source_root / "src" / "backend" / "cluster"
    backend_build = build_root / "src" / "backend" / "cluster"
    common = build_root / "src" / "common" / "libpgcommon_srv.a"
    port = build_root / "src" / "port" / "libpgport_srv.a"
    port_boundary = unit_directory / "cluster_unit_port_stubs.o"

    if binary_name in {"test_cluster_pcm_lock", "test_cluster_lms_outbound"}:
        mutated_object = temporary / f"{row['id'].lower()}-{pathlib.Path(source_relative).stem}.o"
        fixture_flags = (
            ["-Dclock_gettime=cluster_test_pcm_clock_gettime"]
            if binary_name == "test_cluster_pcm_lock"
            else ["-DCLUSTER_LMS_OUTBOUND_UNIT_TEST"]
        )
        _run_compile(
            compiler + common_flags + include_flags + fixture_flags
            + ["-c", str(mutated_path), "-o", str(mutated_object)],
            unit_directory,
            f"{row['id']} mutation object compile",
        )
        link = compiler + common_flags + include_flags
        if binary_name == "test_cluster_pcm_lock":
            link += [
                f'-DGCS_BLOCK_SOURCE_PATH="{backend / "cluster_gcs_block.c"}"',
                f'-DPCM_LOCK_SOURCE_PATH="{mutated_path}"',
                str(unit_source / "test_cluster_pcm_lock.c"),
                str(backend_build / "cluster_version.o"),
                str(mutated_object),
                str(backend_build / "cluster_resource_x_identity.o"),
                str(backend_build / "cluster_resource_x_node_wire.o"),
            ]
        else:
            link += [
                str(unit_source / "test_cluster_lms_outbound.c"),
                str(backend_build / "cluster_version.o"),
                str(mutated_object),
            ]
        link += [str(common), str(port), str(port_boundary), "-o", str(output_binary)]
    else:
        link = compiler + common_flags + shlex.split(pthread_cflags) + include_flags + [
            f'-DGCS_BLOCK_SOURCE_PATH="{mutated_path}"',
            f'-DBUFMGR_SOURCE_PATH="{source_root / "src/backend/storage/buffer/bufmgr.c"}"',
            f'-DLMS_SOURCE_PATH="{backend / "cluster_lms.c"}"',
            f'-DLMS_OUTBOUND_SOURCE_PATH="{backend / "cluster_lms_outbound.c"}"',
            f'-DT400_SOURCE_PATH="{source_root / "src/test/cluster_tap/t/400_pcm_x_queue_4node_liveness.pl"}"',
            f'-DPCM_LOCK_SOURCE_PATH="{backend / "cluster_pcm_lock.c"}"',
            f'-DRUNTIME_VISIBILITY_SOURCE_PATH="{backend / "cluster_runtime_visibility.c"}"',
            f'-DLMON_SOURCE_PATH="{backend / "cluster_lmon.c"}"',
            f'-DT406_SOURCE_PATH="{source_root / "src/test/cluster_tap/t/406_resource_x_finish_flush_failclosed_4node.pl"}"',
            str(unit_source / "test_cluster_gcs_block.c"),
            str(backend_build / "cluster_version.o"),
            str(backend_build / "cluster_resource_x_identity.o"),
            str(backend_build / "cluster_resource_x_retry.o"),
            str(common), str(port), str(port_boundary),
        ] + shlex.split(pthread_libs) + ["-o", str(output_binary)]
    _run_compile(link, unit_directory, f"{row['id']} mutation witness link")
    return output_binary, binary_name, mutation_sha256


def run_edge_removal_mutations(
    report: dict[str, object], source_root: pathlib.Path, unit_directory: pathlib.Path,
    original_outputs: dict[str, str], cc: str, cflags: str, cppflags: str,
    pthread_cflags: str, pthread_libs: str,
) -> int:
    count = 0
    with tempfile.TemporaryDirectory(prefix="pgrac-send-c1-") as directory:
        temporary = pathlib.Path(directory)
        for row in report["rows"]:
            mutated_binary, binary_name, mutation_sha256 = _compile_mutation_binary(
                row, source_root, unit_directory, temporary, cc, cflags, cppflags,
                pthread_cflags, pthread_libs,
            )
            outputs = dict(original_outputs)
            completed = subprocess.run(
                [str(mutated_binary)],
                cwd=unit_directory,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=180,
                check=False,
            )
            outputs[binary_name] = completed.stdout
            if completed.returncode == 0:
                raise GateError(f"{row['id']}: mutation survived with rc=0")
            verify_mutation_outputs(row, outputs)
            if mutation_sha256 == row["mutation_sha256"]:
                raise GateError(f"{row['id']}: mutation hash aliases unexecuted metadata hash")
            count += 1
    return count


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=pathlib.Path, required=True)
    parser.add_argument("--manifest", type=pathlib.Path, required=True)
    parser.add_argument("--artifact", type=pathlib.Path)
    parser.add_argument("--unit-directory", type=pathlib.Path)
    parser.add_argument("--cc", default="cc")
    parser.add_argument("--cflags", default="")
    parser.add_argument("--cppflags", default="")
    parser.add_argument("--pthread-cflags", default="")
    parser.add_argument("--pthread-libs", default="")
    parser.add_argument("--emit", action="store_true")
    args = parser.parse_args(argv)
    try:
        report = audit_product(args.source_root, args.manifest)
        if args.emit:
            sys.stdout.write(canonical_json(report))
        else:
            if args.artifact is None:
                raise GateError("--artifact is required unless --emit is used")
            verify_artifact(report, args.artifact)
            if args.unit_directory is None:
                raise GateError("--unit-directory is required for dynamic witnesses")
            unit_directory = args.unit_directory.resolve()
            original_outputs = run_dynamic_witnesses(report, unit_directory)
            mutation_count = run_edge_removal_mutations(
                report, args.source_root.resolve(), unit_directory, original_outputs,
                args.cc, args.cflags, args.cppflags, args.pthread_cflags,
                args.pthread_libs,
            )
            print(
                "Resource-X SEND-C1: GREEN: rows=11 dynamic_witnesses=22 "
                f"executed_edge_mutations={mutation_count} "
                f"manifest_sha256={report['manifest_sha256']} "
                f"evidence_sha256={report['evidence_sha256']}"
            )
    except (GateError, OSError) as error:
        print(f"Resource-X SEND-C1: RED: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
