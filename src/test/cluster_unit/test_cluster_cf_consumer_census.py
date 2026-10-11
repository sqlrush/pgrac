#!/usr/bin/env python3
"""PGRAC: inventory CF call sites and preserve unrelated caller policies.

Author: SqlRush <sqlrush@gmail.com>
This source check complements executable owner/consumer tests. It does not
certify runtime progress, retirement, or error handling by itself.
"""

import argparse
import collections
import hashlib
import json
import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[3]
API = r"cluster_cf_lock(?:_poll(?:_owned)?)?"
# Public source identities for existing caller boundaries, independent of lines.
EXPECTED_CALLS = [
    (
        'src/backend/access/transam/xlog.c',
        'ClusterCheckpointV3Prepare', 'cluster_cf_lock',
    ),
    (
        'src/backend/access/transam/xlog.c',
        'ClusterStartupCheckpointPublish', 'cluster_cf_lock',
    ),
    (
        'src/backend/access/transam/xlog.c',
        'CreateCheckPoint', 'cluster_cf_lock',
    ),
    (
        'src/backend/cluster/cluster_control_root.c',
        'acquire_clusterwide_cf', 'cluster_cf_lock',
    ),
    (
        'src/backend/cluster/cluster_control_root.c',
        'cluster_control_root_config_poll', 'cluster_cf_lock_poll_owned',
    ),
    (
        'src/backend/cluster/cluster_control_root.c',
        'cluster_control_root_v3_serving_poll', 'cluster_cf_lock_poll_owned',
    ),
    (
        'src/backend/cluster/cluster_control_root.c',
        'cluster_control_root_v3_terminal_history_blocked', 'cluster_cf_lock',
    ),
    (
        'src/backend/cluster/cluster_control_root.c',
        'cluster_control_root_v3_validate_rejoin_terminal', 'cluster_cf_lock',
    ),
    (
        'src/backend/cluster/cluster_control_root.c',
        'failure_v2_open_work', 'cluster_cf_lock',
    ),
    (
        'src/backend/cluster/cluster_control_root.c',
        'failure_v2_self_open_read', 'cluster_cf_lock',
    ),
    (
        'src/backend/cluster/cluster_control_root.c',
        'failure_v2_tail_read', 'cluster_cf_lock',
    ),
    (
        'src/backend/cluster/cluster_control_root.c',
        'initializer_observe_locked', 'cluster_cf_lock',
    ),
    (
        'src/backend/cluster/cluster_control_root.c',
        'read_canonical_version', 'cluster_cf_lock',
    ),
    (
        'src/backend/cluster/cluster_control_root.c',
        'read_canonical_version', 'cluster_cf_lock_poll',
    ),
    (
        'src/backend/cluster/cluster_control_root.c',
        'read_retention_version', 'cluster_cf_lock',
    ),
    (
        'src/backend/cluster/cluster_wal_inputs.c',
        'inputs_root', 'cluster_cf_lock',
    ),
    (
        'src/backend/cluster/cluster_wal_state.c',
        'cluster_wal_state_update_own', 'cluster_cf_lock',
    ),
]
UNCHANGED_CALLERS = {
    (
        'src/backend/access/transam/xlog.c',
        'CreateCheckPoint',
    ): '7ae23b2d177a85b7a27e1339546f301f65a0f55ff22bcf766906098f76908721',
    (
        'src/backend/cluster/cluster_control_root.c',
        'acquire_clusterwide_cf',
    ): 'a0ce7543756f7fc01b957e633df0618262a9bff50bcaaebf5846c1e02b22e474',
    (
        'src/backend/cluster/cluster_control_root.c',
        'cluster_control_root_config_poll',
    ): '085e898c2dbe37bf7d8792294e019a00b7cf2d347a2aa6267b0f1bec1b03602b',
    (
        'src/backend/cluster/cluster_control_root.c',
        'cluster_control_root_v3_serving_poll',
    ): 'f2dd936c1cf0331b2dedd18b73f06df10c83ffbd5384078fbad7280e2591b55b',
    (
        'src/backend/cluster/cluster_control_root.c',
        'cluster_control_root_v3_terminal_history_blocked',
    ): 'a0f7c2a25cfd2f39ba7326627fc8107c3b0a12073447ba392eeb3902e3cb287d',
    (
        'src/backend/cluster/cluster_control_root.c',
        'cluster_control_root_v3_validate_rejoin_terminal',
    ): '40a382733b58509ce9bf32b1bc7dcf726aa8aaf0220e2df4374a2dd11c8269b1',
    (
        'src/backend/cluster/cluster_control_root.c',
        'failure_v2_open_work',
    ): 'd62131c1d92e5dbda40adb0cf3fc2cea9b4fab106a0f9407bea605c69bdfe3fb',
    (
        'src/backend/cluster/cluster_control_root.c',
        'failure_v2_self_open_read',
    ): 'dbd6ec87f0ed181508b56eacc79f31da4b45fb6256bcfe4659de74365fae31b9',
    (
        'src/backend/cluster/cluster_control_root.c',
        'failure_v2_tail_read',
    ): 'dd4ba15492093f7c2ae93e3147485ea2f38ddf70bd8fcd5a81875ea52bafeaf2',
    (
        'src/backend/cluster/cluster_control_root.c',
        'initializer_observe_locked',
    ): '3758d8d9e631c6aec0e3f1d8281b4696c06b1e78f5ee6f70b4384d59dc4fb75b',
    (
        'src/backend/cluster/cluster_control_root.c',
        'read_canonical_version',
    ): 'eec782d1e45d168ca10ad354672071beb58b2e318ec5f9e1e4a7bd8ddbcce027',
    (
        'src/backend/cluster/cluster_control_root.c',
        'read_retention_version',
    ): 'd5f8f7bf8eb2fa7a980f17634ac4fff527ad12ecbab372046c73e013cb9131e7',
    (
        'src/backend/cluster/cluster_wal_inputs.c',
        'inputs_root',
    ): '1a652ae3e4410ead99172ba0802668068cd60800ca93d8dd82f7156edd143b95',
    (
        'src/backend/cluster/cluster_wal_state.c',
        'cluster_wal_state_update_own',
    ): '67589de19f88541f4f4928eeb16c067b1f46489d27a8aa4b045b315ddfa35b99',
}


def lexical_source(source):
    # Preserve offsets/lines and exclude braces or names inside comments/literals.
    pattern = r'/\*.*?\*/|//[^\n]*|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\''
    return re.sub(pattern, lambda m: "".join("\n" if c == "\n" else " "
                                             for c in m.group()), source, flags=re.S)


def functions(source):
    code = lexical_source(source)
    result = []
    for match in re.finditer(r"^(\w+)\([^;{}]*\)\s*\{", code, re.M):
        start = code.index("{", match.start(), match.end())
        end, depth = start + 1, 1
        while depth and end < len(code):
            depth += (code[end] == "{") - (code[end] == "}")
            end += 1
        if depth:
            raise AssertionError("unterminated function " + match.group(1))
        result.append((match.group(1), start, end, match.start()))
    return code, result


def fingerprint(body):
    tokens = r'/\*.*?\*/|//[^\n]*|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|\s+'
    normalized = re.sub(tokens, lambda m: m.group() if m.group()[0] in "\'\"" else "",
                        body, flags=re.S)
    return hashlib.sha256(normalized.encode()).hexdigest()


def inspect(root):
    calls, definitions, bodies = [], [], {}
    for path in sorted((root / "src/backend").rglob("*.c")):
        source = path.read_text()
        if not re.search(API + r"\s*\(", source):
            continue
        code, spans = functions(source)
        relative = path.relative_to(root).as_posix()
        for name, start, end, _ in spans:
            bodies[(relative, name)] = fingerprint(source[start:end])
        for match in re.finditer(r"\b(" + API + r")\s*\(", code):
            owner = next((f for f in spans if f[1] < match.start() < f[2]), None)
            if owner:
                calls.append((relative, owner[0], match.group(1)))
            elif any(f[3] == match.start() for f in spans):
                definitions.append((relative, match.group(1)))
            else:
                raise AssertionError("unclassified CF reference: " + relative)
    assert collections.Counter(calls) == collections.Counter(EXPECTED_CALLS), calls
    assert sorted(definitions) == [
        ("src/backend/cluster/cluster_cf_enqueue.c", name)
        for name in ("cluster_cf_lock", "cluster_cf_lock_poll", "cluster_cf_lock_poll_owned")
    ], definitions
    for boundary, expected in UNCHANGED_CALLERS.items():
        assert bodies.get(boundary) == expected, "caller policy changed: " + str(boundary)
    return {"direct_calls": calls, "definitions": definitions,
            "unchanged_caller_bodies": len(UNCHANGED_CALLERS),
            "runtime_policy_evidence": "test_cluster_cf_enqueue/test_cluster_checkpoint_native"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=pathlib.Path, default=ROOT)
    parser.add_argument("--report", type=pathlib.Path)
    args = parser.parse_args()
    result = inspect(args.source_root)
    if args.report:
        args.report.write_text(json.dumps(result, indent=2) + "\n")
    print("CF census PASS: 17 direct calls, 3 API definitions, "
          + str(result["unchanged_caller_bodies"]) + " unchanged caller bodies")


if __name__ == "__main__":
    main()
