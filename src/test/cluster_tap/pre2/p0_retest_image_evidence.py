"""Retain local read-image facts; SQL, snapshot and process bindings stay open.

Author: SqlRush <sqlrush@gmail.com>
No records are joined. A matching node/PID or relation is a scalar comparison,
not proof of a process incarnation, statement, transport or image authority.
"""
from workload_evidence import SCHEMAS, _group, _scalar
from workload_log import read_trace

FAMILIES = frozenset(("READ_IMAGE", "READ_IMAGE_HOP", "READ_IMAGE_INGRESS"))
RELATION = ("spcOid", "dbOid", "relNumber", "forkNum")


def _number(value, kind="u32"):
    if type(value) not in (int, str):
        return None
    try:
        return _scalar(str(value), kind)
    except ValueError:
        return None


def _same(left, right):
    return None if left is None or right is None else left == right


def image_evidence(capture, logs):
    result = dict(records=[], rejections=[], identity_bound=False, atomic_graph=False,
                  original_path="UNKNOWN", snapshot_binding="UNKNOWN", sql_statement_binding="UNKNOWN",
                  input_source="decoded node logs; positions refer to these supplied texts")
    scope = capture.get("sql_evidence") if capture.get("case") == "P0-10" else None
    scope = scope if isinstance(scope, dict) and scope.get("case") == "P0-10" else {}
    reader = scope.get("reader_sql_identity")
    reader = reader if isinstance(reader, dict) else {}
    locator = scope.get("locator")
    locator = locator if isinstance(locator, dict) else {}
    reader_node, reader_pid = (_number(reader.get(key)) for key in ("node_id", "pid"))
    expected_tag = [_number(locator.get(key)) for key in RELATION]
    blocks = _number(locator.get("blocks"))
    for node, text in enumerate(logs):
        trace = read_trace(text.encode("utf-8"))
        result["rejections"].extend(dict(item, source_node=node) for item in trace["rejections"])
        for item in trace["observations"]:
            fields = item["fields"]
            family = fields["PGRAC_FAMILY"]
            if family not in FAMILIES:
                continue
            groups = {name: _group(fields, *description)
                      for name, description in SCHEMAS.get(family, {}).items()}
            tag = groups.get("tag", {})
            relation_match = within_size = None
            if tag.get("status") == "OBSERVED" and all(v is not None for v in expected_tag):
                relation_match = [tag["values"][key] for key in RELATION] == expected_tag
                if blocks is not None and blocks > 0:
                    within_size = relation_match and tag["values"]["blockNum"] < blocks
            emitter_node, pid = (_number(fields.get(key)) for key in ("node_id", "pid"))
            source_match = _same(emitter_node, node)
            comparisons = (source_match, _same(emitter_node, reader_node),
                           _same(pid, reader_pid) if reader_pid else None)
            reader_match = None if None in comparisons else all(comparisons)
            result["records"].append(dict(source_node=node, line=item["line"], raw=item["raw"],
                family=family, event=fields["event"], fields=fields, groups=groups,
                source_node_matches=source_match, emitter_matches_sql_reader=reader_match,
                relation_matches_sql_target=relation_match, block_within_initial_size=within_size))
    return result
