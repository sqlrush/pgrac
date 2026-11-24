"""Keep HOT member copies and undo observations separate, without granting proof.

Author: SqlRush <sqlrush@gmail.com>
SQL scalar comparisons do not establish a process, logical row, page version,
statement or a cross-record undo edge.
"""
from workload_evidence import ITEM_IMAGES, SCHEMAS, _group, _item_groups, _scalar
from workload_log import read_trace


def _number(value, kind="u32"):
    if type(value) not in (int, str):
        return None
    try:
        return _scalar(str(value), kind)
    except ValueError:
        return None


def hot_evidence(capture, logs):
    result = dict(records=[], rejections=[], identity_bound=False, atomic_graph=False,
                  original_path="UNKNOWN", member_binding="UNKNOWN", page_version_binding="UNKNOWN",
                  sql_statement_binding="UNKNOWN",
                  input_source="decoded node logs; positions refer to these supplied texts")
    scope = capture.get("sql_evidence") if capture.get("case") == "P0-12" else None
    scope = scope if isinstance(scope, dict) and scope.get("case") == "P0-12" else {}
    def owned_dict(name):
        value = scope.get(name)
        return value if isinstance(value, dict) else {}
    reader, locator, snapshot = (owned_dict(name) for name in
                                 ("reader_sql_identity", "locator", "snapshot"))
    reader_node, reader_pid = (_number(reader.get(key)) for key in ("node_id", "pid"))
    relation = ("spcOid", "dbOid", "relNumber", "forkNum")
    expected_relation = [_number(locator.get(key)) for key in relation]
    expected_snapshot = [_number(snapshot.get(key), "u64") for key in ("read_scn", "read_epoch")]
    for node, text in enumerate(logs):
        trace = read_trace(text.encode("utf-8"))
        result["rejections"].extend(dict(item, source_node=node) for item in trace["rejections"])
        for item in trace["observations"]:
            fields = item["fields"]
            family = fields["PGRAC_FAMILY"]
            if family not in ("HOT_FULL", "R4_UNDO"):
                continue
            groups = {name: _group(fields, *description)
                      for name, description in SCHEMAS[family].items()}
            for name, guard in ITEM_IMAGES.get(family, {}).items():
                groups.update(_item_groups(fields, name, guard))
            relation_match = snapshot_match = None
            tag = groups["tag"]
            if tag["status"] == "OBSERVED" and all(v is not None for v in expected_relation):
                relation_match = [tag["values"][key] for key in relation] == expected_relation
            local_snapshot = groups.get("snapshot", {})
            if local_snapshot.get("status") == "OBSERVED" and all(v is not None for v in expected_snapshot):
                snapshot_match = [local_snapshot["values"][key] for key in
                                  ("read_scn", "read_epoch")] == expected_snapshot
            emitter_node, pid = (_number(fields.get(key)) for key in ("node_id", "pid"))
            source_match = None if emitter_node is None else emitter_node == node
            reader_match = (None if None in (emitter_node, pid, reader_node, reader_pid) or not reader_pid
                            else source_match and emitter_node == reader_node and pid == reader_pid)
            result["records"].append(dict(source_node=node, line=item["line"], raw=item["raw"],
                family=family, event=fields["event"], fields=fields, groups=groups,
                source_node_matches=source_match, emitter_matches_sql_reader=reader_match,
                relation_matches_sql_target=relation_match, snapshot_values_match=snapshot_match))
    return result
