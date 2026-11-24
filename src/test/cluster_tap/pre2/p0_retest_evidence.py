"""Project existing failure observations without filling identity gaps.

Carrier equality is diagnostic equality, not a process incarnation, transport
receipt, transaction outcome or an atomic cross-node cut.
"""
import re

from workload_log import SEVERITY, _fields

BOUNDARIES = {
    "PGRAC ordinary TT exchange refused": ("requester", "node", "origin", "epoch"),
    "PGRAC ordinary TT outbound refused": ("outbound", "requester", "destination", "epoch"),
    "PGRAC ordinary TT refused": ("serve_refusal", "requester", "node", "epoch"),
    "PGRAC ordinary TT reply": ("reply_handoff", "requester", "node", "request_epoch"),
}


def _uint(value, bits=64, minimum=0):
    if type(value) is int:
        number = value
    elif isinstance(value, str) and re.fullmatch(r"[0-9]+", value):
        number = int(value)
    else:
        raise ValueError("missing or invalid unsigned integer")
    if not minimum <= number < 2**bits:
        raise ValueError("unsigned integer outside emitter range")
    return number


def request_evidence(logs):
    result = dict(records=[], groups=[], identity_bound=False, atomic_graph=False,
                  detail_budget_exhausted=False)
    grouped = {}
    for source_node, text in enumerate(logs):
        for source_line, raw in enumerate(text.splitlines(), 1):
            severity = SEVERITY.search(raw)
            if not severity or severity[1] != "LOG":
                continue
            message = re.sub(r"^[0-9A-Z]{5}: ", "", raw[severity.end():])
            for prefix, (boundary, requester, origin, epoch) in BOUNDARIES.items():
                if not message.startswith(prefix + ": "):
                    continue
                record = dict(source_node=source_node, source_line=source_line, raw=raw,
                              boundary=boundary, key=None, fields={}, missing_fields=[], error=None)
                index = len(result["records"])
                result["records"].append(record)
                try:
                    fields = record["fields"] = _fields(message[len(prefix) + 2:])
                    result["detail_budget_exhausted"] |= fields.get("detail_budget_exhausted") == "1"
                    record["missing_fields"] = sorted(set(
                        ("node", requester, origin, "backend", "request", epoch, "tag", "xid")) - fields.keys())
                    if record["missing_fields"]:
                        break
                    if _uint(fields["node"], 7) != source_node:
                        raise ValueError("emitter node differs from source node")
                    tag = [_uint(value, 32) for value in fields["tag"].split("/")]
                    if len(tag) != 5:
                        raise ValueError("incomplete BufferTag")
                    key = dict(requester_node=_uint(fields[requester], 7),
                               backend_id=_uint(fields["backend"], 31, 1),
                               origin_node=_uint(fields[origin], 7),
                               request_id=str(_uint(fields["request"], 64, 1)),
                               request_epoch=str(_uint(fields[epoch])), tag=tag,
                               xid=_uint(fields["xid"], 32, 3))
                    record["key"] = key
                    # Tuples avoid float conversion of the original uint64 ID.
                    token = tuple(tuple(value) if isinstance(value, list) else value for value in key.values())
                    group = grouped.setdefault(token, dict(key=key, record_indices=[],
                        observed_boundaries=[], repeated_boundaries=[], identity_bound=False,
                        frontend_consumption="UNKNOWN"))
                    group["record_indices"].append(index)
                    if boundary in group["observed_boundaries"]:
                        if boundary not in group["repeated_boundaries"]:
                            group["repeated_boundaries"].append(boundary)
                    else:
                        group["observed_boundaries"].append(boundary)
                except ValueError as error:
                    record["error"] = str(error)
                break
    result["groups"] = list(grouped.values())
    return result


def itl_evidence(trace):
    """Keep page ITL indices and TT publications in their original namespaces."""
    result = dict(pages=[], tt_publications=[], prefix_complete=False,
                  atomic_graph=False, producer_identity="UNKNOWN")
    if not isinstance(trace, dict):
        return result
    result["prefix_complete"] = (all(trace.get(key) is True for key in
        ("available", "enabled", "export_complete")) and trace.get("overflow") is False
        and type(trace.get("dropped")) is int and trace["dropped"] == 0)
    events = trace.get("events", [])
    if not isinstance(events, list):
        result["prefix_complete"] = False
        return result
    for event in events:
        if not isinstance(event, dict):
            result["prefix_complete"] = False
            continue
        if event.get("kind") in (2, 5, 6):
            result["tt_publications"].append(event)
        if event.get("kind") != 4:
            continue
        slots = event.get("slots", [])
        if not isinstance(slots, list):
            slots = []
        copied = [slot for slot in slots if isinstance(slot, dict) and type(slot.get("index")) is int]
        sixth = [slot for slot in copied if slot["index"] == 6]
        complete = False
        try:
            tag = event["tag"]
            complete = (event.get("page_known") is True and event.get("authoritative") is False
                and isinstance(tag, list) and len(tag) == 5
                and all(_uint(value, 32) == value for value in tag)
                and _uint(event["page_lsn"], minimum=1) > 0
                and _uint(event["page_scn"]) >= 0
                and len(slots) == 8 and sorted(slot["index"] for slot in copied) == list(range(8))
                and all(_slot_copy_complete(slot) for slot in copied))
        except (KeyError, ValueError, TypeError):
            pass
        result["pages"].append(dict(copy=event, copy_complete=complete,
                                   slot6=sixth[0] if len(sixth) == 1 else None))
    return result


def _slot_copy_complete(slot):
    """Check the existing 48-byte ITL projection, without interpreting state."""
    try:
        for field, bits in (("xid", 32), ("itl_wrap", 16), ("flags", 8),
                            ("lock_count", 8), ("commit_scn", 64),
                            ("write_scn", 64), ("first_change_lsn", 64)):
            _uint(slot[field], bits)
        return (slot.get("tt_wrap_known") is False
                and isinstance(slot.get("uba_bytes"), str)
                and re.fullmatch(r"[0-9a-f]{32}", slot["uba_bytes"]) is not None)
    except (KeyError, ValueError, TypeError):
        return False
