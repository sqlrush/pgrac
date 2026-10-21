"""Read existing structured stderr LOG records without assigning workload credit.

All fields remain strings. File order and byte provenance are retained; process
identity, sequence completeness and causal edges require separate evidence.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re


# Ending fields belong to the current emitters. CTRC_RECEIPT predates explicit
# sequence_domain and must not acquire one merely because its family is known.
ENDINGS = dict.fromkeys((
    "READ_IMAGE", "READ_IMAGE_INGRESS", "READ_IMAGE_HOP", "R4_UNDO",
    "R4_HEAP_UPDATE", "HOT_FULL", "CTRC_RECEIPT", "CTRC_RECEIPT_LIFECYCLE",
    "R4_HEAP_PRUNE", "R4_BTREE_DELETE",
), "trace_sequence_exhausted")
ENDINGS.update(CTRC_CONSUME="sequence_exhausted",
               CTRC_ORIGIN_DISPATCH="sequence_exhausted",
               CTRC_ORIGIN_REPLY="sequence_saturated",
               CTRC_ORIGIN_BATCH_REPLY="sequence_saturated")
# These older emitters use node= and disclose neither scope nor domain. Do not
# manufacture modern envelope fields, even for item lines sharing operation_seq.
LEGACY_FAMILIES = frozenset(("R4_HEAP_PRUNE", "R4_BTREE_DELETE"))
SEVERITY = re.compile(r"\b(LOG|DEBUG[1-5]|DETAIL|HINT|CONTEXT|STATEMENT|ERROR|"
                      r"FATAL|PANIC|WARNING|NOTICE|INFO):[ \t]*")
KEY = re.compile(r"[A-Za-z_][A-Za-z_0-9]*=")
FAMILY = re.compile(r"PGRAC_FAMILY=([A-Z][A-Z_0-9]*)(?:[ \t]|$)")


def _fields(message, adjacent=()):
    """Parse emitter scalars and balanced brace blocks, preserving their text."""
    fields = {}
    position = 0
    while position < len(message):
        if message[position] in " \t":
            position += 1
            continue
        match = KEY.match(message, position)
        if match is None:
            raise ValueError("expected key=value field")
        key = match.group()[:-1]
        if key in fields:
            raise ValueError("duplicate field: " + key)
        start = position = match.end()
        if position < len(message) and message[position] == "{":
            depth = 0
            while position < len(message):
                character = message[position]
                depth += (character == "{") - (character == "}")
                position += 1
                if depth == 0:
                    break
            if depth:
                raise ValueError("unclosed brace field: " + key)
            if position < len(message) and message[position] not in " \t":
                raise ValueError("missing separator after brace field: " + key)
        else:
            while position < len(message) and message[position] not in " \t":
                if (any(key == left and message.startswith(right + "=", position)
                        for left, right in adjacent) and
                        re.fullmatch(r"[0-9]+", message[start:position])):
                    break
                if message[position] in '{}="':
                    raise ValueError("invalid scalar field: " + key)
                position += 1
        if position == start:
            raise ValueError("empty scalar field: " + key)
        fields[key] = message[start:position]
    return fields


def _validate_envelope(fields, family):
    legacy = family in LEGACY_FAMILIES
    node_key = "node" if legacy else "node_id"
    required = ("event", "event_seq", node_key, "pid", "authoritative")
    if any(key not in fields for key in required):
        raise ValueError("incomplete emitter envelope")
    if legacy:
        if any(key in fields for key in ("node_id", "sequence_scope", "sequence_domain")):
            raise ValueError("unexpected modern field in legacy envelope")
    else:
        if fields.get("sequence_scope") != "process":
            raise ValueError("unexpected sequence scope")
        domain = fields.get("sequence_domain")
        if domain != (None if family == "CTRC_RECEIPT" else family):
            raise ValueError("missing or unexpected sequence domain")
    sequence = fields["event_seq"]
    if not re.fullmatch(r"[0-9]+", sequence) or not 1 <= int(sequence) <= 2**64 - 1:
        raise ValueError("invalid event sequence")
    for key in (node_key, "pid"):
        if not re.fullmatch(r"[0-9]+", fields[key]):
            raise ValueError("invalid " + key)
    if int(fields["pid"]) == 0 or fields["authoritative"] != "0":
        raise ValueError("unexpected emitter identity or authority")
    ending = ENDINGS[family]
    if next(reversed(fields)) != ending or fields[ending] not in ("0", "1"):
        raise ValueError("missing or misplaced emitter ending: " + ending)


def read_trace(data):
    """Return lossless observations from one raw stderr file supplied as bytes.

    PARSED describes syntax only. This API never supplies normalized workload
    events, sealed identities, gap-free sequence summaries or a run verdict.
    Unsupported families are retained separately from malformed known records.
    """
    result = dict(source=dict(sha256=hashlib.sha256(data).hexdigest(), size_bytes=len(data)),
                  observations=[], rejections=[], unhandled=[], ignored_lines=0,
                  parse_status="NOT_OBSERVED", field_root="OPEN", identity_bound=False,
                  ready_to_run=False)
    offset = 0
    for number, raw in enumerate(data.splitlines(keepends=True), 1):
        provenance = dict(line=number, byte_offset=offset, byte_length=len(raw),
                          sha256=hashlib.sha256(raw).hexdigest())
        offset += len(raw)
        if b"PGRAC_FAMILY=" not in raw:
            result["ignored_lines"] += 1
            continue
        try:
            text = raw.decode("utf-8")
            severity = SEVERITY.search(text)
            if severity is None or severity[1] != "LOG":
                result["ignored_lines"] += 1
                continue
            message = text[severity.end():].removesuffix("\n").removesuffix("\r")
            if not message.startswith("PGRAC_FAMILY="):
                result["ignored_lines"] += 1
                continue
            if not raw.endswith(b"\n"):
                raise ValueError("unterminated LOG line")
            if any(ord(character) < 32 and character != "\t" for character in message):
                raise ValueError("control character in LOG message")
            match = FAMILY.match(message)
            if match is None:
                raise ValueError("invalid family field")
            family = match[1]
            if family not in ENDINGS:
                result["unhandled"].append(dict(provenance, family=family, raw=text))
                continue
            # The existing B-tree summary C format omits this one separator.
            # Recognize its exact literal boundary; retain the original message
            # and reject other concatenations rather than repairing input.
            adjacent = (("after_scn", "conflict_horizon_known"),) if (
                family == "R4_BTREE_DELETE" and re.match(
                    r"PGRAC_FAMILY=R4_BTREE_DELETE event=(?:delete|vacuum)\.applied[ \t]",
                    message)) else ()
            fields = _fields(message, adjacent)
            _validate_envelope(fields, family)
            result["observations"].append(dict(provenance, raw=text, message=message,
                                               fields=fields))
        except (UnicodeDecodeError, ValueError) as error:
            result["rejections"].append(dict(provenance, raw_hex=raw.hex(), reason=str(error)))
    if result["rejections"]:
        result["parse_status"] = "REJECTED"
    elif result["observations"]:
        result["parse_status"] = "PARSED"
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logfile", type=Path, help="one retained raw PostgreSQL stderr log")
    args = parser.parse_args()
    try:
        data = args.logfile.read_bytes()
    except OSError as error:
        parser.error(str(error))
    result = read_trace(data)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return {"PARSED": 0, "REJECTED": 1, "NOT_OBSERVED": 2}[result["parse_status"]]


if __name__ == "__main__":
    raise SystemExit(main())
