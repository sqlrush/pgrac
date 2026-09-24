"""Canonical target management command schema, shared by both channel ends.

Author: SqlRush <sqlrush@gmail.com>
Valid syntax does not grant operation authorization or certify isolation.
"""

import json

from target_journal import RejoinIntent, TargetJournalError, _canonical, _hex, _pairs, _rejoin_intent, _uint
from target_transport import MAX_FRAME_BYTES

IDENTITY_KEYS = frozenset(("version", "action", "challenge"))
OPERATION_KEYS = IDENTITY_KEYS | frozenset((
    "node_id", "system_identifier", "mapping_generation", "protected_set_digest",
    "operation_id", "attempt", "daemon_boot_id", "target_boot_id"))
REJOIN_ACTIONS = frozenset(("rejoin_restore", "rejoin_running", "rejoin_prepare_revoke", "rejoin_complete_off"))
REJOIN_KEYS = OPERATION_KEYS | frozenset(("old_incarnation", "candidate_incarnation", "rejoin_gate_digest"))


def _constant(_value):
    raise TargetJournalError("TARGET_COMMAND_JSON")


def decode_document(payload):
    if type(payload) is not bytes or not 1 <= len(payload) <= MAX_FRAME_BYTES:
        raise TargetJournalError("TARGET_COMMAND_SIZE")
    document = json.loads(payload.decode("ascii", errors="strict"), object_pairs_hook=_pairs,
                          parse_constant=_constant)
    if type(document) is not dict or _canonical(document) != payload:
        raise TargetJournalError("TARGET_COMMAND_SCHEMA")
    return document


def decode_command(payload):
    document = decode_document(payload)
    action = document.get("action")
    if type(action) is not str:
        raise TargetJournalError("TARGET_COMMAND_SCHEMA")
    keys = REJOIN_KEYS if action in REJOIN_ACTIONS else IDENTITY_KEYS if action == "identity" else OPERATION_KEYS
    if action == "rejoin_running":
        keys = keys | {"owner_phase"}
    if (type(document.get("version")) is not int or document["version"] != 1
            or not _hex(document.get("challenge"), 32)
            or action not in REJOIN_ACTIONS | {"identity", "prepare_deny", "complete_off"}
            or document.keys() != keys):
        raise TargetJournalError("TARGET_COMMAND_SCHEMA")
    if action != "identity" and (
            type(document["node_id"]) is not int or not 0 <= document["node_id"] < 128
            or not all(_uint(document[key]) for key in ("system_identifier", "mapping_generation", "attempt"))
            or not all(_hex(document[key], 32) for key in ("operation_id", "daemon_boot_id", "target_boot_id"))
            or not _hex(document["protected_set_digest"], 64)):
        raise TargetJournalError("TARGET_COMMAND_IDENTITY")
    if action in REJOIN_ACTIONS:
        rejoin_intent(document)
    if action == "rejoin_running" and document["owner_phase"] not in ("authorize", "refresh"):
        raise TargetJournalError("TARGET_COMMAND_PHASE")
    return document


def rejoin_intent(document):
    return _rejoin_intent(RejoinIntent(document["system_identifier"], document["node_id"],
                                      document["old_incarnation"], document["candidate_incarnation"],
                                      document["rejoin_gate_digest"]))
