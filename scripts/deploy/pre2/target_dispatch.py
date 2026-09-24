"""Bind authenticated commands to the sole target's durable deny/drain owner.

Author: SqlRush <sqlrush@gmail.com>
Responses are observations, never isolation certificates or DB admission.
"""

import time

from target_command import REJOIN_ACTIONS, decode_command
from target_inventory import resolve_routes
from target_journal import TargetJournal, TargetJournalError, _canonical, _hex, _uint
import target_operation as operation
from target_registry import TargetRegistry
from target_rejoin_operation import rejoin_operation
from target_transport import AuthenticatedTargetRequest, MAX_FRAME_BYTES

def _fresh(deadline, target_boot_id):
    if time.monotonic_ns() >= deadline or operation._kernel_boot_id() != target_boot_id:
        raise TargetJournalError("TARGET_COMMAND_CHANGED")


def dispatch_target(request, registry, journal, root, connection, *, startup_config=None):
    """Execute serially under the owner's exclusive journal/config ownership.

    Only serve_one creates the request after TLS peer verification; no untrusted
    IPC may construct it for this internal API. Native handles and registry are
    owner inputs, not network fields. A failed reply never cancels durable DENY.
    The enclosing owner must bound native calls with its worker watchdog.
    """
    try:
        if (type(request) is not AuthenticatedTargetRequest
                or not _hex(request.peer_sha256, 64)
                or not _uint(request.deadline_mono_ns)
                or time.monotonic_ns() >= request.deadline_mono_ns
                or type(request.payload) is not bytes
                or not 1 <= len(request.payload) <= MAX_FRAME_BYTES
                or type(registry) is not TargetRegistry or type(journal) is not TargetJournal
                or registry.inventory_digest != journal.inventory):
            raise TargetJournalError("TARGET_COMMAND_CONTEXT")
        document = decode_command(request.payload)
        action = document["action"]
        states = journal.denied()  # Also refuses a poisoned or closed journal.
        target_boot = operation._kernel_boot_id()
        if not _hex(target_boot, 32):
            raise TargetJournalError("TARGET_COMMAND_BOOT")
        if action == "identity":
            _fresh(request.deadline_mono_ns, target_boot)
            return _canonical({"version": 1, "status": "IDENTITY_ONLY",
                               "challenge": document["challenge"], "target_boot_id": target_boot,
                               "inventory_digest": registry.inventory_digest})

        node = registry.node(document["node_id"])
        for key in ("system_identifier", "mapping_generation", "protected_set_digest"):
            expected = getattr(node.mapping, key)
            if type(document[key]) is not type(expected) or document[key] != expected:
                raise TargetJournalError("TARGET_COMMAND_BINDING")
        if document["target_boot_id"] != target_boot:
            raise TargetJournalError("TARGET_COMMAND_BOOT")
        identity = registry.drain_identity(document["node_id"], document["operation_id"],
                                           document["attempt"], document["daemon_boot_id"], target_boot)
        _fresh(request.deadline_mono_ns, target_boot)
        if action in REJOIN_ACTIONS:
            result = rejoin_operation(document, registry, journal, root, connection,
                                      identity, startup_config, request.deadline_mono_ns)
            _fresh(request.deadline_mono_ns, target_boot)
            response = {**document, **result, "journal_sequence": journal.sequence,
                        "journal_digest": journal.digest}
            del response["action"]
            return _canonical(response)
        if action == "prepare_deny":
            # Native census and backing-file pins precede durable preparation.
            # A live guest is allowed: the caller powers it OFF only after ACK.
            with resolve_routes(root, journal, identity, node.bindings):
                _fresh(request.deadline_mono_ns, target_boot)
                journal.deny(identity)
                _fresh(request.deadline_mono_ns, target_boot)
            status, phases = "DENY_RECORDED", None
        else:
            if not any(state.identity == identity for state in states):
                raise TargetJournalError("TARGET_COMMAND_NOT_PREPARED")
            completed = operation.complete_off_drain(
                registry, journal, root, connection, document["node_id"], identity.operation_id,
                identity.attempt, identity.daemon_boot_id, target_boot, document["challenge"],
                request.deadline_mono_ns)
            status, phases = "OFF_DRAIN_UNCERTIFIED", list(completed.phases)
        _fresh(request.deadline_mono_ns, target_boot)
        response = {**document, "status": status, "journal_sequence": journal.sequence,
                    "journal_digest": journal.digest}
        del response["action"]
        if phases is not None:
            response["route_phases"] = phases
        return _canonical(response)
    except Exception:
        # Neither malformed input nor native diagnostics disclose payloads/paths.
        raise TargetJournalError("TARGET_COMMAND_UNPROVEN") from None
