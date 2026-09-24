"""Bounded, pinned management client; never retries or grants DB admission.

Author: SqlRush <sqlrush@gmail.com>
Endpoint, trust and route cardinality come from protected owner configuration.
"""

from dataclasses import dataclass
import hashlib
import hmac
import ipaddress
import re
import socket
import ssl
import struct

from target_command import decode_command, decode_document
from target_journal import TargetJournalError, _canonical, _hex, _uint
from target_transport import MAX_FRAME_BYTES, _receive, _remaining


@dataclass(frozen=True)
class TargetEndpoint:
    address: str
    port: int
    server_name: str
    certificate_sha256: str
    inventory_digest: str


def _policy(endpoint, context):
    label = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    if (type(endpoint) is not TargetEndpoint or type(endpoint.address) is not str
            or str(ipaddress.IPv4Address(endpoint.address)) != endpoint.address
            or type(endpoint.port) is not int or not 1 <= endpoint.port <= 65535
            or type(endpoint.server_name) is not str or len(endpoint.server_name) > 253
            or not re.fullmatch(label + r"(?:\." + label + r")*", endpoint.server_name)
            or not _hex(endpoint.certificate_sha256, 64) or not _hex(endpoint.inventory_digest, 64)
            or not isinstance(context, ssl.SSLContext) or context.protocol != ssl.PROTOCOL_TLS_CLIENT
            or context.verify_mode != ssl.CERT_REQUIRED or not context.check_hostname
            or context.hostname_checks_common_name or context.keylog_filename is not None
            or context.minimum_version != ssl.TLSVersion.TLSv1_3
            or context.maximum_version != ssl.TLSVersion.TLSv1_3
            or not context.options & ssl.OP_NO_TICKET):
        raise TargetJournalError("TARGET_CLIENT_POLICY")


def _reply(payload, command, endpoint, route_count):
    reply = decode_document(payload)
    expected = {key: value for key, value in command.items() if key != "action"}
    action = command["action"]
    status = {"identity": "IDENTITY_ONLY", "prepare_deny": "DENY_RECORDED",
              "complete_off": "OFF_DRAIN_UNCERTIFIED", "rejoin_restore": "ACCESS_READY_UNCERTIFIED",
              "rejoin_running": "REJOIN_RUNNING_UNCERTIFIED", "rejoin_prepare_revoke": "REJOIN_REVOKED",
              "rejoin_complete_off": "OFF_DRAIN_UNCERTIFIED"}[action]
    expected["status"] = status
    fields = {"target_boot_id", "inventory_digest"} if action == "identity" else {
        "journal_sequence", "journal_digest"}
    if action in ("complete_off", "rejoin_complete_off"):
        fields.add("route_phases")
    if action == "rejoin_running":
        fields.add("runtime_id")
    if (reply.keys() != expected.keys() | fields
            or any(type(reply[key]) is not type(value) or reply[key] != value
                   for key, value in expected.items())):
        raise TargetJournalError("TARGET_CLIENT_REPLY_IDENTITY")
    if action == "identity":
        if not _hex(reply["target_boot_id"], 32) or reply["inventory_digest"] != endpoint.inventory_digest:
            raise TargetJournalError("TARGET_CLIENT_REPLY_INVENTORY")
    elif not _uint(reply["journal_sequence"]) or not _hex(reply["journal_digest"], 64):
        raise TargetJournalError("TARGET_CLIENT_REPLY_JOURNAL")
    if action == "rejoin_running" and (type(reply["runtime_id"]) is not int
                                       or not 0 < reply["runtime_id"] < (1 << 32) - 1):
        raise TargetJournalError("TARGET_CLIENT_REPLY_RUNTIME")
    if action in ("complete_off", "rejoin_complete_off") and (
            type(reply["route_phases"]) is not list or len(reply["route_phases"]) != route_count
            or any(type(phase) is not int or phase != 3 for phase in reply["route_phases"])):
        raise TargetJournalError("TARGET_CLIENT_REPLY_INCOMPLETE")
    return reply


def request_target(endpoint, context, command, deadline_mono_ns, *, route_count):
    """One exact attempt; disconnect/timeout never cancels remote obligations.

    The caller freezes its context/config and bounds this worker. Local deadlines
    are not comparable with the target host clock. Returned journal/route data is
    an authenticated observation only, not a signed isolation certificate.
    """
    try:
        _policy(endpoint, context)
        if type(route_count) is not int or not 1 <= route_count <= 128:
            raise TargetJournalError("TARGET_CLIENT_ROUTE_COUNT")
        payload = _canonical(command)
        command = decode_command(payload)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as plain:
            plain.settimeout(_remaining(deadline_mono_ns))
            plain.connect((endpoint.address, endpoint.port))
            with context.wrap_socket(plain, server_hostname=endpoint.server_name,
                                     do_handshake_on_connect=False) as secured:
                secured.settimeout(_remaining(deadline_mono_ns))
                secured.do_handshake()
                peer = secured.getpeercert(binary_form=True)
                if (secured.version() != "TLSv1.3" or not peer
                        or not hmac.compare_digest(hashlib.sha256(peer).hexdigest(),
                                                   endpoint.certificate_sha256)):
                    raise TargetJournalError("TARGET_CLIENT_PEER")
                secured.settimeout(_remaining(deadline_mono_ns))
                secured.sendall(struct.pack("!I", len(payload)) + payload)
                size = struct.unpack("!I", _receive(secured, 4, deadline_mono_ns))[0]
                if not 1 <= size <= MAX_FRAME_BYTES:
                    raise TargetJournalError("TARGET_CLIENT_SIZE")
                response = _receive(secured, size, deadline_mono_ns)
                secured.settimeout(_remaining(deadline_mono_ns))
                if secured.recv(1):
                    raise TargetJournalError("TARGET_CLIENT_TRAILING")
                reply = _reply(response, command, endpoint, route_count)
                _remaining(deadline_mono_ns)
                return reply
    except Exception:
        raise TargetJournalError("TARGET_CLIENT_UNPROVEN") from None
