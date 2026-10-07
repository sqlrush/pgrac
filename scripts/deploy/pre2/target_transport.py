"""Bounded mutual-TLS channel for the sole target management owner.

Author: SqlRush <sqlrush@gmail.com>
Transport authentication is not operation authorization or isolation evidence.
The owner supplies protected credentials/configuration and the semantic handler.
"""

from dataclasses import dataclass
import hashlib
import hmac
import ssl
import struct
import time

from target_journal import TargetJournalError, _hex, _uint

MAX_FRAME_BYTES = 4096


@dataclass(frozen=True)
class AuthenticatedTargetRequest:
    peer_sha256: str
    payload: bytes
    deadline_mono_ns: int


def _policy(context, peer_pins):
    if (not isinstance(context, ssl.SSLContext)
            or context.protocol != ssl.PROTOCOL_TLS_SERVER
            or context.verify_mode != ssl.CERT_REQUIRED
            or context.minimum_version != ssl.TLSVersion.TLSv1_3
            or context.maximum_version != ssl.TLSVersion.TLSv1_3
            or context.keylog_filename is not None or context.num_tickets != 0
            or not context.options & ssl.OP_NO_TICKET
            or type(peer_pins) is not tuple or not 1 <= len(peer_pins) <= 4
            or any(not _hex(pin, 64) for pin in peer_pins)
            or len(set(peer_pins)) != len(peer_pins)):
        raise TargetJournalError("TARGET_TRANSPORT_POLICY")


def _remaining(deadline):
    now = time.monotonic_ns()
    if not _uint(deadline) or now >= deadline:
        raise TargetJournalError("TARGET_TRANSPORT_EXPIRED")
    return (deadline - now) / 1_000_000_000


def _receive(connection, size, deadline):
    data = bytearray()
    while len(data) < size:
        connection.settimeout(_remaining(deadline))
        part = connection.recv(size - len(data))
        if not part:
            raise TargetJournalError("TARGET_TRANSPORT_INCOMPLETE")
        data.extend(part)
    return bytes(data)


def serve_one(connection, context, peer_pins, deadline_mono_ns, dispatch):
    """Consume and close one accepted socket; never undo a dispatched operation.

    The single owner freezes context/allowlist for the connection lifetime and
    sets this local envelope before handshake. The dispatcher must independently
    bind semantic requests to its registry and durable operation state. It must
    honor the same deadline; hard worker termination belongs to the owner.
    """
    secured = None
    try:
        _policy(context, peer_pins)
        if not callable(dispatch):
            raise TargetJournalError("TARGET_TRANSPORT_HANDLER")
        connection.settimeout(_remaining(deadline_mono_ns))
        secured = context.wrap_socket(connection, server_side=True, do_handshake_on_connect=False)
        secured.settimeout(_remaining(deadline_mono_ns))
        secured.do_handshake()
        peer = secured.getpeercert(binary_form=True)
        if not peer or secured.version() != "TLSv1.3":
            raise TargetJournalError("TARGET_TRANSPORT_PEER")
        peer_digest = hashlib.sha256(peer).hexdigest()
        if not any(hmac.compare_digest(peer_digest, allowed) for allowed in peer_pins):
            raise TargetJournalError("TARGET_TRANSPORT_PEER")
        size = struct.unpack("!I", _receive(secured, 4, deadline_mono_ns))[0]
        if not 1 <= size <= MAX_FRAME_BYTES:
            raise TargetJournalError("TARGET_TRANSPORT_SIZE")
        payload = _receive(secured, size, deadline_mono_ns)
        _remaining(deadline_mono_ns)
        try:
            response = dispatch(AuthenticatedTargetRequest(peer_digest, payload, deadline_mono_ns))
        except Exception:
            # Even a typed application error may contain private material.
            raise TargetJournalError("TARGET_TRANSPORT_HANDLER") from None
        if type(response) is not bytes or not 1 <= len(response) <= MAX_FRAME_BYTES:
            raise TargetJournalError("TARGET_TRANSPORT_RESPONSE")
        secured.settimeout(_remaining(deadline_mono_ns))
        secured.sendall(struct.pack("!I", len(response)) + response)
        _remaining(deadline_mono_ns)
    except TargetJournalError as error:
        raise error from None
    except Exception:
        # No request, native TLS diagnostic or handler exception reaches logs.
        raise TargetJournalError("TARGET_TRANSPORT_UNPROVEN") from None
    finally:
        if secured is not None:
            secured.close()
        connection.close()
