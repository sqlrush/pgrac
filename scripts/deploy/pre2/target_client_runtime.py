"""One protected provider-client invocation over the existing target channel.

Author: SqlRush <sqlrush@gmail.com>
An authenticated observation is not a signed isolation proof or DB admission.
"""

from dataclasses import dataclass
import fcntl
import os
import select
import sys
import time

from target_client import TargetEndpoint, request_target, validate_endpoint
from target_command import decode_command
from target_credentials import TlsFiles, _lock_secrets, load_client_context
from target_inventory import _path, _uint
from target_journal import TargetJournalError, _canonical
from target_registry import _keys, _read_owned, load_registry
from target_transport import MAX_FRAME_BYTES


@dataclass(frozen=True)
class ClientConfig:
    endpoint: TargetEndpoint
    registry_path: str
    tls: TlsFiles


def load_client_config(path, *, owner_uid=0):
    try:
        if type(owner_uid) is not int or owner_uid < 0:
            raise TargetJournalError("TARGET_CLIENT_OWNER")
        document = _read_owned(path, owner_uid)
        _keys(document, {"version", "endpoint", "registry_path", "tls"})
        if type(document["version"]) is not int or document["version"] != 1:
            raise TargetJournalError("TARGET_CLIENT_VERSION")
        _keys(document["endpoint"], {"address", "port", "server_name", "certificate_sha256", "inventory_digest"})
        endpoint = TargetEndpoint(**document["endpoint"])
        validate_endpoint(endpoint)
        _keys(document["tls"], {"ca_certificate", "certificate", "private_key"})
        _path(document["registry_path"])
        for path in document["tls"].values():
            _path(path)
        return ClientConfig(endpoint, document["registry_path"], TlsFiles(**document["tls"], owner_uid=owner_uid))
    except Exception:
        raise TargetJournalError("TARGET_CLIENT_CONFIG") from None


def _fresh(deadline):
    if (not _uint(deadline, (1 << 64) - 1, 1) or time.monotonic_ns() >= deadline):
        raise TargetJournalError("TARGET_CLIENT_UNPROVEN")


def run_client_request(config_path, payload, deadline_mono_ns):
    """Dedicated Linux root worker; no retry, native target or power action.

    The caller already persisted the C operation and supplies its existing
    local deadline. The registry is independently authenticated, not inferred
    from the command or the target's response. Returned bytes remain uncertified.
    """
    try:
        if sys.platform != "linux" or os.geteuid() != 0:
            raise TargetJournalError("TARGET_CLIENT_PROCESS")
        _fresh(deadline_mono_ns)
        _lock_secrets()
        command = decode_command(payload)
        config = load_client_config(config_path)
        registry = load_registry(config.registry_path, deadline_mono_ns)
        if registry.inventory_digest != config.endpoint.inventory_digest:
            raise TargetJournalError("TARGET_CLIENT_INVENTORY")
        route_count = 1  # Identity-only replies contain no route results.
        if command["action"] != "identity":
            mapping = registry.node(command["node_id"]).mapping
            for key in ("system_identifier", "mapping_generation", "protected_set_digest"):
                value = getattr(mapping, key)
                if type(command[key]) is not type(value) or command[key] != value:
                    raise TargetJournalError("TARGET_CLIENT_BINDING")
            route_count = len(mapping.routes)
        context = load_client_context(config.tls, deadline_mono_ns)
        response = request_target(config.endpoint, context, command, deadline_mono_ns, route_count=route_count)
        payload = _canonical(response)
        if not 1 <= len(payload) <= MAX_FRAME_BYTES:
            raise TargetJournalError("TARGET_CLIENT_SIZE")
        _fresh(deadline_mono_ns)
        return payload
    except Exception:
        raise TargetJournalError("TARGET_CLIENT_UNPROVEN") from None


def _wait(fd, event, deadline):
    _fresh(deadline)
    poller = select.poll()
    poller.register(fd, event)
    remaining = max(1, (deadline - time.monotonic_ns() + 999999) // 1_000_000)
    events = poller.poll(min(remaining, (1 << 31) - 1))
    _fresh(deadline)
    if not events or events[0][1] & (select.POLLERR | select.POLLNVAL):
        raise TargetJournalError("TARGET_CLIENT_UNPROVEN")


def _read_command(fd, deadline):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    try:
        fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
        result = bytearray()
        while len(result) <= MAX_FRAME_BYTES:
            _wait(fd, select.POLLIN, deadline)
            try:
                part = os.read(fd, MAX_FRAME_BYTES + 1 - len(result))
            except (BlockingIOError, InterruptedError):
                continue
            if not part:
                return bytes(result)
            result.extend(part)
        raise TargetJournalError("TARGET_CLIENT_UNPROVEN")
    finally:
        fcntl.fcntl(fd, fcntl.F_SETFL, flags)


def _write_reply(fd, payload, deadline):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    try:
        fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
        offset = 0
        while offset < len(payload):
            _wait(fd, select.POLLOUT, deadline)
            try:
                count = os.write(fd, payload[offset:])
            except (BlockingIOError, InterruptedError):
                continue
            if count <= 0:
                raise TargetJournalError("TARGET_CLIENT_UNPROVEN")
            offset += count
        _fresh(deadline)
    finally:
        fcntl.fcntl(fd, fcntl.F_SETFL, flags)


def run_client_stream(config_path, deadline_mono_ns, *, input_fd=0, output_fd=1):
    """Exactly one pipe request/reply within the existing provider envelope.

    The provider owns and kills its process group if a local/native syscall
    itself becomes uninterruptible. A partial output or nonzero exit is UNKNOWN;
    no local channel failure retracts the target's durable obligations.
    """
    try:
        if sys.platform != "linux" or os.geteuid() != 0:
            raise TargetJournalError("TARGET_CLIENT_PROCESS")
        _fresh(deadline_mono_ns)
        _lock_secrets()
        payload = _read_command(input_fd, deadline_mono_ns)
        reply = run_client_request(config_path, payload, deadline_mono_ns)
        _write_reply(output_fd, reply, deadline_mono_ns)
    except Exception:
        raise TargetJournalError("TARGET_CLIENT_UNPROVEN") from None
