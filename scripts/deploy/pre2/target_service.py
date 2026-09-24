"""Protected management-listener composition; never database admission.

Author: SqlRush <sqlrush@gmail.com>
No automatic export restore, journal repair, power action or certification.
"""

from dataclasses import dataclass
import ipaddress
import os
import signal
import socket
import sys
import time

from target_credentials import TlsFiles, _lock_secrets, load_server_context
from target_inventory import _path, _uint
from target_journal import TargetJournal, TargetJournalError, _hex
import target_operation as operation
from target_native import native_handles
from target_registry import _keys, _read_owned, load_registry
from target_startup import _closed_template, _layout
from target_worker import TargetWorker


@dataclass(frozen=True)
class ServiceConfig:
    listen_address: str
    listen_port: int
    state_directory: str
    registry_path: str
    template_path: str
    tls: TlsFiles
    peer_pins: tuple
    target_boot_id: str
    command_timeout_ms: int
    service_mode: str


def load_service_config(path, *, owner_uid=0):
    """Read exact owner inputs, never request-supplied locators or commands."""
    try:
        if type(owner_uid) is not int or owner_uid < 0:
            raise TargetJournalError("TARGET_SERVICE_OWNER")
        document = _read_owned(path, owner_uid)
        _keys(document, {"version", "listen_address", "listen_port", "state_directory",
                         "registry_path", "template_path", "tls", "peer_pins", "target_boot_id",
                         "command_timeout_ms", "service_mode"})
        address = ipaddress.IPv4Address(document["listen_address"])
        if (type(document["version"]) is not int or document["version"] != 1
                or type(document["listen_address"]) is not str
                or str(address) != document["listen_address"] or address.is_unspecified
                or address.is_multicast or address == ipaddress.IPv4Address("255.255.255.255")
                or not _uint(document["listen_port"], 65535, 1)
                or not _uint(document["command_timeout_ms"], 600000, 1)
                or document["service_mode"] not in ("normal", "closed-reconcile")
                or not _hex(document["target_boot_id"], 32)
                or type(document["peer_pins"]) is not list or not 1 <= len(document["peer_pins"]) <= 4
                or any(not _hex(pin, 64) for pin in document["peer_pins"])
                or len(set(document["peer_pins"])) != len(document["peer_pins"])):
            raise TargetJournalError("TARGET_SERVICE_SCHEMA")
        _keys(document["tls"], {"ca_certificate", "certificate", "private_key"})
        for name in ("state_directory", "registry_path", "template_path"):
            _path(document[name])
        for value in document["tls"].values():
            _path(value)
        return ServiceConfig(document["listen_address"], document["listen_port"],
                             document["state_directory"], document["registry_path"], document["template_path"],
                             TlsFiles(**document["tls"], owner_uid=owner_uid), tuple(document["peer_pins"]),
                             document["target_boot_id"], document["command_timeout_ms"], document["service_mode"])
    except Exception:
        raise TargetJournalError("TARGET_SERVICE_CONFIG") from None


def check_service_boot(owner, target_boot_id):
    """Refuse old target-boot obligations; never rebase or discard them."""
    if type(owner) is not TargetWorker or not _hex(target_boot_id, 32):
        raise TargetJournalError("TARGET_SERVICE_OWNER")
    owner._usable()
    if operation._kernel_boot_id() != target_boot_id:
        raise TargetJournalError("TARGET_SERVICE_RECONCILIATION_REQUIRED")
    with TargetJournal(owner.directory, owner.registry.inventory_digest,
                       owner_uid=owner.owner_uid) as journal:
        if not owner.closed_reconcile and any(state.identity.target_boot_id != target_boot_id
                                              for state in journal.denied()):
            raise TargetJournalError("TARGET_SERVICE_RECONCILIATION_REQUIRED")


def run_service(config_path):
    """Dedicated Linux root entry: validate everything before opening a port.

    The deployment must already provide the protected directory, initialized
    journal and closed-start policy. Starting this service does not create any
    of them, certify a provider, change target exports or start a guest.
    """
    try:
        if sys.platform != "linux" or os.geteuid() != 0:
            raise TargetJournalError("TARGET_SERVICE_PROCESS")
        _lock_secrets()  # The restore template can contain CHAP credentials.
        config = load_service_config(config_path)
        deadline = time.monotonic_ns() + config.command_timeout_ms * 1_000_000
        registry = load_registry(config.registry_path, deadline)
        context = load_server_context(config.tls, deadline)
        template = _read_owned(config.template_path, 0)
        with TargetWorker(config.state_directory, registry, context, config.peer_pins,
                          native_handles, startup_config=template,
                          closed_reconcile=config.service_mode == "closed-reconcile") as owner:
            check_service_boot(owner, config.target_boot_id)
            with TargetJournal(owner.directory, registry.inventory_digest) as journal:
                _closed_template(template, *_layout(registry, journal))
            if time.monotonic_ns() >= deadline:
                raise TargetJournalError("TARGET_SERVICE_START_EXPIRED")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
                listener.bind((config.listen_address, config.listen_port))
                listener.listen(4)
                print("TARGET_SERVICE_LISTENING_UNCERTIFIED", flush=True)
                serve_listener(listener, owner, config)
    except TargetJournalError:
        raise
    except Exception:
        raise TargetJournalError("TARGET_SERVICE_UNPROVEN") from None


class _ServiceStop(BaseException):
    pass


def _stop(_signum, _frame):
    raise _ServiceStop()


def serve_listener(listener, owner, config):
    """Single main-thread loop; listener readiness is explicitly uncertified.

    The caller acquires ownership and checks configuration/boot before binding.
    Shutdown preserves every durable obligation, including failed operations.
    """
    if (type(config) is not ServiceConfig or type(owner) is not TargetWorker
            or not _uint(config.command_timeout_ms, 600000, 1)
            or config.service_mode not in ("normal", "closed-reconcile")
            or owner.closed_reconcile != (config.service_mode == "closed-reconcile")
            or listener.family != socket.AF_INET
            or listener.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE) != socket.SOCK_STREAM
            or listener.getsockopt(socket.SOL_SOCKET, socket.SO_ACCEPTCONN) != 1):
        raise TargetJournalError("TARGET_SERVICE_LISTENER")
    previous = {}
    reported = set()
    try:
        for signum in (signal.SIGTERM, signal.SIGINT):
            previous[signum] = signal.signal(signum, _stop)
        check_service_boot(owner, config.target_boot_id)
        while True:
            connection, _peer = listener.accept()
            try:
                check_service_boot(owner, config.target_boot_id)
                deadline = time.monotonic_ns() + config.command_timeout_ms * 1_000_000
                try:
                    owner.serve(connection, deadline)
                except TargetJournalError as error:
                    if owner.poisoned or owner.active_pid is not None:
                        raise TargetJournalError("TARGET_SERVICE_OWNER_UNPROVEN") from None
                    reason = (str(error) if str(error) in ("TARGET_WORKER_EXPIRED", "TARGET_WORKER_UNPROVEN")
                              else "TARGET_SERVICE_REQUEST_UNPROVEN")
                    if reason not in reported:
                        print(reason, file=sys.stderr, flush=True)
                        reported.add(reason)
            finally:
                connection.close()
    except _ServiceStop:
        # TargetWorker has already killed/reaped any active owned child. A
        # surviving worker leaves owner.close() refusing and its lock inherited.
        return
    finally:
        listener.close()
        for signum, handler in previous.items():
            signal.signal(signum, handler)
