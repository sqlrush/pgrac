"""Stage exact target exports closed, before the exclusive owner's admission.

Author: SqlRush <sqlrush@gmail.com>
The caller owns target configuration and supplies a protected restore template.
No exported readiness or physical drain follows from this staging operation.
"""

from copy import deepcopy
from dataclasses import dataclass
import os
import re
import time

from target_inventory import _Census, _native_inventory, _path, _pin_files
from target_journal import TargetJournal, TargetJournalError, _canonical
from target_operation import _kernel_boot_id
from target_registry import TargetRegistry


@dataclass(frozen=True)
class ClosedTargetStage:
    target_boot_id: str
    inventory_digest: str
    journal_sequence: int
    journal_digest: str
    tpg_keys: tuple


def _keys(value, required, optional=()):
    if type(value) is not dict or not set(required) <= value.keys() <= set(required) | set(optional):
        raise TargetJournalError("TARGET_STARTUP_SCHEMA")


def _list(value):
    if type(value) is not list or not 0 < len(value) <= 4096:
        raise TargetJournalError("TARGET_STARTUP_SCHEMA")
    return value


def _same(value, expected):
    # Unlike ordinary dict equality, this cannot accept True for integer 1.
    if _canonical(value) != _canonical(expected):
        raise TargetJournalError("TARGET_STARTUP_IDENTITY")


def _layout(registry, journal):
    if (type(registry) is not TargetRegistry or type(journal) is not TargetJournal
            or journal.inventory != registry.inventory_digest or not 0 < len(registry.nodes) <= 4):
        raise TargetJournalError("TARGET_STARTUP_REGISTRY")
    guests = {n.mapping.guest_uuid: n for n in registry.nodes}
    if len(guests) != len(registry.nodes):
        raise TargetJournalError("TARGET_STARTUP_REGISTRY")
    for state in journal.denied():
        old = state.identity
        node = guests.get(old.guest_uuid)
        if (node is None or node.mapping.mapping_generation != old.mapping_generation
                or node.mapping.protected_set_digest != old.protected_set_digest
                or tuple(r.digest for r in node.mapping.routes) != old.route_digests):
            raise TargetJournalError("TARGET_STARTUP_DENY_UNMAPPED")
    tpgs, stores, names = {}, {}, {}
    for node in registry.nodes:
        for binding in node.bindings:
            _path(binding.file_path)
            # rtslib embeds this name in LIO's comma/newline-delimited control
            # string. A literal legal filename must not become another option.
            # Selected kernel FD_MAX_DEV_NAME is 256, including the terminator.
            if (len(os.fsencode(binding.file_path)) >= 256
                    or any(c == "," or ord(c) < 0x20 or ord(c) == 0x7f for c in binding.file_path)):
                raise TargetJournalError("TARGET_STARTUP_STORAGE_PATH")
            tpgs.setdefault((binding.target, binding.tpg), []).append(binding)
            match = re.fullmatch(r"/sys/kernel/config/target/core/fileio_([0-9]+)/([A-Za-z0-9_.-]+)",
                                 binding.storage_path)
            if match is None:
                raise TargetJournalError("TARGET_STARTUP_STORAGE")
            index, name = int(match[1]), match[2]
            expected = dict(name=name, index=index, plugin="fileio", dev=binding.file_path,
                            size=binding.size, wwn=binding.serial, write_back=False, aio=False,
                            attributes={"emulate_write_cache": 0})
            if binding.storage_path in stores:
                _same(stores[binding.storage_path], expected)
            if name in names and names[name] != binding.storage_path:
                raise TargetJournalError("TARGET_STARTUP_STORAGE_ALIAS")
            stores[binding.storage_path], names[name] = expected, binding.storage_path
    if not tpgs or not stores:
        raise TargetJournalError("TARGET_STARTUP_EMPTY")
    return tpgs, stores


def _acls(config, bindings, authentication):
    groups, seen = {}, set()
    for binding in bindings:
        group = groups.setdefault(binding.initiator, {})
        prior = group.setdefault(binding.mapped_lun, binding.tpg_lun)
        if prior != binding.tpg_lun:
            raise TargetJournalError("TARGET_STARTUP_LUN_ALIAS")
    auth_fields = {"chap_userid", "chap_password", "chap_mutual_userid", "chap_mutual_password"}
    for acl in _list(config):
        _keys(acl, {"node_wwn", "mapped_luns"}, auth_fields)
        iqn = acl["node_wwn"]
        if type(iqn) is not str or iqn not in groups or iqn in seen:
            raise TargetJournalError("TARGET_STARTUP_ACL")
        seen.add(iqn)
        expected = [dict(index=index, tpg_lun=lun, write_protect=False)
                    for index, lun in sorted(groups[iqn].items())]
        actual = _list(acl["mapped_luns"])
        _same(sorted(actual, key=lambda row: row["index"]), expected)
        for key in auth_fields & acl.keys():
            value = acl[key]
            if type(value) is not str or not 1 <= len(value) <= 255 or any(not 0x21 <= ord(c) <= 0x7e for c in value):
                raise TargetJournalError("TARGET_STARTUP_AUTH")
        for first, second in (("chap_userid", "chap_password"), ("chap_mutual_userid", "chap_mutual_password")):
            if (first in acl) != (second in acl):
                raise TargetJournalError("TARGET_STARTUP_AUTH")
        if authentication == 1 and not {"chap_userid", "chap_password"} <= acl.keys():
            raise TargetJournalError("TARGET_STARTUP_AUTH")
    if seen != groups.keys():
        raise TargetJournalError("TARGET_STARTUP_ACL")


def _tpg_config(tpg, bindings):
    _keys(tpg, {"tag", "enable", "attributes", "parameters", "luns", "portals", "node_acls"})
    if type(tpg["tag"]) is not int or type(tpg["enable"]) is not bool:
        raise TargetJournalError("TARGET_STARTUP_SCHEMA")
    authentication = tpg["attributes"].get("authentication") if type(tpg["attributes"]) is dict else None
    if type(authentication) is not int or authentication not in (0, 1):
        raise TargetJournalError("TARGET_STARTUP_AUTH")
    _same(tpg["attributes"], dict(generate_node_acls=0, cache_dynamic_acls=0,
                                  demo_mode_write_protect=1, authentication=authentication))
    _same(tpg["parameters"], {"ErrorRecoveryLevel": "0"})
    ports = {(b.portal, b.port) for b in bindings}
    _same(sorted(_list(tpg["portals"]), key=lambda p: (p["ip_address"], p["port"])),
          [dict(ip_address=ip, port=port, iser=False, offload=False) for ip, port in sorted(ports)])
    luns = {}
    for b in bindings:
        storage = "/backstores/fileio/" + b.storage_path.rsplit("/", 1)[1]
        prior = luns.setdefault(b.tpg_lun, storage)
        if prior != storage:
            raise TargetJournalError("TARGET_STARTUP_LUN_ALIAS")
    _same(sorted(_list(tpg["luns"]), key=lambda row: row["index"]),
          [dict(index=index, storage_object=storage) for index, storage in sorted(luns.items())])
    _acls(tpg["node_acls"], bindings, authentication)
    tpg["enable"] = False


def _closed_template(config, tpgs, stores):
    _keys(config, {"targets", "storage_objects"})
    if len(_canonical(config)) > 4 * 1024 * 1024:
        raise TargetJournalError("TARGET_STARTUP_TOO_LARGE")
    result = deepcopy(config)
    _same(sorted(_list(result["storage_objects"]), key=lambda s: s["name"]),
          sorted(stores.values(), key=lambda s: s["name"]))
    targets, seen_tpgs = set(), set()
    for target in _list(result["targets"]):
        _keys(target, {"wwn", "fabric", "tpgs"})
        if (type(target["wwn"]) is not str or target["wwn"] in targets
                or target["fabric"] != "iscsi"):
            raise TargetJournalError("TARGET_STARTUP_TARGET")
        targets.add(target["wwn"])
        for tpg in _list(target["tpgs"]):
            key = target["wwn"], tpg["tag"]
            if key not in tpgs or key in seen_tpgs:
                raise TargetJournalError("TARGET_STARTUP_TPG")
            _tpg_config(tpg, tpgs[key])
            seen_tpgs.add(key)
    if seen_tpgs != tpgs.keys():
        raise TargetJournalError("TARGET_STARTUP_TPG")
    return result


def _closed_native(registry, root, tpgs, stores):
    census, found, storage_paths = _Census(), set(), set()
    for storage in census.items(root.storage_objects):
        if storage.path not in stores or storage.path in storage_paths:
            raise TargetJournalError("TARGET_STARTUP_NATIVE_STORAGE")
        storage_paths.add(storage.path)
    if storage_paths != stores.keys():
        raise TargetJournalError("TARGET_STARTUP_NATIVE_STORAGE")
    for target in census.items(root.targets):
        for tpg in census.items(target.tpgs):
            key = target.wwn, tpg.tag
            if key not in tpgs or key in found or tpg.enable is not False:
                raise TargetJournalError("TARGET_STARTUP_EXPORT_NOT_CLOSED")
            found.add(key)
            actual = [acl.node_wwn for acl in census.items(tpg.node_acls)]
            expected = {b.initiator for b in tpgs[key]}
            if len(actual) != len(expected) or set(actual) != expected:
                raise TargetJournalError("TARGET_STARTUP_NATIVE_ACL")
    if found != tpgs.keys():
        raise TargetJournalError("TARGET_STARTUP_NATIVE_TPG")
    for node in registry.nodes:
        groups = {}
        for binding in node.bindings:
            groups.setdefault((binding.target, binding.tpg, binding.initiator), []).append(binding)
        # This is a closed configuration census, deliberately no history or
        # completed drain claim. Every configured ACL must currently exist.
        _native_inventory(root, groups, (0,) * len(node.bindings))


def stage_closed_exports(registry, journal, root, config, deadline_mono_ns):
    """No existing objects may be cleared and no export may be enabled.

    Caller must already hold exclusive configuration ownership, suppress default
    target restore and prevent external startup/ON. This token is not readiness.
    Partial native failure keeps objects closed; it does not clear their evidence.
    """
    descriptors = []
    try:
        if type(deadline_mono_ns) is not int or deadline_mono_ns <= time.monotonic_ns():
            raise TargetJournalError("TARGET_STARTUP_ENVELOPE_EXPIRED")
        tpgs, stores = _layout(registry, journal)
        closed = _closed_template(config, tpgs, stores)
        snapshot = journal.sequence, journal.digest
        boot = _kernel_boot_id()
        if any(root.targets) or any(root.storage_objects):
            raise TargetJournalError("TARGET_STARTUP_EXISTING_OBJECTS")
        for node in registry.nodes:
            _pin_files(node.bindings, descriptors)
        if time.monotonic_ns() >= deadline_mono_ns:
            raise TargetJournalError("TARGET_STARTUP_ENVELOPE_EXPIRED")
        errors = root.restore(closed, clear_existing=False, abort_on_error=True)
        if type(errors) is not list or errors:
            raise TargetJournalError("TARGET_STARTUP_NATIVE_ERROR")
        _closed_native(registry, root, tpgs, stores)
        journal.denied()  # Also rejects a poisoned/closed owner.
        if ((journal.sequence, journal.digest) != snapshot or _kernel_boot_id() != boot
                or time.monotonic_ns() >= deadline_mono_ns):
            raise TargetJournalError("TARGET_STARTUP_CHANGED")
        return ClosedTargetStage(boot, registry.inventory_digest, snapshot[0], snapshot[1], tuple(sorted(tpgs)))
    except TargetJournalError as error:
        raise error from None
    except Exception:
        raise TargetJournalError("TARGET_STARTUP_UNPROVEN") from None
    finally:
        for fd in descriptors:
            os.close(fd)
