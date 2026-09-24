"""Linux root runtime with actual signed registry, protected TLS and journal.

Author: SqlRush <sqlrush@gmail.com>
Native handles alone are fixtures; no host targets or guests are changed.
Usage: sudo python3 run_target_service_runtime.py /absolute/map-verifier
"""

from contextlib import contextmanager
import ctypes
import hashlib
import json
import os
from pathlib import Path
import resource
import signal
import socket
import ssl
import sys
import time
from types import SimpleNamespace as Object
import unittest
from unittest.mock import patch

import run_target_registry as registry_tests
import run_target_worker as worker_tests
import test_target_service_config as config_tests
import test_target_transport as tls_tests
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_client import TargetEndpoint, request_target
from target_journal import TargetJournal, TargetJournalError
import target_service


class ServiceRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if sys.platform != "linux" or os.geteuid() != 0:
            raise AssertionError("run this fixture test as Linux root")
        registry_tests.TargetRegistryTests.setUpClass()
        tls_tests.TargetTransportTests.setUpClass.__func__(cls)
        cls.tls_directory = cls.directory
        for path in cls.tls_directory.iterdir():
            path.chmod(0o600)

    def setUp(self):
        self.assertTrue(callable(getattr(target_service, "run_service", None)),
                        "protected target service runtime is missing")
        self.r = registry_tests.TargetRegistryTests()
        self.addCleanup(self.r.doCleanups)
        self.r.setUp()
        self.registry = self.r.load_ok()
        self.c = config_tests.ServiceConfigTests()
        self.addCleanup(self.c.doCleanups)
        self.c.setUp()
        self.state = self.r.directory / "state"
        self.state.mkdir(mode=0o700)
        with TargetJournal(str(self.state), self.registry.inventory_digest, initialize=True):
            pass
        self.boot_id = Path("/proc/sys/kernel/random/boot_id").read_text().strip().replace("-", "")
        with socket.socket() as port:
            port.bind(("127.0.0.1", 0))
            self.port = port.getsockname()[1]
        self.template = self.r.directory / "template.json"
        self.write_template()
        self.c.document.update(listen_port=self.port, state_directory=str(self.state),
                               registry_path=str(self.r.registry), template_path=str(self.template),
                               peer_pins=[self.pin], target_boot_id=self.boot_id, command_timeout_ms=3000,
                               tls={"ca_certificate": str(self.tls_directory / "client.pem"),
                                    "certificate": str(self.tls_directory / "server.pem"),
                                    "private_key": str(self.tls_directory / "server.key")})
        self.c.write()
        self.endpoint = TargetEndpoint("127.0.0.1", self.port, "server", hashlib.sha256(
            ssl.PEM_cert_to_DER_cert((self.tls_directory / "server.pem").read_text())).hexdigest(),
            self.registry.inventory_digest)
        self.server = None
        self.addCleanup(self.stop)

    def write_template(self):
        bindings = self.registry.node(2).bindings
        document = {"storage_objects": [], "targets": [{"wwn": bindings[0].target,
                                                        "fabric": "iscsi", "tpgs": []}]}
        for b in bindings:
            document["storage_objects"].append(dict(plugin="fileio", name=f"disk{b.ordinal}",
                index=b.ordinal, dev=b.file_path, size=b.size, wwn=b.serial, write_back=False,
                aio=False, attributes={"emulate_write_cache": 0}))
            document["targets"][0]["tpgs"].append(dict(tag=b.tpg, enable=True,
                attributes=dict(generate_node_acls=0, cache_dynamic_acls=0, demo_mode_write_protect=1,
                                authentication=0), parameters={"ErrorRecoveryLevel": "0"},
                luns=[dict(index=b.tpg_lun, storage_object=f"/backstores/fileio/disk{b.ordinal}")],
                portals=[dict(ip_address=b.portal, port=b.port, iser=False, offload=False)],
                node_acls=[dict(node_wwn=b.initiator,
                    mapped_luns=[dict(index=b.mapped_lun, tpg_lun=b.tpg_lun, write_protect=False)])]))
        self.template.write_text(json.dumps(document))
        self.template.chmod(0o600)

    @contextmanager
    def native(self):
        # This factory must run only inside the authenticated worker, with
        # dump protection inherited from the real dedicated runtime.
        if (resource.getrlimit(resource.RLIMIT_CORE) != (0, 0)
                or ctypes.CDLL(None).prctl(3, 0, 0, 0, 0) != 0):
            raise AssertionError("secrets not protected")
        (self.state / "native-opened").write_text(str(os.getpid()))
        bindings = self.registry.node(2).bindings
        target = Object(wwn=bindings[0].target, fabric_module=Object(name="iscsi"), tpgs=[])
        target.path = "/sys/kernel/config/target/iscsi/" + target.wwn
        config = {}
        for b in bindings:
            tpg = Object(tag=b.tpg, parent_target=target, path=target.path + f"/tpgt_{b.tpg}",
                         get_attribute=lambda key: {"generate_node_acls": "0", "cache_dynamic_acls": "0",
                                                     "demo_mode_write_protect": "1"}[key],
                         get_parameter=lambda key: {"ErrorRecoveryLevel": "0"}[key])
            target.tpgs.append(tpg)
            portal = Object(ip_address=b.portal, port=b.port, parent_tpg=tpg,
                            path=tpg.path + f"/np/{b.portal}:{b.port}")
            config[portal.path + "/iser"] = config[portal.path + "/cxgbit"] = "0"
            storage = Object(plugin="fileio", path=b.storage_path, wwn=b.serial,
                             udev_path=b.file_path, size=b.size)
            config[storage.path + "/info"] = f"File: {b.file_path}  Size: {b.size}  Mode: O_DSYNC Async: 0\n"
            config[storage.path + "/attrib/emulate_write_cache"] = "0"
            lun = Object(lun=b.tpg_lun, path=tpg.path + f"/lun/lun_{b.tpg_lun}", storage_object=storage)
            acl = Object(node_wwn=b.initiator, parent_tpg=tpg, path=tpg.path + "/acls/" + b.initiator)
            acl.mapped_luns = [Object(mapped_lun=b.mapped_lun, tpg_lun=lun,
                                      path=acl.path + f"/lun_{b.mapped_lun}")]
            tpg.node_acls, tpg.luns, tpg.network_portals = [acl], [lun], [portal]
        with patch("target_inventory._read_config", side_effect=config.__getitem__):
            yield Object(targets=[target]), object()

    def spawn(self):
        read_fd, write_fd = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(read_fd)
            os.dup2(write_fd, 1)
            os.dup2(write_fd, 2)
            os.close(write_fd)
            status = 2
            try:
                with patch("target_service.native_handles", self.native):
                    target_service.run_service(str(self.c.path))
                status = 0
            except BaseException as error:
                reason = str(error) if isinstance(error, TargetJournalError) else "UNEXPECTED_TEST_ERROR"
                os.write(2, (reason + "\n").encode())
            os._exit(status)
        os.close(write_fd)
        try:
            output = worker_tests.read_pipe(read_fd, 6)
        finally:
            os.close(read_fd)
        return pid, output

    def start(self):
        self.server, output = self.spawn()
        self.assertEqual(output, b"TARGET_SERVICE_LISTENING_UNCERTIFIED\n")

    def stop(self):
        if self.server is not None:
            found, status = os.waitpid(self.server, os.WNOHANG)
            if not found:
                os.kill(self.server, signal.SIGTERM)
                status = worker_tests.reap(self.server)
            self.server = None
            return status

    def request(self, document):
        tls = tls_tests.TargetTransportTests()
        tls.directory = self.tls_directory
        context = tls.context(server=False)
        context.hostname_checks_common_name = False
        return request_target(self.endpoint, context, document, time.monotonic_ns() + 3_000_000_000,
                              route_count=4)

    def refused(self):
        before = (self.state / "deny.journal").read_bytes()
        self.server, output = self.spawn()
        self.assertRegex(output.decode(), r"^TARGET_[A-Z_]+\n$")
        self.assertNotIn(b"LISTENING", output)
        self.assertEqual(worker_tests.reap(self.server), 2 << 8)
        self.server = None
        self.assertEqual((self.state / "deny.journal").read_bytes(), before)
        self.assertFalse((self.state / "native-opened").exists())
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", self.port))

    def test_actual_config_signature_tls_dump_protection_and_durable_prepare(self):
        self.start()
        self.assertFalse((self.state / "native-opened").exists())
        reply = self.request({"version": 1, "action": "identity", "challenge": "ab" * 16})
        self.assertEqual(reply["status"], "IDENTITY_ONLY")
        self.assertEqual(reply["target_boot_id"], self.boot_id)
        native_pid = int((self.state / "native-opened").read_text())
        self.assertNotEqual(native_pid, self.server)
        mapping = self.registry.node(2).mapping
        reply = self.request(dict(version=1, action="prepare_deny", challenge="ac" * 16,
            node_id=2, system_identifier=mapping.system_identifier,
            mapping_generation=mapping.mapping_generation, protected_set_digest=mapping.protected_set_digest,
            operation_id="ad" * 16, attempt=1, daemon_boot_id="ae" * 16, target_boot_id=self.boot_id))
        self.assertEqual(reply["status"], "DENY_RECORDED")
        self.assertEqual(self.stop(), 0)
        with TargetJournal(str(self.state), self.registry.inventory_digest) as journal:
            self.assertEqual(journal.denied()[0].identity.operation_id, "ad" * 16)

    def test_wrong_boot_refuses_before_listen(self):
        self.c.document["target_boot_id"] = "ee" * 16
        self.c.write()
        self.refused()

    def test_bad_signature_refuses_before_native_or_listen(self):
        self.r.document["nodes"][0]["signed_map"] = self.r.packet[:-1].hex() + "00"
        self.r.write()
        self.refused()

    def test_extra_template_access_refuses_before_listen(self):
        value = json.loads(self.template.read_text())
        value["targets"][0]["tpgs"][0]["attributes"]["generate_node_acls"] = 1
        self.template.write_text(json.dumps(value))
        self.refused()

    def test_world_readable_secret_template_refuses_before_listen(self):
        self.template.chmod(0o644)
        self.refused()

    def test_competing_owner_cannot_even_rebind_listener(self):
        self.start()
        pid, output = self.spawn()
        self.assertEqual(worker_tests.reap(pid), 2 << 8)
        self.assertEqual(output, b"TARGET_WORKER_BUSY\n")
        reply = self.request({"version": 1, "action": "identity", "challenge": "ab" * 16})
        self.assertEqual(reply["status"], "IDENTITY_ONLY")
        self.assertEqual(self.stop(), 0)


if __name__ == "__main__":
    if len(sys.argv) != 2 or not Path(sys.argv[1]).is_absolute():
        raise SystemExit("expected absolute verifier path")
    registry_tests.EXECUTABLE = sys.argv[1]
    unittest.main(argv=[sys.argv[0]], verbosity=2)
