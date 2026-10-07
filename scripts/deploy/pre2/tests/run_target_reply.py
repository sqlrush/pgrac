"""Real C commands and observation consumer around the production dispatcher.

Author: SqlRush <sqlrush@gmail.com>
Only native libvirt/configfs calls use the established fixtures. No live changes
or signed isolation/admission claim. Takes the real C test executable path.
"""

from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import unittest
from unittest.mock import patch
import uuid

import test_target_dispatch as dispatch_tests
import test_target_permissions as permission_tests
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_dispatch import dispatch_target
from target_journal import TargetJournal, _canonical
from target_registry import NodeRegistry, TargetRegistry
from target_transport import AuthenticatedTargetRequest


class RealCReplyTests(unittest.TestCase):
    def fixture(self, rejoin=False):
        f = permission_tests.PermissionTests() if rejoin else dispatch_tests.TargetDispatchTests()
        f.arm_initial = False
        self.addCleanup(f.doCleanups)
        f.setUp()
        f.journal.close()
        directory = f.directory / "c-owner-journal"
        directory.mkdir(mode=0o700)
        f.journal = TargetJournal(str(directory), "ee" * 32, initialize=True, owner_uid=os.geteuid())
        self.addCleanup(f.journal.close)
        f.identity = replace(f.identity, operation_id="33" * 16, attempt=2 if rejoin else 3,
                             daemon_boot_id="11" * 16, target_boot_id="cc" * 16,
                             guest_uuid="77" * 16, mapping_generation=7, protected_set_digest="66" * 32)
        mapping = replace(f.registry.nodes[0].mapping, system_identifier=18446744073709551601,
                          guest_uuid=f.identity.guest_uuid, mapping_generation=7,
                          protected_set_digest=f.identity.protected_set_digest)
        f.registry = TargetRegistry("ee" * 32, (NodeRegistry(mapping, f.bindings),))
        f.connection.domain.uuid = str(uuid.UUID(hex=f.identity.guest_uuid))
        boot = patch("target_operation._kernel_boot_id", return_value=f.identity.target_boot_id)
        boot.start()
        self.addCleanup(boot.stop)
        if rejoin:
            f.journal.deny(f.identity)
            for n in range(4):
                for phase in (1, 2, 3):
                    f.journal.advance(f.identity.operation_id, f.identity.target_boot_id, n, phase)
            f.intent = replace(f.intent, system_identifier=18446744073709551601,
                               old_incarnation=91, candidate_incarnation=92, rejoin_gate_digest="99" * 32)
            f.patch_native()
        return f

    def decode(self, action, attempt, reply, expected=0):
        result = subprocess.run([EXECUTABLE, "--decode", str(action), str(attempt)],
                                input=reply, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, expected, result.stdout + result.stderr)

    def exchange(self, f, action, attempt=3):
        command = subprocess.run([EXECUTABLE, "--command", str(action), str(attempt)],
                                 capture_output=True, timeout=10, check=True).stdout
        request = AuthenticatedTargetRequest("ab" * 32, command, time.monotonic_ns() + 5_000_000_000)
        reply = dispatch_target(request, f.registry, f.journal, f.root, f.connection,
                                startup_config=getattr(f, "template", None))
        self.decode(action, attempt, reply)
        return reply

    def test_c_identity_prepare_and_drain_round_trip(self):
        f = self.fixture()
        before = f.journal.sequence
        self.exchange(f, 0)
        self.assertEqual(f.journal.sequence, before)
        prepared = self.exchange(f, 1)
        self.assertEqual(f.journal.denied()[0].identity, f.identity)
        completed = self.exchange(f, 2)
        self.assertEqual(f.events, ["teardown"])
        self.assertTrue(f.journal.completion_recorded(f.identity.operation_id, f.identity.target_boot_id))
        self.decode(2, 3, prepared, expected=1)
        self.decode(1, 3, completed, expected=1)
        changed = json.loads(completed)
        changed["operation_id"] = "34" * 16
        self.decode(2, 3, _canonical(changed), expected=1)
        changed = json.loads(completed)
        changed["status"] = "PROVEN"
        self.decode(2, 3, _canonical(changed), expected=1)

    def test_c_restore_running_and_compensating_revoke_round_trip(self):
        f = self.fixture(rejoin=True)
        self.exchange(f, 3)
        self.assertEqual(f.journal.denied()[0].rejoin.phase, 3)
        f.connection.domain.active, f.connection.domain.status = 1, (1, 1)
        f.connection.domain.ID = lambda: 12
        running = self.exchange(f, 4)
        self.decode(4, 4, running, expected=1)
        self.exchange(f, 5, attempt=4)
        self.assertEqual(f.journal.denied()[0].rejoin.phase, 4)
        f.connection.domain.active, f.connection.domain.status = 0, (5, 2)
        completed = self.exchange(f, 6, attempt=4)
        self.assertEqual(f.tpg.node_acls, [])
        self.assertEqual(json.loads(completed)["route_phases"], [3] * 4)


if __name__ == "__main__":
    if len(sys.argv) != 2 or not Path(sys.argv[1]).is_absolute():
        raise SystemExit("provide the real C target-reply test executable")
    EXECUTABLE = sys.argv.pop()
    unittest.main()
