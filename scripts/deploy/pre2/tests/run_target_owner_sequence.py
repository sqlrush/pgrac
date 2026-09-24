"""Consume real C coordinator callback identities in the production journal.

Author: SqlRush <sqlrush@gmail.com>
Usage: python3 run_target_owner_sequence.py /absolute/test_pgrac_fenced_rejoin
No native power/storage operations; the C provider fixture remains explicit.
"""

from dataclasses import replace
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from target_journal import DrainIdentity, RejoinIntent, TargetJournal, TargetJournalError
from target_command import decode_command


class RealOwnerSequenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with (tempfile.NamedTemporaryFile(prefix="pgrac-owner-trace-") as trace,
              tempfile.NamedTemporaryFile(prefix="pgrac-owner-command-") as commands):
            result = subprocess.run([EXECUTABLE, str(trace.fileno()), str(commands.fileno())],
                                    pass_fds=(trace.fileno(), commands.fileno()),
                                    capture_output=True, text=True, timeout=30)
            if result.returncode != 0 or "# All 10 tests passed." not in result.stdout:
                raise AssertionError(result.stdout + result.stderr)
            trace.seek(0)
            cls.records = []
            for raw in trace.read().decode("ascii").splitlines():
                fields = raw.split()
                if len(fields) != 14:
                    raise AssertionError("incomplete C callback trace")
                call, op, attempt, boot, guest, generation, digest, sysid, node, old, candidate, gate, opcode, state = fields
                identity = DrainIdentity(op, int(attempt), boot, "cc" * 16, guest,
                                         int(generation), digest, ("11" * 32, "22" * 32))
                intent = RejoinIntent(int(sysid), int(node), int(old), int(candidate), gate)
                cls.records.append((call, identity, intent, int(opcode), int(state)))
            commands.seek(0)
            cls.commands = [decode_command(raw) for raw in commands.read().splitlines(keepends=True)]

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="pgrac-real-owner-")
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name).resolve()
        self.journal = TargetJournal(str(self.directory), "ab" * 32, initialize=True, owner_uid=os.geteuid())
        self.addCleanup(self.journal.close)

    def drain(self, identity):
        for n in range(2):
            for phase in (1, 2, 3):
                self.journal.advance(identity.operation_id, identity.target_boot_id, n, phase)

    def run_sequence(self, operation):
        rows = [row for row in self.records if row[1].operation_id == operation]
        self.assertGreaterEqual(len(rows), 4)
        for call, identity, intent, opcode, state in rows:
            if opcode == 1:  # Actual ADMIN readback; no rejoin intent is armed yet.
                self.journal.deny(identity)
                self.drain(identity)
            elif call == "on":
                self.assertEqual(opcode, 5)
                self.journal.authorize_rejoin(identity, intent)
                self.journal.advance_rejoin(identity, intent, 2)
                self.journal.advance_rejoin(identity, intent, 3)
            elif call == "off":
                self.assertIn(opcode, (5, 7))
                self.assertEqual(state, 1)  # PGRAC_FENCED_TARGET_OFF
                self.journal.revoke_rejoin(identity, intent)
            elif opcode == 7:
                self.journal.refresh_rejoin(identity, intent)
            else:
                self.assertEqual(self.journal.denied()[0].identity, identity)
        return rows

    def test_actual_admin_authorize_refresh_sequence(self):
        rows = self.run_sequence("a6" * 16)
        self.assertEqual([(r[0], r[1].attempt, r[3]) for r in rows],
                         [("readback", 1, 1), ("on", 2, 5), ("readback", 2, 5), ("readback", 3, 7)])
        self.assertTrue(self.journal.denied()[0].rejoin.refresh_started)
        self.assertFalse(self.journal.completion_recorded(rows[-1][1].operation_id, "cc" * 16))
        with self.assertRaises(TargetJournalError):
            self.journal.authorize_rejoin(rows[1][1], rows[1][2])

    def test_actual_daemon_restart_and_repeated_compensating_off(self):
        rows = self.run_sequence("b3" * 16)
        off = [r for r in rows if r[0] == "off"]
        self.assertEqual([r[1].attempt for r in off], [3, 4])
        self.assertEqual({r[1].daemon_boot_id for r in off}, {"82" * 16})
        self.assertNotEqual(rows[0][1].daemon_boot_id, off[0][1].daemon_boot_id)
        self.assertEqual(self.journal.denied()[0].rejoin.phase, 4)
        self.assertEqual(self.journal.denied()[0].phases, (0, 0))
        self.drain(off[-1][1])
        self.assertTrue(self.journal.completion_recorded(off[-1][1].operation_id, "cc" * 16))
        with self.assertRaises(TargetJournalError):
            self.journal.revoke_rejoin(off[0][1], off[0][2])

    def test_next_fence_of_the_real_candidate_cannot_reuse_old_isolation(self):
        rows = self.run_sequence("a6" * 16)
        identity, intent = rows[-1][1:3]
        successor = replace(identity, operation_id="fe" * 16, attempt=1, daemon_boot_id="dd" * 16)
        self.journal.supersede_rejoin(successor, intent.candidate_incarnation)
        self.assertEqual(self.journal.denied()[0].phases, (0, 0))
        self.assertFalse(self.journal.completion_recorded(successor.operation_id, "cc" * 16))

    def test_actual_callbacks_encode_exact_canonical_target_commands(self):
        self.assertEqual(len(self.commands), len(self.records))
        for command, (call, identity, intent, opcode, state) in zip(self.commands, self.records):
            self.assertEqual(command["operation_id"], identity.operation_id)
            self.assertEqual(command["attempt"], identity.attempt)
            self.assertEqual(command["daemon_boot_id"], identity.daemon_boot_id)
            self.assertEqual(command["mapping_generation"], identity.mapping_generation)
            self.assertEqual(command["protected_set_digest"], identity.protected_set_digest)
            self.assertEqual(command["system_identifier"], intent.system_identifier)
            self.assertEqual(command["node_id"], intent.old_node_id)
            self.assertEqual(command["target_boot_id"], "cc" * 16)
            self.assertEqual(command["challenge"], "dd" * 16)
            expected = "rejoin_prepare_revoke" if call == "off" else (
                "rejoin_restore" if call == "on" else "complete_off" if opcode == 1 else (
                    "rejoin_complete_off" if state == 1 else "rejoin_running"))
            self.assertEqual(command["action"], expected)
            if opcode != 1:
                self.assertEqual(command["old_incarnation"], intent.old_incarnation)
                self.assertEqual(command["candidate_incarnation"], intent.candidate_incarnation)
                self.assertEqual(command["rejoin_gate_digest"], intent.rejoin_gate_digest)
            if expected == "rejoin_running":
                self.assertEqual(command["owner_phase"], "authorize" if opcode == 5 else "refresh")


if __name__ == "__main__":
    if len(sys.argv) != 2 or not Path(sys.argv[1]).is_absolute():
        raise SystemExit("provide the real C test executable")
    EXECUTABLE = sys.argv.pop()
    unittest.main()
