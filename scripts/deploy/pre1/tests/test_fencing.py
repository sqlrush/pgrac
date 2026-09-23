"""Exact storage-fencing evidence tests; no power operations.

Author: SqlRush <sqlrush@gmail.com>
"""

import importlib.util
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import PreflightError


class FencingTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(importlib.util.find_spec("fencing"), "fence evidence verifier is missing")
        import fencing
        self.fencing = fencing
        self.mapping = {f"node{n}": f"00000000-0000-4000-8000-{n + 1:012d}" for n in range(4)}
        host_map = ";".join(f"{name}:{uuid}" for name, uuid in self.mapping.items())
        self.xml = ('<cib><configuration><crm_config><cluster_property_set>'
                    '<nvpair name="stonith-enabled" value="true"/>'
                    '<nvpair name="stonith-action" value="off"/>'
                    '<nvpair name="no-quorum-policy" value="freeze"/>'
                    '<nvpair name="maintenance-mode" value="false"/>'
                    '</cluster_property_set></crm_config><resources>'
                    '<primitive id="fence" class="stonith" type="fence_virsh">'
                    '<instance_attributes><nvpair name="pcmk_host_check" value="static-list"/>'
                    '<nvpair name="pcmk_reboot_action" value="off"/>'
                    '<nvpair name="pcmk_host_list" value="node0 node1 node2 node3"/>'
                    f'<nvpair name="pcmk_host_map" value="{host_map}"/>'
                    '</instance_attributes></primitive></resources></configuration></cib>')

    def test_exact_unconditional_mapping_is_required(self):
        actual = self.fencing.verify_configuration(self.xml, "fence", self.mapping)
        self.assertEqual(actual, self.mapping)
        for bad in (self.xml.replace('value="off"', 'value="reboot"'),
                    self.xml.replace("node0:", "unknown:"),
                    self.xml.replace("static-list", "dynamic-list"),
                    self.xml.replace('<instance_attributes>', '<instance_attributes><rule/>'),
                    self.xml.replace('</instance_attributes>',
                                     '<nvpair name="missing_as_off" value="1"/></instance_attributes>')):
            with self.subTest(xml=bad):
                with self.assertRaises(PreflightError):
                    self.fencing.verify_configuration(bad, "fence", self.mapping)

    def test_duplicate_mapping_and_unknown_resource_are_refused(self):
        bad = self.xml.replace("node1:", "node0:")
        with self.assertRaises(PreflightError):
            self.fencing.verify_configuration(bad, "fence", self.mapping)
        with self.assertRaises(PreflightError):
            self.fencing.verify_configuration(self.xml, "absent", self.mapping)

    def test_dlm_reboot_requests_must_be_mapped_to_off(self):
        for bad in (self.xml.replace('<nvpair name="pcmk_reboot_action" value="off"/>', ''),
                    self.xml.replace('name="pcmk_reboot_action" value="off"',
                                     'name="pcmk_reboot_action" value="reboot"'),
                    self.xml.replace('name="pcmk_reboot_action" value="off"',
                                     'name="pcmk_reboot_action" value="on"')):
            with self.subTest(xml=bad), self.assertRaises(PreflightError):
                self.fencing.verify_configuration(bad, "fence", self.mapping)

    def test_action_and_target_overrides_cannot_bypass_exact_off_mapping(self):
        for parameters in ({"pcmk_off_action": "reboot"},
                           {"pcmk_host_argument": "none", "port": "other-domain"},
                           {"pcmk_host_argument": "plug"},
                           {"port": "other-domain"}, {"plug": "other-domain"},
                           {"action": "reboot"}):
            extra = "".join(f'<nvpair name="{k}" value="{v}"/>' for k, v in parameters.items())
            xml = self.xml.replace('</instance_attributes>', extra + '</instance_attributes>')
            with self.subTest(parameters=parameters), self.assertRaises(PreflightError):
                self.fencing.verify_configuration(xml, "fence", self.mapping)
        safe = self.xml.replace('</instance_attributes>',
                                '<nvpair name="pcmk_off_action" value="off"/>'
                                '<nvpair name="pcmk_host_argument" value="port"/>'
                                '</instance_attributes>')
        self.assertEqual(self.fencing.verify_configuration(safe, "fence", self.mapping), self.mapping)

    def test_unknown_vm_or_failed_status_cannot_be_off(self):
        self.assertEqual(self.fencing.domain_state(0, "shut off (destroyed)\n"), "OFF")
        self.assertEqual(self.fencing.domain_state(0, "running (booted)\n"), "ON")
        for rc, output in ((1, "shut off (destroyed)"), (0, ""), (0, "unknown"),
                           (0, "shut off\nrunning"), (255, "not found")):
            with self.subTest(rc=rc, output=output):
                with self.assertRaises(PreflightError):
                    self.fencing.domain_state(rc, output)

    def test_scratch_fencing_requires_all_four_database_empty_guests(self):
        observations = [{"status": "PASS", "node_id": n, "observation": {
            "node_id": n, "pgdata_state": "EMPTY", "control": None,
            "pidfile": None, "processes": []}} for n in range(4)]
        self.fencing.require_database_empty(observations)
        for field, value in (("pgdata_state", "INITIALIZED"), ("pidfile", {"pid": 99}),
                             ("processes", [{"pid": 99}])):
            changed = json.loads(json.dumps(observations))
            changed[0]["observation"][field] = value
            with self.subTest(field=field), self.assertRaises(PreflightError):
                self.fencing.require_database_empty(changed)
        with self.assertRaises(PreflightError):
            self.fencing.require_database_empty(observations[:3])

    def event(self, syscall, result, status="OK", case="fence-writer"):
        return {"case": case, "syscall": syscall, "result": result, "errno": 0,
                "offset": 0, "length": 8192, "crc32": 1234, "token": "a" * 32,
                "node": 0, "mono_ns": 1000 + result, "status": status}

    def test_writer_requires_synchronized_progress_not_natural_timeout(self):
        events = [self.event("fence-progress", 1, "READY"), self.event("fence-progress", 2, "SYNCED")]
        raw = "\n".join(json.dumps(event) for event in events)
        self.assertEqual(self.fencing.writer_progress(raw, "a" * 32, 0), 2)
        exhausted = self.event("witness-budget-exhausted", -1, "INCOMPLETE")
        with self.assertRaises(PreflightError):
            self.fencing.writer_progress(raw + "\n" + json.dumps(exhausted), "a" * 32, 0)
        with self.assertRaises(PreflightError):
            self.fencing.writer_progress(raw, "b" * 32, 0)
        with self.assertRaises(PreflightError):
            self.fencing.writer_progress(raw, "a" * 32, 1)

    def test_observation_requires_whole_payload_and_complete_marker(self):
        events = [self.event("verify", 0, case="observe"),
                  self.event("observed-version", 42, case="observe"),
                  self.event("observed-writer", 0, case="observe"),
                  self.event("complete", 0, "PASS", "observe")]
        raw = "\n".join(json.dumps(event) for event in events)
        self.assertEqual(self.fencing.observed_payload(raw, "a" * 32, 0), (42, 0, 1234))
        for subset in (events[1:], events[:-1]):
            with self.assertRaises(PreflightError):
                self.fencing.observed_payload("\n".join(json.dumps(event) for event in subset), "a" * 32, 0)


if __name__ == "__main__":
    unittest.main()
