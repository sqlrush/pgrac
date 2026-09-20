"""Scoped teardown call-order checks. Author: SqlRush <sqlrush@gmail.com>"""

import importlib.machinery
import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
loader = importlib.machinery.SourceFileLoader("demo_controller", str(Path(__file__).resolve().parents[1] / "pgrac-demo"))
spec = importlib.util.spec_from_loader(loader.name, loader)
controller = importlib.util.module_from_spec(spec)
loader.exec_module(controller)


class Teardown(unittest.TestCase):
    def deployment(self, root):
        demo = object.__new__(controller.Demo)
        demo.args = Mock(name="demo")
        demo.args.name = "demo"
        demo.state = {"phase": "READY", "fixture": {"nodes": [
            {"id": i, "pgdata": "/demo/node%d" % i} for i in range(4)]},
            "loops": [{"device": "/dev/loop7", "permissions": [0, 6, 0o660]}]}
        demo.log_offsets = Mock(return_value=[0]*4)
        demo.running = Mock(side_effect=[[0, 1, 2, 3], []])
        demo.clean_controls = Mock(return_value=[True]*4)
        demo.new_logs = Mock(return_value=["cluster normal-stop: protocol closed after shutdown checkpoint\n"
                                          "database system is shut down\n"]*4)
        demo.host_path = lambda _path: root / "absent.pid"
        demo.save = Mock()
        return demo

    def test_permissions_restore_precedes_detach_after_clean_shutdown(self):
        with tempfile.TemporaryDirectory() as temporary:
            demo = self.deployment(Path(temporary))
            events = []
            demo.check_loop = Mock(side_effect=lambda record: events.append("identity"))
            with patch.object(controller, "run", side_effect=lambda args: events.append(args)), \
                    patch.object(controller.os, "chown", side_effect=lambda *args: events.append(("chown", args))), \
                    patch.object(controller.os, "chmod", side_effect=lambda *args: events.append(("chmod", args))):
                demo.stop()
            self.assertEqual(events[-4:], ["identity", ("chown", ("/dev/loop7", 0, 6)),
                                           ("chmod", ("/dev/loop7", 0o660)),
                                           ["losetup", "--detach", "/dev/loop7"]])
            self.assertEqual(demo.state["phase"], "CLEAN")

    def test_identity_drift_prevents_permission_change_and_detach(self):
        with tempfile.TemporaryDirectory() as temporary:
            demo = self.deployment(Path(temporary))
            demo.check_loop = Mock(side_effect=ValueError("wrong backing"))
            with patch.object(controller, "run") as run, patch.object(controller.os, "chown") as chown:
                with self.assertRaises(ValueError):
                    demo.stop()
                chown.assert_not_called()
                self.assertFalse(any(call.args[0][0] == "losetup" for call in run.call_args_list))
            self.assertEqual(demo.state["phase"], "STOPPING")
