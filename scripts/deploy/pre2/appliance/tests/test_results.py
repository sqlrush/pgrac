"""Offline result checks; no VM or database is started."""
import importlib.util
from pathlib import Path
import unittest
import tempfile
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from quorum import validate_work_directory

MODULE = Path(__file__).resolve().parents[1] / 'results.py'
spec = importlib.util.spec_from_file_location('results', MODULE)
results = importlib.util.module_from_spec(spec)
spec.loader.exec_module(results)


class Results(unittest.TestCase):
    def test_namespace_sources_survive_service_mounts(self):
        for path in ('/run/a', '/etc/corosync/a', '/var/lib/corosync/a', '/dev/shm/a'):
            with self.assertRaises(ValueError):
                validate_work_directory(path)
        self.assertEqual(validate_work_directory('/var/lib/pgrac-single/services'),
                         Path('/var/lib/pgrac-single/services').resolve())

    def test_start_waits_for_pid_and_log_files(self):
        with tempfile.TemporaryDirectory() as d:
            data, log = Path(d), Path(d)/'postgres.log'
            self.assertFalse(results.startup_ready(data, log, 0))
            log.write_text('LOG: starting\n')
            self.assertFalse(results.startup_ready(data, log, 0))
            (data/'postmaster.pid').write_text(f'123\n{data}\n1\n5432\nsocket\naddr\n0\nstarting\n')
            self.assertFalse(results.startup_ready(data, log, 0))
            (data/'postmaster.pid').write_text(f'123\n{data}\n1\n5432\nsocket\naddr\n0\nready\n')
            self.assertTrue(results.startup_ready(data, log, 0))
            log.write_text('FATAL: startup refused\n')
            with self.assertRaises(RuntimeError):
                results.startup_ready(data, log, 0)

    def sample(self, failed=0, rc=0):
        return dict(rc=rc, started=10., ended=70.5, stdout=(
            'number of transactions actually processed: 600\n'
            f'number of failed transactions: {failed} (0.000%)\n'
            'tps = 10.000000 (without initial connection time)\n'))

    def test_common_window_and_sum_are_distinct(self):
        value = results.score_point([self.sample() for _ in range(4)], [])
        self.assertTrue(value['valid'])
        self.assertEqual(value['transactions'], 2400)
        self.assertEqual(value['sum_pgbench_tps'], 40.)
        self.assertAlmostEqual(value['common_window_tps'], 2400/60.5)

    def test_errors_and_partial_samples_cannot_score(self):
        for samples, events in [
            ([self.sample(failed=1)]+[self.sample()]*3, []),
            ([self.sample(rc=1)]+[self.sample()]*3, []),
            ([self.sample()]*3, []),
            ([self.sample()]*4, ['ERROR: refusal']),
            ([dict(rc=0, started=10, ended=70, stdout='')]*4, []),
        ]:
            self.assertFalse(results.score_point(samples, events)['valid'])

    def test_shutdown_requires_normal_records_and_stopped_processes(self):
        good = dict(rc=0, stopped=True, log='LOG: database system is shut down\n')
        self.assertTrue(results.shutdown_ok([good]*4))
        for change in [dict(rc=1), dict(stopped=False), dict(log=''),
                       dict(log=good['log']+'PANIC: failed checkpoint\n')]:
            self.assertFalse(results.shutdown_ok([dict(good, **change)]+[good]*3))
        self.assertFalse(results.shutdown_ok([good]*3))


if __name__ == '__main__':
    unittest.main()
