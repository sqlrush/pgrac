"""Author: SqlRush. Exact-identity client-tail attribution tests."""
import pathlib
import sys
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from join_update_trace import join_and_analyze


class JoinTests(unittest.TestCase):
    def test_tail_is_chosen_before_join_not_from_fast_matched_subset(self):
        clients = [dict(node_id=0, pid=11, backend_sequence=i, client_total_ns=i * 100,
                        status=0, measurement=True) for i in range(1, 6)]
        servers = [dict(op_id=i, node_id=0, pid=11, backend_sequence=i, total_ns=i * 50,
                        status='ok', phases={}, exclusive_phases={'cr': i * 40},
                        unattributed_ns=i * 10) for i in range(1, 5)]
        report = join_and_analyze(clients, servers)
        self.assertEqual(report['client_p80_ns'], 400)
        self.assertEqual(report['client_tail_count'], 1)
        self.assertEqual(report['client_tail_unmatched'], 1)
        self.assertEqual(report['client_tail_server_attribution']['completed_ops'], 0)
        self.assertFalse(report['complete_identity_coverage'])

    def test_pid_and_node_are_both_part_of_identity(self):
        clients = [dict(node_id=0, pid=11, backend_sequence=1, client_total_ns=100,
                        status=0, measurement=True)]
        server = dict(op_id=1, node_id=1, pid=11, backend_sequence=1, total_ns=40,
                      status='ok', phases={})
        self.assertEqual(join_and_analyze(clients, [server])['unmatched_clients'], 1)

    def test_duplicate_identity_is_rejected(self):
        server = dict(node_id=0, pid=11, backend_sequence=1)
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            join_and_analyze([], [server, server])

    def test_measurement_and_tail_keep_separate_time_budgets(self):
        clients = [dict(node_id=0, pid=11, backend_sequence=i,
                        client_start_ns=0, client_update_end_ns=i*80,
                        client_end_ns=i*100, client_total_ns=i*100,
                        status=0, measurement=True) for i in range(1, 6)]
        servers = [dict(op_id=i, node_id=0, pid=11, backend_sequence=i,
                        total_ns=i*50, status='ok', phases={}, affected_rows=1,
                        exclusive_phases={'cr': i*40}, unattributed_ns=i*10)
                   for i in range(1, 6)]
        report = join_and_analyze(clients, servers)
        self.assertEqual(report['measurement_timing']['client_wall_ns'], 1500)
        self.assertEqual(report['measurement_timing']['client_commit_ns'], 300)
        self.assertEqual(report['measurement_timing']['outside_executor_ns'], 450)
        self.assertEqual(report['client_tail_timing']['server_executor_ns'], 250)
        self.assertEqual(report['measurement_server_attribution']['completed_ops'], 5)
        self.assertEqual(report['client_tail_server_attribution']['completed_ops'], 1)

    def test_identity_time_mismatch_is_not_accepted_as_attribution(self):
        client = dict(node_id=0, pid=11, backend_sequence=1,
                      client_start_ns=0, client_update_end_ns=80,
                      client_end_ns=100, client_total_ns=100,
                      status=0, measurement=True)
        server = dict(op_id=1, node_id=0, pid=11, backend_sequence=1,
                      total_ns=90, status='ok', phases={}, affected_rows=1)
        with self.assertRaisesRegex(ValueError, 'exceeds client UPDATE'):
            join_and_analyze([client], [server])


if __name__ == '__main__':
    unittest.main()
