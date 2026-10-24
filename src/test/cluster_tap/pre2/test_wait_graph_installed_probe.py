"""Acceptance of an independently identified real HW dependency."""
import copy
import unittest
import wait_graph_capture as graph
import wait_graph_installed_probe as probe


class ProbeTests(unittest.TestCase):
    def setUp(self):
        self.resid = [5,16389,1663,0,242,1]
        holder = dict(node=0,pid=101,procno=2,backend_id=3,backend_start=4,proc_generation=11,
                      grants=[dict(resid=self.resid,holder=[0,2,7,40],mode=7)])
        waiter = dict(node=1,pid=202,procno=3,backend_id=4,backend_start=5,proc_generation=12,
                      enqueue=dict(resid=self.resid,holder=[1,3,7,0],request=50,epoch=7,wait_seq=60,mode=7))
        snapshots = [dict(node=n,epoch_begin=7,epoch_end=7,owners=owners)
                     for n,owners in enumerate(([holder],[waiter],[],[]))]
        resource = dict(kind='enqueue',node=2,resid=self.resid,status=0,stable=True,
                        route_known=True,master=2,master_generation=1,shard_phase=0,epoch=7,
                        members=[dict(kind=0,id=[0,2,7,40],mode=7),
                                 dict(kind=1,id=[1,3,7,50],wait_seq=60,mode=7)])
        self.evidence = graph.build_graph(snapshots,[resource])

    def test_real_graph_join_accepts_only_expected_native_hw_dependency(self):
        edge = probe.require_hw_edge(self.evidence,101,202,self.resid)
        self.assertEqual(edge['quality'],'exact_ges_holder')
        self.assertEqual(edge['resource']['resid'],self.resid)

    def test_wrong_observed_pid_or_resource_is_not_a_qualified_probe(self):
        for holder,waiter,resid in ((102,202,self.resid),(101,203,self.resid),
                                   (101,202,[5,16390,1663,0,242,1])):
            with self.assertRaises(ValueError):
                probe.require_hw_edge(self.evidence,holder,waiter,resid)

    def test_candidate_incomplete_and_ambiguous_incarnation_are_rejected(self):
        mutations = [lambda g:g.update(complete=False),
                     lambda g:g['edges'][0].update(quality='candidate_ges_process'),
                     lambda g:g['vertices'].append(dict(g['vertices'][0],proc_generation=99))]
        for mutate in mutations:
            evidence=copy.deepcopy(self.evidence);mutate(evidence)
            with self.assertRaises(ValueError):probe.require_hw_edge(evidence,101,202,self.resid)

    def test_missing_or_duplicate_matching_edge_is_rejected(self):
        for edges in ([],self.evidence['edges']*2):
            evidence=dict(self.evidence,edges=edges)
            with self.assertRaises(ValueError):probe.require_hw_edge(evidence,101,202,self.resid)


if __name__ == '__main__':
    unittest.main()
