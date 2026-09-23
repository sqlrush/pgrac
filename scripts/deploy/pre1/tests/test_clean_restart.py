"""Clean-restart guards must not reinterpret changed state as clean startup.
Author: SqlRush <sqlrush@gmail.com>
"""
import copy
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from common import PreflightError
import clean_restart
from clean_restart import require_same_cold, require_clear_votes


class CleanRestartTests(unittest.TestCase):
    def capture(self):
        return dict(node_id=0,binary_sha256='a'*64,identity=dict(boot_id='one'),
                    control=dict(state='shut down',system_identifier='123'),
                    processes=[],pidfile=None,pgdata_sha256='b'*64,
                    shared_sha256='c'*64,config_sha256='d'*64,tool_sha256='e'*64)

    def test_cold_control_content_binary_and_identity_must_not_change(self):
        old=self.capture()
        require_same_cold(old,copy.deepcopy(old),include_shared=True)
        for field,value in (('binary_sha256','f'*64),('identity',dict(boot_id='two')),
                            ('control',dict(state='in production',system_identifier='123')),
                            ('processes',[12]),('pidfile',dict(pid=12)),('pgdata_sha256','f'*64),
                            ('shared_sha256','f'*64),('config_sha256','f'*64),('tool_sha256','f'*64)):
            changed=copy.deepcopy(old);changed[field]=value
            with self.subTest(field=field),self.assertRaises(PreflightError):
                require_same_cold(old,changed,include_shared=True)

    def test_local_recheck_after_first_member_start_does_not_claim_shared_quiescence(self):
        old=self.capture();now=copy.deepcopy(old);now['shared_sha256']=None
        require_same_cold(old,now,include_shared=False)
        now['pgdata_sha256']='f'*64
        with self.assertRaises(PreflightError):
            require_same_cold(old,now,include_shared=False)

    def test_vote_clear_requires_exact_all_disks_slots_and_no_reformatted_history(self):
        votes=[dict(index=i,wwid='vote%d'%i,capacity=16777216,logical_sector=512,
                    read_bytes=525824,direct=True,read_only=True,strict_authority=True,
                    crc32c=100+i,members=[dict(node_id=n,valid=True,flags=0,incarnation=n+20,
                                               epoch=1,generation=30) for n in range(128)]) for i in range(3)]
        require_clear_votes(votes,copy.deepcopy(votes))
        for kind in ('missing','alive','crc','incarnation','nonstrict','duplicate'):
            changed=copy.deepcopy(votes)
            if kind=='missing':changed.pop()
            elif kind=='alive':changed[1]['members'][2]['flags']=1
            elif kind=='crc':changed[2]['crc32c']+=1
            elif kind=='incarnation':changed[0]['members'][2]['incarnation']=0
            elif kind=='nonstrict':changed[0]['strict_authority']=False
            else:changed[1]=copy.deepcopy(changed[0])
            with self.subTest(kind=kind),self.assertRaises(PreflightError):
                require_clear_votes(votes,changed)

    def patch_fixture(self):
        source=dict(binary_sha256='a'*64,shared_root='/shared/data',dataset_id='original')
        base=dict(kind='pre1-clean-restart-prepared',status='PASS',state='CLEAN_RESTART_PREPARED',
                  binary_sha256='a'*64,source=source,config=dict(shared_root='/shared/data'),
                  system_identifier='123',closure=dict(clean_stop_proven=True,state='CLEAN_STOPPED'),
                  captures=[dict(self.capture(),node_id=n) for n in range(4)])
        declaration=dict(kind='pre1-format-compatible-cold-patch',status='APPROVED',
                         from_binary_sha256='a'*64,to_binary_sha256='f'*64,
                         from_source_commit='1'*40,to_source_commit='2'*40,
                         persistent_format_unchanged=True,wire_unchanged=True,
                         config_unchanged=True,no_mixed_version=True)
        return base,declaration

    def test_explicit_cold_patch_keeps_seed_provenance_and_only_rebinds_binary(self):
        base,declaration=self.patch_fixture();unchanged=copy.deepcopy(base)
        source=clean_restart.rebound_source(base,declaration)
        self.assertEqual(base,unchanged)
        self.assertEqual(source['binary_sha256'],'f'*64)
        self.assertEqual(source['kind'],'pre1-cold-rebound-source')
        self.assertEqual(source['previous_source'],base['source'])
        self.assertEqual(source['compatibility'],declaration)
        self.assertEqual(source['shared_root'],base['source']['shared_root'])

    def test_cold_patch_rejects_unproved_compatibility_or_missing_member(self):
        for field,value in (('status','PROPOSED'),('from_binary_sha256','c'*64),
                            ('to_binary_sha256','a'*64),('to_binary_sha256','invalid'),
                            ('from_source_commit','HEAD'),('to_source_commit','HEAD'),
                            ('persistent_format_unchanged',False),('wire_unchanged',False),
                            ('config_unchanged',False),('no_mixed_version',False)):
            base,declaration=self.patch_fixture();declaration[field]=value
            with self.subTest(field=field,value=value),self.assertRaises(PreflightError):
                clean_restart.rebound_source(base,declaration)
        for kind in ('missing','duplicate','wrong_binary','unclean','wrong_source'):
            base,declaration=self.patch_fixture()
            if kind=='missing':base['captures'].pop()
            elif kind=='duplicate':base['captures'][1]=copy.deepcopy(base['captures'][0])
            elif kind=='wrong_binary':base['captures'][2]['binary_sha256']='c'*64
            elif kind=='unclean':base['closure']['clean_stop_proven']=False
            else:base['source']['binary_sha256']='c'*64
            with self.subTest(kind=kind),self.assertRaises(PreflightError):
                clean_restart.rebound_source(base,declaration)

    def test_cold_patch_checks_actual_cold_state_not_just_compatibility_claim(self):
        old=self.capture();new=dict(old,binary_sha256='f'*64)
        clean_restart.require_cold_patch(old,new,'a'*64,'f'*64)
        for field,value in (('binary_sha256','c'*64),('identity',dict(boot_id='two')),
                            ('control',dict(state='in production',system_identifier='123')),
                            ('processes',[12]),('pidfile',dict(pid=12)),('pgdata_sha256','f'*64),
                            ('shared_sha256','f'*64),('config_sha256','f'*64),('tool_sha256','f'*64)):
            changed=copy.deepcopy(new);changed[field]=value
            with self.subTest(field=field),self.assertRaises(PreflightError):
                clean_restart.require_cold_patch(old,changed,'a'*64,'f'*64)

    def test_rebind_rechecks_original_closure_and_all_four_physical_captures(self):
        from test_clean_closure import CleanClosureTests
        fixture=CleanClosureTests();fixture.setUp();self.addCleanup(fixture.doCleanups)
        base,declaration=self.patch_fixture()
        declaration['from_binary_sha256']=fixture.binary
        base['binary_sha256']=fixture.binary
        base['source']['binary_sha256']=fixture.binary
        config=dict(base['config'],nodes=fixture.nodes)
        for vote in fixture.after_votes:vote['crc32c']=100+vote['index']
        before=dict(starts=fixture.starts,states=fixture.states,votes=fixture.before_votes)
        after=dict(status='PASS',source_binary_sha256=fixture.binary,
                   observations=fixture.observations,logs=fixture.logs,after_votes=fixture.after_votes,
                   post_stop_not_before_ns=fixture.post_stop_not_before_ns)
        refs={name:dict(path='/evidence/'+name,sha256='d'*64) for name in
              ('base_prepared','compatibility','rebound_source','config','source','closed_before','closed_after')}
        old=dict(config=refs['config'],source=refs['source'],closed_before=refs['closed_before'],
                 closed_after=refs['closed_after'],guest_config=refs['config'],observer={'path':'/observer'})
        base.update(config=config,request=old,closure=fixture.run_closure(),
                    system_identifier='7654321000123456789')
        for capture in base['captures']:
            capture.update(binary_sha256=fixture.binary,votes=copy.deepcopy(fixture.after_votes))
        source=clean_restart.rebound_source(base,declaration)
        records=dict(base_prepared=base,compatibility=declaration,rebound_source=source,
                     config=config,source=base['source'],closed_before=before,closed_after=after)
        request={key:refs[key] for key in ('base_prepared','compatibility','rebound_source')}
        request['guest_tool_root']='/opt/pgrac-pre1-restart-tools/pre1'
        actual=[dict(c,binary_sha256='f'*64) for c in base['captures']]
        def read(ref):return records[Path(ref['path']).name]
        def observe(node,program,value):
            self.assertEqual(value['source'],source)
            self.assertEqual(value['binary_sha256'],'f'*64)
            return dict(rc=0,timed_out=False,truncated=False,stdout=json.dumps(actual[node['node_id']]))
        with patch.object(clean_restart.runtime,'read_bound',side_effect=read), \
             patch.object(clean_restart.bootstrap_config,'render'), \
             patch.object(clean_restart,'tool_digest',return_value='e'*64), \
             patch.object(clean_restart.remote,'run_ssh',side_effect=observe) as calls:
            result=clean_restart.rebind(request)
            self.assertEqual(calls.call_count,4)
            self.assertFalse(result['qualification_inherited'])
            self.assertEqual(result['source'],source)
            self.assertEqual(result['request']['source'],refs['rebound_source'])
            self.assertEqual(result['captures'],actual)
            actual[2]['binary_sha256']=fixture.binary
            with self.assertRaises(PreflightError):clean_restart.rebind(request)
            actual[2]['binary_sha256']='f'*64
            actual[3]['pgdata_sha256']='0'*64
            with self.assertRaises(PreflightError):clean_restart.rebind(request)
            calls.reset_mock()
            after['logs'][0]['raw']+='ERROR: dirty shutdown\n'
            with self.assertRaises(PreflightError):clean_restart.rebind(request)
            calls.assert_not_called()


if __name__=='__main__':
    unittest.main()
