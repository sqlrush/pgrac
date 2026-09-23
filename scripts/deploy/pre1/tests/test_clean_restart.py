"""Clean-restart guards must not reinterpret changed state as clean startup.
Author: SqlRush <sqlrush@gmail.com>
"""
import copy
from pathlib import Path
import sys
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from common import PreflightError
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


if __name__=='__main__':
    unittest.main()
