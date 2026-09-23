"""Author: SqlRush <sqlrush@gmail.com>. User checks cannot certify deployment."""
import copy
import importlib.util
import os
import json
import shutil
from types import SimpleNamespace
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from common import PreflightError


class VerifyTests(unittest.TestCase):
    def module(self):
        self.assertIsNotNone(importlib.util.find_spec('verify'))
        import verify
        return verify

    def test_fixed_read_only_sql_rejects_identifier_injection(self):
        module=self.module()
        sql=module.copy_sql('public.demo_account','id','value')
        self.assertEqual(sql,'COPY (SELECT "id","value" FROM "public"."demo_account" ORDER BY "id") TO STDOUT WITH (FORMAT csv)')
        for relation in ('public.demo;DROP TABLE x','demo','public."demo"','public.demo--'):
            with self.assertRaises(PreflightError):module.copy_sql(relation,'id','value')
        with self.assertRaises(PreflightError):module.copy_sql('public.demo','id);DELETE','value')

    def test_complete_bytes_and_expected_keys_not_only_counts_or_hashes(self):
        module=self.module()
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);a=root/'a';b=root/'b'
            a.write_bytes(b'1,100\n2,200\n');b.write_bytes(a.read_bytes())
            module.validate_expected(a,2);module.require_same_files(a,b)
            for raw in (b'1,100\n2,201\n',b'1,100\n'):
                b.write_bytes(raw)
                with self.assertRaises(PreflightError):module.require_same_files(a,b)
            for raw in (b'1,100\n1,200\n',b'2,200\n1,100\n',b'1,100\n',b'1,100,extra\n2,200\n'):
                b.write_bytes(raw)
                with self.assertRaises(PreflightError):module.validate_expected(b,2)

    def test_health_requires_actual_four_node_identity_quorum_and_open(self):
        module=self.module()
        row=dict(node_id=2,system_identifier='123',in_recovery=False,in_quorum=True,phase='open',writer_path='target')
        module.validate_health(row,2,'123')
        for key,value in (('node_id',1),('system_identifier','456'),('in_recovery',True),('in_quorum',False),('phase','closed'),('writer_path','unknown')):
            bad=copy.deepcopy(row);bad[key]=value
            with self.assertRaises(PreflightError):module.validate_health(bad,2,'123')

    def test_read_only_timeout_is_verification_only_and_credentials_not_in_argv(self):
        module=self.module()
        argv,env=module.command('/opt/pgrac/bin/psql','192.0.2.12:5432','pgrac','postgres','SELECT 1',
            {'PGPASSWORD':'secret','PGOPTIONS':'-c statement_timeout=0','PGSERVICE':'evil'})
        self.assertNotIn('secret',repr(argv));self.assertNotIn('PGSERVICE',env)
        self.assertIn('default_transaction_read_only=on',env['PGOPTIONS'])
        self.assertIn('statement_timeout=600000',env['PGOPTIONS'])
        self.assertIn('-w',argv);self.assertIn('-XqAt',argv)

    def test_native_nonzero_is_preserved_and_cannot_pass(self):
        module=self.module()
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp).resolve();prefix=root/'failed'
            with self.assertRaises(PreflightError):
                module.capture(shutil.which('false'),dict(node_id=0,sql_addr='192.0.2.12:5432'),
                               'pgrac','postgres','SELECT 1',prefix)
            packet=json.loads((root/'failed.json').read_text())
            self.assertNotEqual(packet['rc'],0);self.assertFalse(packet['timed_out'])
            with self.assertRaises(FileExistsError):
                module.capture(shutil.which('false'),dict(node_id=0,sql_addr='192.0.2.12:5432'),
                               'pgrac','postgres','SELECT 1',prefix)

    def test_four_actual_queries_per_phase_are_required_not_release_permission(self):
        module=self.module()
        from test_bootstrap_config import BootstrapConfigTests
        fixture=BootstrapConfigTests();fixture.setUp()
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp).resolve();config=root/'config.json';expected=root/'expected.csv';psql=root/'psql'
            config.write_text(json.dumps(fixture.request));expected.write_bytes(b'1,100\n');psql.write_bytes(b'unit-only')
            args=SimpleNamespace(config=config,expected_csv=expected,psql=psql,out=root/'result',rows=1,
                system_identifier='123',relation='public.demo_account',key_column='id',value_column='value',
                user='pgrac',database='postgres')
            calls=[]
            def capture(_psql,node,_user,_database,sql,prefix):
                calls.append((node['node_id'],sql))
                path=Path(str(prefix)+'.stdout')
                if sql==module.HEALTH_SQL:
                    path.write_text(json.dumps(dict(node_id=node['node_id'],system_identifier='123',
                        in_recovery=False,in_quorum=True,phase='open',writer_path='target')))
                else:path.write_bytes(expected.read_bytes())
                return dict(stdout=dict(path=str(path)))
            with patch.object(module,'capture',side_effect=capture):result=module.run(args)
            self.assertEqual(len(calls),12);self.assertEqual(result['status'],'PASS')
            self.assertFalse(result['deployment_qualified']);self.assertFalse(result['formal_pre_pass'])
            self.assertFalse(result['restart_allowed'])


if __name__=='__main__':unittest.main()
