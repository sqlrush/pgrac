#!/usr/bin/env python3
"""Read-only four-node user checks; not the formal PRE judge or lifecycle authority.

Author: SqlRush <sqlrush@gmail.com>
"""
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time

import bootstrap_config
from common import PreflightError, endpoint, load_json, paths_disjoint, publish_artifact
from preflight import SafeParser
import seed
from snapshot import new_directory, require_outside_source

HEALTH_SQL="""SELECT json_build_object(
'node_id',current_setting('cluster.node_id')::int,
'system_identifier',(pg_control_system()).system_identifier::text,
'in_recovery',pg_is_in_recovery(),
'in_quorum',(SELECT in_quorum FROM pg_cluster_quorum_state),
'phase',(SELECT value FROM pg_cluster_state WHERE category='pcm' AND key='resource_x_gate_phase'),
'writer_path',(SELECT value FROM pg_cluster_state WHERE category='pcm' AND key='resource_x_writer_path'))"""


def identifier(value):
    if type(value) is not str or not re.fullmatch(r'[a-z_][a-z0-9_]{0,62}',value):
        raise PreflightError('VERIFY_IDENTIFIER_INVALID')
    return '"'+value+'"'


def copy_sql(relation,key,value):
    parts=relation.split('.')
    if len(parts)!=2:raise PreflightError('VERIFY_SCHEMA_QUALIFIED_RELATION_REQUIRED')
    name='.'.join(identifier(p) for p in parts)
    key,value=identifier(key),identifier(value)
    if key==value:raise PreflightError('VERIFY_COLUMNS_MUST_DIFFER')
    return f'COPY (SELECT {key},{value} FROM {name} ORDER BY {key}) TO STDOUT WITH (FORMAT csv)'


def validate_expected(path,count):
    if type(count) is not int or count<=0:raise PreflightError('VERIFY_ROW_COUNT_INVALID')
    seed.checked_hash(path)
    previous=0;seen=0
    with Path(path).open(encoding='utf-8',newline='') as stream:
        for row in csv.reader(stream,strict=True):
            seen+=1
            if (len(row)!=2 or not re.fullmatch(r'[1-9][0-9]*',row[0])
                    or int(row[0])<=previous or seen>count):
                raise PreflightError('VERIFY_EXPECTED_KEYS_OR_ROWS_INVALID')
            previous=int(row[0])
    if seen!=count:raise PreflightError('VERIFY_EXPECTED_ROW_COUNT_DIFFERS')


def require_same_files(expected,actual):
    # Compare all bytes; a count or checksum alone is not the comparison.
    with Path(expected).open('rb') as left,Path(actual).open('rb') as right:
        while True:
            a,b=left.read(1048576),right.read(1048576)
            if a!=b:raise PreflightError('VERIFY_FULL_ROW_BYTES_DIFFER')
            if not a:return


def validate_health(row,node_id,system_identifier):
    expected=dict(node_id=node_id,system_identifier=system_identifier,
                  in_recovery=False,in_quorum=True,phase='open',writer_path='target')
    if type(row) is not dict or row!=expected:
        raise PreflightError('VERIFY_HEALTH_OR_IDENTITY_FAILED')


def command(psql,address,user,database,sql,base):
    identifier(user);identifier(database)
    host,port=endpoint(address,'sql_addr')
    env=dict(base)
    for key in ('PGSERVICE','PGSERVICEFILE','PGOPTIONS','PGHOSTADDR','PGDATABASE','PGUSER'):
        env.pop(key,None)
    env.update(PGCONNECT_TIMEOUT='5',PGCLIENTENCODING='UTF8',LC_ALL='C',
               PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=600000')
    return [str(psql),'-XqAt','-w','-v','ON_ERROR_STOP=1','-h',host,'-p',str(port),
            '-U',user,'-d',database,'-c',sql],env


def capture(psql,node,user,database,sql,prefix):
    argv,env=command(psql,node['sql_addr'],user,database,sql,os.environ)
    files=[Path(str(prefix)+suffix) for suffix in ('.stdout','.stderr')]
    began=time.monotonic_ns();timed_out=False;rc=None
    with os.fdopen(os.open(files[0],os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600),'wb') as out:
        with os.fdopen(os.open(files[1],os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600),'wb') as err:
            process=subprocess.Popen(argv,stdout=out,stderr=err,env=env)
            try:rc=process.wait(timeout=605)
            except subprocess.TimeoutExpired:
                timed_out=True;process.kill();process.wait()
            out.flush();err.flush();os.fsync(out.fileno());os.fsync(err.fileno())
    result=dict(node_id=node['node_id'],rc=rc,timed_out=timed_out,
                elapsed_ns=time.monotonic_ns()-began,sql_sha256=hashlib.sha256(sql.encode()).hexdigest(),
                stdout=dict(path=str(files[0]),**seed.checked_hash(files[0])),
                stderr=dict(path=str(files[1]),**seed.checked_hash(files[1])))
    publish_artifact(Path(str(prefix)+'.json'),result)
    if timed_out:raise PreflightError('VERIFY_QUERY_INCOMPLETE')
    if rc!=0 or files[1].stat().st_size:raise PreflightError('VERIFY_QUERY_FAILED_OR_WARNING')
    return result


def run(args):
    config=load_json(args.config);bootstrap_config.render(config)
    psql=Path(args.psql)
    seed.canonical_directory(psql.parent)
    psql_identity=seed.checked_hash(psql)
    expected=Path(args.expected_csv)
    seed.canonical_directory(expected.parent)
    validate_expected(expected,args.rows);expected_identity=seed.checked_hash(expected)
    if not re.fullmatch(r'[1-9][0-9]{0,19}',args.system_identifier):
        raise PreflightError('VERIFY_SYSTEM_IDENTIFIER_REQUIRED')
    sql=copy_sql(args.relation,args.key_column,args.value_column)
    identifier(args.user);identifier(args.database)
    out=Path(args.out)
    paths_disjoint([out,Path(args.config),expected,psql],'verify-output')
    require_outside_source(out,dict(config=config))
    new_directory(out)
    report=dict(schema_version=1,kind='pre1-user-verification',status='INCOMPLETE',
        deployment_qualified=False,formal_pre_pass=False,restart_allowed=False,
        psql=psql_identity,expected=expected_identity,row_count=args.rows,nodes=[])
    try:
        for node in sorted(config['nodes'],key=lambda n:n['node_id']):
            n=node['node_id'];proof={}
            for phase in ('pre','rows','post'):
                raw=capture(psql,node,args.user,args.database,sql if phase=='rows' else HEALTH_SQL,
                            out/('node%d-%s'%(n,phase)))
                path=Path(raw['stdout']['path'])
                if phase=='rows':require_same_files(expected,path)
                else:
                    if path.stat().st_size>1048576:raise PreflightError('VERIFY_HEALTH_OUTPUT_TOO_LARGE')
                    validate_health(json.loads(path.read_text()),n,args.system_identifier)
                proof[phase]=raw
            report['nodes'].append(dict(node_id=n,evidence=proof))
        if seed.checked_hash(expected)!=expected_identity or seed.checked_hash(psql)!=psql_identity:
            raise PreflightError('VERIFY_INPUT_CHANGED')
        report.update(status='PASS',state='USER_CHECKS_PASSED_NOT_DEPLOYMENT_CERTIFIED')
    except PreflightError as error:
        report.update(status='FAIL',reason=error.reason)
    except (OSError,ValueError,TypeError,KeyError,subprocess.SubprocessError):
        report.update(status='ERROR',reason='VERIFY_OBSERVATION_INCOMPLETE')
    publish_artifact(out/'result.json',report)
    return report


def main():
    try:
        parser=SafeParser(description=__doc__)
        for name in ('config','psql','expected-csv','out'):
            parser.add_argument('--'+name,type=Path,required=True)
        for name in ('system-identifier','relation','key-column','value-column','user','database'):
            parser.add_argument('--'+name,required=True)
        parser.add_argument('--rows',type=int,required=True)
        result=run(parser.parse_args())
    except PreflightError as error:
        result=dict(status=error.status,reason=error.reason)
    except (OSError,ValueError,TypeError,KeyError,csv.Error):
        result=dict(status='ERROR',reason='VERIFY_INPUT_OR_OUTPUT_INVALID')
    print(json.dumps({k:v for k,v in result.items() if k in ('status','state','reason')},sort_keys=True))
    return 0 if result['status']=='PASS' else 2


if __name__=='__main__':raise SystemExit(main())
