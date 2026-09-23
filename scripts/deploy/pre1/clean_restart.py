#!/usr/bin/env python3
"""Guarded all-member clean restart; never bootstrap or repair a dataset.
Author: SqlRush <sqlrush@gmail.com>
"""
from concurrent.futures import ThreadPoolExecutor
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time

import bootstrap_config
import bootstrap_runtime as runtime
from clean_closure import keyed, verify_closure
from common import PreflightError, document_sha, load_json, paths_disjoint, publish_artifact, safe_remote_path
import guest_status
from preflight import SafeParser
import remote
import seed


def require_same_cold(before, after, *, include_shared):
    keys=('node_id','binary_sha256','identity','control','pgdata_sha256','config_sha256','tool_sha256')
    if (any(before[key]!=after[key] for key in keys)
            or before['processes'] or after['processes']
            or before['pidfile'] is not None or after['pidfile'] is not None
            or before['control']['state']!='shut down'
            or (include_shared and before['shared_sha256']!=after['shared_sha256'])):
        raise PreflightError('CLEAN_RESTART_STATE_CHANGED')


def require_cold_patch(before, after, old_binary, new_binary):
    """An explicit cold patch may change the executable, never the dataset."""
    if before['binary_sha256']!=old_binary or after['binary_sha256']!=new_binary:
        raise PreflightError('COLD_PATCH_BINARY_CHANGED')
    require_same_cold(dict(before,binary_sha256=new_binary),after,include_shared=True)


def rebound_source(base, declaration):
    """Retain seed provenance; compatibility is an explicit operator attestation."""
    try:
        captures=keyed(base['captures'],'node_id',range(4))
        if (base['kind']!='pre1-clean-restart-prepared' or base['status']!='PASS'
                or base['state']!='CLEAN_RESTART_PREPARED'
                or base['closure']['clean_stop_proven'] is not True
                or base['closure']['state']!='CLEAN_STOPPED'
                or declaration['kind']!='pre1-format-compatible-cold-patch'
                or declaration['status']!='APPROVED'
                or declaration['from_binary_sha256']!=base['binary_sha256']
                or declaration['to_binary_sha256']==base['binary_sha256']
                or base['source']['binary_sha256']!=base['binary_sha256']
                or base['source']['shared_root']!=base['config']['shared_root']
                or any(c['binary_sha256']!=base['binary_sha256'] for c in captures.values())
                or any(not re.fullmatch(r'[a-f0-9]{64}',declaration[k]) for k in
                       ('from_binary_sha256','to_binary_sha256'))
                or any(not re.fullmatch(r'[a-f0-9]{40}',declaration[k]) for k in
                       ('from_source_commit','to_source_commit'))
                or any(declaration[k] is not True for k in ('persistent_format_unchanged',
                       'wire_unchanged','config_unchanged','no_mixed_version'))):
            raise ValueError
    except (KeyError,TypeError,ValueError):
        raise PreflightError('COLD_PATCH_COMPATIBILITY_UNPROVEN') from None
    return dict(base['source'],kind='pre1-cold-rebound-source',
                binary_sha256=declaration['to_binary_sha256'],
                previous_source=base['source'],compatibility=declaration,
                previous_prepared_sha256=document_sha(base),bootstrap_ready=False)


def require_clear_votes(before, after):
    try:
        old,new=keyed(before,'index',range(3)),keyed(after,'index',range(3))
        if len({r['wwid'] for r in after})!=3:
            raise ValueError
        for disk in range(3):
            a,b=old[disk],new[disk]
            for row in (a,b):
                if (row['direct'] is not True or row['read_only'] is not True
                        or row['strict_authority'] is not True or row['read_bytes']!=525824
                        or row['logical_sector']!=512):
                    raise ValueError
                slots=keyed(row['members'],'node_id',range(128))
                if any(s['valid'] is not True or s['flags']!=0 for s in slots.values()):
                    raise ValueError
            if any(a[k]!=b[k] for k in ('wwid','capacity','logical_sector','crc32c','members')):
                raise ValueError
    except (KeyError,ValueError,TypeError):
        raise PreflightError('CLEAN_RESTART_VOTING_CHANGED') from None


def tool_digest():
    names=('clean_restart.py','clean_closure.py','bootstrap_runtime.py','bootstrap_config.py',
           'guest_status.py','seed.py','seed_clone.py','common.py','preflight.py','remote.py','lifecycle.py',
           'profile.schema.json')
    return document_sha({name:seed.checked_hash(Path(__file__).with_name(name))['sha256'] for name in names})


def vote_observations(config, observer):
    path=Path(observer['path'])
    if path.resolve(strict=True)!=path or seed.checked_hash(path)['sha256']!=observer['sha256']:
        raise PreflightError('CLEAN_RESTART_OBSERVER_CHANGED')
    rows=[]
    for index,wwid in enumerate(config['voting_wwids']):
        device=Path('/dev/disk/by-id/scsi-'+wwid).resolve(strict=True)
        s=device.stat()
        argv=[str(path),'inspect',str(device),wwid,str(index),'16777216',
              str(os.major(s.st_rdev)),str(os.minor(s.st_rdev))]
        p=subprocess.run(argv,capture_output=True,text=True,timeout=15)
        if p.returncode:
            raise PreflightError('CLEAN_RESTART_VOTE_OBSERVATION_FAILED')
        row=json.loads(p.stdout)
        if row['wwid']!=wwid or row['index']!=index or row['status']!='OBSERVED':
            raise PreflightError('CLEAN_RESTART_VOTE_IDENTITY_CHANGED')
        rows.append(row)
    return rows


def capture(request, *, include_shared=True):
    config=request['config']
    files=bootstrap_config.render(config)
    node=next(n for n in config['nodes'] if n['node_id']==request['node_id'])
    source=request['source']
    if source['shared_root']!=config['shared_root'] or source['binary_sha256']!=request['binary_sha256']:
        raise PreflightError('CLEAN_RESTART_SOURCE_CHANGED')
    seed.require_seed_mount(source)
    runtime.require_votes(config,node)
    query=dict(node=node,binary_sha256=request['binary_sha256'])
    before=runtime.observation(query)
    expected={k:node[k] for k in ('vm_uuid','boot_id','machine_id')}
    if before['identity']!=expected or before['binary_sha256']!=request['binary_sha256']:
        raise PreflightError('CLEAN_RESTART_GUEST_CHANGED')
    runtime.require_stopped_control(before,request['system_identifier'],'shut down')
    data=runtime.cold_tree(node['pgdata'])
    for name,entry in runtime.config_entries(files[node['node_id']]).items():
        if data.get(name)!=entry:
            raise PreflightError('CLEAN_RESTART_CONFIGURATION_CHANGED')
    shared=runtime.cold_tree(config['shared_root']) if include_shared else None
    votes=vote_observations(config,request['observer']) if include_shared else None
    after=runtime.observation(query)
    if before!=after:
        raise PreflightError('CLEAN_RESTART_OBSERVATION_CHANGED')
    return dict(node_id=node['node_id'],binary_sha256=request['binary_sha256'],identity=expected,
                control=before['control']['parsed'],native_control=before['control'],
                processes=before['processes'],pidfile=before['pidfile'],
                pgdata_sha256=document_sha(data),config_sha256=document_sha(files[node['node_id']]),
                shared_sha256=document_sha(shared) if include_shared else None,votes=votes,
                tool_sha256=tool_digest())


def prepare(request):
    config=runtime.read_bound(request['config'])
    bootstrap_config.render(config)
    source=runtime.read_bound(request['source'])
    before=runtime.read_bound(request['closed_before'])
    after=runtime.read_bound(request['closed_after'])
    binary=request['binary_sha256']
    nodes=sorted(config['nodes'],key=lambda n:n['node_id'])
    system_id=after['closure']['system_identifier']
    closure=verify_closure(nodes,binary,after['observations'],system_id,
        before['starts'],after['logs'],before['votes'],after['after_votes'],before['states'],
        post_stop_not_before_ns=after['post_stop_not_before_ns'])
    if (after['status']!='PASS' or after['source_binary_sha256']!=binary
            or not closure['clean_stop_proven'] or closure['state']!='CLEAN_STOPPED'
            or request['guest_config']['sha256']!=request['config']['sha256']):
        raise PreflightError('CLEAN_RESTART_CLOSURE_UNPROVEN')
    tools=safe_remote_path(request['guest_tool_root'],'guest_tool_root')
    program=('import sys,json; sys.path.insert(0,%r); import clean_restart; '
             'print(json.dumps(clean_restart.capture(json.load(sys.stdin))))')%str(tools)
    def one(node):
        value=dict(config=config,source=source,binary_sha256=binary,system_identifier=system_id,
                   node_id=node['node_id'],observer=request['observer'])
        transport=remote.run_ssh(node,program,value)
        if transport['rc'] or transport['timed_out'] or transport['truncated']:
            raise PreflightError('CLEAN_RESTART_CAPTURE_FAILED')
        snapshot=json.loads(transport['stdout'])
        if snapshot['tool_sha256']!=tool_digest():
            raise PreflightError('CLEAN_RESTART_TOOL_CHANGED')
        require_clear_votes(after['after_votes'],snapshot['votes'])
        return snapshot
    with ThreadPoolExecutor(max_workers=4) as pool:
        captures=list(pool.map(one,nodes))
    if len({c['shared_sha256'] for c in captures})!=1:
        raise PreflightError('CLEAN_RESTART_SHARED_VIEWS_DIFFER')
    return dict(schema_version=1,kind='pre1-clean-restart-prepared',status='PASS',
                state='CLEAN_RESTART_PREPARED',request=request,config=config,source=source,
                binary_sha256=binary,system_identifier=system_id,captures=captures,
                closure=closure,bootstrap_ready=False,restart_allowed=False,deployment_qualified=False)


def rebind(request):
    """Recheck an all-member cold patch. Never install, repair or start a member."""
    base=runtime.read_bound(request['base_prepared'])
    declaration=runtime.read_bound(request['compatibility'])
    source=rebound_source(base,declaration)
    if runtime.read_bound(request['rebound_source'])!=source:
        raise PreflightError('COLD_PATCH_SOURCE_CHANGED')
    old=base['request']
    config=runtime.read_bound(old['config'])
    bootstrap_config.render(config)
    before=runtime.read_bound(old['closed_before'])
    after=runtime.read_bound(old['closed_after'])
    nodes=sorted(config['nodes'],key=lambda n:n['node_id'])
    closure=verify_closure(nodes,base['binary_sha256'],after['observations'],base['system_identifier'],
        before['starts'],after['logs'],before['votes'],after['after_votes'],before['states'],
        post_stop_not_before_ns=after['post_stop_not_before_ns'])
    if (config!=base['config'] or runtime.read_bound(old['source'])!=base['source']
            or after['status']!='PASS' or after['source_binary_sha256']!=base['binary_sha256']
            or not closure['clean_stop_proven'] or closure!=base['closure']
            or old['guest_config']['sha256']!=old['config']['sha256']):
        raise PreflightError('COLD_PATCH_CLOSURE_CHANGED')
    snapshots=keyed(base['captures'],'node_id',range(4))
    tools=safe_remote_path(request['guest_tool_root'],'guest_tool_root')
    program=('import sys,json; sys.path.insert(0,%r); import clean_restart; '
             'print(json.dumps(clean_restart.capture(json.load(sys.stdin))))')%str(tools)
    binary=declaration['to_binary_sha256']
    def one(node):
        value=dict(config=config,source=source,binary_sha256=binary,
                   system_identifier=base['system_identifier'],node_id=node['node_id'],observer=old['observer'])
        transport=remote.run_ssh(node,program,value)
        if transport['rc'] or transport['timed_out'] or transport['truncated']:
            raise PreflightError('COLD_PATCH_CAPTURE_FAILED')
        fresh=json.loads(transport['stdout'])
        if fresh['node_id']!=node['node_id'] or fresh['tool_sha256']!=tool_digest():
            raise PreflightError('COLD_PATCH_GUEST_OR_TOOL_CHANGED')
        previous=snapshots[node['node_id']]
        require_cold_patch(previous,fresh,base['binary_sha256'],binary)
        require_clear_votes(previous['votes'],fresh['votes'])
        require_clear_votes(after['after_votes'],fresh['votes'])
        return fresh
    with ThreadPoolExecutor(max_workers=4) as pool:
        captures=list(pool.map(one,nodes))
    if len({c['shared_sha256'] for c in captures})!=1:
        raise PreflightError('COLD_PATCH_SHARED_VIEWS_DIFFER')
    updated=dict(old,binary_sha256=binary,source=request['rebound_source'],
                 guest_tool_root=request['guest_tool_root'])
    return dict(base,request=updated,source=source,binary_sha256=binary,captures=captures,
                cold_patch=request,qualification_inherited=False)


def start_guest(reference,node_id,output):
    prepared=runtime.read_bound(reference)
    if (prepared['kind']!='pre1-clean-restart-prepared' or prepared['status']!='PASS'
            or prepared['state']!='CLEAN_RESTART_PREPARED' or not prepared['closure']['clean_stop_proven']):
        raise PreflightError('CLEAN_RESTART_NOT_PREPARED')
    config=prepared['config']
    bootstrap_config.render(config)
    node=next(n for n in config['nodes'] if n['node_id']==node_id)
    if runtime.read_bound(prepared['request']['guest_config'])!=config:
        raise PreflightError('CLEAN_RESTART_CONFIG_REFERENCE_CHANGED')
    snapshots=keyed(prepared['captures'],'node_id',range(4))
    root=Path(node['pgdata']); logs=Path(node['log_root'])
    stamp=document_sha(prepared)[:16]
    log=logs/('clean-start-'+stamp+'.log')
    marker=logs/('clean-start-'+stamp+'-attempt.json')
    paths_disjoint([Path(output),root,Path(prepared['source']['shared_mount']),log,marker],'restart-output')
    if Path(output).parent!=logs or os.path.lexists(output) or log.exists() or marker.exists():
        raise PreflightError('CLEAN_RESTART_OUTPUT_OR_ATTEMPT_INVALID')
    fd=os.open(root.parent/'.pre1-runtime.lock',os.O_WRONLY|os.O_CREAT|os.O_NOFOLLOW|os.O_NONBLOCK,0o600)
    with os.fdopen(fd,'w') as lock:
        metadata=os.fstat(lock.fileno())
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink!=1:
            raise PreflightError('CLEAN_RESTART_LOCK_INVALID')
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        value=dict(config=config,source=prepared['source'],binary_sha256=prepared['binary_sha256'],
                   system_identifier=prepared['system_identifier'],node_id=node_id,
                   observer=prepared['request']['observer'])
        # Controller starts node0 first, then checks each successful return.
        # After node0 starts, shared data/voting may legitimately change. Every
        # joining node still rechecks its own unchanged native-clean dataset.
        fresh=capture(value,include_shared=node_id==0)
        require_same_cold(snapshots[node_id],fresh,include_shared=node_id==0)
        if node_id==0:
            require_clear_votes(snapshots[0]['votes'],fresh['votes'])
        runtime.write_new(marker,(json.dumps(reference,sort_keys=True)+'\n').encode(),dict(uid=0,gid=0))
        runtime.write_new(log,b'',node)
        result=dict(schema_version=1,kind='pre1-clean-start',status='ERROR',state='START_INCOMPLETE',
                    prepared=reference,node=node,log=str(log),process_identity=None,
                    request=dict(config=prepared['request']['guest_config'],node_id=node_id,
                                 binary_sha256=prepared['binary_sha256']),
                    bootstrap_ready=False,restart_allowed=False,deployment_qualified=False)
        try:
            launched=int(time.time())
            argv=[str(Path(node['install_root'])/'bin/pg_ctl'),'-D',str(root),'-l',str(log),
                  '-o','-c config_file='+str(root/'pre1-runtime.conf'),'-w','-t','60','start']
            result['command']=seed.run_native(argv,node)
            result['observation']=runtime.observation(dict(node=node,binary_sha256=prepared['binary_sha256']))
            result['process_identity']=seed.seed_process(result['observation'],
                dict(node=node,binary_sha256=prepared['binary_sha256']),launched)
            if seed.native_succeeded(result['command']):
                result.update(status='PASS',state='CLEAN_PROCESS_STARTED_NOT_ADMITTED')
        except Exception as error:
            result['reason']=getattr(error,'reason','CLEAN_RESTART_START_UNAVAILABLE')
        result['artifact_sha256']=publish_artifact(output,result)
    return result


def main(argv=None):
    try:
        parser=SafeParser(description=__doc__)
        parser.add_argument('action',choices=('prepare','rebind','start-guest'))
        parser.add_argument('--request',type=Path,required=True)
        parser.add_argument('--sha256')
        parser.add_argument('--node-id',type=int,choices=range(4))
        parser.add_argument('--out',type=Path,required=True)
        args=parser.parse_args(argv)
        if os.path.lexists(args.out):
            raise PreflightError('ARTIFACT_EXISTS')
        if args.action in ('prepare','rebind'):
            operation=prepare if args.action=='prepare' else rebind
            result=operation(load_json(args.request))
            result['artifact_sha256']=publish_artifact(args.out,result)
        else:
            result=start_guest(dict(path=str(args.request),sha256=args.sha256),args.node_id,args.out)
        answer={k:result[k] for k in ('status','state','artifact_sha256')}
    except PreflightError as error:
        answer=dict(status=error.status,reason=error.reason)
    except (OSError,ValueError,KeyError,TypeError,subprocess.SubprocessError):
        answer=dict(status='ERROR',reason='CLEAN_RESTART_UNAVAILABLE')
    print(json.dumps(answer,sort_keys=True))
    return {'PASS':0,'BLOCKED':2,'ERROR':3}[answer['status']]


if __name__=='__main__':
    raise SystemExit(main())
