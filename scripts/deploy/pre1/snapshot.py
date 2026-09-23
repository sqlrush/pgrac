#!/usr/bin/env python3
"""Copy and verify complete, already-clean PRE1 cold sets.

Author: SqlRush <sqlrush@gmail.com>

Restore creates an independent offline collection, never overwrites PGDATA or
a voting device, and never grants startup or crash-recovery permission.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import stat

import bootstrap_runtime as runtime
import clean_restart
from common import PreflightError, canonical_bytes, document_sha, load_json, paths_disjoint, publish_artifact
import seed
from seed_clone import copy_tree_new, open_directory
from voting import IMAGE_BYTES, crc32c


def owner():
    return dict(uid=os.getuid(),gid=os.getgid())


def new_directory(path):
    path=Path(path)
    parent=open_directory(path.parent)
    try:
        os.mkdir(path.name,0o700,dir_fd=parent)
        os.fsync(parent)
    finally:
        os.close(parent)
    return path


def copy_tree(source,target):
    new_directory(target)
    copy_tree_new(source,target,owner())
    return runtime.cold_tree(target)


def require_final_source(prepared,final):
    before={c['node_id']:c for c in prepared['captures']}
    after={c['node_id']:c for c in final['captures']}
    if len(prepared['captures'])!=4 or len(final['captures'])!=4 or set(before)!=set(range(4)) or set(after)!=set(range(4)):
        raise PreflightError('SNAPSHOT_ALL_MEMBERS_REQUIRED')
    for n in range(4):
        clean_restart.require_same_cold(before[n],after[n],include_shared=True)
        clean_restart.require_clear_votes(before[n]['votes'],after[n]['votes'])


def require_vote_image(path,evidence):
    """Bind an archive image to the native read, not to its own manifest."""
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd,'rb') as stream:
        held=os.fstat(stream.fileno())
        if (not stat.S_ISREG(held.st_mode) or held.st_nlink!=1
                or evidence['capacity']!=16777216 or held.st_size!=evidence['capacity']):
            raise PreflightError('SNAPSHOT_VOTE_IMAGE_SIZE_INVALID')
        raw=stream.read(IMAGE_BYTES)
        if len(raw)!=IMAGE_BYTES or crc32c(raw)!=evidence['crc32c']:
            raise PreflightError('SNAPSHOT_VOTE_SOURCE_DIFFERS')


def copy_vote(config,index,target,evidence):
    """Only read the exact already-validated device; output is a new regular file."""
    path=Path('/dev/disk/by-id/scsi-'+config['voting_wwids'][index]).resolve(strict=True)
    before=path.stat()
    capacity=evidence['capacity']
    if (not stat.S_ISBLK(before.st_mode) or capacity!=16777216
            or evidence['index']!=index or evidence['wwid']!=config['voting_wwids'][index]
            or (os.major(before.st_rdev),os.minor(before.st_rdev))!=(evidence['major'],evidence['minor'])):
        raise PreflightError('SNAPSHOT_VOTE_IDENTITY_INVALID')
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    with os.fdopen(fd,'rb') as incoming:
        held=os.fstat(incoming.fileno())
        if (held.st_dev,held.st_ino,held.st_rdev)!=(before.st_dev,before.st_ino,before.st_rdev):
            raise PreflightError('SNAPSHOT_VOTE_CHANGED')
        out=os.open(target,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        with os.fdopen(out,'wb') as stream:
            left=capacity
            while left:
                raw=incoming.read(min(left,1048576))
                if not raw:raise PreflightError('SNAPSHOT_VOTE_SHORT_READ')
                stream.write(raw);left-=len(raw)
            stream.flush();os.fsync(stream.fileno())
    require_vote_image(target,evidence)


def require_outside_source(destination,prepared):
    """Moved snapshot pieces still cannot be restored inside original roots."""
    destination=Path(destination)
    if not destination.is_absolute() or '..' in destination.parts:
        raise PreflightError('SNAPSHOT_DESTINATION_INVALID')
    config,source=prepared.get('config',{}),prepared.get('source',{})
    roots=[node[key] for node in config.get('nodes',[])
           for key in ('pgdata','install_root','log_root') if key in node]
    roots += [config[key] for key in ('shared_root',) if key in config]
    roots += [source[key] for key in ('shared_mount','shared_root','backup','log','schema_path') if key in source]
    for path in roots:
        paths_disjoint([destination,Path(path)],'snapshot-original-source')


def create_piece(prepared,node_id,destination):
    if (prepared.get('kind')!='pre1-clean-restart-prepared' or prepared.get('status')!='PASS'
            or prepared.get('state')!='CLEAN_RESTART_PREPARED'
            or prepared.get('closure',{}).get('clean_stop_proven') is not True
            or type(node_id) is not int or node_id not in range(4)):
        raise PreflightError('SNAPSHOT_CLEAN_CLOSURE_REQUIRED')
    config=prepared['config']
    require_outside_source(destination,prepared)
    node=next(n for n in config['nodes'] if n['node_id']==node_id)
    destination=Path(destination)
    paths_disjoint([destination,Path(node['pgdata']),Path(node['install_root']),
                    Path(prepared['source']['shared_mount'])],'snapshot-target')
    lockpath=Path(node['pgdata']).parent/'.pre1-runtime.lock'
    fd=os.open(lockpath,os.O_WRONLY|os.O_CREAT|os.O_NOFOLLOW|os.O_NONBLOCK,0o600)
    with os.fdopen(fd,'w') as lock:
        info=os.fstat(lock.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1:
            raise PreflightError('SNAPSHOT_LOCK_INVALID')
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        request=dict(config=config,source=prepared['source'],binary_sha256=prepared['binary_sha256'],
                     system_identifier=prepared['system_identifier'],node_id=node_id,
                     observer=prepared['request']['observer'])
        before=clean_restart.capture(request)
        expected=next(c for c in prepared['captures'] if c['node_id']==node_id)
        clean_restart.require_same_cold(expected,before,include_shared=True)
        clean_restart.require_clear_votes(expected['votes'],before['votes'])
        new_directory(destination)
        trees=dict(pgdata=copy_tree(node['pgdata'],destination/'pgdata'))
        if document_sha(trees['pgdata'])!=before['pgdata_sha256']:
            raise PreflightError('SNAPSHOT_LOCAL_COPY_DIFFERS')
        if node_id==0:
            trees['shared']=copy_tree(config['shared_root'],destination/'shared')
            if document_sha(trees['shared'])!=before['shared_sha256']:
                raise PreflightError('SNAPSHOT_SHARED_COPY_DIFFERS')
            new_directory(destination/'votes')
            votes={v['index']:v for v in before['votes']}
            for i in range(3):copy_vote(config,i,destination/'votes'/('%d.img'%i),votes[i])
            trees['votes']=runtime.cold_tree(destination/'votes')
        after=clean_restart.capture(request)
        clean_restart.require_same_cold(before,after,include_shared=True)
        clean_restart.require_clear_votes(before['votes'],after['votes'])
        piece=dict(schema_version=1,kind='pre1-cold-piece',status='PASS',node_id=node_id,
            prepared_sha256=document_sha(prepared),binary_sha256=prepared['binary_sha256'],
            system_identifier=prepared['system_identifier'],trees=trees,restart_allowed=False)
        runtime.write_new(destination/'piece.json',canonical_bytes(piece),owner())
        return piece


def verify_piece(path):
    path=seed.canonical_directory(path)
    piece=runtime.read_bound(dict(path=str(path/'piece.json'),
                                 sha256=seed.checked_hash(path/'piece.json')['sha256']))
    n=piece.get('node_id')
    if (piece.get('schema_version')!=1 or piece.get('kind')!='pre1-cold-piece'
            or piece.get('status')!='PASS' or type(n) is not int or n not in range(4)
            or piece.get('restart_allowed') is not False):
        raise PreflightError('SNAPSHOT_PIECE_INVALID')
    expected={'pgdata','shared','votes'} if n==0 else {'pgdata'}
    if set(piece['trees'])!=expected or set(os.listdir(path))!=expected|{'piece.json'}:
        raise PreflightError('SNAPSHOT_PIECE_INCOMPLETE')
    for name in expected:
        if runtime.cold_tree(path/name)!=piece['trees'][name]:
            raise PreflightError('SNAPSHOT_PIECE_BYTES_CHANGED')
    if n==0 and set(piece['trees']['votes'])!={'0.img','1.img','2.img'}:
        raise PreflightError('SNAPSHOT_THREE_VOTES_REQUIRED')
    local=piece['trees']['pgdata']
    if not {'PG_VERSION','global/pg_control','pg_wal'}<=set(local):
        raise PreflightError('SNAPSHOT_NATIVE_SET_INCOMPLETE')
    return piece


def check_pieces(paths,prepared_sha256):
    pieces=[]
    for path in paths:
        path=seed.canonical_directory(path)
        piece=verify_piece(path)
        if piece['prepared_sha256']!=prepared_sha256:
            raise PreflightError('SNAPSHOT_GENERATION_DIFFERS')
        pieces.append(dict(path=str(path),piece=piece))
    if (len(pieces)!=4 or {r['piece']['node_id'] for r in pieces}!=set(range(4))
            or len({r['piece']['binary_sha256'] for r in pieces})!=1
            or len({r['piece']['system_identifier'] for r in pieces})!=1):
        raise PreflightError('SNAPSHOT_ALL_MEMBERS_REQUIRED')
    return dict(schema_version=1,kind='pre1-cold-collection',prepared_sha256=prepared_sha256,
                pieces=sorted(pieces,key=lambda r:r['piece']['node_id']),restart_allowed=False)


def require_piece_source(collection,prepared):
    captures={c['node_id']:c for c in prepared['captures']}
    if len(prepared['captures'])!=4 or set(captures)!=set(range(4)):
        raise PreflightError('SNAPSHOT_ALL_MEMBERS_REQUIRED')
    base_votes=captures[0]['votes']
    for capture in captures.values():
        clean_restart.require_clear_votes(base_votes,capture['votes'])
    votes={v['index']:v for v in base_votes}
    if any(votes[i]['wwid']!=prepared['config']['voting_wwids'][i] for i in range(3)):
        raise PreflightError('SNAPSHOT_VOTE_IDENTITY_INVALID')
    for row in collection['pieces']:
        piece=row['piece'];n=piece['node_id']
        if (piece['binary_sha256']!=prepared['binary_sha256']
                or piece['system_identifier']!=prepared['system_identifier']
                or document_sha(piece['trees']['pgdata'])!=captures[n]['pgdata_sha256']
                or (n==0 and document_sha(piece['trees']['shared'])!=captures[n]['shared_sha256'])):
            raise PreflightError('SNAPSHOT_PIECE_SOURCE_DIFFERS')
        if n==0:
            for i in range(3):
                require_vote_image(Path(row['path'])/'votes'/('%d.img'%i),votes[i])


def seal_set(prepared,paths,output):
    collection=check_pieces(paths,document_sha(prepared))
    require_piece_source(collection,prepared)
    final=clean_restart.prepare(prepared['request'])
    require_final_source(prepared,final)
    # Embed the complete source config and closure documents, not only paths
    # that might vanish when the controller's original workspace is archived.
    provenance={name:runtime.read_bound(prepared['request'][name])
                for name in ('config','source','closed_before','closed_after')}
    artifact=dict(collection,status='PASS',state='COLD_SET_VERIFIED',
                  prepared=prepared,final_source=final,provenance=provenance,
                  deployment_qualified=False)
    publish_artifact(output,artifact)
    return artifact


def restore_collection(collection,destination):
    if collection.get('kind')!='pre1-cold-collection' or collection.get('restart_allowed') is not False:
        raise PreflightError('SNAPSHOT_COLLECTION_INVALID')
    paths=[r['path'] for r in collection['pieces']]
    verified=check_pieces(paths,collection['prepared_sha256'])
    if verified['pieces']!=collection['pieces']:
        raise PreflightError('SNAPSHOT_COLLECTION_CHANGED')
    if 'prepared' in collection:
        require_piece_source(verified,collection['prepared'])
    destination=Path(destination)
    for name in ('prepared','final_source','provenance'):
        if name in collection:require_outside_source(destination,collection[name])
    paths_disjoint([destination]+[Path(p) for p in paths],'snapshot-restore-target')
    new_directory(destination)
    copied=[]
    for row in verified['pieces']:
        target=destination/('node%d'%row['piece']['node_id'])
        copy_tree(row['path'],target)
        copied.append(target)
    final=check_pieces(copied,collection['prepared_sha256'])
    if [r['piece'] for r in final['pieces']]!=[r['piece'] for r in verified['pieces']]:
        raise PreflightError('SNAPSHOT_RESTORE_DIFFERS')
    # Keep all closure/config provenance when restoring a sealed collection.
    for key in ('prepared','final_source','provenance'):
        if key in collection:final[key]=collection[key]
    final.update(status='PASS',state='COLD_SET_RESTORED_NOT_STARTED',deployment_qualified=False)
    publish_artifact(destination/'restored.json',final)
    return final


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=('create-piece','verify-piece','seal','restore'))
    parser.add_argument('--request',type=Path,required=True,help='JSON with a hash-bound prepared/collection reference')
    parser.add_argument('--out',type=Path,required=True)
    args=parser.parse_args()
    request=load_json(args.request)
    if args.action=='create-piece':
        result=create_piece(runtime.read_bound(request['prepared']),request['node_id'],args.out)
    elif args.action=='verify-piece':
        result=verify_piece(request['piece']);publish_artifact(args.out,result)
    elif args.action=='seal':
        result=seal_set(runtime.read_bound(request['prepared']),request['pieces'],args.out)
    else:
        collection=runtime.read_bound(request['collection'])
        if collection.get('state')!='COLD_SET_VERIFIED' or collection.get('status')!='PASS':
            raise PreflightError('SNAPSHOT_SEALED_SET_REQUIRED')
        result=restore_collection(collection,args.out)
    print(json.dumps(dict(status=result['status'],restart_allowed=False,deployment_qualified=False)))


if __name__=='__main__':main()
