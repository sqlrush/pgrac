"""Cold sets must include every member, shared bytes and votes without mixing.

Author: SqlRush <sqlrush@gmail.com>
"""
import copy
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from common import PreflightError, document_sha
import bootstrap_runtime as runtime
from voting import canonical_image, crc32c, IMAGE_BYTES


class SnapshotTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(importlib.util.find_spec('snapshot'),
                             'complete cold-set copy/restore guard missing')
        import snapshot
        self.module=snapshot
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve()

    def piece(self,n,stamp='a'*64):
        root=self.root/('piece%d'%n);root.mkdir()
        data=root/'pgdata';data.mkdir()
        (data/'PG_VERSION').write_text('16\n')
        (data/'pg_wal').mkdir();(data/'pg_wal/one').write_bytes(b'wal'+bytes([n]))
        (data/'global').mkdir();(data/'global/pg_control').write_bytes(b'control'+bytes([n]))
        manifests={'pgdata':runtime.cold_tree(data)}
        if n==0:
            shared=root/'shared';shared.mkdir();(shared/'relation').write_bytes(b'rows')
            votes=root/'votes';votes.mkdir()
            for i in range(3):
                with (votes/('%d.img'%i)).open('wb') as stream:
                    stream.write(canonical_image(i));stream.truncate(16777216)
            manifests.update(shared=runtime.cold_tree(shared),votes=runtime.cold_tree(votes))
        body=dict(schema_version=1,kind='pre1-cold-piece',status='PASS',node_id=n,
                  prepared_sha256=stamp,binary_sha256='b'*64,system_identifier='123',
                  trees=manifests,restart_allowed=False)
        runtime.write_new(root/'piece.json',__import__('json').dumps(body).encode(),
                          dict(uid=os.getuid(),gid=os.getgid()))
        return root

    def test_complete_set_restore_copies_all_wal_shared_votes_and_refuses_overwrite(self):
        pieces=[self.piece(n) for n in range(4)]
        manifest=self.module.check_pieces(pieces,'a'*64)
        destination=self.root/'restored'
        restored=self.module.restore_collection(manifest,destination)
        self.assertFalse(restored['restart_allowed'])
        for n in range(4):
            self.assertEqual((destination/('node%d'%n)/'pgdata/pg_wal/one').read_bytes(),b'wal'+bytes([n]))
        self.assertEqual((destination/'node0/shared/relation').read_bytes(),b'rows')
        with (destination/'node0/votes/2.img').open('rb') as stream:
            self.assertEqual(stream.read(IMAGE_BYTES),canonical_image(2))
        self.assertEqual((destination/'node0/votes/2.img').stat().st_size,16777216)
        with self.assertRaises((FileExistsError,PreflightError)):
            self.module.restore_collection(manifest,destination)

    def test_local_piece_copies_real_files_only_between_clean_captures(self):
        original=self.piece(0)
        local=original/'pgdata';shared=original/'shared'
        captures=dict(node_id=0,binary_sha256='b'*64,identity=dict(boot_id='one'),
            control=dict(state='shut down',system_identifier='123'),processes=[],pidfile=None,
            pgdata_sha256=document_sha(runtime.cold_tree(local)),
            shared_sha256=document_sha(runtime.cold_tree(shared)),
            config_sha256='f'*64,tool_sha256='1'*64,votes=self.votes())
        prepared=dict(kind='pre1-clean-restart-prepared',status='PASS',state='CLEAN_RESTART_PREPARED',
            closure=dict(clean_stop_proven=True),binary_sha256='b'*64,system_identifier='123',
            request=dict(observer=dict(path='/fixed-observer',sha256='f'*64)),
            source=dict(shared_mount=str(shared)),captures=[captures],
            config=dict(nodes=[dict(node_id=0,pgdata=str(local),install_root='/opt/pgrac')],
                        shared_root=str(shared),voting_wwids=['vote0','vote1','vote2']))
        def copy_vote(_config,index,target,evidence):
            self.assertEqual(evidence['capacity'],16777216)
            with target.open('wb') as stream:
                stream.write(canonical_image(index));stream.truncate(16777216)
        destination=self.root/'copy'
        # Native observations/device reads are boundary fixtures. Tree reads,
        # no-follow/exclusive writes, manifests and corruption checks are real.
        with patch.object(self.module.clean_restart,'capture',return_value=captures), \
                patch.object(self.module,'copy_vote',side_effect=copy_vote):
            self.module.create_piece(prepared,0,destination)
        self.assertEqual((destination/'pgdata/pg_wal/one').read_bytes(),b'wal\x00')
        self.module.verify_piece(destination)
        changed=copy.deepcopy(captures);changed['processes']=[123]
        with patch.object(self.module.clean_restart,'capture',side_effect=[captures,changed]), \
                patch.object(self.module,'copy_vote',side_effect=copy_vote):
            with self.assertRaises(PreflightError):
                self.module.create_piece(prepared,0,self.root/'raced')
        self.assertFalse((self.root/'raced/piece.json').exists())

    def votes(self):
        return [dict(index=i,wwid='vote%d'%i,capacity=16777216,logical_sector=512,
                    read_bytes=525824,direct=True,read_only=True,strict_authority=True,
                    major=8,minor=16+i,crc32c=crc32c(canonical_image(i)),
                    members=[dict(node_id=n,valid=True,flags=0,incarnation=0,
                                  epoch=0,generation=0) for n in range(128)]) for i in range(3)]

    def test_missing_member_duplicate_member_and_cross_generation_are_rejected(self):
        pieces=[self.piece(n) for n in range(4)]
        for paths,stamp in ((pieces[:3],'a'*64),(pieces[:3]+[pieces[0]],'a'*64),(pieces,'c'*64)):
            with self.subTest(paths=paths,stamp=stamp),self.assertRaises(PreflightError):
                self.module.check_pieces(paths,stamp)

    def test_changed_wal_missing_vote_extra_file_and_links_are_not_restorable(self):
        for kind in ('wal','vote','extra','link'):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as tmp:
                old=self.root;self.root=Path(tmp).resolve()
                pieces=[self.piece(n) for n in range(4)]
                manifest=self.module.check_pieces(pieces,'a'*64)
                if kind=='wal':(pieces[2]/'pgdata/pg_wal/one').write_bytes(b'other-generation')
                elif kind=='vote':(pieces[0]/'votes/1.img').unlink()
                elif kind=='extra':(pieces[3]/'pgdata/extra').write_text('unknown')
                else:
                    (pieces[1]/'pgdata/pg_wal/one').unlink()
                    (pieces[1]/'pgdata/pg_wal/one').symlink_to(pieces[0]/'pgdata/pg_wal/one')
                destination=self.root/'bad-restore'
                with self.assertRaises((PreflightError,OSError)):
                    self.module.restore_collection(manifest,destination)
                self.assertFalse(destination.exists())
                self.root=old

    def test_source_change_after_copy_refuses_complete_set(self):
        votes=self.votes()
        old=dict(node_id=0,binary_sha256='b'*64,identity=dict(boot_id='one'),
            control=dict(state='shut down',system_identifier='123'),processes=[],pidfile=None,
            pgdata_sha256='d'*64,shared_sha256='e'*64,config_sha256='f'*64,tool_sha256='1'*64,votes=votes)
        prepared=dict(captures=[dict(old,node_id=n) for n in range(4)])
        final=copy.deepcopy(prepared)
        final['captures'][3]['pgdata_sha256']='0'*64
        with self.assertRaises(PreflightError):self.module.require_final_source(prepared,final)

    def test_self_consistent_wrong_generation_bytes_cannot_be_sealed_as_source(self):
        pieces=[self.piece(n) for n in range(4)]
        collection=self.module.check_pieces(pieces,'a'*64)
        prepared=dict(binary_sha256='b'*64,system_identifier='123',config=dict(voting_wwids=['vote0','vote1','vote2']),captures=[dict(
            node_id=n,pgdata_sha256=document_sha(collection['pieces'][n]['piece']['trees']['pgdata']),
            shared_sha256=document_sha(collection['pieces'][0]['piece']['trees']['shared']),votes=self.votes()) for n in range(4)])
        self.assertTrue(callable(getattr(self.module,'require_piece_source',None)),
                        'source capture must bind actual restored bytes, not only piece labels')
        self.module.require_piece_source(collection,prepared)
        prepared['captures'][1]['pgdata_sha256']='0'*64
        with self.assertRaises(PreflightError):self.module.require_piece_source(collection,prepared)

    def test_rehashed_short_or_other_voting_bytes_cannot_match_native_source(self):
        import json
        pieces=[self.piece(n) for n in range(4)]
        original=self.module.check_pieces(pieces,'a'*64)
        prepared=dict(binary_sha256='b'*64,system_identifier='123',config=dict(voting_wwids=['vote0','vote1','vote2']),captures=[dict(
            node_id=n,pgdata_sha256=document_sha(original['pieces'][n]['piece']['trees']['pgdata']),
            shared_sha256=document_sha(original['pieces'][0]['piece']['trees']['shared']),votes=self.votes()) for n in range(4)])
        for size in (16,16777216):
            with (pieces[0]/'votes/0.img').open('wb') as stream:
                stream.write(b'unrelated-medium');stream.truncate(size)
            body=json.loads((pieces[0]/'piece.json').read_text())
            body['trees']['votes']=runtime.cold_tree(pieces[0]/'votes')
            (pieces[0]/'piece.json').write_text(json.dumps(body))
            changed=self.module.check_pieces(pieces,'a'*64)
            with self.subTest(size=size),self.assertRaises(PreflightError):
                self.module.require_piece_source(changed,prepared)

    def test_restore_cannot_write_under_any_original_persistent_or_install_root(self):
        pieces=[self.piece(n) for n in range(4)]
        collection=self.module.check_pieces(pieces,'a'*64)
        source=self.root/'original';source.mkdir()
        # Retained provenance is an independent exclusion even when the
        # verified pieces were moved to another disk/controller directory.
        collection['prepared']=dict(config=dict(nodes=[dict(pgdata=str(source),install_root='/opt/pgrac',log_root='/var/log/pgrac')]),
                                    source=dict(shared_mount='/shared',shared_root='/shared/data',backup='/seed/backup'))
        with patch.object(self.module,'require_piece_source'):
            with self.assertRaises(PreflightError):
                self.module.restore_collection(collection,source/'bad-target')
        self.assertEqual(list(source.iterdir()),[])


if __name__=='__main__':unittest.main()
