"""Run the remote-locator fixture without a database or VM."""
import json
from pathlib import Path
import subprocess
import unittest

HERE = Path(__file__).resolve().parent


def perl(code, value=None):
    result = subprocess.run(
        ['perl', '-I'+str(HERE), '-MJSON::PP', '-MM2CanonicalOrigin', '-e', code],
        input=json.dumps(value), text=True, capture_output=True, timeout=10)
    if result.returncode:
        raise AssertionError(result.stderr)
    return json.loads(result.stdout)


class OriginFixture(unittest.TestCase):
    def setUp(self):
        self.locator = dict(uba0='4294967297', uba1='262146', xid=4195408,
                            wrap=0, itl_kind=1, itl_slot_index=0,
                            data_segment=1, tt_slot=2, origin=0,
                            authoritative=False)
        self.key = dict(xid=4195408, origin=0, segment=1, slot=2, wrap=7,
                        generation=1, root=9, root_generation=2)

    def query(self, caller='freeze_data', locator=None, key=None):
        return perl(r'''my $v=decode_json(do {local $/; <STDIN>});
my $sql=eval { M2CanonicalOrigin::origin_query(@$v) };
print encode_json({sql=>$sql,error=>$@});''',
                    [locator or self.locator, key or self.key, caller])

    def test_copied_locator_query_needs_no_heap_read_and_keeps_page_wrap_zero(self):
        result = self.query()
        self.assertEqual(result['error'], '')
        self.assertEqual(result['sql'],
                         "SELECT test_pgrac_m2_sample_locator(4294967297,262146,"
                         "'4195408'::xid,0,1,0)")

    def test_canonical_caller_requires_real_different_data_segment(self):
        locator = dict(self.locator, uba0='4294967298', data_segment=2)
        self.assertEqual(self.query('sample_canonical', locator)['error'], '')
        for caller, loc in [('freeze_data', locator),
                            ('sample_canonical', self.locator)]:
            with self.subTest(caller=caller):
                result = self.query(caller, loc)
                self.assertIsNone(result['sql'])
                self.assertIn('caller mismatch', result['error'])

    def test_wrong_producer_identity_and_slot_refuse_before_scheduling(self):
        for field, value in [('xid', 4195456), ('origin', 1), ('tt_slot', 3)]:
            with self.subTest(field=field):
                result = self.query(locator=dict(self.locator, **{field: value}))
                self.assertIsNone(result['sql'])
                self.assertIn('identity', result['error'])

    def test_missing_or_non_numeric_values_cannot_become_sql(self):
        for field in ('uba0', 'uba1', 'xid', 'wrap', 'itl_kind', 'itl_slot_index',
                      'data_segment', 'tt_slot', 'origin'):
            with self.subTest(field=field):
                locator = dict(self.locator)
                del locator[field]
                self.assertIn('locator', self.query(locator=locator)['error'])
        for value in ('0); SELECT 1', '-1', '18446744073709551616', '1.5'):
            with self.subTest(value=value):
                self.assertIn('locator', self.query(locator=dict(self.locator,
                                                               uba0=value))['error'])

    def prepare_data(self, reply='244', locator=None):
        return perl(r'''my $v=decode_json(do {local $/; <STDIN>});
my @queries;
{package DataOwner; sub query_safe {push @queries,$_[1]; return $_[0]->{reply};}}
my $owner=bless {reply=>$v->{reply}},'DataOwner';
my $result=eval {M2CanonicalOrigin::prepare_data($owner,$v->{locator},$v->{key},'freeze_data')};
print encode_json({result=>$result,error=>$@,queries=>\@queries});''',
                    dict(reply=reply, locator=locator or self.locator, key=self.key))

    def test_prepare_reads_exact_uba_on_owner_without_changing_the_resolver(self):
        result = self.prepare_data()
        self.assertEqual(result['error'], '')
        # Native 16-byte UBA consumed by the existing SQL reader on this host.
        self.assertEqual(result['queries'], [
            "SELECT octet_length(cluster_undo_get_record("
            "decode('01000000010000000200040000000000','hex')))"])
        self.assertEqual(result['result'], dict(record_bytes=244, uba0='4294967297',
                                                uba1='262146', xid=4195408,
                                                authoritative=0))

    def test_prepare_refuses_identity_drift_before_any_owner_read(self):
        result = self.prepare_data(locator=dict(self.locator, xid=4195456))
        self.assertIn('identity', result['error'])
        self.assertEqual(result['queries'], [])

    def preparation_stage(self, kind, caller, reply):
        source = (HERE.parent / 't/480_pre2_m2_canonical_interleave.pl').read_text()
        start = source.index("  my $path=$remote ? 'origin' : $caller;")
        stage = source[start:source.index("  $cell->{stage}='sampling';", start)]
        locator = dict(self.locator)
        if caller == 'sample_canonical':
            locator.update(data_segment=2, uba0='4294967298')
        return perl(r'''my $v=decode_json(do {local $/; <STDIN>});
my (@events,@saved);
{package DataOwner; sub query_safe {push @events,{read=>$_[1]}; return $_[0]->{reply};}}
sub save_json {push @saved,$_[1];}
my $cell={locator=>$v->{locator},stage=>'caller_qualification'};
my ($kind,$caller,$key)=@{$v}{qw(kind caller key)};
my $remote=$caller eq 'freeze_data' || $caller eq 'sample_canonical';
my $w=bless {reply=>$v->{reply}},'DataOwner';
my ($table,$xid,$name)=('fixture',$key->{xid},'fixture');
my $error=''; my $sql;
eval {
''' + stage.replace('my $sql=', '$sql=') + r'''
push @events,{pause=>$kind},{reader=>$sql};
1;} or $error=$@;
print encode_json({events=>\@events,error=>$error,saved=>\@saved});''',
                    dict(kind=kind, caller=caller, reply=reply,
                         locator=locator, key=self.key))

    def test_real_fixture_rewarms_after_pressure_before_each_remote_pause_and_reader(self):
        # Removing the preparation call must leave no read before either pause.
        for kind in ('A1', 'A2'):
            for caller in ('freeze_data', 'sample_canonical', 'physical', 'owner'):
                with self.subTest(kind=kind, caller=caller):
                    result = self.preparation_stage(kind, caller, '244')
                    self.assertEqual(result['error'], '')
                    events = result['events']
                    if caller in ('freeze_data', 'sample_canonical'):
                        self.assertEqual(len(events), 3)
                        self.assertIn('cluster_undo_get_record', events[0]['read'])
                        self.assertEqual(events[1], dict(pause=kind))
                        self.assertIn('test_pgrac_m2_sample_locator', events[2]['reader'])
                        self.assertEqual(len(result['saved']), 1)
                    else:
                        self.assertEqual(len(events), 2)
                        self.assertEqual(events[0], dict(pause=kind))

    def test_failed_owner_read_prevents_remote_pause_and_sampling(self):
        for reply in (None, '', '0', '-1', 'unreadable', '8193'):
            with self.subTest(reply=reply):
                result = self.preparation_stage('A2', 'freeze_data', reply)
                self.assertIn('DATA preparation', result['error'])
                self.assertEqual(len(result['events']), 1)
                self.assertIn('read', result['events'][0])
                self.assertEqual(result['saved'], [])

    def test_freeze_producers_are_prepared_before_rollover_without_reordering_cells(self):
        result = perl(r'''
my (@events,@records);
my $n=0;
my @cases=map {my $kind=$_; map {{kind=>$kind,caller=>$_,number=>++$n}}
 qw(physical owner freeze_data sample_canonical)} qw(A1 A2);
M2CanonicalOrigin::run_prepared_cases(\@cases,
 sub {my ($c)=@_; push @events,"prepare$c->{number}"; return $c->{number};},
 sub {my ($c,$p)=@_; die "wrong resource" unless $p==$c->{number};
      push @events,"body$c->{number}";},
 sub {push @events,"cleanup$_[0]->{number}";},
 sub {my ($c,$r)=@_; push @records,{number=>$c->{number},%$r};});
print encode_json({events=>\@events,records=>\@records});
''')
        events = result['events']
        self.assertEqual(events[:2], ['prepare3', 'prepare7'])
        self.assertLess(events.index('prepare7'), events.index('prepare4'))
        self.assertEqual([e for e in events if e.startswith('body')],
                         ['body'+str(i) for i in range(1, 9)])
        self.assertTrue(all(r['completed'] and not r['error']
                            for r in result['records']))

    def test_failed_preparation_and_rollover_keep_each_other_cell_and_cleanup(self):
        result = perl(r'''
my (@prepared,@ran,@cleaned,@records);
my $n=0;
my @cases=map {my $kind=$_; map {{kind=>$kind,caller=>$_,number=>++$n}}
 qw(physical owner freeze_data sample_canonical)} qw(A1 A2);
push @cases,{number=>9,caller=>'physical',kind=>'CANCEL'};
M2CanonicalOrigin::run_prepared_cases(\@cases,
 sub {my ($c)=@_; push @prepared,$c->{number};
      die "prepare7 failed\n" if $c->{number}==7; return {};},
 sub {my ($c)=@_; push @ran,$c->{number}; die "rollover failed\n" if $c->{number}==4;},
 sub {push @cleaned,$_[0]->{number};},
 sub {my ($c,$r)=@_; push @records,{number=>$c->{number},%$r};});
print encode_json({prepared=>\@prepared,ran=>\@ran,cleaned=>\@cleaned,records=>\@records});
''')
        self.assertEqual(result['prepared'].count(7), 1)
        self.assertEqual(result['ran'], [1, 2, 3, 4, 5, 6, 8, 9])
        self.assertEqual(result['cleaned'], list(range(1, 10)))
        self.assertEqual(result['records'][3]['error'], 'rollover failed\n')
        self.assertEqual(result['records'][6]['error'], 'prepare7 failed\n')
        self.assertTrue(result['records'][-1]['completed'])

    def test_real_session_cleanup_keeps_the_other_prepared_producer_open(self):
        source = (HERE.parent / 't/480_pre2_m2_canonical_interleave.pl').read_text()
        helpers = source[source.index('sub session {'):source.index('sub json_result {')]
        result = perl(r'''
my %sessions;
my (@closed,@queries);
{package FixtureNode; sub background_psql {return bless {id=>++$_[0]->{id}},'FixtureSession';}}
{package FixtureSession; sub query_safe {push @queries,[$_[0]->{id},$_[1]];}
 sub quit {push @closed,$_[0]->{id};}}
''' + helpers + r'''
my $node=bless {},'FixtureNode';
session($node,{number=>3}); session($node,{number=>7});
finish_sessions({number=>3});
my @after3=@closed;
my @remaining=sort keys %sessions;
finish_sessions({number=>7});
print encode_json({after3=>\@after3,remaining=>\@remaining,closed=>\@closed,queries=>\@queries});
''')
        self.assertEqual(result['after3'], [1])
        self.assertEqual(result['remaining'], ['7'])
        self.assertEqual(result['closed'], [1, 2])
        self.assertEqual(result['queries'], [[1, 'SET statement_timeout=90000'],
                                             [2, 'SET statement_timeout=90000']])

    def test_test_module_preloads_locally_after_native_creation_before_start(self):
        source = (HERE.parent / 't/480_pre2_m2_canonical_interleave.pl').read_text()
        formation = source[source.index('$cluster=PostgreSQL::Test::ClusterPRE2'):
                           source.index('is(scalar(@nodes),4')]
        # Execute the actual fixture setup; only external cluster/node calls
        # are recorded. This is sequencing coverage, not native admission.
        result = perl(r'''
my ($cluster,@events);
{package PostgreSQL::Test::ClusterPRE2;
 sub new_cluster {
  my ($class,$name,%opts)=@_;
  push @events,{op=>'fresh_init',options=>\%opts};
  return bless {nodes=>[map {bless {id=>$_},'FixtureNode'} 0..$opts{nodes}-1]},$class;
 }
 sub nodes {return @{$_[0]->{nodes}};}
 sub start_cluster {push @events,{op=>'shared_start'};}
}
{package FixtureNode;
 sub append_conf {push @events,{op=>'append',node=>$_[0]->{id},
                               file=>$_[1],text=>$_[2]};}
}
''' + formation + r'''
print encode_json(\@events);
''')
        self.assertEqual([row['op'] for row in result],
                         ['fresh_init'] + ['append'] * 4 + ['shared_start'])
        settings = result[0]['options']['extra_conf']
        self.assertFalse(any('shared_preload_libraries' in line for line in settings))
        self.assertIn('shared_buffers=512MB', settings)
        self.assertEqual(result[0]['options']['nodes'], 4)
        for node, row in enumerate(result[1:-1]):
            self.assertEqual(row, dict(op='append', node=node, file='postgresql.conf',
                                      text="shared_preload_libraries='test_pgrac_shared_config'\n"))


if __name__ == '__main__':
    unittest.main()
