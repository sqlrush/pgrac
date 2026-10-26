#!/usr/bin/env perl
# Nine ordinary transactions share one block in the normal four-member fixture.
# Existing diagnostics select an owned client; they supply no transaction authority.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils qw(slurp_file);
use Test::More;
use JSON::PP;
use IPC::Run qw(run timeout);
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC sleep);

my ($cluster,$reader,@sessions);
END {
 eval { $_->quit } for @sessions;
 eval { $cluster->stop_cluster } if defined $cluster;
}
my $evidence=$ENV{TESTLOGDIR} // '.';
sub save_json {
 my ($name,$value)=@_;
 open my $f,'>',"$evidence/m2-b07-$name.json" or die $!;
 print {$f} JSON::PP->new->canonical->pretty->encode($value);
 close $f or die $!;
}
sub session {
 my ($node)=@_;
 my $s=$node->background_psql('postgres',timeout=>30);
 push @sessions,$s;
 $s->query_safe('SET statement_timeout=10000');
 return $s;
}
sub select_blocker {
 my ($case)=@_;
 my $input=encode_json($case);
 my ($out,$err)=('','');
 my $ok=run([$ENV{PYTHON} // 'python3',"$FindBin::RealBin/../pre2/m2_itl_capacity.py"],
  '<',\$input,'>',\$out,'2>',\$err,timeout(2));
 die "blocker evaluator produced no result: $err" unless $out ne '';
 my $result=decode_json($out);
 return ($ok ? $result->{selected} : undef,$result->{error} // $err);
}
sub activity {
 my ($node,$pid)=@_;
 return decode_json($node->safe_psql('postgres',"SELECT row_to_json(s) FROM (
 SELECT pid,state,query,wait_event FROM pg_stat_activity WHERE pid=$pid) s"));
}

$cluster=PostgreSQL::Test::ClusterPRE2->new_cluster('m2_b07',nodes=>4,blackbox=>1,
 required_operations=>[],extra_conf=>['autovacuum=off','cluster.trace_tx_identity=on',
  'log_error_verbosity=verbose','log_line_prefix=\'%m [%p] \'']);
$cluster->start_cluster;
my @nodes=$cluster->nodes;
is(scalar(@nodes),4,'four normal PRE2 members are admitted');
my $node=$nodes[0];
my @decisions;
my $completed=eval {
 $node->safe_psql('postgres',q{
  CREATE FUNCTION test_pgrac_native_itl_trace() RETURNS text
  AS '$libdir/test_pgrac_shared_config','test_pgrac_native_itl_trace'
  LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
  CREATE FUNCTION test_pgrac_native_itl_page(regclass,integer) RETURNS text
  AS '$libdir/test_pgrac_shared_config','test_pgrac_native_itl_page'
  LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
  REVOKE ALL ON FUNCTION test_pgrac_native_itl_trace() FROM PUBLIC;
  REVOKE ALL ON FUNCTION test_pgrac_native_itl_page(regclass,integer) FROM PUBLIC;
  CREATE TABLE m2_b07(id int PRIMARY KEY,v int);
  INSERT INTO m2_b07 SELECT g,0 FROM generate_series(1,9) g;
 });
 $node->safe_psql('postgres','VACUUM(FREEZE) m2_b07');
 my @tids=split /\n/,$node->safe_psql('postgres','SELECT id,ctid FROM m2_b07 ORDER BY id');
 my %blocks;
 for my $tid (@tids) {
  die "invalid original row location: $tid" unless $tid =~ /^\d+\|\((\d+),\d+\)$/;
  $blocks{$1}=1;
 }
 die 'the original nine rows do not share one heap block' unless @tids==9 && keys(%blocks)==1;
 my ($block)=keys %blocks;
 my $tagtext=$node->safe_psql('postgres',"SELECT
 COALESCE(NULLIF(c.reltablespace,0),d.dattablespace)||'|'||d.oid||'|'||
 pg_relation_filenode(c.oid)||'|0|$block' FROM pg_class c,pg_database d
 WHERE c.oid='m2_b07'::regclass AND d.datname=current_database()");
 my @tag=map {0+$_} split /\|/,$tagtext;
 die 'missing full relation tag' unless @tag==5;
 save_json('setup',{tag=>\@tag,ctids=>\@tids,producer_node=>0,waiter_node=>0,passive_nodes=>[1,2,3]});
 my (@holders,%by_pid);
 for my $row (1..8) {
  my $s=session($node);
  my $pid=0+$s->query_safe('SELECT pg_backend_pid()');
  $s->query_safe("BEGIN; UPDATE m2_b07 SET v=1 WHERE id=$row");
  my $xid=0+$s->query_safe('SELECT pg_current_xact_id()::text');
  push @holders,{row=>$row,node=>0,pid=>$pid,xid=>$xid};
  $by_pid{$pid}=$s;
 }
 save_json('holders',\@holders);
 pass('eight real UPDATE transactions remain open on distinct rows');
 $reader=session($node);
 my $pid=0+$reader->query_safe('SELECT pg_backend_pid()');
 $reader->query_safe('BEGIN');
 my $sql='UPDATE m2_b07 SET v=1 WHERE id=9;';
 my $offset=-s $node->logfile;
 my $until=clock_gettime(CLOCK_MONOTONIC)+10;
 $reader->query_until(qr/B07_SENT/,"\\echo B07_SENT\n$sql\n");
 my $done=0;
 my $last_error='no current exact capacity wait';
 while(clock_gettime(CLOCK_MONOTONIC)<$until) {
  # Pump queued client input/output without issuing another query or
  # renewing its timer. A pre-query echo never counts as server execution.
  $reader->{run}->pump_nb();
  my $state=activity($node,$pid);
  save_json('activity-latest',$state);
  if($state->{state} eq 'idle in transaction' && $state->{query} eq $sql) {
   $done=1; last;
  }
  if(($state->{wait_event} // '') eq 'GesTxEnqueueWait') {
   my $page=decode_json($node->safe_psql('postgres',"SELECT test_pgrac_native_itl_page('m2_b07',$block)"));
   die 'native copied page was not recorded' unless $page->{stored} && $page->{page_known};
   my $trace=decode_json($node->safe_psql('postgres','SELECT test_pgrac_native_itl_trace()'));
   my $case={holders=>\@holders,trace=>$trace,page_sequence=>$page->{sequence},tag=>\@tag,
    log=>substr(slurp_file($node->logfile),$offset),waiter_pid=>$pid,waiter_node=>0};
   save_json('candidate-latest',$case);
   my ($selected,$error)=select_blocker($case);
   $last_error=$error;
   if(defined($selected)) {
    die 'more than the original eight owners were selected' if @decisions>=8;
    save_json('selection-'.scalar(@decisions),{evidence=>$case,selected=>$selected,activity=>$state});
    # Commit precisely this still-owned real transaction. Remaining owners
    # are kept open until independently selected or the ninth UPDATE finishes.
    $by_pid{$selected->{pid}}->query_safe('COMMIT');
    push @decisions,$selected;
    @holders=grep { $_->{pid} != $selected->{pid} } @holders;
    save_json('decisions',\@decisions);
   }
  }
  sleep(0.02);
 }
 die "ninth UPDATE did not complete under its original deadline: $last_error" unless $done;
 die 'the ninth transaction never entered an exact capacity wait' unless @decisions;
 pass('ninth UPDATE waited on observed exact owners and resumed without renewing its deadline');
 is($reader->query_safe('SELECT v FROM m2_b07 WHERE id=9'),'1','ninth UPDATE completed in its original transaction');
 for my $holder (@holders) {
  my $state=activity($node,$holder->{pid});
  is($state->{state},'idle in transaction','unselected owner remained open through waiter progress');
  $by_pid{$holder->{pid}}->query_safe('COMMIT');
 }
 $reader->query_safe('COMMIT');
 my $expected=join "\n",map {"$_|1"} 1..9;
 for my $n (@nodes) {
  is($n->safe_psql('postgres','SELECT id,v FROM m2_b07 ORDER BY id'),$expected,
   'all nine committed values reconcile on a normal member');
 }
 1;
};
my $error=$@;
if(defined($reader)) {
 save_json('reader-client',{map { $_=>$reader->{$_} } qw(stdin stdout stderr)});
}
save_json('execution',{completed=>$completed ? JSON::PP::true : JSON::PP::false,
 error=>$error,decisions=>\@decisions});
if(!$completed) { fail('B07 native cell completed'); diag($error); }
my @cleanup;
for my $s (@sessions) { eval { $s->quit; 1; } or push @cleanup,$@; }
@sessions=();
save_json('cleanup',{errors=>\@cleanup});
ok(!@cleanup,'all original test sessions close');
$cluster->stop_cluster;
undef $cluster;
pass('all four normal members complete the original stop proof');
for my $n (@nodes) {
 unlike(slurp_file($n->logfile),qr/PANIC|checksum verification failed|invalid page in block/,
  'member has no corruption or PANIC');
}
done_testing();
