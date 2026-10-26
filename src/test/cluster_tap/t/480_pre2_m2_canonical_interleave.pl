#!/usr/bin/env perl
# Four normal members, with scheduling and data checks on nodes 0 and 1.
# No authority or state is injected.
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

my $cluster;
my @sessions;
END {
 eval { $_->quit } for @sessions;
 eval { $cluster->stop_cluster } if defined $cluster;
}
my $pause = 12000000;
my $evidence = $ENV{TESTLOGDIR} // '.';
sub save_json {
 my ($name,$value)=@_;
 open my $f,'>',"$evidence/m2-$name.json" or die $!;
 print {$f} JSON::PP->new->canonical->pretty->encode($value);
 close $f or die $!;
}
sub session {
 my ($node)=@_;
 my $s=$node->background_psql('postgres',timeout=>180);
 push @sessions,$s;
 $s->query_safe('SET statement_timeout=90000');
 return $s;
}
sub finish_sessions {
 $_->quit for @sessions;
 @sessions=();
}
sub json_result {
 my ($text)=@_;
 my @lines=grep { /^\{"outcome":/ } split /\n/,$text;
 die "missing native provider result: $text" unless @lines == 1;
 return decode_json($lines[0]);
}
sub result_pair {
 my ($r)=@_;
 return {outcome=>0+$r->{outcome},scn=>0+$r->{scn}};
}
sub logs_since {
 my ($nodes,$offset)=@_;
 return join "\n",map { substr(slurp_file($nodes->[$_]->logfile),$offset->[$_]) } 0..$#$nodes;
}
sub pause_witness {
 my ($node,$pid,$pattern,$offset)=@_;
 my $until=clock_gettime(CLOCK_MONOTONIC)+5;
 while(clock_gettime(CLOCK_MONOTONIC)<$until) {
  my $log=substr(slurp_file($node->logfile),$offset);
  my ($actual) = defined($pid) ? ($pid) : ($log =~ $pattern);
  if(defined($actual) && $log =~ $pattern &&
     $node->safe_psql('postgres',"SELECT count(*) FROM pg_stat_activity WHERE pid=$actual AND wait_event='PgSleep'") eq '1') {
   return 0+$actual;
  }
  sleep(0.03);
 }
 return undef;
}
sub producer_key {
 my ($trace,$xid)=@_;
 my @rows=grep { $_->{kind} == 2 && $_->{key_known} && $_->{key}{xid} == $xid }
  @{$trace->{events}};
 return {} unless $trace->{export_complete} && !$trace->{overflow} && @rows == 1;
 my $k=$rows[0]{key};
 return {xid=>0+$k->{xid},origin=>0+$k->{origin_node_id},segment=>0+$k->{segment_id},
  slot=>0+$k->{slot_offset},wrap=>0+$k->{slot_wrap},generation=>0+$k->{segment_generation},
  root=>0+$k->{root_id},root_generation=>0+$k->{root_generation}};
}
sub judge {
 my ($name,$case,$log)=@_;
 $case->{raw_log}=$log;
 my $input=encode_json($case);
 my ($out,$err)=('','');
 # Parsing and adjudication are shared with the negative host tests.
 my $code=q{import json,sys; from m2_canonical_matrix import traces,check_a1,check_a2
c=json.load(sys.stdin); c['traces']=traces(c.pop('raw_log'),c['xid'],c['caller'],c.get('sample_pid'))
ok,reason=(check_a1 if c['kind']=='A1' else check_a2)(c)
print(json.dumps(dict(passed=ok,reason=reason,case=c)))};
 local $ENV{PYTHONPATH}="$FindBin::RealBin/../pre2";
 run([$ENV{PYTHON} // 'python3','-c',$code],'<',\$input,'>',\$out,'2>',\$err,timeout(10))
  or die "matrix evidence evaluator failed: $err";
 my $r=decode_json($out);
 save_json($name,$r);
 ok($r->{passed},"$name: $r->{reason}");
}
sub reload_pause {
 my ($node,$armed)=@_;
 my $value=$armed ? "cluster-tx-canonical-copy:sleep:$pause" : '';
 $node->append_conf('postgresql.conf',"cluster.injection_points='$value'\n");
 $node->safe_psql('postgres','SELECT pg_reload_conf()');
 # Only the existing SIGHUP assign hook arms LMS. No SQL backend arm is
 # claimed as an LMS arm; actual PID, same-xid COPY and PgSleep are required.
 sleep(0.2);
}

$cluster=PostgreSQL::Test::ClusterPRE2->new_cluster('m2_a08',nodes=>4,blackbox=>1,
 required_operations=>[],extra_conf=>['autovacuum=off','cluster.trace_tx_identity=on',
  'log_error_verbosity=verbose','log_line_prefix=\'%m [%p] \'']);
$cluster->start_cluster;
my @nodes=$cluster->nodes;
is(scalar(@nodes),4,'original PRE2 formation admits four normal members');
my ($a,$b)=@nodes;
my @active=($a,$b);
save_json('formation',{
 open=>[map { $cluster->observe('open_observation',{node=>$_}) } 0..$#nodes],
 producer_node=>0,peer_reader_node=>1,passive_nodes=>[2,3],
 # These two providers are local-origin APIs. Their direct diagnostic
 # calls stay on node 0; origin RPC readers run on node 1.
 local_providers=>[qw(physical owner)],remote_providers=>[qw(freeze_data sample_canonical)]});
$a->safe_psql('postgres',q{
 CREATE FUNCTION test_pgrac_native_itl_trace() RETURNS text
 AS '$libdir/test_pgrac_shared_config', 'test_pgrac_native_itl_trace'
 LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
 CREATE FUNCTION test_pgrac_m2_sample(text,regclass,integer,xid) RETURNS text
 AS '$libdir/test_pgrac_shared_config', 'test_pgrac_m2_sample'
 LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
 CREATE FUNCTION test_pgrac_m2_guards_clear() RETURNS boolean
 AS '$libdir/test_pgrac_shared_config', 'test_pgrac_m2_guards_clear'
 LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
 REVOKE ALL ON FUNCTION test_pgrac_native_itl_trace() FROM PUBLIC;
 REVOKE ALL ON FUNCTION test_pgrac_m2_sample(text,regclass,integer,xid) FROM PUBLIC;
 REVOKE ALL ON FUNCTION test_pgrac_m2_guards_clear() FROM PUBLIC;
});

# Real DATA rollover is induced with one ordinary transaction. The padding
# is stored inline; no TT/UBA/generation setter or forced rollover is used.
# The exact caller still has to be witnessed; a failed rollover is NOT_REACHED.
$a->safe_psql('postgres',q{SET cluster.trace_tx_identity=off;
 CREATE TABLE m2_roll(id int,v int,pad text);
 ALTER TABLE m2_roll ALTER pad SET STORAGE PLAIN;
 INSERT INTO m2_roll SELECT i,0,repeat(md5(i::text),215) FROM generate_series(1,10000)i;
},timeout=>180);
$a->safe_psql('postgres','VACUUM(FREEZE) m2_roll');
$a->safe_psql('postgres','CHECKPOINT');

my $number=0;
for my $kind (qw(A1 A2)) {
 for my $caller (qw(physical owner freeze_data sample_canonical)) {
  my $name=lc("$kind-$caller");
  my $table='m2_case_'.++$number;
  $a->safe_psql('postgres',"CREATE TABLE $table(id int,v int); INSERT INTO $table VALUES(1,10)");
  $a->safe_psql('postgres',"VACUUM(FREEZE) $table");
  my $w=session($a);
  my $remote=$caller eq 'freeze_data' || $caller eq 'sample_canonical';
  my $r=session($remote ? $b : $a);
  my $wpid=0+$w->query_safe('SELECT pg_backend_pid()');
  my $rpid=0+$r->query_safe('SELECT pg_backend_pid()');
  $w->query_safe("BEGIN; UPDATE $table SET v=20 WHERE id=1");
  my $xid=0+$w->query_safe('SELECT pg_current_xact_id()::text');
  if($caller eq 'sample_canonical') {
   $w->query_safe("SET cluster.trace_tx_identity=off; UPDATE m2_roll SET v=v+1; SET cluster.trace_tx_identity=on; UPDATE $table SET v=21 WHERE id=1");
  }
  my $before=decode_json($a->safe_psql('postgres','SELECT test_pgrac_native_itl_trace()'));
  my $key=producer_key($before,$xid);
  my @offset=map {-s $_->logfile} @nodes;
  my $path=$remote ? 'origin' : $caller;
  my $sql="SELECT test_pgrac_m2_sample('$path','$table',0,'$xid'::xid)";
  my $sample_pid;
  my $reached=0;
  my $hits;
  if($kind eq 'A1') {
   if($remote) { reload_pause($a,1); }
   else { $r->query_safe("SELECT cluster_inject_fault('cluster-tx-canonical-copy','sleep',$pause)"); }
   $r->query_until(qr/READER_SENT/,"\\echo READER_SENT\n$sql;\n");
   $sample_pid=pause_witness($a,$remote ? undef : $rpid,
    qr/TX_CANONICAL phase=COPY caller=$caller pid=(\d+) .*xid=$xid /,$offset[0]);
   $reached=defined($sample_pid);
   $w->query_until(qr/COMMIT_SENT/,"\\echo COMMIT_SENT\nCOMMIT;\n");
  } else {
   $w->query_safe("SELECT cluster_inject_fault('cluster-tx-abort-tt-after','sleep',$pause)");
   $w->query_until(qr/ABORT_SENT/,"\\echo ABORT_SENT\nROLLBACK;\n");
   # Producer pause is verified by its own PID and original wait event.
   my $until=clock_gettime(CLOCK_MONOTONIC)+5;
   while(clock_gettime(CLOCK_MONOTONIC)<$until) {
    if($a->safe_psql('postgres',"SELECT count(*) FROM pg_stat_activity WHERE pid=$wpid AND wait_event='PgSleep'") eq '1') { $reached=1; last; }
    sleep(0.03);
   }
   $r->query_until(qr/READER_SENT/,"\\echo READER_SENT\n$sql;\n");
  }
  my $result=json_result($r->query_safe("SELECT 'READER_DONE'"));
  $w->query_safe("SELECT 'WRITER_DONE'");
  if($kind eq 'A1' && !$remote) {
   $hits=$r->query_safe("SELECT hits||'|'||fault_type||'|'||param FROM pg_stat_cluster_injections WHERE name='cluster-tx-canonical-copy'");
   $reached=0 unless $hits eq '1|none|0';
  } elsif($kind eq 'A2') {
   $hits=$w->query_safe("SELECT hits||'|'||fault_type||'|'||param FROM pg_stat_cluster_injections WHERE name='cluster-tx-abort-tt-after'");
   $reached=0 unless $hits eq '1|none|0';
  }
  reload_pause($a,0) if $kind eq 'A1' && $remote;
  my $log=logs_since(\@nodes,\@offset);
  my $after=decode_json($a->safe_psql('postgres','SELECT test_pgrac_native_itl_trace()'));
  my @terminal=grep { $_->{kind} == 5 && $_->{native_binding_known} &&
   $_->{native_binding}{xid} == $xid } @{$after->{events}};
  my $scn=@terminal == 1 ? 0+$terminal[0]{tt_slot}{commit_scn} : 0;
  my $fresh=json_result($r->query_safe($sql));
  my $expected=$kind eq 'A1' ? ($caller eq 'sample_canonical' ? 21 : 20) : 10;
  my @values=map { $_->safe_psql('postgres',"SELECT id,v FROM $table ORDER BY id") } @active;
  my $case={kind=>$kind,caller=>$caller,xid=>$xid,key=>$key,
   producer_node=>0,probe_node=>$remote ? 1 : 0,value_nodes=>[0,1],
   producer_pid=>$wpid,reader_pid=>$rpid,reached=>$reached ? JSON::PP::true : JSON::PP::false,
   guard_clear=>($result->{guard_clear} && $fresh->{guard_clear}) ? JSON::PP::true : JSON::PP::false,
   values_correct=>(!grep { $_ ne "1|$expected" } @values) ? JSON::PP::true : JSON::PP::false,
   result=>result_pair($result),fresh_result=>result_pair($fresh),producer_scn=>$scn,
   producer_complete=>($before->{export_complete} && !$before->{overflow} &&
    $after->{export_complete} && !$after->{overflow}) ? JSON::PP::true : JSON::PP::false,
   values=>\@values,pause_hits=>$hits,producer_before=>$before,producer_after=>$after};
  $case->{sample_pid}=$sample_pid if defined $sample_pid;
  judge($name,$case,$log);
  finish_sessions();
 }
}

# Cancel the actual reader inside its original SCUR scope. Check the same
# backend after ERROR and then exercise the same logical row again.
$a->safe_psql('postgres','CREATE TABLE m2_cancel(id int,v int); INSERT INTO m2_cancel VALUES(1,10)');
$a->safe_psql('postgres','VACUUM(FREEZE) m2_cancel');
my $w=session($a);
my $r=session($a);
$w->query_safe('BEGIN; UPDATE m2_cancel SET v=20');
my $xid=0+$w->query_safe('SELECT pg_current_xact_id()::text');
my $rpid=0+$r->query_safe('SELECT pg_backend_pid()');
$r->query_safe("\\set ON_ERROR_STOP 0\nSELECT cluster_inject_fault('cluster-tx-canonical-copy','sleep',$pause)");
my $offset=-s $a->logfile;
$r->query_until(qr/CANCEL_SENT/,"\\echo CANCEL_SENT\nSELECT test_pgrac_m2_sample('physical','m2_cancel',0,'$xid'::xid);\n");
my $hit=pause_witness($a,$rpid,qr/TX_CANONICAL phase=COPY caller=physical pid=$rpid .*xid=$xid /,$offset);
my $cancel=$a->safe_psql('postgres',"SELECT pg_cancel_backend($rpid)");
my ($cancel_out,$cancel_error)=$r->query("\\echo CANCEL_SQLSTATE :SQLSTATE\nSELECT test_pgrac_m2_guards_clear()");
save_json('cancel',{reached=>defined($hit) ? JSON::PP::true : JSON::PP::false,
 cancel=>$cancel,output=>$cancel_out,error=>$cancel_error,stderr=>$r->{stderr}});
ok(defined($hit) && $cancel eq 't' && $cancel_out =~ /CANCEL_SQLSTATE 57014/ &&
 $cancel_out =~ /\nt\s*$/,'reader cancellation has SQLSTATE 57014 and leaves no guard in the same backend');
$w->query_safe('ROLLBACK');
is($b->safe_psql('postgres','UPDATE m2_cancel SET v=30; SELECT id,v FROM m2_cancel'),'1|30',
 'same row remains writable from the peer after cancellation cleanup');
finish_sessions();
$cluster->stop_cluster;
undef $cluster;
pass('all four native members complete the original normal-stop proof');
for my $node (@nodes) {
 unlike(slurp_file($node->logfile),qr/PANIC|checksum verification failed|invalid page in block/,
  'closed native member has no corruption or PANIC');
}
done_testing();
