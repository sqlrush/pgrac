#!/usr/bin/env perl
# Four normal members, with scheduling and data checks on nodes 0 and 1.
# No authority or state is injected.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use lib "$FindBin::RealBin/../pre2";
use M2CanonicalCases qw(run_cases);
use M2CanonicalOrigin;
use M2CanonicalRollover qw(rollover_workload check_rollover_budget);
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils qw(slurp_file);
use Test::More;
use JSON::PP;
use IPC::Run qw(run timeout);
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC sleep);

my $cluster;
my %sessions;
my %pause_controls;
END {
 eval { $_->quit } for map { @$_ } values %sessions;
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
 my ($node,$cell)=@_;
 my $s=$node->background_psql('postgres',timeout=>180);
 push @{$sessions{$cell->{number}}},$s;
 $s->query_safe('SET statement_timeout=90000');
 return $s;
}
sub finish_sessions {
 my ($cell)=@_;
 my @errors;
 for my $s (@{delete($sessions{$cell->{number}}) // []}) {
  eval { $s->quit; 1; } or push @errors,$@;
 }
 die join('',@errors) if @errors;
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
sub start_reader {
 my ($cell,$reader,$sql)=@_;
 $reader->query_until(qr/READER_SENT/,"\\echo READER_SENT\n$sql;\n");
 # This echo precedes SQL execution. Queued bytes and the requested SQL
 # are client facts only; COPY/PgSleep remain the original pause witness.
 save_json("reader-echo-$cell->{number}",{
  (map { $_=>$cell->{$_} } qw(number kind caller xid producer_pid reader_pid)),
  phase=>'CLIENT_ECHO_SEEN',query=>$sql,pending_input_bytes=>length($reader->{stdin}),
  monotonic=>clock_gettime(CLOCK_MONOTONIC),authoritative=>JSON::PP::false});
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
sub remote_pause {
 my ($cell,$writer,$armed)=@_;
 $pause_controls{$cell->{number}}=$writer if $armed;
 my $duration=$armed ? $pause : 0;
 my $receipt=decode_json($writer->query_safe(
  "SELECT test_pgrac_m2_pause(0,'$cell->{caller}','$cell->{xid}'::xid,$duration)"));
 $cell->{$armed ? 'pause_arm' : 'pause_disarm'}=$receipt;
 save_json("pause-control-$cell->{number}-".($armed ? 'arm' : 'disarm'),
  {context=>$cell,receipt=>$receipt,authoritative=>JSON::PP::false});
 delete $pause_controls{$cell->{number}} unless $armed;
 return $receipt;
}

$cluster=PostgreSQL::Test::ClusterPRE2->new_cluster('m2_a08',nodes=>4,blackbox=>1,
 required_operations=>[],extra_conf=>['shared_buffers=512MB','autovacuum=off','cluster.trace_tx_identity=on',
  'log_error_verbosity=verbose','log_line_prefix=\'%m [%p] \'']);
my @nodes=$cluster->nodes;
# Preload is local postmaster setup after the original native creation;
# it is not a supported shared bootstrap-profile parameter.
$_->append_conf('postgresql.conf',"shared_preload_libraries='test_pgrac_shared_config'\n") for @nodes;
$cluster->start_cluster;
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
 CREATE FUNCTION test_pgrac_m2_locator(regclass,integer,xid) RETURNS text
 AS '$libdir/test_pgrac_shared_config', 'test_pgrac_m2_locator'
 LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
 CREATE FUNCTION test_pgrac_m2_sample_locator(bigint,bigint,xid,integer,integer,integer) RETURNS text
 AS '$libdir/test_pgrac_shared_config', 'test_pgrac_m2_sample_locator'
 LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
 CREATE FUNCTION test_pgrac_m2_guards_clear() RETURNS boolean
 AS '$libdir/test_pgrac_shared_config', 'test_pgrac_m2_guards_clear'
 LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
 CREATE FUNCTION test_pgrac_m2_limits() RETURNS text
 AS '$libdir/test_pgrac_shared_config', 'test_pgrac_m2_limits'
 LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
 CREATE FUNCTION test_pgrac_m2_pause(integer,text,xid,integer) RETURNS text
 AS '$libdir/test_pgrac_shared_config', 'test_pgrac_m2_pause'
 LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
 REVOKE ALL ON FUNCTION test_pgrac_native_itl_trace() FROM PUBLIC;
 REVOKE ALL ON FUNCTION test_pgrac_m2_sample(text,regclass,integer,xid) FROM PUBLIC;
 REVOKE ALL ON FUNCTION test_pgrac_m2_locator(regclass,integer,xid) FROM PUBLIC;
 REVOKE ALL ON FUNCTION test_pgrac_m2_sample_locator(bigint,bigint,xid,integer,integer,integer) FROM PUBLIC;
 REVOKE ALL ON FUNCTION test_pgrac_m2_guards_clear() FROM PUBLIC;
 REVOKE ALL ON FUNCTION test_pgrac_m2_limits() FROM PUBLIC;
 REVOKE ALL ON FUNCTION test_pgrac_m2_pause(integer,text,xid,integer) FROM PUBLIC;
});

# A short-lived SQL connection must not leave a shared arm behind.
my $control=$a->background_psql('postgres');
my $exit_arm=decode_json($control->query_safe(
 "SELECT test_pgrac_m2_pause(0,'freeze_data','3'::xid,1)"));
$control->quit;
$a->poll_query_until('postgres',
 "SELECT NOT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$exit_arm->{owner_pid})")
 or die 'pause owner did not exit';
my @control_rows=split /\n/,$a->safe_psql('postgres',q{
 SELECT test_pgrac_m2_pause(0,'freeze_data','3'::xid,1);
 SELECT test_pgrac_m2_pause(0,'freeze_data','3'::xid,0);
});
my $exit_rearm=decode_json($control_rows[0]);
my $exit_disarm=decode_json($control_rows[1]);
save_json('pause-owner-exit',{arm=>$exit_arm,rearm=>$exit_rearm,disarm=>$exit_disarm});
ok($exit_rearm->{armed} && $exit_disarm->{disarmed} && !$exit_disarm->{consumed},
 'test pause arm clears on owner exit and supports explicit disarm');

# Request enough DATA for rollover in one ordinary transaction. The padding
# is stored inline; no TT/UBA/generation setter or forced rollover is used.
# The exact caller still has to be witnessed; a failed rollover is NOT_REACHED.
my $rollover=rollover_workload();
save_json('rollover-workload',$rollover);
$a->safe_psql('postgres',$rollover->{setup_sql},timeout=>180);
$a->safe_psql('postgres','VACUUM(FREEZE) m2_roll');
$a->safe_psql('postgres','CHECKPOINT');

my $number=0;
my @matrix=map {
 my $kind=$_;
 map { {kind=>$kind,caller=>$_,number=>++$number} }
  qw(physical owner freeze_data sample_canonical)
} qw(A1 A2);
sub record_cell {
 my ($cell,$execution)=@_;
 my $name=lc("$cell->{kind}-$cell->{caller}");
 if(defined($cell->{xid}) && defined($cell->{log_offsets})) {
  my $log=logs_since(\@nodes,$cell->{log_offsets});
  my @trace=grep { /(?:TX_CANONICAL|M2_PROBE) / && /xid=$cell->{xid} / } split /\n/,$log;
  save_json("$name-caller-trace",{context=>$cell,rows=>\@trace,authoritative=>JSON::PP::false});
 }
 save_json("$name-execution",{context=>$cell,%$execution});
 if(!$execution->{completed} || $execution->{cleanup_error} ne '') {
  fail("$name: cell execution or cleanup failed");
  diag($execution->{error}.$execution->{cleanup_error});
 }
}
sub cleanup_cell {
 my ($cell)=@_;
 my @errors;
 # The controlling writer stays alive until explicit shared-flag disarm.
 if($cell->{pause_armed}) {
  eval { remote_pause($cell,$pause_controls{$cell->{number}},0); 1; } or push @errors,$@;
 }
 eval { finish_sessions($cell); 1; } or push @errors,$@;
 die join('',@errors) if @errors;
}
sub prepare_case {
  my ($cell)=@_;
  my ($kind,$caller,$number)=@{$cell}{qw(kind caller number)};
  my $name=lc("$kind-$caller");
  my $table='m2_case_'.$number;
  $cell->{stage}='setup';
  $a->safe_psql('postgres',"CREATE TABLE $table(id int,v int); INSERT INTO $table VALUES(1,10)");
  $a->safe_psql('postgres',"VACUUM(FREEZE) $table");
  my $w=session($a,$cell);
  my $remote=$caller eq 'freeze_data' || $caller eq 'sample_canonical';
  my $r=session($remote ? $b : $a,$cell);
  my $wpid=0+$w->query_safe('SELECT pg_backend_pid()');
  my $rpid=0+$r->query_safe('SELECT pg_backend_pid()');
  $w->query_safe("BEGIN; UPDATE $table SET v=20 WHERE id=1");
  my $xid=0+$w->query_safe('SELECT pg_current_xact_id()::text');
  @{$cell}{qw(xid producer_pid reader_pid)}=($xid,$wpid,$rpid);
  $cell->{stage}='rollover_preflight' if $caller eq 'sample_canonical';
  save_json("context-$number",$cell);
  if($caller eq 'sample_canonical') {
   # Measure this producer's actual input and policy before the ordinary
   # UPDATE. Preserve raw facts even if preflight fails this isolated cell.
   my $limits=decode_json($w->query_safe('SELECT test_pgrac_m2_limits()'));
   $w->query_safe('SET cluster.trace_tx_identity=off');
   my $stats=decode_json($w->query_safe(q{SELECT json_build_object(
    'rows',count(*),'min_tuple_bytes',min(pg_column_size(r)),
    'max_tuple_bytes',max(pg_column_size(r))) FROM m2_roll r}));
   $w->query_safe('SET cluster.trace_tx_identity=on');
   $cell->{rollover_limits}=$limits;
   $cell->{rollover_stats}=$stats;
   save_json("rollover-budget-$number",{limits=>$limits,stats=>$stats,authoritative=>JSON::PP::false});
   $cell->{rollover_budget}=check_rollover_budget($limits,$stats);
   $cell->{stage}='rollover';
   save_json("context-$number",$cell);
   $w->query_safe("SET cluster.trace_tx_identity=off; UPDATE m2_roll SET v=v+1; SET cluster.trace_tx_identity=on; UPDATE $table SET v=21 WHERE id=1");
  }
  if($remote) {
   $cell->{stage}='locator_prepare';
   # Copy the actual producer page before any pause/abort. No peer heap
   # acquisition or process-local locator cache belongs in the timed window.
   $cell->{locator}=decode_json($w->query_safe("SELECT test_pgrac_m2_locator('$table',0,'$xid'::xid)"));
   save_json("locator-$number",{context=>$cell,authoritative=>JSON::PP::false});
  }
  return {writer=>$w,reader=>$r};
}
M2CanonicalOrigin::run_prepared_cases(\@matrix,\&prepare_case,sub {
  my ($cell,$prepared)=@_;
  my ($kind,$caller,$number,$xid,$wpid,$rpid)=@{$cell}{qw(kind caller number xid producer_pid reader_pid)};
  my $name=lc("$kind-$caller");
  my $table='m2_case_'.$number;
  my ($w,$r)=@{$prepared}{qw(writer reader)};
  my $remote=$caller eq 'freeze_data' || $caller eq 'sample_canonical';
  my $before=decode_json($a->safe_psql('postgres','SELECT test_pgrac_native_itl_trace()'));
  my $key=producer_key($before,$xid);
  my @offset=map {-s $_->logfile} @nodes;
  $cell->{log_offsets}=\@offset;
  $cell->{key}=$key;
  $cell->{stage}='caller_qualification';
  save_json("context-$number",$cell);
  my $path=$remote ? 'origin' : $caller;
  my $sql=$remote ? M2CanonicalOrigin::origin_query($cell->{locator},$key,$caller)
   : "SELECT test_pgrac_m2_sample('$path','$table',0,'$xid'::xid)";
  if($remote) {
   $cell->{stage}='data_resident_prepare';
   $cell->{data_prepare}=M2CanonicalOrigin::prepare_data($w,$cell->{locator},$key,$caller);
   save_json("data-prepare-$number",{kind=>$kind,caller=>$caller,key=>$key,
    locator=>$cell->{locator},producer_node=>0,producer_pid=>$wpid,
    preparation=>$cell->{data_prepare},authoritative=>JSON::PP::false});
  }
  $cell->{stage}='sampling';
  save_json("context-$number",$cell);
  note("M2_CALLER_REQUEST case=$number kind=$kind caller=$caller xid=$xid producer=$wpid reader=$rpid path=$path");
  my $sample_pid;
  my $reached=0;
  my $hits;
  if($kind eq 'A1') {
   if($remote) { $cell->{pause_armed}=1; remote_pause($cell,$w,1); }
   else { $r->query_safe("SELECT cluster_inject_fault('cluster-tx-canonical-copy','sleep',$pause)"); }
   start_reader($cell,$r,$sql);
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
   start_reader($cell,$r,$sql);
  }
  my $result=json_result($r->query_safe("SELECT 'READER_DONE'"));
  $cell->{probe_results}{initial}=$result;
  save_json("context-$number",$cell);
  $w->query_safe("SELECT 'WRITER_DONE'");
  if($kind eq 'A1' && !$remote) {
   $hits=$r->query_safe("SELECT hits||'|'||fault_type||'|'||param FROM pg_stat_cluster_injections WHERE name='cluster-tx-canonical-copy'");
   $reached=0 unless $hits eq '1|none|0';
  } elsif($kind eq 'A2') {
   $hits=$w->query_safe("SELECT hits||'|'||fault_type||'|'||param FROM pg_stat_cluster_injections WHERE name='cluster-tx-abort-tt-after'");
   $reached=0 unless $hits eq '1|none|0';
  }
  if($kind eq 'A1' && $remote) {
   my $disarm=remote_pause($cell,$w,0);
   $reached=0 unless $disarm->{disarmed} && $disarm->{consumed} &&
    $disarm->{lease} eq $cell->{pause_arm}{lease};
   $cell->{pause_armed}=0;
  }
  my $log=logs_since(\@nodes,\@offset);
  my $after=decode_json($a->safe_psql('postgres','SELECT test_pgrac_native_itl_trace()'));
  my @terminal=grep { $_->{kind} == 5 && $_->{native_binding_known} &&
   $_->{native_binding}{xid} == $xid } @{$after->{events}};
  my $scn=@terminal == 1 ? 0+$terminal[0]{tt_slot}{commit_scn} : 0;
  my $fresh=json_result($r->query_safe($sql));
  $cell->{probe_results}{fresh}=$fresh;
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
  $cell->{stage}='judged';
},\&cleanup_cell,\&record_cell);

# Cancel the actual reader inside its original SCUR scope. Check the same
# backend after ERROR and then exercise the same logical row again.
run_cases([{kind=>'CANCEL',caller=>'physical',number=>9}],sub {
my ($cell)=@_;
$cell->{stage}='cancel';
$a->safe_psql('postgres','CREATE TABLE m2_cancel(id int,v int); INSERT INTO m2_cancel VALUES(1,10)');
$a->safe_psql('postgres','VACUUM(FREEZE) m2_cancel');
my $w=session($a,$cell);
my $r=session($a,$cell);
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
$cell->{stage}='judged';
},\&cleanup_cell,\&record_cell);
$cluster->stop_cluster;
undef $cluster;
pass('all four native members complete the original normal-stop proof');
for my $node (@nodes) {
 unlike(slurp_file($node->logfile),qr/PANIC|checksum verification failed|invalid page in block/,
  'closed native member has no corruption or PANIC');
}
done_testing();
