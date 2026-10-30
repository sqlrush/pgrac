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
 my ($case,$option)=@_;
 my $input=encode_json($case);
 my ($out,$err)=('','');
 my $ok=run([$ENV{PYTHON} // 'python3',"$FindBin::RealBin/../pre2/m2_itl_capacity.py",
  defined($option) ? ($option) : ()],
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

sub exit_owners {
 my ($node,$holders)=@_;
 return [map {
  my $h=$_;
  my $state=decode_json($node->safe_psql('postgres',"SELECT row_to_json(s) FROM (
   SELECT pid,state,wait_event,backend_xid::text AS xid FROM pg_stat_activity
   WHERE pid=$h->{pid}) s"));
  die 'an original owner disappeared' unless defined($state);
  +{%$h,%$state};
 } @$holders];
}

sub exact_exit_wait {
 my ($node,$r,$pid,$sql,$block,$tag,$holders,$offset,$until,$name)=@_;
 my $last='no exact capacity wait';
 while(clock_gettime(CLOCK_MONOTONIC)<$until) {
  $r->{run}->pump_nb();
  my $state=activity($node,$pid);
  save_json("b08-$name-activity",$state);
  die 'waiter disappeared' unless defined($state);
  return (undef,undef)
   if $state->{state} eq 'idle in transaction' && $state->{query} eq $sql;
  die 'waiter failed before the exact wait was observed'
   if $state->{state} eq 'idle in transaction (aborted)';
  if($state->{query} eq $sql && ($state->{wait_event} // '') eq 'GesTxEnqueueWait') {
   my $page=decode_json($node->safe_psql('postgres',"SELECT test_pgrac_native_itl_page('$name',$block)"));
   my $trace=decode_json($node->safe_psql('postgres','SELECT test_pgrac_native_itl_trace()'));
   my $case={holders=>[map {+{%$_}} @$holders],trace=>$trace,page_sequence=>$page->{sequence},tag=>$tag,
    log=>substr(slurp_file($node->logfile),$offset),waiter_pid=>$pid,waiter_node=>0};
   save_json("b08-$name-candidate",$case);
   my ($selected,$error)=select_blocker($case);
   $last=$error;
   return ($case,$selected) if defined($selected);
  }
  sleep(0.02);
 }
 die "exact capacity wait was not observed under the original deadline: $last";
}

sub wait_exit_cell {
 my ($node,$nodes,$kind)=@_;
 my $name="m2_b08_$kind";
 my $first_session=scalar(@sessions);
 my ($r,$exit,$retry);
 my $completed=eval {
  $node->safe_psql('postgres',"CREATE TABLE $name(id int PRIMARY KEY,v int);
   INSERT INTO $name SELECT g,0 FROM generate_series(1,9) g;");
  $node->safe_psql('postgres',"VACUUM(FREEZE) $name");
  my @tids=split /\n/,$node->safe_psql('postgres',"SELECT id,ctid FROM $name ORDER BY id");
  my %blocks;
  for my $tid (@tids) {
   die "invalid original row location: $tid" unless $tid =~ /^\d+\|\((\d+),\d+\)$/;
   $blocks{$1}=1;
  }
  die 'nine original rows do not share one block' unless @tids==9 && keys(%blocks)==1;
  my ($block)=keys %blocks;
  my @tag=map {0+$_} split /\|/,$node->safe_psql('postgres',"SELECT
   COALESCE(NULLIF(c.reltablespace,0),d.dattablespace)||'|'||d.oid||'|'||
   pg_relation_filenode(c.oid)||'|0|$block' FROM pg_class c,pg_database d
   WHERE c.oid='$name'::regclass AND d.datname=current_database()");
  die 'missing full relation tag' unless @tag==5;
  my (@holders,%by_pid);
  for my $row (1..8) {
   my $s=session($node);
   my $pid=0+$s->query_safe('SELECT pg_backend_pid()');
   $s->query_safe("BEGIN; UPDATE $name SET v=1 WHERE id=$row");
   push @holders,{row=>$row,node=>0,pid=>$pid,xid=>0+$s->query_safe('SELECT pg_current_xact_id()::text')};
   $by_pid{$pid}=$s;
  }
  $r=session($node);
  $r->query_safe('\\set VERBOSITY verbose');
  my $pid=0+$r->query_safe('SELECT pg_backend_pid()');
  save_json("b08-$name-setup",{tag=>\@tag,ctids=>\@tids,holders=>\@holders,pid=>$pid,
   statement_timeout=>$r->query_safe('SHOW statement_timeout'),
   ges_request_timeout=>$r->query_safe('SHOW cluster.ges_request_timeout_ms')});
  my $original_ges=$kind eq 'capacity_timeout'
   ? $r->query_safe('SHOW cluster.ges_request_timeout_ms') : undef;
  $r->query_safe('BEGIN');
  my $settings;
  if($kind eq 'capacity_timeout') {
   # A transaction-local test input uses the existing finite ITL budget;
   # the ordinary defaults and the original SQL timeout are unchanged.
   $r->query_safe('SET LOCAL cluster.ges_request_timeout_ms=5000');
   $settings=decode_json($r->query_safe(q{SELECT json_build_object(
    'capacity_timeout_ms',(SELECT setting::bigint FROM pg_settings WHERE name='cluster.ges_request_timeout_ms'),
    'statement_timeout_ms',(SELECT setting::bigint FROM pg_settings WHERE name='statement_timeout'),
    'waiter_xid',pg_current_xact_id()::text)}));
  }
  my $sql="UPDATE $name SET v=1 WHERE id=9;";
  my $offset=-s $node->logfile;
  my $until=clock_gettime(CLOCK_MONOTONIC)+10;
  $r->query_until(qr/B08_SENT/,"\\echo B08_SENT\n$sql\n");
  my ($start,$selected)=exact_exit_wait($node,$r,$pid,$sql,$block,\@tag,\@holders,$offset,$until,$name);
  die 'UPDATE completed without the required exact capacity wait' unless defined($selected);
  my $cancel_sent=JSON::PP::false;
  if($kind eq 'cancel') {
   die 'original backend cancel was refused'
    unless $node->safe_psql('postgres',"SELECT pg_cancel_backend($pid)") eq 't';
   $cancel_sent=JSON::PP::true;
  }
  # The echo is queued behind the existing UPDATE. It issues no replacement
  # SQL and cannot restart either the statement or capacity deadline.
  my ($out,$had_error)=$r->query("\\echo B08_SQLSTATE :SQLSTATE\n");
  $exit={start=>$start,kind=>$kind,cancel_sent=>$cancel_sent,stdout=>$out,stderr=>$r->{stderr},
   client_had_error=>$had_error ? JSON::PP::true : JSON::PP::false,
   log=>substr(slurp_file($node->logfile),$offset),reader=>activity($node,$pid),
   holders=>exit_owners($node,\@holders),settings=>$settings};
  save_json("b08-$name-exit",$exit);
  my ($judge_out,$judge_error)=('','');
  my $input=encode_json($exit);
  my $judged=run([$ENV{PYTHON} // 'python3',"$FindBin::RealBin/../pre2/m2_itl_capacity.py",'--exit'],
   '<',\$input,'>',\$judge_out,'2>',\$judge_error,timeout(2));
  save_json("b08-$name-judge",{passed=>$judged ? JSON::PP::true : JSON::PP::false,
   stdout=>$judge_out,stderr=>$judge_error});
  my $judgement=eval {decode_json($judge_out)};
  die "native wait exit evidence refused: $judge_out $judge_error"
   unless $judged && ref($judgement) eq 'HASH' && ref($judgement->{exit}) eq 'HASH'
    && ($judgement->{exit}->{kind} // '') eq $kind
    && ($judgement->{exit}->{sqlstate} // '') eq ($kind eq 'capacity_timeout' ? '53R70' : '57014')
    && ($judgement->{exit}->{wait_seq} // 0)==$selected->{wait_seq}
    && exists($judgement->{exit}->{authoritative}) && !$judgement->{exit}->{authoritative};
  $r->{stderr}='';
  $r->query_safe('ROLLBACK');
  die 'transaction-local capacity budget escaped rollback'
   if defined($original_ges) && $r->query_safe('SHOW cluster.ges_request_timeout_ms') ne $original_ges;
  die 'cancelled connection was replaced' unless $r->query_safe('SELECT pg_backend_pid()') eq "$pid";
  $r->query_safe('BEGIN');
  $offset=-s $node->logfile;
  $until=clock_gettime(CLOCK_MONOTONIC)+10;
  $r->query_until(qr/B08_RETRY_SENT/,"\\echo B08_RETRY_SENT\n$sql\n");
  my @decisions;
  my $done=0;
  while(clock_gettime(CLOCK_MONOTONIC)<$until) {
   $r->{run}->pump_nb();
   my $state=activity($node,$pid);
   if(defined($state) && $state->{state} eq 'idle in transaction' && $state->{query} eq $sql) {
    $done=1; last;
   }
   my ($case,$next)=exact_exit_wait($node,$r,$pid,$sql,$block,\@tag,\@holders,$offset,$until,$name);
   if(!defined($next)) {$done=1; last;}
   die 'fresh wait reused the cancelled registration' unless $next->{wait_seq}>$selected->{wait_seq};
   die 'more than the original eight owners were selected' if @decisions>=8;
   save_json("b08-$name-selection-".scalar(@decisions),{evidence=>$case,selected=>$next});
   $by_pid{$next->{pid}}->query_safe('COMMIT');
   push @decisions,$next;
   @holders=grep {$_->{pid}!=$next->{pid}} @holders;
  }
  die 'fresh UPDATE did not resume under its original deadline' unless $done && @decisions;
  die 'fresh UPDATE did not change the ninth row' unless $r->query_safe("SELECT v FROM $name WHERE id=9") eq '1';
  my $remaining=exit_owners($node,\@holders);
  for my $i (0..$#holders) {
   die 'unselected owner changed before fresh progress'
    unless $remaining->[$i]->{state} eq 'idle in transaction'
     && $remaining->[$i]->{xid} eq $holders[$i]->{xid};
   $by_pid{$holders[$i]->{pid}}->query_safe('COMMIT');
  }
  $r->query_safe('COMMIT');
  my $expected=join "\n",map {"$_|1"} 1..9;
  for my $member (@$nodes) {
   die 'committed rows disagree after fresh progress'
    unless $member->safe_psql('postgres',"SELECT id,v FROM $name ORDER BY id") eq $expected;
  }
  $retry={pid=>$pid,decisions=>\@decisions,remaining=>$remaining};
  1;
 };
 my $error=$@;
 save_json("b08-$name-client",{map {$_=>$r->{$_}} qw(stdin stdout stderr)}) if defined($r);
 my @cleanup;
 for my $s (splice @sessions,$first_session) {
  eval {$s->quit or die 'psql did not exit normally'; 1;} or push @cleanup,$@;
 }
 save_json("b08-$name-execution",{completed=>$completed ? JSON::PP::true : JSON::PP::false,
  error=>$error,cleanup=>\@cleanup,retry=>$retry});
 diag($error) unless $completed;
 diag(join '\n',@cleanup) if @cleanup;
 return $completed && !@cleanup;
}

sub judge_requalification {
 my ($case,$option)=@_;
 my %keys=('--requalify-mutation'=>'mutation','--requalify'=>'requalified',
  '--cross-page-setup'=>'pair_setup','--cross-page-wait'=>'pair','--cross-page'=>'cross_page');
 die 'unknown requalification evaluator' unless exists($keys{$option});
 my ($input,$out,$err)=(encode_json($case),'','');
 my $ok=run([$ENV{PYTHON} // 'python3',"$FindBin::RealBin/../pre2/m2_itl_capacity.py",$option],
  '<',\$input,'>',\$out,'2>',\$err,timeout(2));
 die "requalification evaluator produced no result: $err" unless $out ne '';
 my $result=decode_json($out);
 return ($ok ? $result->{$keys{$option}} : undef,
  $result->{error} // $err);
}

sub commit_requalification_mutator {
 my ($node,$r,$mutator,$start,$selected,$kind,$name,$before,$sql,$waiter_xid,$offset,$until)=@_;
 die 'mutation exceeded the original deadline' unless clock_gettime(CLOCK_MONOTONIC)<$until;
 die 'invalid requalification cell' unless ($kind eq 'update' || $kind eq 'delete')
  && $name eq "m2_b06_$kind";
 my $pid=0+$mutator->query_safe('SELECT pg_backend_pid()');
 my $xid=0+$mutator->query_safe('SELECT pg_current_xact_id()::text');
 die 'selected mutator identity changed' unless $pid==$selected->{pid} && $xid==$selected->{xid};
 die 'mutation exceeded the original deadline' unless clock_gettime(CLOCK_MONOTONIC)<$until;
 my $change=$kind eq 'update' ? "UPDATE $name SET v=10 WHERE id=9" : "DELETE FROM $name WHERE id=9";
 my $changed=decode_json($mutator->query_safe("WITH changed AS ($change
  RETURNING id,v,ctid::text AS ctid,xmin::text AS xmin,xmax::text AS xmax)
  SELECT row_to_json(changed) FROM changed"));
 $r->{run}->pump_nb();
 my $case={start=>$start,kind=>$kind,sql=>$sql,waiter_xid=>$waiter_xid,before=>$before,
  mutation_row=>$changed,mutator_pid=>$pid,mutator_xid=>$xid,
  log=>substr(slurp_file($node->logfile),$offset),reader=>activity($node,$start->{waiter_pid}),
  holders=>exit_owners($node,$start->{holders})};
 save_json("b06-$name-mutation",$case);
 my ($proof,$error)=judge_requalification($case,'--requalify-mutation');
 my $json=JSON::PP->new->canonical;
 die "mutation evidence refused: $error" unless defined($proof)
  && $json->encode($proof) eq $json->encode({kind=>$kind,selected=>$selected,authoritative=>JSON::PP::false});
 die 'mutation exceeded the original deadline' unless clock_gettime(CLOCK_MONOTONIC)<$until;
 # Only the still-owned client named by the full live wait evidence commits.
 $mutator->query_safe('COMMIT');
 return $case;
}

sub requalification_cell {
 my ($node,$nodes,$kind)=@_;
 my $name="m2_b06_$kind";
 my $first_session=scalar(@sessions);
 my ($r,$proof);
 my $completed=eval {
  $node->safe_psql('postgres',"CREATE TABLE $name(id int PRIMARY KEY,v int);
   INSERT INTO $name SELECT g,0 FROM generate_series(1,9) g;");
  $node->safe_psql('postgres',"VACUUM(FREEZE) $name");
  my @tids=split /\n/,$node->safe_psql('postgres',"SELECT id,ctid FROM $name ORDER BY id");
  my %blocks;
  for my $tid (@tids) {
   die "invalid original row location: $tid" unless $tid =~ /^\d+\|\((\d+),\d+\)$/;
   $blocks{$1}=1;
  }
  die 'requalification rows do not share one block' unless @tids==9 && keys(%blocks)==1;
  my ($block)=keys %blocks;
  my @tag=map {0+$_} split /\|/,$node->safe_psql('postgres',"SELECT
   COALESCE(NULLIF(c.reltablespace,0),d.dattablespace)||'|'||d.oid||'|'||
   pg_relation_filenode(c.oid)||'|0|$block' FROM pg_class c,pg_database d
   WHERE c.oid='$name'::regclass AND d.datname=current_database()");
  die 'missing full requalification relation tag' unless @tag==5;
  my $before=decode_json($node->safe_psql('postgres',"SELECT row_to_json(t) FROM (
   SELECT id,v,ctid::text AS ctid,xmin::text AS xmin,xmax::text AS xmax FROM $name WHERE id=9) t"));
  my (@holders,%by_pid);
  for my $row (1..8) {
   my $s=session($node);
   my $pid=0+$s->query_safe('SELECT pg_backend_pid()');
   $s->query_safe("BEGIN; UPDATE $name SET v=1 WHERE id=$row");
   push @holders,{row=>$row,node=>0,pid=>$pid,xid=>0+$s->query_safe('SELECT pg_current_xact_id()::text')};
   $by_pid{$pid}=$s;
  }
  $r=session($node);
  $r->query_safe('\\set VERBOSITY verbose');
  my $pid=0+$r->query_safe('SELECT pg_backend_pid()');
  $r->query_safe('BEGIN');
  my $waiter_xid=0+$r->query_safe('SELECT pg_current_xact_id()::text');
  save_json("b06-$name-setup",{tag=>\@tag,ctids=>\@tids,before=>$before,holders=>\@holders,pid=>$pid,
   waiter_xid=>$waiter_xid,statement_timeout=>$r->query_safe('SHOW statement_timeout'),
   ges_request_timeout=>$r->query_safe('SHOW cluster.ges_request_timeout_ms')});
  my $sql="UPDATE $name SET v=v+1 WHERE id=9;";
  my $offset=-s $node->logfile;
  my $until=clock_gettime(CLOCK_MONOTONIC)+10;
  $r->query_until(qr/B06_SENT/,"\\echo B06_SENT\n$sql\n");
  my ($start,$selected)=exact_exit_wait($node,$r,$pid,$sql,$block,\@tag,\@holders,$offset,$until,$name);
  die 'original UPDATE did not enter the exact capacity wait' unless defined($selected);
  my $mutation=commit_requalification_mutator($node,$r,$by_pid{$selected->{pid}},$start,$selected,
   $kind,$name,$before,$sql,$waiter_xid,$offset,$until);
  my @waits=($start);
  @holders=grep {$_->{pid}!=$selected->{pid}} @holders;
  my $done=0;
  while(clock_gettime(CLOCK_MONOTONIC)<$until) {
   $r->{run}->pump_nb();
   my $state=activity($node,$pid);
   die 'original requalification backend disappeared' unless defined($state);
   if($state->{state} eq 'idle in transaction' && $state->{query} eq $sql) {
    $done=1; last;
   }
   die 'original requalification UPDATE failed' if $state->{state} eq 'idle in transaction (aborted)';
   my ($case,$next)=exact_exit_wait($node,$r,$pid,$sql,$block,\@tag,\@holders,$offset,$until,$name);
   if(!defined($next)) {$done=1; last;}
   die 'more than the original eight owners were selected' if @waits>=8;
   die 'fresh capacity wait reused a registration' unless $next->{wait_seq}>$selected->{wait_seq};
   save_json("b06-$name-selection-".scalar(@waits),{evidence=>$case,selected=>$next});
   die 'owner commit exceeded the original deadline' unless clock_gettime(CLOCK_MONOTONIC)<$until;
   $by_pid{$next->{pid}}->query_safe('COMMIT');
   push @waits,$case;
   @holders=grep {$_->{pid}!=$next->{pid}} @holders;
  }
  die 'original UPDATE did not requalify under its original deadline' unless $done;
  my $state=activity($node,$pid);
  my ($out,$had_error)=$r->query("\\echo B06_SQLSTATE :SQLSTATE\n");
  my $post_xid=0+$r->query_safe('SELECT pg_current_xact_id()::text');
  my $rows=decode_json($r->query_safe("SELECT COALESCE(json_agg(t),'[]'::json) FROM (
   SELECT id,v,ctid::text AS ctid,xmin::text AS xmin,xmax::text AS xmax FROM $name WHERE id=9) t"));
  my $case={mutation=>$mutation,waits=>\@waits,log=>substr(slurp_file($node->logfile),$offset),
   stdout=>$out,stderr=>$r->{stderr},client_had_error=>$had_error ? JSON::PP::true : JSON::PP::false,
   reader=>$state,waiter_xid=>$post_xid,rows=>$rows,remaining=>exit_owners($node,\@holders)};
  save_json("b06-$name-finish",$case);
  my ($result,$error)=judge_requalification($case,'--requalify');
  save_json("b06-$name-judge",{result=>$result,error=>$error});
  my $json=JSON::PP->new->canonical;
  die "requalification evidence refused: $error" unless defined($result)
   && ($result->{kind} // '') eq $kind && ($result->{completed_waits} // 0)>0
   && ($result->{released_owners} // 0)==@waits
   && exists($result->{authoritative}) && !$result->{authoritative}
   && $json->encode($result->{selected}) eq $json->encode($selected);
  $proof=$result;
  # Prove the original UPDATE before explicitly finishing the other clients.
  $by_pid{$_->{pid}}->query_safe('COMMIT') for @holders;
  die 'original UPDATE connection was replaced' unless $r->query_safe('SELECT pg_backend_pid()') eq "$pid";
  $r->query_safe('COMMIT');
  my $expected=join "\n",((map {"$_|1"} 1..8),($kind eq 'update' ? ('9|11') : ()));
  for my $member (@$nodes) {
   die 'committed requalified rows disagree across members'
    unless $member->safe_psql('postgres',"SELECT id,v FROM $name ORDER BY id") eq $expected;
  }
  1;
 };
 my $error=$@;
 save_json("b06-$name-client",{map {$_=>$r->{$_}} qw(stdin stdout stderr)}) if defined($r);
 my @cleanup;
 for my $s (splice @sessions,$first_session) {
  eval {$s->quit or die 'psql did not exit normally'; 1;} or push @cleanup,$@;
 }
 save_json("b06-$name-execution",{completed=>$completed ? JSON::PP::true : JSON::PP::false,
  error=>$error,cleanup=>\@cleanup,proof=>$proof});
 diag($error) unless $completed;
 diag(join '\n',@cleanup) if @cleanup;
 return $completed && !@cleanup;
}

sub commit_cross_page_owner {
 my ($node,$r,$owner,$case,$selected,$offset,$until)=@_;
 die 'pair release exceeded the original deadline' unless clock_gettime(CLOCK_MONOTONIC)<$until;
 my $pid=0+$owner->query_safe('SELECT pg_backend_pid()');
 my $xid=0+$owner->query_safe('SELECT pg_current_xact_id()::text');
 die 'selected pair owner identity changed' unless $pid==$selected->{pid} && $xid==$selected->{xid};
 die 'pair release exceeded the original deadline' unless clock_gettime(CLOCK_MONOTONIC)<$until;
 $r->{run}->pump_nb();
 my $observed={%$case,log=>substr(slurp_file($node->logfile),$offset),
  reader=>activity($node,$case->{start}->{waiter_pid}),holders=>exit_owners($node,$case->{start}->{holders})};
 save_json('b06-pair-before-release',$observed);
 my ($proof,$error)=judge_requalification($observed,'--cross-page-wait');
 my $json=JSON::PP->new->canonical;
 die "pair evidence refused: $error" unless defined($proof)
  && $json->encode($proof) eq $json->encode({kind=>'pair',selected=>$selected,
   source_tag=>$case->{source_tag},destination_tag=>$case->{start}->{tag},authoritative=>JSON::PP::false});
 die 'pair release exceeded the original deadline' unless clock_gettime(CLOCK_MONOTONIC)<$until;
 $owner->query_safe('COMMIT');
 return $observed;
}

sub check_cross_page_setup {
 my ($case)=@_;
 my ($proof,$error)=judge_requalification($case,'--cross-page-setup');
 my $json=JSON::PP->new->canonical;
 die "pair preflight evidence refused: $error" unless defined($proof)
  && $json->encode($proof) eq $json->encode({kind=>'pair_setup',source_tag=>$case->{source_tag},
   destination_tag=>$case->{destination_tag},update_payload_bytes=>12+$case->{before}->{tuple_bytes},
   inline_max_bytes=>$case->{settings}->{inline_max_bytes},authoritative=>JSON::PP::false});
 return $proof;
}

sub cross_page_cell {
 my ($node,$nodes)=@_;
 my $name='m2_b06_pair';
 my $first_session=scalar(@sessions);
 my ($r,$proof);
 my $completed=eval {
  # Inline fillers leave space for the new small tuple only on the full
  # destination. Actual row locations and wait evidence must confirm this;
  # there is no buffer-choice override or fallback success path.
  $node->safe_psql('postgres',"CREATE TABLE $name(id int PRIMARY KEY,v int,pad text STORAGE PLAIN);
   INSERT INTO $name SELECT g,0,'' FROM generate_series(1,8) g;
   INSERT INTO $name VALUES(10,0,repeat('p',6472));
   INSERT INTO $name VALUES(9,0,repeat('o',900));
   INSERT INTO $name VALUES(11,0,repeat('q',6600));");
  $node->safe_psql('postgres',"VACUUM(FREEZE) $name");
  my $layout=decode_json($node->safe_psql('postgres',"SELECT json_agg(t ORDER BY id) FROM (
   SELECT id,ctid::text AS ctid,length(pad) AS payload_bytes FROM $name) t"));
  die 'missing original cross-page layout' unless @$layout==11;
  my ($destination_block)=$layout->[0]->{ctid} =~ /^\((\d+),\d+\)$/;
  my ($source_block)=$layout->[8]->{ctid} =~ /^\((\d+),\d+\)$/;
  die 'ordinary input did not form distinct source and destination pages'
   unless defined($source_block) && defined($destination_block) && $source_block!=$destination_block;
  my @tag=map {0+$_} split /\|/,$node->safe_psql('postgres',"SELECT
   COALESCE(NULLIF(c.reltablespace,0),d.dattablespace)||'|'||d.oid||'|'||
   pg_relation_filenode(c.oid)||'|0|$destination_block' FROM pg_class c,pg_database d
   WHERE c.oid='$name'::regclass AND d.datname=current_database()");
  die 'missing full cross-page relation tag' unless @tag==5;
  my $source_tag=[@tag[0..3],0+$source_block];
  my $before=decode_json($node->safe_psql('postgres',"SELECT row_to_json(t) FROM (
   SELECT id,v,ctid::text AS ctid,xmin::text AS xmin,xmax::text AS xmax,
    length(pad) AS payload_bytes,pg_column_size(original) AS tuple_bytes
    FROM $name AS original WHERE id=9) t"));
  my $settings=decode_json($node->safe_psql('postgres',"SELECT json_build_object(
   'block_size',current_setting('block_size')::int,
   'inline_max_bytes',current_setting('cluster.undo_record_inline_max_bytes')::int,'payload_storage',
   (SELECT attstorage::text FROM pg_attribute WHERE attrelid='$name'::regclass AND attname='pad'))"));
  my $sql="UPDATE m2_b06_pair SET v=v+1,pad=repeat('n',480) WHERE id=9;";
  my $setup={source_tag=>$source_tag,destination_tag=>\@tag,settings=>$settings,
   layout=>$layout,before=>$before,sql=>$sql};
  save_json('b06-pair-preflight',$setup);
  check_cross_page_setup($setup);
  my (@holders,%by_pid);
  for my $row (1..8) {
   my $s=session($node);
   my $pid=0+$s->query_safe('SELECT pg_backend_pid()');
   $s->query_safe("BEGIN; UPDATE $name SET v=1 WHERE id=$row");
   push @holders,{row=>$row,node=>0,pid=>$pid,xid=>0+$s->query_safe('SELECT pg_current_xact_id()::text'),
    ctid=>$s->query_safe("SELECT ctid FROM $name WHERE id=$row")};
   $by_pid{$pid}=$s;
  }
  $r=session($node);
  $r->query_safe('\\set VERBOSITY verbose');
  my $pid=0+$r->query_safe('SELECT pg_backend_pid()');
  $r->query_safe('BEGIN');
  my $waiter_xid=0+$r->query_safe('SELECT pg_current_xact_id()::text');
  save_json('b06-pair-setup',{layout=>$layout,settings=>$settings,source_tag=>$source_tag,
   destination_tag=>\@tag,before=>$before,holders=>\@holders,pid=>$pid,waiter_xid=>$waiter_xid});
  my $offset=-s $node->logfile;
  my $until=clock_gettime(CLOCK_MONOTONIC)+10;
  $r->query_until(qr/B06_PAIR_SENT/,"\\echo B06_PAIR_SENT\n$sql\n");
  my ($start,$selected)=exact_exit_wait($node,$r,$pid,$sql,$destination_block,\@tag,
   \@holders,$offset,$until,$name);
  die 'original UPDATE did not wait on the full destination page' unless defined($selected);
  my $case={start=>$start,source_tag=>$source_tag,settings=>$settings,layout=>$layout,before=>$before,
   sql=>$sql,waiter_xid=>$waiter_xid};
  my $original=commit_cross_page_owner($node,$r,$by_pid{$selected->{pid}},$case,$selected,$offset,$until);
  my @waits=($start);
  my $last_wait_seq=$selected->{wait_seq};
  @holders=grep {$_->{pid}!=$selected->{pid}} @holders;
  my $done=0;
  while(clock_gettime(CLOCK_MONOTONIC)<$until) {
   $r->{run}->pump_nb();
   my ($current,$next)=exact_exit_wait($node,$r,$pid,$sql,$destination_block,\@tag,
    \@holders,$offset,$until,$name);
   if(!defined($next)) {$done=1; last;}
   die 'more than eight original pair owners were selected' if @waits>=8;
   die 'fresh pair wait reused a registration' unless $next->{wait_seq}>$last_wait_seq;
   save_json('b06-pair-selection-'.scalar(@waits),{evidence=>$current,selected=>$next});
   die 'pair release exceeded the original deadline' unless clock_gettime(CLOCK_MONOTONIC)<$until;
   $by_pid{$next->{pid}}->query_safe('COMMIT');
   push @waits,$current;
   $last_wait_seq=$next->{wait_seq};
   @holders=grep {$_->{pid}!=$next->{pid}} @holders;
  }
  die 'original two-page UPDATE did not finish under its original deadline' unless $done;
  my $state=activity($node,$pid);
  my ($out,$had_error)=$r->query("\\echo B06_PAIR_SQLSTATE :SQLSTATE\n");
  my $post_xid=0+$r->query_safe('SELECT pg_current_xact_id()::text');
  my $rows=decode_json($r->query_safe("SELECT COALESCE(json_agg(t),'[]'::json) FROM (
   SELECT id,v,ctid::text AS ctid,xmin::text AS xmin,xmax::text AS xmax,length(pad) AS payload_bytes,
    pad=repeat('n',480) AS payload_matches FROM $name WHERE id=9) t"));
  my $finish={cross_page=>$original,waits=>\@waits,log=>substr(slurp_file($node->logfile),$offset),
   stdout=>$out,stderr=>$r->{stderr},client_had_error=>$had_error ? JSON::PP::true : JSON::PP::false,
   reader=>$state,waiter_xid=>$post_xid,rows=>$rows,remaining=>exit_owners($node,\@holders)};
  save_json('b06-pair-finish',$finish);
  my ($result,$error)=judge_requalification($finish,'--cross-page');
  save_json('b06-pair-judge',{result=>$result,error=>$error});
  my $json=JSON::PP->new->canonical;
  die "pair completion evidence refused: $error" unless defined($result)
   && ($result->{kind} // '') eq 'pair' && ($result->{completed_waits} // 0)>0
   && ($result->{released_owners} // 0)==@waits
   && exists($result->{authoritative}) && !$result->{authoritative}
   && $json->encode($result->{selected}) eq $json->encode($selected)
   && $json->encode($result->{source_tag}) eq $json->encode($source_tag)
   && $json->encode($result->{destination_tag}) eq $json->encode(\@tag);
  $proof=$result;
  $by_pid{$_->{pid}}->query_safe('COMMIT') for @holders;
  die 'original two-page UPDATE connection was replaced' unless $r->query_safe('SELECT pg_backend_pid()') eq "$pid";
  $r->query_safe('COMMIT');
  my $expected=join "\n",((map {"$_|1|0|t"} 1..8),'9|1|480|t','10|0|6472|t','11|0|6600|t');
  for my $member (@$nodes) {
   die 'committed two-page rows disagree across members' unless $member->safe_psql('postgres',"SELECT
    id,v,length(pad),pad=CASE id WHEN 9 THEN repeat('n',480) WHEN 10 THEN repeat('p',6472)
     WHEN 11 THEN repeat('q',6600) ELSE '' END FROM $name ORDER BY id") eq $expected;
  }
  1;
 };
 my $error=$@;
 save_json('b06-pair-client',{map {$_=>$r->{$_}} qw(stdin stdout stderr)}) if defined($r);
 my @cleanup;
 for my $s (splice @sessions,$first_session) {
  eval {$s->quit or die 'psql did not exit normally'; 1;} or push @cleanup,$@;
 }
 save_json('b06-pair-execution',{completed=>$completed ? JSON::PP::true : JSON::PP::false,
  error=>$error,cleanup=>\@cleanup,proof=>$proof});
 diag($error) unless $completed;
 diag(join '\n',@cleanup) if @cleanup;
 return $completed && !@cleanup;
}

sub deadlock_victim {
 my ($node,$clients,$until)=@_;
 my $completed=0;
 while(clock_gettime(CLOCK_MONOTONIC)<$until) {
  my @aborted;
  for my $client (@$clients) {
   $client->{session}->{run}->pump_nb();
   my $state=activity($node,$client->{pid});
   die 'deadlock participant disappeared' unless defined($state);
   if($state->{state} eq 'idle in transaction (aborted)') {
    die 'failed backend was not running the original UPDATE' unless $state->{query} eq $client->{sql};
    push @aborted,$client;
   }
   $completed=1 if $state->{state} eq 'idle in transaction' && $state->{query} eq $client->{sql};
  }
  die 'more than one deadlock participant failed' if @aborted>1;
  return $aborted[0] if @aborted==1;
  # pg_stat_activity reads are separate snapshots. A survivor can finish
  # between reading its still-active victim and reading the survivor itself.
  sleep(0.02);
 }
 die 'UPDATE completed without a deadlock victim under the original deadline' if $completed;
 die 'no native deadlock victim under the original deadline';
}

sub deadlock_table {
 my ($node,$name)=@_;
 $node->safe_psql('postgres',"CREATE TABLE $name(id int PRIMARY KEY,v int);
  INSERT INTO $name SELECT g,0 FROM generate_series(1,9) g;");
 $node->safe_psql('postgres',"VACUUM(FREEZE) $name");
 my @tids=split /\n/,$node->safe_psql('postgres',"SELECT id,ctid FROM $name ORDER BY id");
 my %blocks;
 for my $tid (@tids) {
  die "invalid original row location: $tid" unless $tid =~ /^\d+\|\((\d+),\d+\)$/;
  $blocks{$1}=1;
 }
 die 'deadlock rows do not share one block' unless @tids==9 && keys(%blocks)==1;
 my ($block)=keys %blocks;
 my @tag=map {0+$_} split /\|/,$node->safe_psql('postgres',"SELECT
  COALESCE(NULLIF(c.reltablespace,0),d.dattablespace)||'|'||d.oid||'|'||
  pg_relation_filenode(c.oid)||'|0|$block' FROM pg_class c,pg_database d
  WHERE c.oid='$name'::regclass AND d.datname=current_database()");
 die 'missing full deadlock relation tag' unless @tag==5;
 return {name=>$name,block=>0+$block,tag=>\@tag,ctids=>\@tids,holders=>[]};
}

sub deadlock_page {
 my ($node,$page,$offset)=@_;
 my $copied=decode_json($node->safe_psql('postgres',
  "SELECT test_pgrac_native_itl_page('$page->{name}',$page->{block})"));
 return {holders=>[map {+{%$_}} @{$page->{holders}}],
  trace=>decode_json($node->safe_psql('postgres','SELECT test_pgrac_native_itl_trace()')),
  page_sequence=>$copied->{sequence},tag=>$page->{tag},
  log=>substr(slurp_file($node->logfile),$offset)};
}

sub deadlock_cell {
 my ($node,$nodes)=@_;
 my $first_session=scalar(@sessions);
 my (@clients,$proof);
 my $completed=eval {
  my @pages=map {deadlock_table($node,$_)} ('m2_b08_deadlock_a','m2_b08_deadlock_b');
  my %by_pid;
  for my $page (@pages) {
   for my $row (1..8) {
    my $s=session($node);
    $s->query_safe('\\set VERBOSITY verbose');
    my $pid=0+$s->query_safe('SELECT pg_backend_pid()');
    $s->query_safe("BEGIN; UPDATE $page->{name} SET v=1 WHERE id=$row");
    my $holder={table=>$page->{name},row=>$row,node=>0,pid=>$pid,
     xid=>0+$s->query_safe('SELECT pg_current_xact_id()::text')};
    push @{$page->{holders}},$holder;
    $by_pid{$pid}=$s;
   }
  }
  save_json('b08-deadlock-setup',\@pages);
  my $offset=-s $node->logfile;
  my $page=deadlock_page($node,$pages[1],$offset);
  my ($second,$why)=select_blocker($page,'--page-owner');
  die "second page has no exact observed owner: $why" unless defined($second);
  my $first_sql="UPDATE $pages[0]->{name} SET v=1 WHERE id=9;";
  my $until=clock_gettime(CLOCK_MONOTONIC)+10;
  $by_pid{$second->{pid}}->query_until(qr/B08_DEADLOCK_FORWARD/,
   "\\echo B08_DEADLOCK_FORWARD\n$first_sql\n");
  my ($forward,$first)=exact_exit_wait($node,$by_pid{$second->{pid}},$second->{pid},$first_sql,
   $pages[0]->{block},$pages[0]->{tag},$pages[0]->{holders},$offset,$until,$pages[0]->{name});
  die 'first UPDATE did not enter the required ITL capacity wait' unless defined($first);
  my $reverse=deadlock_page($node,$pages[1],$offset);
  $reverse->{waiter_pid}=$first->{pid};
  $reverse->{waiter_node}=0;
  my $second_sql="UPDATE $pages[1]->{name} SET v=1 WHERE id=9;";
  @clients=({%$second,session=>$by_pid{$second->{pid}},sql=>$first_sql,target=>$pages[0]->{name}},
            {%$first,session=>$by_pid{$first->{pid}},sql=>$second_sql,target=>$pages[1]->{name}});
  save_json('b08-deadlock-start',{forward=>$forward,reverse=>$reverse,selected=>[$first,$second]});
  $by_pid{$first->{pid}}->query_until(qr/B08_DEADLOCK_REVERSE/,
   "\\echo B08_DEADLOCK_REVERSE\n$second_sql\n");
  my $victim=deadlock_victim($node,\@clients,$until);
  my ($out,$had_error)=$victim->{session}->query("\\echo B08_SQLSTATE :SQLSTATE\n");
  my @holders=map {@{$_->{holders}}} @pages;
  my $last='original waits have not both ended';
  while(clock_gettime(CLOCK_MONOTONIC)<$until) {
   $_->{session}->{run}->pump_nb() for @clients;
   my $case={forward=>$forward,reverse=>$reverse,log=>substr(slurp_file($node->logfile),$offset),
    victim_pid=>$victim->{pid},cancel_sent=>JSON::PP::false,stdout=>$out,
    stderr=>$victim->{session}->{stderr},client_had_error=>$had_error ? JSON::PP::true : JSON::PP::false,
    holders=>exit_owners($node,\@holders)};
   save_json('b08-deadlock-exit',$case);
   my ($input,$judged_out,$judged_error)=(encode_json($case),'','');
   my $ok=run([$ENV{PYTHON} // 'python3',"$FindBin::RealBin/../pre2/m2_itl_capacity.py",'--deadlock'],
    '<',\$input,'>',\$judged_out,'2>',\$judged_error,timeout(2));
   save_json('b08-deadlock-judge',{passed=>$ok ? JSON::PP::true : JSON::PP::false,
    stdout=>$judged_out,stderr=>$judged_error});
   my $result=eval {decode_json($judged_out)};
   if($ok && ref($result) eq 'HASH' && ref($result->{deadlock}) eq 'HASH'
      && ($result->{deadlock}->{sqlstate} // '') eq '40P01'
      && ($result->{deadlock}->{victim_pid} // 0)==$victim->{pid}
      && exists($result->{deadlock}->{authoritative}) && !$result->{deadlock}->{authoritative}) {
    $proof=$result->{deadlock}; last;
   }
   $last="$judged_out $judged_error";
   sleep(0.02);
  }
  die "native deadlock evidence refused under the original deadline: $last" unless defined($proof);
  # Only after the cycle, original error, exact ends and fourteen untouched
  # owners are proved does the fixture explicitly finish its own clients.
  $victim->{session}->{stderr}='';
  $victim->{session}->query_safe('ROLLBACK');
  die 'victim connection was replaced'
   unless $victim->{session}->query_safe('SELECT pg_backend_pid()') eq "$victim->{pid}";
  my ($survivor)=grep {$_->{pid}!=$victim->{pid}} @clients;
  for my $holder (@holders) {
   next if $holder->{pid}==$victim->{pid} || $holder->{pid}==$survivor->{pid};
   $by_pid{$holder->{pid}}->query_safe('COMMIT');
  }
  my ($survived,$failed)=$survivor->{session}->query("\\echo B08_SURVIVOR :SQLSTATE\n");
  die 'surviving original UPDATE did not complete normally'
   unless !$failed && $survived eq 'B08_SURVIVOR 00000';
  die 'survivor did not update the ninth row'
   unless $survivor->{session}->query_safe("SELECT v FROM $survivor->{target} WHERE id=9") eq '1';
  $survivor->{session}->query_safe('COMMIT');
  $victim->{session}->query_safe("BEGIN;
   UPDATE $victim->{table} SET v=1 WHERE id=$victim->{row};
   UPDATE $victim->{target} SET v=1 WHERE id=9; COMMIT;");
  my $expected=join "\n",map {"$_|1"} 1..9;
  for my $member (@$nodes) {
   for my $original (@pages) {
    die 'committed deadlock rows disagree across members'
     unless $member->safe_psql('postgres',"SELECT id,v FROM $original->{name} ORDER BY id") eq $expected;
   }
  }
  1;
 };
 my $error=$@;
 for my $client (@clients) {
  save_json("b08-deadlock-client-$client->{pid}",
   {map {$_=>$client->{session}->{$_}} qw(stdin stdout stderr)});
 }
 my @cleanup;
 for my $s (splice @sessions,$first_session) {
  eval {$s->quit or die 'psql did not exit normally'; 1;} or push @cleanup,$@;
 }
 save_json('b08-deadlock-execution',{completed=>$completed ? JSON::PP::true : JSON::PP::false,
  error=>$error,cleanup=>\@cleanup,proof=>$proof});
 diag($error) unless $completed;
 diag(join '\n',@cleanup) if @cleanup;
 return $completed && !@cleanup;
}

$cluster=PostgreSQL::Test::ClusterPRE2->new_cluster('m2_b07',nodes=>4,blackbox=>1,
 required_operations=>[],extra_conf=>['autovacuum=off','cluster.trace_tx_identity=on',
 'cluster.trace_control_config=on',
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
for my $kind ('cancel','statement_timeout','capacity_timeout') {
 ok(wait_exit_cell($node,\@nodes,$kind),
  "B08 $kind ends only the original wait and the same backend resumes with exact owners");
}
ok(deadlock_cell($node,\@nodes),
 'B08 two exact ITL capacity waits form a confirmed deadlock and the original backend recovers');
for my $kind ('update','delete') {
 ok(requalification_cell($node,\@nodes,$kind),
  "B06 the original UPDATE requalifies after the exact blocker $kind changes its target row");
}
ok(cross_page_cell($node,\@nodes),
 'B06 the original cross-page UPDATE waits on exact destination owners and requalifies its successor');
$cluster->stop_cluster;
undef $cluster;
pass('all four normal members complete the original stop proof');
for my $n (@nodes) {
 unlike(slurp_file($n->logfile),qr/PANIC|checksum verification failed|invalid page in block/,
  'member has no corruption or PANIC');
}
done_testing();
