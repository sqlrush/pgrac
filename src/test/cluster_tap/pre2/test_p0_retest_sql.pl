#!/usr/bin/env perl
use strict;
use warnings;
use FindBin;
use lib $FindBin::RealBin;
use Test::More;
use JSON::PP;

my $loaded = eval {require P0RetestSql; 1};
ok($loaded, 'targeted SQL driver loads');
if (!$loaded) {diag($@); done_testing(); exit 1;}

{
 package RetestClient;
 sub query_safe {
  my ($s,$sql)=@_;
  push @{$s->{calls}},$sql;
  die "original mutation failed\n" if $s->{fail} && $sql =~ /remote_/;
  my $j=JSON::PP->new->canonical;
  my $rows=[map {+{id=>$_,v=>"seed_$_"}} 1..64];
  if ($sql =~ /pg_stat_activity/) {
   my $pid=$s->{pid};
   $pid++ if $s->{drift} && $s->{identity_reads}++;
   return $j->encode({node_id=>$s->{node},pid=>$pid,database_oid=>5,
    backend_start=>'birth',postmaster_start=>'postmaster',system_identifier=>'123'});
  }
  if ($sql =~ /pg_export_snapshot/) {
   return $j->encode({export_id=>'00000001-00000001-1',contents=>
    "pid:$s->{pid}\ndbid:5\niso:2\nro:1\nclu_src:1\nclu_scn:4097\nclu_epoch:9\n"});
  }
  return $j->encode({spcOid=>1663,dbOid=>5,relNumber=>501,forkNum=>0,blocks=>1})
   if $sql =~ /pg_relation_size/;
  return $j->encode('4195456') if $sql =~ /pg_current_xact_id/;
  return '4096' if $sql =~ /SELECT v FROM .*churn/;
  $s->{snapshot}=1 if $sql =~ /^BEGIN ISOLATION/;
  $s->{snapshot}=0 if $sql eq 'ROLLBACK';
  return '{}' if $sql =~ /pg_cluster_state/;
  return $j->encode([map {+{%$_,block=>0,cr_off=>$_->{id}}} @$rows])
   if $sql =~ /cluster_cr_test_target_image/;
  if ($sql =~ /WITH changed AS/) {
   if ($sql =~ /INSERT INTO/) {$s->{state}{current}=$rows;}
   if ($sql =~ /UPDATE/) {
    $rows=[map {+{%$_}} @{$s->{state}{current}}];
    if ($sql =~ /v='(remote_|hot_[123]_)'/) {$_->{v}=$1.$_->{id} for @$rows;}
   }
   $rows=[grep {$_->{id}%4==0} @$rows] if $sql =~ /id % 4 = 0/;
   if ($sql =~ /id=id\+64/) {
    $_->{id}+=64 for @$rows;
    my @unchanged=grep {$_->{id}%4!=0} @{$s->{state}{current}};
    $s->{state}{current}=[@unchanged,@$rows];
   } elsif ($sql =~ /DELETE/) {
    $rows=[grep {$_->{id}%8==1} @{$s->{state}{current}}];
    $s->{state}{current}=[grep {$_->{id}%8!=1} @{$s->{state}{current}}];
   } else {$s->{state}{current}=$rows;}
   return $j->encode([map {+{%$_,ctid=>'(0,'.$_->{id}.')'}} @$rows]);
  }
  if ($sql =~ /jsonb_agg/) {
   $rows=$s->{state}{current} unless $s->{snapshot};
   $rows->[0]{v}='wrong' if $s->{bad_rows} && ++$s->{reads}>1;
   return $j->encode($rows);
  }
  return '';
 }
}

sub make_driver {
 my (%options)=@_;
 my $state={};
 my @sessions=map {bless {node=>$_,pid=>100+$_,calls=>[],state=>$state},'RetestClient'} 0..3;
 my $hot=bless {node=>2,pid=>200,calls=>[],state=>$state},'RetestClient';
 my @events;
 my $driver=P0RetestSql->new(sessions=>\@sessions,hot_reader=>$hot,schema=>'p0_retest',
  emit=>sub {push @events,$_[0]},between_lanes=>sub {},receipt_barrier=>sub {});
 return ($driver,\@sessions,$hot,\@events);
}

for my $case ('P0-5','P0-10','P0-12') {
 my ($driver,$sessions,$hot,$events)=make_driver();
 my $result=$driver->run_targeted($case);
 is($result->{sql_comparison},'MATCH',"$case compares ordinary reads and all-block FULL");
 is(scalar(grep {$_->{kind} eq 'targeted_sql_complete'} @$events),1,"$case completes once");
 my $all=join '\n',map {@{$_->{calls}}} (@$sessions,$hot);
 unlike($all,qr/pg_sleep|ALTER SYSTEM|PREPARE TRANSACTION|m2_canonical_pause/,
  "$case has no scheduling or authority injection");
 like($all,qr/ROLLBACK/,"$case releases its original snapshot");
 if ($case eq 'P0-5') {
  is(scalar(grep {/^UPDATE .*churn/} @{$sessions->[3]{calls}}),4096,'history work is bounded and retained');
  is($result->{old_creator_full_xid},'4195456','old creator is recorded from the actual seed transaction');
 }
 if ($case eq 'P0-12') {
  like($all,qr/VACUUM/, 'HOT scenario keeps VACUUM');
  like($all,qr/SET LOCAL cluster.cr_tuple_level_fastpath = on/, 'HOT scenario compares tuple path');
  like($all,qr/SET LOCAL cluster.cr_tuple_level_fastpath = off/, 'HOT scenario compares ordinary FULL path');
 }
 eval {$driver->run_targeted($case)};
 like($@,qr/already consumed/,"$case cannot silently replay");
}
{
 my ($driver,$sessions,$hot)=make_driver();
 $sessions->[0]{fail}=1;
 eval {$driver->run_targeted('P0-10')};
 like($@,qr/original mutation failed/,'first mutation error survives cleanup');
 like(join('\n',@{$sessions->[1]{calls}}),qr/ROLLBACK/,'failed remote mutation still retires the reader');
}
{
 my ($driver,$sessions)=make_driver();
 $sessions->[1]{bad_rows}=1;
 eval {$driver->run_targeted('P0-10')};
 like($@,qr/row comparison mismatch/,'incorrect visible data is not a successful retest');
}
{
 my ($driver,$sessions,$hot)=make_driver();
 $hot->{drift}=1;
 eval {$driver->run_targeted('P0-12')};
 like($@,qr/session identity changed/,'the dedicated HOT reader is rechecked after completion');
}
done_testing();
