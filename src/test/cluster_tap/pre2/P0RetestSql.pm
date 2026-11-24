# Author: SqlRush <sqlrush@gmail.com>
# Targeted ordinary SQL schedules using the existing snapshot/FULL controls.
package P0RetestSql;
use strict;
use warnings;
use parent 'WorkloadSql';
use JSON::PP;

sub run_targeted
{
 my ($self,$case)=@_;
 die "unknown targeted SQL case\n" unless $case =~ /\AP0-(5|10|12)\z/;
 die "driver attempt already consumed\n" if $self->{attempted}++;
 my ($writer,$reader)= $case eq 'P0-12' ? (2,4) : $case eq 'P0-5' ? (3,0) : (0,1);
 my $table=$self->_table('target');
 my $lane;
 my $ok=eval {
  for my $n (0..4) {
   $self->{identities}[$n]=$self->_identity($n);
   for my $k (qw(database_oid system_identifier)) {
    die "four-member identity mismatch\n"
     unless $self->{identities}[$n]{$k} eq $self->{identities}[0]{$k};
   }
  }
  die "HOT reader is not a distinct backend on the writer instance\n"
   if $case eq 'P0-12' && ($self->{identities}[2]{pid} eq $self->{identities}[4]{pid}
    || $self->{identities}[2]{postmaster_start} ne $self->{identities}[4]{postmaster_start});
  $self->_query($writer,'create_schema',qq{CREATE SCHEMA "$self->{schema}"});
  $self->_query($writer,'create_relation',"CREATE TABLE $table(id int PRIMARY KEY,v text) WITH (fillfactor=50)");
  $self->{open}{$writer}='release_seed_writer';
  $self->_query($writer,'begin_seed','BEGIN');
  $self->_change($writer,'seed',"INSERT INTO $table SELECT g,'seed_'||g FROM generate_series(1,64) g",64);
  my $old_xid=$self->_json_query($writer,'old_creator','SELECT to_json(pg_current_xact_id()::text)');
  die "missing original creator FullXid\n" unless defined($old_xid) && $old_xid =~ /\A[1-9][0-9]*\z/;
  $self->_query($writer,'commit_seed','COMMIT',sub {delete $self->{open}{$writer}});
  $lane={case=>$case,table=>$table,writer=>$writer,reader=>$self->{session_nodes}[$reader],
   reader_session=>$reader,writer_sql_identity=>$self->{identities}[$writer],
   reader_sql_identity=>$self->{identities}[$reader],old_creator_full_xid=>$old_xid,
   locator=>$self->_locator($writer,$table),tuple_fastpath_control=>$case eq 'P0-12' ? JSON::PP::true : JSON::PP::false};
  $lane->{snapshot}=$self->_hold_snapshot($reader);
  $self->_query($reader,'baseline_full_path','SET LOCAL cluster.cr_tuple_level_fastpath = off')
   if $lane->{tuple_fastpath_control};
  $lane->{baseline}=$self->_read($reader,$table);
  my $json=JSON::PP->new->canonical;
  my $seed=[map {+{id=>$_,v=>"seed_$_"}} 1..64];
  die "baseline row comparison mismatch\n" unless $json->encode($lane->{baseline}) eq $json->encode($seed);
  $self->_emit({kind=>'targeted_setup',case=>$case,evidence=>$lane});
  if ($case eq 'P0-5') {
   my $churn=$self->_table('churn');
   $self->_query($writer,'create_churn',"CREATE TABLE $churn(id int PRIMARY KEY,v int); INSERT INTO $churn VALUES(1,0)");
   for my $round (1..4096) {
    # Each acknowledged command is an ordinary autocommit transaction.
    $self->_query($writer,'history_churn',"UPDATE $churn SET v=v+1 WHERE id=1");
   }
   $lane->{history_transactions}=4096;
   die "history churn count mismatch\n"
    unless $self->_query($writer,'history_count',"SELECT v FROM $churn WHERE id=1") eq '4096';
  } elsif ($case eq 'P0-10') {
   $self->_change($writer,'remote_update',"UPDATE $table SET v='remote_'||id",64);
  } else {
   $self->{open}{$writer}='release_hot_writer';
   $self->_query($writer,'begin_hot_writer','BEGIN');
   for my $pass (1..3) {
    $self->_change($writer,'hot_candidate_update',"UPDATE $table SET v='hot_${pass}_'||id",64);
   }
   $self->_change($writer,'indexed_key_update',"UPDATE $table SET id=id+64 WHERE id % 4 = 0",16);
   $self->_change($writer,'delete',"DELETE FROM $table WHERE id % 8 = 1",8);
   $self->_query($writer,'commit_hot_writer','COMMIT',sub {delete $self->{open}{$writer}});
   $self->_query($writer,'vacuum',"VACUUM $table");
  }
  $self->_compare($lane);
  $self->_query($reader,'release_reader','ROLLBACK',sub {delete $self->{open}{$reader}});
  my $expected=$case eq 'P0-5' ? $seed : $case eq 'P0-10'
   ? [map {+{id=>$_,v=>"remote_$_"}} 1..64]
   : [sort {$a->{id}<=>$b->{id}} map {+{id=>$_%4==0 ? $_+64 : $_,v=>"hot_3_$_"}}
      grep {$_%8!=1} 1..64];
  for my $n (0..3) {
   my $rows=$self->_read($n,$table);
   die "committed rows disagree on member $n\n" unless $json->encode($rows) eq $json->encode($expected);
  }
  for my $n (0..4) {
   die "original SQL session identity changed\n"
    unless $json->encode($self->_identity($n)) eq $json->encode($self->{identities}[$n]);
  }
  $self->_emit({kind=>'targeted_sql_complete',case=>$case,evidence=>$lane});
  1;
 };
 my $error=$ok ? '' : ($@ || "targeted SQL failed\n");
 for my $n (sort keys %{$self->{open}}) {
  eval {$self->_query($n,$self->{open}{$n},'ROLLBACK'); 1} or $error.=$@;
 }
 die $error if length($error);
 return $lane;
}

1;
