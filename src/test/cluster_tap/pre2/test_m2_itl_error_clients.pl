#!/usr/bin/env perl
# Exercise the native driver's expected-error clients without a database.
use strict;
use warnings;
use FindBin;
use File::Temp qw(tempfile);
use JSON::PP qw(encode_json decode_json);
use Test::More;

open my $source,'<',"$FindBin::RealBin/../t/482_pre2_m2_itl_capacity.pl" or die $!;
my $text=do {local $/; <$source>};
close $source or die $!;
my @sessions;
for my $name (qw(session wait_exit_cell deadlock_cell)) {
 my @body=$text =~ /(^sub \Q$name\E \{.*?^\})/msg;
 die "exactly one actual $name body is required" unless @body==1;
 eval $body[0];
 die $@ if $@;
}

my ($kind,$victim_index,$next_pid,@clients,%saved);
my ($log,$log_path)=tempfile();
sub CLOCK_MONOTONIC () {1}
sub clock_gettime {return 0}
sub slurp_file {return ''}
sub activity {return {state=>'idle in transaction (aborted)'}}
sub exit_owners {return []}
sub save_json {
 my ($name,$value)=@_;
 $saved{$name}=$value;
 die "original exit evidence captured\n" if $name =~ /-exit$/;
}
sub deadlock_table {
 return {name=>$_[1],block=>0,tag=>[1663,5,20000,0,0],holders=>[]};
}
sub deadlock_page {return $_[1]}
sub select_blocker {return ($_[0]->{holders}->[0],'')}
sub exact_exit_wait {
 return ({}, {%{$_[6]->[0]},wait_seq=>1});
}
sub deadlock_victim {
 my $victim=$_[1]->[$victim_index];
 $victim->{session}->{pending_error}='40P01';
 return $victim;
}

{
 package ErrorClientNode;
 sub logfile {return $log_path}
 sub background_psql {
  my $s=bless {pid=>++$next_pid,stop_on_error=>1,alive=>1,
   stdin=>'',stdout=>'',stderr=>'',run=>bless({},'ErrorClientPump')},'ErrorClient';
  push @clients,$s;
  return $s;
 }
 sub safe_psql {
  my ($self,$db,$sql)=@_;
  return join("\n",map {"$_|(0,$_)"} 1..9) if $sql =~ /SELECT id,ctid/;
  return '1663|5|20000|0|0' if $sql =~ /pg_relation_filenode/;
  return 't' if $sql =~ /pg_cancel_backend/;
  return '';
 }
 package ErrorClient;
 sub query_safe {
  my ($self,$sql)=@_;
  die 'client already exited' unless $self->{alive};
  $self->{stop_on_error}=0 if $sql eq '\\set ON_ERROR_STOP 0';
  return $self->{pid} if $sql =~ /SELECT pg_backend_pid/;
  return $self->{pid}+1000 if $sql =~ /SELECT pg_current_xact_id/;
  return '10000' if $sql eq 'SHOW statement_timeout';
  return '60000' if $sql eq 'SHOW cluster.ges_request_timeout_ms';
  return main::encode_json({capacity_timeout_ms=>5000,statement_timeout_ms=>10000,
   waiter_xid=>$self->{pid}+1000}) if $sql =~ /SELECT json_build_object/;
  return '';
 }
 sub query_until {
  my ($self,$pattern,$sql)=@_;
  $self->{pending_error}=$kind eq 'capacity_timeout' ? '53R70' : '57014'
   if $sql =~ /B08_SENT/;
 }
 sub query {
  my ($self,$sql)=@_;
  die 'unexpected driver query' unless $sql eq "\\echo B08_SQLSTATE :SQLSTATE\n";
  my $error=$self->{pending_error} // die 'no original error';
  $self->{stderr}="ERROR: $error: original wait failed\n";
  if($self->{stop_on_error}) {
   $self->{alive}=0;
   die 'process ended prematurely';
  }
  $self->{stdout}="B08_SQLSTATE $error";
  return ($self->{stdout},1);
 }
 sub quit {return $_[0]->{alive}}
 package ErrorClientPump;
 sub pump_nb {}
}

sub reset_clients {
 ($kind,$victim_index)=@_;
 ($next_pid,@clients,%saved)=(100);
}
my $node=bless {},'ErrorClientNode';
for my $error_kind (qw(cancel statement_timeout capacity_timeout)) {
 subtest "$error_kind captures the original error before recovery checks" => sub {
  reset_clients($error_kind,0);
  my $result;
  { local *main::diag=sub {};
    $result=wait_exit_cell($node,[$node],$error_kind);
  }
  ok(!$result,'the offline cut does not claim native completion');
  my $exit=$saved{"b08-m2_b08_$error_kind-exit"};
  my $state=$error_kind eq 'capacity_timeout' ? '53R70' : '57014';
  is($exit->{stdout},"B08_SQLSTATE $state",'the original SQLSTATE reaches the real exit evidence');
  ok($exit->{client_had_error},'the error is retained rather than converted into success');
  my $execution=$saved{"b08-m2_b08_$error_kind-execution"};
  like($execution->{error},qr/original exit evidence captured/,'execution reached the intended evidence boundary');
  is_deeply($execution->{cleanup},[],'the original client remains available for explicit cleanup');
  is(scalar(grep {$_->{stop_on_error}} @clients),8,'all eight ordinary owners still fail on unexpected errors');
 };
}
for my $victim (0,1) {
 subtest "deadlock participant $victim can retain its original error" => sub {
  reset_clients('deadlock',$victim);
  my $result;
  { local *main::diag=sub {}; $result=deadlock_cell($node,[$node]); }
  ok(!$result,'the offline cut does not claim native deadlock completion');
  my $exit=$saved{'b08-deadlock-exit'};
  is($exit->{stdout},'B08_SQLSTATE 40P01','either actual selected participant can supply the original error');
  ok($exit->{client_had_error},'the victim error remains visible to the original judge');
  my $execution=$saved{'b08-deadlock-execution'};
  like($execution->{error},qr/original exit evidence captured/,'both original sends reached the error evidence boundary');
  is_deeply($execution->{cleanup},[],'neither selected client is lost to error-stop');
  my @kept=grep {!$_->{stop_on_error}} @clients;
  is_deeply([sort {$a<=>$b} map {$_->{pid}} @kept],[101,109],
   'only the exact two participants keep running after expected errors');
 };
}
close $log or die $!;
done_testing();
