#!/usr/bin/env perl
# Actual TAP reader and Python judge, with only the SQL/time leaves supplied.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use FindBin;
use JSON::PP qw(encode_json decode_json);
use Test::More;
use IPC::Run qw(timeout);

open my $source,'<',"$FindBin::RealBin/../t/482_pre2_m2_itl_capacity.pl" or die $!;
my $text=do {local $/; <$source>};
close $source or die $!;
my ($now,$mode,$case,@actions,@saved);
sub CLOCK_MONOTONIC () {1}
sub clock_gettime {$now}
sub save_json {push @saved,[@_]}
sub run {
 push @actions,'judge';
 my $ok=IPC::Run::run(@_);
 $now=2 if $mode eq 'expired-during-judge';
 return $ok;
}
for my $function ('judge_requalification','capture_cross_page_retry') {
 my @body=$text =~ /(^sub \Q$function\E \{.*?^\})/msg;
 is(scalar(@body),1,"one actual $function boundary exists");
 if(@body!=1) {done_testing(); exit 1;}
 eval $body[0]; die $@ if $@;
}
{
 package RetryNode;
 sub safe_psql {
  my ($self,$db,$sql)=@_;
  die "unexpected query: $sql" unless $sql eq 'SELECT test_pgrac_native_itl_trace()';
  push @actions,'trace';
  $now=2 if $mode eq 'expired-during-read';
  return JSON::PP::encode_json($case->{trace});
 }
}
sub invoke {
 ($mode)=@_; $now=$mode eq 'expired-entry' ? 2 : 0;
 my ($out,$err)=('','');
 local $ENV{PYTHONPATH}=$FindBin::RealBin;
 IPC::Run::run([$ENV{PYTHON} // 'python3','-B','-c',
  'import json; from test_m2_itl_retry import observed; print(json.dumps(observed()))'],
  '>',\$out,'2>',\$err,timeout(2)) or die "fixture failed: $err";
 $case=decode_json($out); @actions=(); @saved=();
 if($mode eq 'wrong-actor') {$case->{trace}->{events}->[-1]->{backend_start}='901';}
 if($mode eq 'missing-record') {pop @{$case->{trace}->{events}};}
 if($mode eq 'changed-receipt') {$case->{trace}->{events}->[-1]->{update_retry}->{reservation_sequence}='999';}
 if($mode eq 'claimed-ready') {$case->{trace}->{events}->[-1]->{update_retry}->{ready_known}=JSON::PP::true;}
 if($mode eq 'no-vm') {
  for my $trace ($case->{trace},$case->{finish}->{cross_page}->{start}->{trace},
                 $case->{finish}->{waits}->[0]->{trace}) {
   for my $event (@{$trace->{events}}) {
    next unless $event->{kind}==7 && $event->{update_retry}->{phase}==1;
    $event->{update_retry}->{old_vm}=0; $event->{update_retry}->{new_vm}=0;
   }
  }
 }
 my $pair={completed_waits=>1};
 my $result=eval {capture_cross_page_retry(bless({},'RetryNode'),$case->{finish},$pair,1)};
 return ($result,$@);
}
subtest 'actual reader uses original identity and exports only diagnostic scope' => sub {
 my ($proof,$error)=invoke('ok');
 is($error,'','actual capture and Python evaluator complete');
 is_deeply(\@actions,['trace','judge'],'one read then actual judge');
 is($proof->{reservation_sequence},'9007199254740993','original local identity keeps precision');
 is($proof->{scope},'local_projection_only','projection scope');
 ok(!$proof->{ready_known} && !$proof->{shared_retirement_known}
  && !$proof->{vm_reference_counts_known} && !$proof->{authoritative},'no invented authority');
 is($saved[0]->[0],'b06-pair-retry-observation','raw observation saved before judgement');
};
subtest 'missing actual identity phase or reservation refuses the cell' => sub {
 for my $kind ('wrong-actor','missing-record','changed-receipt') {
  my ($proof,$error)=invoke($kind);
  like($error,qr/retry observation evidence refused/,"$kind refuses");
  ok(!defined($proof),'no positive observation proof');
 }
};
subtest 'no VM exposure or fabricated READY cannot count as B06 coverage' => sub {
 for my $kind ('no-vm','claimed-ready') {
  my ($proof,$error)=invoke($kind);
  like($error,qr/retry observation evidence refused/,"$kind refuses");
  ok(!defined($proof),'no coverage proof');
 }
};
subtest 'diagnostic reads and evaluation cannot renew the original deadline' => sub {
 for my $kind ('expired-entry','expired-during-read','expired-during-judge') {
  my ($proof,$error)=invoke($kind);
  like($error,qr/original deadline/,"$kind refuses");
  ok(!defined($proof),'no late proof');
  is_deeply(\@actions,[],'spent entry performs no SQL or judge') if $kind eq 'expired-entry';
  is_deeply(\@actions,['trace'],'late read performs no judgement') if $kind eq 'expired-during-read';
 }
};
done_testing();
