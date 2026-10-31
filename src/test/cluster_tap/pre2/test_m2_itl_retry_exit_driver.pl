#!/usr/bin/env perl
# Read the original failure observation through the actual Python evaluator.
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
my ($case,@actions,@saved);
sub save_json {push @saved,[@_]}
sub run {push @actions,'judge'; return IPC::Run::run(@_)}
for my $function ('judge_requalification','capture_wait_exit_retry') {
 my @body=$text =~ /(^sub \Q$function\E \{.*?^\})/msg;
 is(scalar(@body),1,"one actual $function boundary exists");
 if(@body!=1) {done_testing(); exit 1;}
 eval $body[0]; die $@ if $@;
}
{
 package FailedRetryNode;
 sub safe_psql {
  my ($self,$db,$sql)=@_;
  die "unexpected observer query: $sql" unless $sql eq 'SELECT test_pgrac_native_itl_trace()';
  push @actions,'trace';
  return JSON::PP::encode_json($case->{trace});
 }
}
sub invoke {
 my ($kind,$mode)=@_;
 my ($out,$err)=('','');
 local $ENV{PYTHONPATH}=$FindBin::RealBin;
 IPC::Run::run([$ENV{PYTHON} // 'python3','-B','-c',
  'import json,sys; from test_m2_itl_retry_exit import retry_exit; print(json.dumps(retry_exit(sys.argv[1])))',
  $kind],'>',\$out,'2>',\$err,timeout(2)) or die "fixture failed: $err";
 $case=decode_json($out); @actions=(); @saved=();
 if($mode eq 'wrong-actor') {$case->{trace}->{events}->[-2]->{backend_start}='901';}
 if($mode eq 'changed-receipt') {$case->{trace}->{events}->[-2]->{update_retry}->{reservation_sequence}='999';}
 if($mode eq 'fake-ready') {$case->{trace}->{events}->[-2]->{update_retry}->{ready_known}=JSON::PP::true;}
 if($mode eq 'missing-wait-end') {$case->{exit}->{log}=$case->{exit}->{start}->{log};}
 if($mode eq 'different-error') {$case->{exit}->{stdout}='B08_SQLSTATE 00000';}
 my $original={kind=>$kind,sqlstate=>$kind eq 'capacity_timeout' ? '53R70' : '57014',
  wait_seq=>9,end_seq=>18};
 my $proof=eval {capture_wait_exit_retry(bless({},'FailedRetryNode'),$case->{exit},$original)};
 return ($proof,$@);
}
subtest 'each failed original UPDATE is observed before rollback or replacement SQL' => sub {
 for my $kind ('cancel','statement_timeout','capacity_timeout') {
  my ($proof,$error)=invoke($kind,'ok');
  is($error,'',"$kind observation and evaluator complete");
  is_deeply(\@actions,['trace','judge'],'one observer read and actual judge only');
  is($proof->{reservation_sequence},'9007199254740993','original local reservation precision');
  is($proof->{exit_kind},$kind,'original cause retained');
  is($saved[0]->[0],"b08-$kind-retry-observation",'raw observation saved before judgement');
 }
};
subtest 'same-page failure with no VM handle grants no cleanup or READY authority' => sub {
 my ($proof,$error)=invoke('cancel','ok');
 is($error,'','no handle is not an invented coverage requirement for an error');
 is($proof->{vm_handle_slots_observed},0,'actual zero-handle observation retained');
 is($proof->{last_phase},2,'no completed retry is fabricated');
 is($proof->{wait_deadline_us},'0','unobserved first wait deadline is unknown');
 for my $field (qw(authoritative ready_known shared_retirement_known vm_reference_counts_known
                   abort_cleanup_known wait_deadline_known)) {
  ok(exists($proof->{$field}) && !$proof->{$field},"$field remains false");
 }
};
subtest 'identity receipt and claimed authority faults refuse the observation' => sub {
 for my $mode ('wrong-actor','changed-receipt','fake-ready') {
  my ($proof,$error)=invoke('cancel',$mode);
  like($error,qr/failed UPDATE retry evidence refused/,"$mode refuses");
  ok(!defined($proof),'no positive failed-wait observation proof');
 }
};
subtest 'original error and exact wait exit remain mandatory' => sub {
 for my $mode ('missing-wait-end','different-error') {
  my ($proof,$error)=invoke('cancel',$mode);
  like($error,qr/failed UPDATE retry evidence refused/,"$mode refuses");
  ok(!defined($proof),'no replacement error or missing wait can pass');
 }
};
done_testing();
