#!/usr/bin/env perl
# Exercise the actual native driver's observation boundary without a database.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use FindBin;
use JSON::PP qw(encode_json decode_json);
use Test::More;
use subs qw(sleep);

open my $source,'<',"$FindBin::RealBin/../t/482_pre2_m2_itl_capacity.pl" or die $!;
my $text=do {local $/; <$source>};
close $source or die $!;
my @bodies=$text =~ /(^sub exact_exit_wait \{.*?^\})/msg;
die 'exactly one actual observer body is required' unless @bodies==1;

my ($now,$activity,$select_calls,$pumps,$accept,$saved);
sub CLOCK_MONOTONIC () {1}
sub clock_gettime {return $now}
sub sleep {$now+=$_[0]}
sub activity {return shift @$activity if @$activity>1; return $activity->[0]}
sub slurp_file {return 'actual observation bytes'}
sub save_json {$saved++}
sub select_blocker {$select_calls++; return $accept ? ({wait_seq=>9},'') : (undef,'unmatched')}
eval $bodies[0];
die $@ if $@;

{
 package NativeExitReader;
 sub pump_nb {$pumps++}
 package NativeExitNode;
 sub logfile {'unused'}
 sub safe_psql {
  return $_[2] =~ /test_pgrac_native_itl_page/ ? '{"sequence":90}' : '{"events":[]}';
 }
}

sub observe {
 my ($states,$matched)=@_;
 ($now,$activity,$select_calls,$pumps,$accept,$saved)=(0,[@$states],0,0,$matched,0);
 local $SIG{ALRM}=sub {die 'offline observer watchdog expired'};
 alarm 5;
 my @result=eval {exact_exit_wait(bless({},'NativeExitNode'),
  {run=>bless({},'NativeExitReader')},200,'UPDATE m2_b08_cancel SET v=1 WHERE id=9;',
  0,[1663,5,20000,0,0],[],0,0.1,'m2_b08_cancel')};
 my $error=$@;
 alarm 0;
 return (\@result,$error);
}
my $waiting={state=>'active',query=>'UPDATE m2_b08_cancel SET v=1 WHERE id=9;',
 wait_event=>'GesTxEnqueueWait'};

subtest 'pre-query echo is followed by actual server execution' => sub {
 my ($result,$error)=observe([{state=>'idle in transaction',query=>'BEGIN'},$waiting],1);
 is($error,'','queued UPDATE is allowed to reach the server');
 is($result->[1]->{wait_seq},9,'the real wait observation selects its exact registration') unless $error;
 cmp_ok($pumps,'>=',2,'queued client input was pumped until server execution');
 is($select_calls,1,'only an actual capacity wait is evaluated');
};
subtest 'completion between polls returns to the caller' => sub {
 my ($result,$error)=observe([{state=>'idle in transaction',query=>$waiting->{query}}],1);
 is($error,'','completed UPDATE is returned rather than misclassified');
 ok(!defined($result->[0]) && !defined($result->[1]),'completion grants no invented blocker');
 is($select_calls,0,'no page or resolver observation is used as a success substitute');
};
subtest 'aborted or missing backend is an explicit failure' => sub {
 for my $state (undef,{state=>'idle in transaction (aborted)',query=>$waiting->{query}}) {
  my ($result,$error)=observe([$state],1);
  like($error,qr/waiter disappeared|waiter.*failed/,'missing or failed backend refuses');
  is($select_calls,0,'no owner can be selected after failure');
 }
};
subtest 'a sent echo alone never proves an exact wait' => sub {
 my ($result,$error)=observe([{state=>'idle in transaction',query=>'BEGIN'}],1);
 like($error,qr/original deadline/,'no server execution fails at the original observation deadline');
 cmp_ok($now,'>=',0.1,'the deadline is not reset by polling');
 is($select_calls,0,'a queued UPDATE supplies no selected owner');
};
subtest 'unmatched diagnostics never release an arbitrary owner' => sub {
 my ($result,$error)=observe([$waiting],0);
 like($error,qr/original deadline: unmatched/,'unmatched key stays a failure');
 cmp_ok($select_calls,'>',1,'fresh samples are inspected within one observation deadline');
 cmp_ok($now,'>=',0.1,'rejected samples do not renew the deadline');
};
done_testing();
