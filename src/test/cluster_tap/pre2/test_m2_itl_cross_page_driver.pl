#!/usr/bin/env perl
# Exercise the real cross-page owner release boundary with local SQL leaves.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use FindBin;
use JSON::PP qw(encode_json decode_json);
use Test::More;

open my $source,'<',"$FindBin::RealBin/../t/482_pre2_m2_itl_capacity.pl" or die $!;
my $text=do {local $/; <$source>};
close $source or die $!;
my @body=$text =~ /(^sub commit_cross_page_owner \{.*?^\})/msg;
is(scalar(@body),1,'one actual cross-page owner release boundary exists');
if(@body!=1) {done_testing(); exit 1;}
my ($now,$pid,$xid,$mode,@actions);
sub CLOCK_MONOTONIC () {1}
sub clock_gettime {$now}
sub save_json {}
sub slurp_file {'actual wait log'}
sub activity {return {pid=>200,state=>'active',wait_event=>'GesTxEnqueueWait'}}
sub exit_owners {return []}
sub judge_requalification {
 my ($case,$option)=@_;
 push @actions,'judge';
 return (undef,'unproved') if $mode eq 'refuse';
 my $proof={kind=>'pair',selected=>{pid=>100,xid=>1200,wait_seq=>9,key=>{root_id=>32768}},
  source_tag=>[1663,5,20000,0,1],destination_tag=>[1663,5,20000,0,0],authoritative=>JSON::PP::false};
 $proof->{selected}->{key}->{root_id}=999 if $mode eq 'wrong-key';
 $proof->{source_tag}->[4]=0 if $mode eq 'wrong-page';
 $now=2 if $mode eq 'expired-after-proof';
 return ($proof,'');
}
{
 package PairOwner;
 sub query_safe {
  my ($self,$sql)=@_;
  push @actions,$sql;
  return $pid if $sql eq 'SELECT pg_backend_pid()';
  if($sql eq 'SELECT pg_current_xact_id()::text') {
   $now=2 if $mode eq 'expired-during-identity';
   return $xid;
  }
  return '' if $sql eq 'COMMIT';
  die "unexpected native query: $sql";
 }
 package PairNode;
 sub logfile {'unused'}
 package PairPump;
 sub pump_nb {push @actions,'pump'}
}
eval $body[0]; die $@ if $@;
sub invoke {
 ($mode)=@_;
 ($now,$pid,$xid)=(0,100,1200);
 @actions=();
 $pid=999 if $mode eq 'wrong-pid';
 $xid=999 if $mode eq 'wrong-xid';
 $now=2 if $mode eq 'expired-entry';
 my $selected={pid=>100,xid=>1200,wait_seq=>9,key=>{root_id=>32768}};
 my $case={start=>{holders=>[],waiter_pid=>200,tag=>[1663,5,20000,0,0]},source_tag=>[1663,5,20000,0,1]};
 my $result=eval {commit_cross_page_owner(bless({},'PairNode'),{run=>bless({},'PairPump')},
  bless({},'PairOwner'),$case,$selected,0,1)};
 return ($result,$@);
}
subtest 'only a freshly proved pair wait releases one exact owner' => sub {
 my ($result,$error)=invoke('ok');
 is($error,'','actual release boundary completes');
 is($result->{log},'actual wait log','returns the observation preceding this commit') unless $error;
 my ($judge)=grep {$actions[$_] eq 'judge'} 0..$#actions;
 my @commits=grep {$actions[$_] eq 'COMMIT'} 0..$#actions;
 is(scalar(@commits),1,'one exact commit');
 ok(defined($judge) && $judge<$commits[0],'fresh pair evidence precedes commit');
};
subtest 'a substituted session or transaction cannot commit' => sub {
 for my $kind ('wrong-pid','wrong-xid') {
  my ($result,$error)=invoke($kind);
  like($error,qr/identity/,'original client identity refuses');
  is(scalar(grep {$_ eq 'COMMIT'} @actions),0,'no commit');
 }
};
subtest 'missing evidence or a substituted full key or page refuses' => sub {
 for my $kind ('refuse','wrong-key','wrong-page') {
  my ($result,$error)=invoke($kind);
  like($error,qr/pair evidence/,'unproved pair refuses');
  is(scalar(grep {$_ eq 'COMMIT'} @actions),0,'no commit');
 }
};
subtest 'identity and evidence reads cannot renew the original deadline' => sub {
 for my $kind ('expired-entry','expired-during-identity','expired-after-proof') {
  my ($result,$error)=invoke($kind);
  like($error,qr/original deadline/,'original deadline refuses');
  is(scalar(grep {$_ eq 'COMMIT'} @actions),0,'no late commit');
  is(scalar(@actions),0,'spent entry deadline performs no SQL') if $kind eq 'expired-entry';
 }
};
done_testing();
