#!/usr/bin/env perl
# Execute the actual native mutation/commit boundary with local SQL leaves.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use FindBin;
use JSON::PP qw(encode_json decode_json);
use Test::More;

open my $source,'<',"$FindBin::RealBin/../t/482_pre2_m2_itl_capacity.pl" or die $!;
my $text=do {local $/; <$source>};
close $source or die $!;
my @body=$text =~ /(^sub commit_requalification_mutator \{.*?^\})/msg;
is(scalar(@body),1,'one actual mutation-to-commit boundary exists');
if(@body!=1) {done_testing(); exit 1;}
my ($now,$actor_pid,$actor_xid,$reject,$expire,@actions);
sub CLOCK_MONOTONIC () {1}
sub clock_gettime {$now}
sub save_json {}
sub slurp_file {'original wait log'}
sub activity {return {pid=>200,state=>'active',query=>'UPDATE original',wait_event=>'GesTxEnqueueWait'}}
sub exit_owners {return []}
sub judge_requalification {
 my ($case,$option)=@_;
 push @actions,'judge';
 return (undef,'refused') if $reject eq 'refuse';
 my $selected={pid=>100,xid=>1200,wait_seq=>9,key=>{root_id=>32768}};
 $selected->{pid}=101 if $reject eq 'wrong-owner';
 $selected->{key}->{root_id}=32769 if $reject eq 'wrong-key';
 return ({kind=>$case->{kind},selected=>$selected,authoritative=>JSON::PP::false},'');
}
{
 package B06Mutator;
 sub query_safe {
  my ($self,$sql)=@_;
  push @actions,$sql;
  return $actor_pid if $sql eq 'SELECT pg_backend_pid()';
  if($sql eq 'SELECT pg_current_xact_id()::text') {
   $now=2 if $expire eq 'identity';
   return $actor_xid;
  }
  if($sql =~ /^WITH changed AS/) {
   $now=2 if $expire;
   return main::encode_json({id=>9,v=>10,ctid=>'(0,18)',xmin=>'1200',xmax=>'0'});
  }
  return '' if $sql eq 'COMMIT';
  die "unexpected native query: $sql";
 }
 package B06Node;
 sub logfile {'unused'}
 package B06Pump;
 sub pump_nb {push @actions,'pump'}
}
eval $body[0]; die $@ if $@;
sub invoke {
 my ($mode)=@_;
 ($now,$actor_pid,$actor_xid,$reject,$expire)=(0,100,1200,'',0);
 @actions=();
 $actor_pid=999 if $mode eq 'wrong-session';
 $actor_xid=999 if $mode eq 'wrong-transaction';
 $reject='refuse' if $mode eq 'refuse';
 $reject='wrong-owner' if $mode eq 'wrong-proof';
 $reject='wrong-key' if $mode eq 'wrong-key';
 $now=2 if $mode eq 'expired-before';
 $expire=1 if $mode eq 'expired-after';
 $expire='identity' if $mode eq 'expired-during-identity';
 my $selected={pid=>100,xid=>1200,wait_seq=>9,key=>{root_id=>32768}};
 my $result=eval {commit_requalification_mutator(bless({},'B06Node'),
  {run=>bless({},'B06Pump')},bless({},'B06Mutator'),{holders=>[]},$selected,
  'update','m2_b06_update',{id=>9},'UPDATE original',1400,0,1)};
 return ($result,$@);
}
subtest 'only a proved mutation can commit the original owner' => sub {
 my ($result,$error)=invoke('ok');
 is($error,'','actual native boundary completes');
 is($result->{mutator_pid},100,'original client identity retained') unless $error;
 my ($mutation)=grep {$actions[$_] =~ /^WITH changed AS/} 0..$#actions;
 my ($judge)=grep {$actions[$_] eq 'judge'} 0..$#actions;
 my @commits=grep {$actions[$_] eq 'COMMIT'} 0..$#actions;
 is(scalar(@commits),1,'one exact commit');
 ok(defined($mutation) && defined($judge) && $mutation<$judge && $judge<$commits[0],
  'mutation is observed and judged before commit');
};
subtest 'unrelated session refuses before any mutation' => sub {
 for my $mode ('wrong-session','wrong-transaction') {
  my ($result,$error)=invoke($mode);
  like($error,qr/identity/,'wrong original client identity refuses');
  is(scalar(grep {/^WITH changed AS|^COMMIT$/} @actions),0,'no write or commit');
 }
};
subtest 'missing or substituted proof cannot release an owner' => sub {
 for my $mode ('refuse','wrong-proof','wrong-key') {
  my ($result,$error)=invoke($mode);
  like($error,qr/mutation evidence/,'unproved mutation refuses');
  is(scalar(grep {$_ eq 'COMMIT'} @actions),0,'no commit after refusal');
 }
};
subtest 'the original deadline is checked before mutation and commit' => sub {
 for my $mode ('expired-before','expired-after','expired-during-identity') {
  my ($result,$error)=invoke($mode);
  like($error,qr/original deadline/,'original deadline refuses');
  is(scalar(grep {$_ eq 'COMMIT'} @actions),0,'deadline cannot be renewed by mutation');
  is(scalar(@actions),0,'no SQL after a spent entry deadline') if $mode eq 'expired-before';
  is(scalar(grep {/^WITH changed AS/} @actions),0,'no mutation after identity reads spent the deadline')
   if $mode eq 'expired-during-identity';
 }
};
done_testing();
