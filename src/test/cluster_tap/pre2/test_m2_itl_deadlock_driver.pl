#!/usr/bin/env perl
# Exercise the actual native observer; only clock, IPC and activity are leaves.
use strict;
use warnings;
use FindBin;
use Test::More;
use subs qw(sleep);

my ($now,$states,$pumps);
sub CLOCK_MONOTONIC () {1}
sub clock_gettime {return $now}
sub sleep {$now+=$_[0]}
sub activity {
 my ($node,$pid)=@_;
 my $queue=$states->{$pid};
 return shift @$queue if @$queue>1;
 return $queue->[0];
}
{
 package DeadlockClient;
 sub pump_nb {$pumps++}
}
open my $f,'<',"$FindBin::RealBin/../t/482_pre2_m2_itl_capacity.pl" or die $!;
my $text=do {local $/; <$f>};
close $f or die $!;
my @body=$text =~ /(^sub deadlock_victim \{.*?^\})/msg;
ok(@body==1,'the actual native deadlock observer exists exactly once');
if(@body!=1) {done_testing(); exit 1;}
eval $body[0];
die $@ if $@;

sub observe {
 my ($input)=@_;
 ($now,$states,$pumps)=(0,$input,0);
 my @clients=map {+{pid=>$_,sql=>"UPDATE page$_;",session=>{run=>bless({},'DeadlockClient')}}} (100,200);
 local $SIG{ALRM}=sub {die 'offline observer watchdog expired'};
 alarm 5;
 my $result=eval {deadlock_victim(undef,\@clients,0.1)};
 my $error=$@;
 alarm 0;
 return ($result,$error);
}
my $waiting=sub {+{state=>'active',query=>"UPDATE page$_[0];",wait_event=>'GesTxEnqueueWait'}};
my $aborted=sub {+{state=>'idle in transaction (aborted)',query=>"UPDATE page$_[0];"}};

subtest 'a sent echo is not yet an executed UPDATE' => sub {
 my ($result,$error)=observe({100=>[{state=>'idle in transaction',query=>'BEGIN'},$waiting->(100)],
  200=>[$waiting->(200),$aborted->(200)]});
 is($error,'','both clients are pumped until actual failure');
 is($result->{pid},200,'actual aborted participant is returned') unless $error;
 cmp_ok($pumps,'>=',4,'pre-query state did not select an owner');
};
subtest 'either native participant may be the victim' => sub {
 for my $pid (100,200) {
  my ($result,$error)=observe({map {$_=>[$_==$pid ? $aborted->($_) : $waiting->($_)]} (100,200)});
  is($error,'','one actual aborted backend is observed');
  is($result->{pid},$pid,'victim is observed rather than predetermined') unless $error;
 }
};
subtest 'both aborts, a missing backend or the wrong query fail explicitly' => sub {
 for my $input ({100=>[$aborted->(100)],200=>[$aborted->(200)]},
               {100=>[undef],200=>[$waiting->(200)]},
               {100=>[{state=>'idle in transaction (aborted)',query=>'old query'}],200=>[$waiting->(200)]}) {
  my ($result,$error)=observe($input);
  like($error,qr/more than one|disappeared|original UPDATE/,'ambiguous failure supplies no victim');
  ok(!defined($result),'no owner can be released from this observation');
 }
};
subtest 'no deadlock victim keeps one original observation deadline' => sub {
 my ($result,$error)=observe({100=>[$waiting->(100)],200=>[$waiting->(200)]});
 like($error,qr/original deadline/,'long waiting is not deadlock evidence');
 cmp_ok($now,'>=',0.1,'polling does not renew the deadline');
 ok(!defined($result),'no victim or implicit cancellation is manufactured');
};
subtest 'ordinary completion cannot substitute for a deadlock exit' => sub {
 my ($result,$error)=observe({100=>[{state=>'idle in transaction',query=>'UPDATE page100;'}],
  200=>[$waiting->(200)]});
 like($error,qr/completed without/,'a broken or missing cycle fails');
 ok(!defined($result),'completion selects no victim');
};
subtest 'activity reads may straddle victim abort and survivor completion' => sub {
 my ($result,$error)=observe({100=>[$waiting->(100),$aborted->(100)],
  200=>[{state=>'idle in transaction',query=>'UPDATE page200;'}]});
 is($error,'','a non-atomic activity snapshot is allowed to settle');
 is($result->{pid},100,'the actual victim is observed on the next poll') unless $error;
 cmp_ok($pumps,'>=',4,'ordinary completion alone did not terminate observation');
};
done_testing();
