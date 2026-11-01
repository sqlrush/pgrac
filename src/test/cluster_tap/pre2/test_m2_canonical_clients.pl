#!/usr/bin/env perl
# Exercise the A08 client's real IPC timer without a database or cluster.
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::Cluster;
use Test::More;
use Time::HiRes qw(sleep clock_gettime CLOCK_MONOTONIC);

# Use the actual driver entry and cleanup, not a second client policy.
open my $source, '<', "$FindBin::RealBin/../t/480_pre2_m2_canonical_interleave.pl" or die $!;
my $text = do { local $/; <$source> };
close $source or die $!;
my %sessions;
for my $name (qw(session finish_sessions)) {
 my @body = $text =~ /(^sub \Q$name\E \{.*?^\})/msg;
 die "exactly one actual $name body is required" unless @body == 1;
 eval $body[0];
 die $@ if $@;
}

# A pipe process implements only psql's banner protocol. BackgroundPsql and
# IPC::Run, including expiration and process reaping, are the real modules.
my $client = q{
 use strict;
 use warnings;
 use IO::Handle;
 STDOUT->autoflush(1);
 STDERR->autoflush(1);
 my $statement_timeout = 'unset';
 while (my $line = <STDIN>) {
  sleep 20 if $line =~ /^SELECT stall/;
  if (substr($line, 0, 1) eq chr(92)) {
   my $meta = substr($line, 1);
   exit 0 if $meta =~ /^q/;
   if ($meta =~ /^(echo|warn) '(.*)'/) {
    if ($1 eq 'echo') { print "$2\n"; }
    else { print STDERR "$2\n"; }
   }
  }
  $statement_timeout = $1 if $line =~ /^SET statement_timeout=(\d+)/;
  print "$statement_timeout\n" if $line =~ /^SHOW statement_timeout/;
  print "ready\n" if $line =~ /^SELECT ready/;
  print "$$\n" if $line =~ /^SELECT child_pid/;
 }
};
my (@requested, @children);
{
 package CanonicalClientNode;
 our @ISA = ('PostgreSQL::Test::Cluster');
 sub background_psql {
  my ($self, $db, %params) = @_;
  push @requested, {database => $db, %params};
  # Only the offline clock scale differs; the native request remains 180s.
  my $s = PostgreSQL::Test::BackgroundPsql->new(0, [$^X, '-e', $client], 2, 1);
  push @children, $s;
  return $s;
 }
}
END {
 for my $s (@children) {
  eval { $s->{run}->kill_kill(grace => 0) };
 }
}
sub client_pid {
 my ($s) = @_;
 my $pid = $s->query_safe('SELECT child_pid');
 die 'pipe child did not report its process identity' unless $pid =~ /^\d+$/ && $pid > 1;
 return 0 + $pid;
}
my $node = bless {}, 'CanonicalClientNode';
subtest 'prepared pair outlives one client interval and still exits cleanly' => sub {
 my $cell = {number => 7};
 my @pair = map { session($node, $cell) } 1..2;
 my @pids = map { client_pid($_) } @pair;
 sleep(4);
 my @errors;
 for my $s (@pair) {
  eval {
   is($s->query_safe('SHOW statement_timeout'), '90000', 'SQL timeout remains 90 seconds');
   is($s->query_safe('SELECT ready'), 'ready', 'the original prepared client still responds');
   1;
  } or push @errors, $@;
 }
 eval { finish_sessions($cell); 1 } or push @errors, $@;
 is_deeply(\@errors, [], 'idle matrix time does not consume later query or quit time');
 ok(!grep({ kill(0, $_) } @pids), 'both prepared child processes were reaped');
 is_deeply(\@requested, [map { {database => 'postgres', timeout => 180} } 1..2],
  'native clients retain their original bounded timeout request');
};
subtest 'an unresponsive query still expires' => sub {
 my $cell = {number => 8};
 my $s = session($node, $cell);
 my $pid = client_pid($s);
 my $begin = clock_gettime(CLOCK_MONOTONIC);
 my $ok = eval { $s->query_safe('SELECT stall'); 1 };
 my $error = $@;
 ok(!$ok, 'a stalled command cannot complete successfully');
 like($error, qr/(?:timeout on timer|psql query timed out)/, 'the real IPC deadline remains enforced');
 cmp_ok(clock_gettime(CLOCK_MONOTONIC) - $begin, '<', 10, 'failure precedes the stalled response');
 $s->{run}->kill_kill(grace => 0);
 ok(!kill(0, $pid), 'the owned stalled child was reaped');
 delete $sessions{$cell->{number}};
};
done_testing();
