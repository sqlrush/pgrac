#!/usr/bin/env perl
# Author: SqlRush <sqlrush@gmail.com>
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use lib "$FindBin::RealBin/../pre2";
use P0RetestSql;
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils qw(slurp_file);
use JSON::PP;
use Test::More;

my $case=$ENV{PGRAC_P0_RETEST_CASE} // '';
die 'select one targeted case' unless $case =~ /\AP0-(5|8|10|12)\z/;
if ($case eq 'P0-8') {
 $ENV{PGRAC_ITL_CAPACITY_ONLY}=1;
 exec $^X,"$FindBin::RealBin/482_pre2_m2_itl_capacity.pl";
 die "capacity driver exec: $!";
}
my $logdir=$ENV{TESTLOGDIR} // die 'TESTLOGDIR required';
my ($cluster,@nodes,@sessions,$driver,$sql_ok,$stop_ok);
my $result={version=>1,case=>$case,sql_completed=>JSON::PP::false,
 normal_stop=>JSON::PP::false,cleanup_errors=>[],nodes=>[]};
open my $journal,'>',"$logdir/p0-sql.jsonl" or die $!;
my $ok=eval {
 $cluster=PostgreSQL::Test::ClusterPRE2->new_cluster('p0_targeted',nodes=>4,blackbox=>1,
  required_operations=>[],extra_conf=>['autovacuum=off','shared_buffers=512MB',
   'cluster.trace_normal_stop=on','cluster.trace_tx_identity=on','cluster.trace_control_config=on',
   "log_error_verbosity=verbose","log_line_prefix='%m [%p] '"]);
 @nodes=$cluster->nodes;
 $cluster->start_cluster;
 die 'four normal members required' unless @nodes==4;
 $result->{nodes}=[0,1,2,3];
 for my $n (@nodes,$nodes[2]) {
  my $s=$n->background_psql('postgres',timeout=>120);
  push @sessions,$s;
  $s->query_safe('SET statement_timeout=90000');
 }
 $driver=P0RetestSql->new(sessions=>[@sessions[0..3]],hot_reader=>$sessions[4],schema=>'p0_targeted',
  emit=>sub {print {$journal} encode_json($_[0])."\n" or die $!},
  between_lanes=>sub {},receipt_barrier=>sub {});
 $result->{sql_evidence}=$driver->run_targeted($case);
 $result->{sql_completed}=JSON::PP::true;
 1;
};
$result->{first_error}=$@ unless $ok;
ok($ok,'targeted ordinary SQL and complete row comparisons');
diag($result->{first_error}) unless $ok;
for my $s (@sessions) {
 eval {$s->quit or die 'original psql did not quit'; 1}
  or push @{$result->{cleanup_errors}},$@;
}
@sessions=();
ok(!@{$result->{cleanup_errors}},'all original clients close');
$stop_ok=eval {
 die 'no running cohort' unless $cluster && $cluster->{running};
 $cluster->stop_cluster;
 die 'normal stop is unproven' unless $cluster->{normal_stop_proven};
 $result->{normal_stop}=JSON::PP::true;
 1;
};
$result->{stop_error}=$@ unless $stop_ok;
ok($stop_ok,'normal four-member stop proof');
diag($result->{stop_error}) unless $stop_ok;
for my $i (0..$#nodes) {
 open my $out,'>',"$logdir/node$i.log" or die $!;
 print {$out} slurp_file($nodes[$i]->logfile);
 close $out or die $!;
}
close $journal or die $!;
open my $out,'>',"$logdir/p0-result.json" or die $!;
print {$out} JSON::PP->new->canonical->pretty->encode($result);
close $out or die $!;
done_testing();
