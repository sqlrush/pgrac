#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 356_scaling_efficiency.pl
#	  spec-6.12 scaling-efficiency probe (report-only, run via make check).
#	  READ  scenario: pgbench -S    WRITE scenario: pgbench -N
#	  solo (node0 alone) vs pair (node0+node1 concurrent).
#	  efficiency = pair_tps / (2 * solo_tps); decay = 1 - efficiency.
#	  All 6.12 waves ON.  Env: SC_SECS(8) SC_CLIENTS(4) SC_SCALE(10).
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/356_scaling_efficiency.pl
#
#-------------------------------------------------------------------------
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../lib";
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

my $SECS    = $ENV{SC_SECS}    // 8;
my $CLIENTS = $ENV{SC_CLIENTS} // 4;
my $SCALE   = $ENV{SC_SCALE}   // 10;
my $PGBENCH = $ENV{PGBENCH}    // 'pgbench';

sub tps_of { my $o = shift // ''; return ($o =~ /tps\s*=\s*([\d.]+)/) ? $1 + 0.0 : undef; }

our ($SCRIPT);   # set per scenario
sub bench
{
	my ($node) = @_;
	my $conn = "-h '" . $node->host . "' -p " . $node->port . ' postgres';
	my $out  = `$PGBENCH -n -f $SCRIPT -T $SECS -c $CLIENTS $conn 2>&1`;
	return tps_of($out) // 0;
}

sub bench_pair
{
	my ($n0, $n1) = @_;
	my $c0  = "-h '" . $n0->host . "' -p " . $n0->port . ' postgres';
	my $c1  = "-h '" . $n1->host . "' -p " . $n1->port . ' postgres';
	my $f0  = "/tmp/sc_n0_$$"; my $f1 = "/tmp/sc_n1_$$";
	system("$PGBENCH -n -f $SCRIPT -T $SECS -c $CLIENTS $c0 >$f0 2>&1 & "
		 . "$PGBENCH -n -f $SCRIPT -T $SECS -c $CLIENTS $c1 >$f1 2>&1 & wait");
	my $o0 = do { local $/; open my $h, '<', $f0; <$h> } // '';
	my $o1 = do { local $/; open my $h, '<', $f1; <$h> } // '';
	unlink $f0, $f1;
	main::diag("---- pair pgbench node0 raw ----\n$o0\n---- pair pgbench node1 raw ----\n$o1");
	return (tps_of($o0) // 0, tps_of($o1) // 0);
}

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'sc612',
	storage_backend     => $ENV{SC_BACKEND} // 'cluster_fs',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
		'cluster.read_scache = on',
		'cluster.page_scn_shortcut = on',
		'cluster.block_self_contained = on',
		'cluster.crossnode_cr_data_plane = on',
		'cluster.crossnode_runtime_visibility = on',
		'cluster.shared_cr_pool_enabled = on',
		'cluster.shared_cr_pool_size_blocks = 256',
		'cluster.ges_handoff = on',
		'cluster.space_affinity = static',
		'cluster.gcs_block_retransmit_max_retries = 60',
		'cluster.gcs_block_dedup_max_entries = 16384',
		'cluster.gcs_block_recovery_wait_ms = 2000',
	]);
$pair->start_pair;
usleep(2_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'pair up n0->n1');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'pair up n1->n0');
my ($n0, $n1) = ($pair->node0, $pair->node1);

# Phantom-shared model = shared DATA, SEPARATE catalogs.  pgbench -i only
# populates ONE node's catalog, so create the SAME table on BOTH nodes (same
# DDL -> same relfilenode -> shared storage), populate once on node0.
my $NROWS = 200 * $SCALE;   # small: the phantom pair storms on bulk load
$n0->safe_psql('postgres', 'CREATE TABLE st (id int, v int)');
$n1->safe_psql('postgres', 'CREATE TABLE st (id int, v int)');
my $q0 = $n0->safe_psql('postgres', q{SELECT pg_relation_filepath('st')});
my $q1 = $n1->safe_psql('postgres', q{SELECT pg_relation_filepath('st')});
ok($q0 eq $q1, "shared st relfilenode coincidence ($q0)");
$n0->safe_psql('postgres', "INSERT INTO st SELECT g, g FROM generate_series(1,$NROWS) g");
$n0->safe_psql('postgres', 'CHECKPOINT');
usleep(1_500_000);

# Custom pgbench scripts against the shared table.
my $rfile = "/tmp/sc_read_$$.sql";
my $wfile = "/tmp/sc_write_$$.sql";
open my $rf, '>', $rfile; print $rf "\\set id random(1, $NROWS)\nSELECT v FROM st WHERE id = :id;\n"; close $rf;
open my $wf, '>', $wfile; print $wf "\\set id random(1, $NROWS)\nUPDATE st SET v = v + 1 WHERE id = :id;\n"; close $wf;

for my $sc (['READ', $rfile], ['WRITE', $wfile])
{
	my ($label, $script) = @$sc;
	$SCRIPT = $script;
	my $solo = bench($n0);
	usleep(1_000_000);
	my ($p0, $p1) = bench_pair($n0, $n1);
	my $pair_tps = $p0 + $p1;
	my $ideal = 2 * $solo;
	my $eff   = $solo > 0 ? $pair_tps / $ideal * 100 : 0;

	diag(sprintf(
		"\n==================== %s (pgbench -f -c %d %ds, %d shared rows) ====================\n"
		  . "  1-node solo      : %.0f tps\n"
		  . "  2-node node0     : %.0f tps\n"
		  . "  2-node node1     : %.0f tps\n"
		  . "  2-node combined  : %.0f tps\n"
		  . "  ideal linear x2  : %.0f tps\n"
		  . "  >>> %s scaling efficiency = %.1f%%   (decay = %.1f%%)\n",
		$label, $CLIENTS, $SECS, $NROWS, $solo, $p0, $p1, $pair_tps, $ideal,
		$label, $eff, 100 - $eff));

	ok($solo > 0, "$label solo produced tps");
	ok($pair_tps > 0, "$label pair produced tps");
}

unlink $rfile, $wfile;
$pair->stop_pair if $pair->can('stop_pair');
done_testing();
