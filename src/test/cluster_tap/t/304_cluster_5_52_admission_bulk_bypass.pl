#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 304_cluster_5_52_admission_bulk_bypass.pl
#	  spec-5.52 D2/D3 — shared CR pool admission policy, real-server e2e for the
#	  seqscan bulk-bypass executor wiring (v0.108.1 closes the dead-path gap).
#
#	  The admission predicate's CR_SCAN_BULK reject path was unit-proven
#	  (test_cluster_cr_admit::test_scan_resistant_bulk_rejects) but, before
#	  v0.108.1, had NO production caller: nothing set CR_SCAN_BULK from a real
#	  scan, so a live seqscan was never tagged BULK and the bypass was dormant.
#	  v0.108.1 wires nodeSeqscan.c SeqNext() to set the marker around the serial
#	  table fetch under the scan_resistant policy.  This test proves the wiring
#	  is LIVE: a real serial seqscan that constructs CR images under the
#	  scan_resistant policy bumps cr_pool.admit_reject_bulk, the bulk images are
#	  NOT published into the shared pool, and the query results are still correct
#	  (admission never affects correctness -- the bypassed access bare-constructs
#	  and serves the same image).  The admit_all contrast leg proves the marker
#	  is policy-gated (no reject when admit_all).
#
#	  Single cluster-enabled node (allow_single_node), cr_mvcc_gate=on +
#	  cr_gate_no_peer_fastpath=off so the gate really constructs; same quiescent
#	  same-read_scn mechanism as spec-5.50 t/300 / spec-5.51 t/303
#	  (pg_export_snapshot fail-closes in cluster mode by design).  Serial seqscan
#	  forced via max_parallel_workers_per_gather=0 (parallel workers are tagged
#	  PARALLEL intrinsically, a different bypass path).
#
#	  Spec: spec-5.52-cr-cache-admission-policy.md (FROZEN v0.4)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/304_cluster_5_52_admission_bulk_bypass.pl
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('spec_5_52_admit');
$node->init;
$node->append_conf(
	'postgresql.conf', join("\n",
		'cluster.enabled = on',
		'cluster.node_id = 0',
		'cluster.allow_single_node = on',
		'cluster.interconnect_tier = stub',
		'cluster.cr_mvcc_gate = on',
		'cluster.cr_gate_no_peer_fastpath = off',
		'cluster.shared_cr_pool_enabled = on',
		'cluster.shared_cr_pool_size_blocks = 256',
		'cluster.cr_pool_admission_policy = scan_resistant',
		'autovacuum = off',
		'jit = off',
		'synchronize_seqscans = off',
		''));
$node->start;

my $pool = sub {
	my ($k) = @_;
	return $node->safe_psql('postgres',
		qq{SELECT value::bigint FROM pg_cluster_state WHERE category='cr_pool' AND key='$k'});
};
my $cr = sub {
	my ($k) = @_;
	return $node->safe_psql('postgres',
		qq{SELECT value::bigint FROM pg_cluster_state WHERE category='cr' AND key='$k'});
};

# Open a serial repeatable-read reader: forces a serial seqscan (no parallel
# workers) and returns the backend handle.  read_scn == cluster_scn_current().
my $open_serial_reader = sub {
	my $b = $node->background_psql('postgres', on_error_die => 1);
	$b->query_safe('SET max_parallel_workers_per_gather = 0');
	$b->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ');
	return $b;
};


# ----------
# L1: pool + admission GUCs; scan_resistant policy active.
# ----------
{
	is($node->safe_psql('postgres', 'SELECT 1'), '1', 'L1a node alive');
	is( $node->safe_psql('postgres',
			q{SELECT setting FROM pg_settings WHERE name='cluster.cr_pool_admission_policy'}),
		'scan_resistant', 'L1b admission policy = scan_resistant');
	is( $node->safe_psql('postgres',
			q{SELECT count(*) FROM pg_cluster_state WHERE category='cr_pool' AND key LIKE 'admit%'}),
		'8', 'L1c cr_pool exposes 8 admission counters');
}


# ----------
# L2: serial seqscan under scan_resistant -> CR_SCAN_BULK wired -> bypass.
# ----------
{
	$node->safe_psql('postgres',
		"CREATE TABLE t_bulk (id int, pad text);
		 INSERT INTO t_bulk SELECT g, repeat('x', 120) FROM generate_series(1, 3000) g;");

	# Reader snapshots BEFORE the post-snapshot writer so it needs CR for every
	# block it later scans.
	my $r = $open_serial_reader->();
	$r->query_safe('SELECT count(*) FROM t_bulk');	# fix snapshot (also warms)

	# Post-snapshot writer touches every row; settle base_page_lsn afterwards so
	# the CR keys are stable during measurement.
	$node->safe_psql('postgres', 'UPDATE t_bulk SET pad = pad');
	$node->safe_psql('postgres', 'SELECT count(*) FROM t_bulk') for 1 .. 3;

	my $pre_reject  = $pool->('admit_reject_bulk');
	my $pre_publish = $pool->('publish_count');
	my $pre_constr  = $cr->('cr_construct_count');

	# The measured serial seqscan: constructs CR images for the modified blocks;
	# the v0.108.1 wiring tags each fetch BULK -> admission rejects -> bypass.
	my $cnt = $r->query_safe('SELECT count(*) FROM t_bulk');
	chomp $cnt;

	my $constr_delta = $cr->('cr_construct_count') - $pre_constr;
	my $reject_delta = $pool->('admit_reject_bulk') - $pre_reject;
	my $pub_delta    = $pool->('publish_count') - $pre_publish;

	is($cnt, '3000', 'L2a serial seqscan result correct (CR served despite bypass)');
	ok($constr_delta > 0,
		"L2b serial seqscan constructed CR images (delta=$constr_delta)");
	ok($reject_delta > 0,
		"L2c admit_reject_bulk rose (delta=$reject_delta) -- seqscan bulk-bypass wiring is LIVE");
	is($pub_delta, '0',
		"L2d bulk CR images NOT published to the shared pool (publish delta=$pub_delta)");

	$r->query_safe('COMMIT');
	$r->quit;
}


# ----------
# L3: admit_all contrast -> marker policy-gated -> no bulk reject, images admitted.
# ----------
{
	$node->safe_psql('postgres',
		q{ALTER SYSTEM SET cluster.cr_pool_admission_policy = admit_all});
	$node->reload;
	is( $node->safe_psql('postgres',
			q{SELECT setting FROM pg_settings WHERE name='cluster.cr_pool_admission_policy'}),
		'admit_all', 'L3a policy switched to admit_all (SIGHUP)');

	$node->safe_psql('postgres',
		"CREATE TABLE t_admit (id int, pad text);
		 INSERT INTO t_admit SELECT g, repeat('y', 120) FROM generate_series(1, 3000) g;");

	my $r = $open_serial_reader->();
	$r->query_safe('SELECT count(*) FROM t_admit');
	$node->safe_psql('postgres', 'UPDATE t_admit SET pad = pad');
	$node->safe_psql('postgres', 'SELECT count(*) FROM t_admit') for 1 .. 3;

	my $pre_reject  = $pool->('admit_reject_bulk');
	my $pre_publish = $pool->('publish_count');

	my $cnt = $r->query_safe('SELECT count(*) FROM t_admit');
	chomp $cnt;

	is($cnt, '3000', 'L3b admit_all serial seqscan result correct');
	is($pool->('admit_reject_bulk') - $pre_reject, '0',
		'L3c admit_all: no bulk reject (marker is policy-gated to scan_resistant)');
	ok($pool->('publish_count') - $pre_publish > 0,
		'L3d admit_all: seqscan CR images ARE published (populate-on-construct)');

	$r->query_safe('COMMIT');
	$r->quit;
}


# ----------
# L4: correctness guard -- no corruption, non-CR read byte-faithful.
# ----------
{
	is($cr->('cr_corruption_count'), '0', 'L4a no CR corruption across admission e2e');
	$node->safe_psql('postgres',
		'CREATE TABLE t_g (id int, v int); INSERT INTO t_g VALUES (1, 42);');
	is($node->safe_psql('postgres', 'SELECT v FROM t_g WHERE id = 1'), '42',
		'L4b non-CR read correct (admission never affects correctness)');
}


$node->stop;
done_testing();
