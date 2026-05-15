#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 107_lms_enabled.pl
#	  spec-2.20 regression smoke for cluster.lms_enabled.
#
#	  Verifies the startup-only LMS ownership fallback:
#	    L1 default cluster.lms_enabled=on exposes one LMS backend
#	    L2 cluster.lms_enabled=off is visible as a postmaster bool GUC
#	    L3 cluster.lms_enabled=off keeps LMS un-forked after steady state
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use PgracClusterNode;


my $node_on = PgracClusterNode->new('lms_on');
$node_on->init;
$node_on->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$node_on->start;

is($node_on->safe_psql('postgres', q{SHOW "cluster.lms_enabled"}),
   'on',
   'L1 cluster.lms_enabled default is on');

ok($node_on->poll_query_until(
	'postgres',
	q{SELECT count(*) = 1 FROM pg_stat_activity WHERE backend_type = 'lms'}),
   'L1a LMS aux process visible when cluster.lms_enabled=on');

$node_on->stop;


my $node_off = PgracClusterNode->new('lms_off');
$node_off->init;
$node_off->append_conf('postgresql.conf',
	"cluster.node_id = 0\ncluster.lms_enabled = off\n");
$node_off->start;

my $lms_guc_meta = $node_off->safe_psql('postgres', q{
	SELECT setting, vartype, context
	  FROM pg_settings
	 WHERE name = 'cluster.lms_enabled'});
is($lms_guc_meta, 'off|bool|postmaster',
   'L2 cluster.lms_enabled=off visible in pg_settings as postmaster bool');

# Give ServerLoop a chance to run its respawn branch; it must still not fork LMS.
sleep 2;

is($node_off->safe_psql(
	'postgres',
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'lms'}),
   '0',
   'L3 cluster.lms_enabled=off keeps LMS aux process un-forked');

$node_off->stop;


# ============================================================
# spec-2.21 D11:  L4-L8 cluster gate regression.
# ============================================================

my $node_gate = PgracClusterNode->new('lms_gate');
$node_gate->init;
$node_gate->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$node_gate->start;

# L4: pg_advisory_xact_lock — xact-level advisory enters cluster gate
#     (single-node MVP path: LMS handler grants immediately).
my $xact_advisory = $node_gate->safe_psql('postgres', q{
	BEGIN;
	DO $$ BEGIN PERFORM pg_advisory_xact_lock(42); END $$;
	COMMIT;
	SELECT 'L4_ok';
});
is($xact_advisory, 'L4_ok',
   'L4 cluster.lms_enabled=on + pg_advisory_xact_lock(42) acquires cluster path then releases');

# L5: reentrant xact advisory — same xid same key second acquire goes through
#     LOCALLOCK reentrant path (HC10), does NOT re-enter 7-step.
my $reentrant = $node_gate->safe_psql('postgres', q{
	BEGIN;
	DO $$ BEGIN
		PERFORM pg_advisory_xact_lock(43);
		PERFORM pg_advisory_xact_lock(43);
		PERFORM pg_advisory_xact_lock(43);
	END $$;
	COMMIT;
	SELECT 'L5_ok';
});
is($reentrant, 'L5_ok',
   'L5 reentrant pg_advisory_xact_lock — LOCALLOCK reentrant path (HC10)');

# L6: pg_advisory_lock — session-level stays native (HC11).
my $session_advisory = $node_gate->safe_psql('postgres', q{
	DO $$ BEGIN
		PERFORM pg_advisory_lock(44);
		PERFORM pg_advisory_unlock(44);
	END $$;
	SELECT 'L6_ok';
});
is($session_advisory, 'L6_ok',
   'L6 session-level pg_advisory_lock — HC11 session advisory stays native');

# L7: non-advisory lock (SELECT FOR UPDATE on relation) — gate predicate
#     filters out non-ADVISORY locktag types.
$node_gate->safe_psql('postgres',
	q{CREATE TABLE l7_test(id int PRIMARY KEY, v int); INSERT INTO l7_test VALUES(1, 10);});
my $non_advisory = $node_gate->safe_psql('postgres', q{
	BEGIN;
	DO $$ DECLARE v_l7 int; BEGIN
		SELECT v INTO v_l7 FROM l7_test WHERE id = 1 FOR UPDATE;
	END $$;
	COMMIT;
	SELECT 'L7_ok';
});
is($non_advisory, 'L7_ok',
   'L7 SELECT FOR UPDATE (non-advisory) — gate predicate skips cluster path');

# L8: cluster.lock_acquire_cluster_path emergency bypass — when set false
#     (PGC_POSTMASTER), entire gate is bypassed.
$node_gate->stop;
$node_gate->append_conf('postgresql.conf',
	"cluster.lock_acquire_cluster_path = off\n");
$node_gate->start;

my $bypass = $node_gate->safe_psql('postgres', q{
	BEGIN;
	DO $$ BEGIN PERFORM pg_advisory_xact_lock(45); END $$;
	COMMIT;
	SELECT 'L8_ok';
});
is($bypass, 'L8_ok',
   'L8 cluster.lock_acquire_cluster_path=off emergency bypass — gate skipped');

$node_gate->stop;

done_testing();
