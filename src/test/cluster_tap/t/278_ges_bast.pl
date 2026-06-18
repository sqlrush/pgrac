#!/usr/bin/perl
#-------------------------------------------------------------------------
#
# 278_ges_bast.pl
#    spec-5.1c — GES BAST rewrite (simplified -> full) + self-conflict
#    exclusion acceptance.
#
#    spec-5.1c is beta LOGIC-first with two LIVE changes whose end-to-end
#    paths are remote-master only (a single-node cluster takes the local
#    fast path and never routes through the LMS master grant), so the LIVE
#    behaviour is verified by the cluster_unit suite + a source contract
#    here, and the real cross-node e2e is an honest SKIP (Q9=B, mirrors
#    4.9/4.11/4.12b/5.1b):
#
#      L1   clean start: the frozen matrix still drives the GRD grant
#           decision (matches PG's live conflict table) and ordinary DML
#           round-trips.
#      L2   D6 observability: the BAST lifecycle counters (sent / received /
#           stale_drop) are exposed in the grd dump and are 0 at a fresh
#           start (no new counter was added -- they pre-date 5.1c).
#      L3   D5 (LIVE): the master conflict scan excludes a same-backend
#           prior-mode hold by {node_id, procno} -- the fix for the
#           pre-existing cross-node self-deadlock is WIRED in
#           cluster_grd_entry_enqueue_or_grant (source contract).
#      L4   D1 (LIVE): local BAST delivery is WIRED -- send_bast_targeted
#           resolves the PGPROC and signals with proc->backendId (NOT the
#           pgprocno), guarded by the best-effort deliver-ok predicate, and
#           is no longer the spec-2.23 counter-only no-op.
#      L5   D4: BAST_ACK stays release-coupled (HC19); the standalone
#           opcode-5 handler is counter-only (no live producer).
#
#    NOT reachable single-node (honest forward, Q9=B inspection-only):
#      Both LIVE changes (D1 local delivery, D5 self-conflict exclusion) sit
#      on the LMS remote-master grant path; a single-node cluster never
#      reaches it (local fast path).  The real SendProcSignal to a live
#      backend has no multi-node harness, so the delivery e2e is SKIPed
#      rather than faked (rule 8 / rule 18 / L341).  The bug-prone logic
#      (backendId resolution + the range/epoch/liveness guard) is covered by
#      test_cluster_grd (U9a-c, U10); the SendProcSignal primitive itself is
#      a verbatim mirror of the shipped + tested CANCEL delivery
#      (cluster_lmd_tarjan.c).  The multi-mode downconvert target is a pure
#      function covered by test_cluster_ges_mode.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/278_ges_bast.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    Spec: spec-5.1c-ges-bast-rewrite-convert-consumption.md
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


my $node = PgracClusterNode->new('main');
$node->init;
$node->start;


# ----------------------------------------------------------------------
# L1 -- clean start: the frozen matrix drives the grant decision, DML works.
# ----------------------------------------------------------------------
is($node->safe_psql('postgres', q{SELECT cluster_ges_mode_matches_pg()}),
	't', 'L1: live frozen matrix == PG conflict table (drives the GRD grant)');

$node->safe_psql('postgres',
	q{CREATE TABLE t278_bast (a int); INSERT INTO t278_bast VALUES (1), (2), (3)});
is($node->safe_psql('postgres', q{SELECT count(*) FROM t278_bast}),
	'3', 'L1b: ordinary locking/DML still round-trips');


# ----------------------------------------------------------------------
# L2 -- D6: BAST lifecycle counters exposed in the grd dump, 0 at start, and
# NO new counter was added (zero shmem growth).
# ----------------------------------------------------------------------
is( $node->safe_psql('postgres', q{
		SELECT count(*) FROM pg_cluster_state
		WHERE category = 'grd'
		  AND key IN ('grd_bast_sent_count',
		              'grd_bast_received_count',
		              'grd_bast_stale_drop_count')}),
	'3', 'L2: BAST sent/received/stale_drop counters present in the grd dump (D6)');

is( $node->safe_psql('postgres', q{
		SELECT bool_and(value = '0') FROM pg_cluster_state
		WHERE category = 'grd' AND key LIKE 'grd_bast_%'}),
	't', 'L2b: BAST counters are 0 at a fresh start (no BAST fired single-node)');


# ----------------------------------------------------------------------
# L3/L4/L5 -- source contracts for the LIVE changes whose e2e is remote-master
# only (Q9=B: verify the mechanism is wired, do not fake the cross-node e2e).
# ----------------------------------------------------------------------
my $root = "$FindBin::RealBin/../../../..";

sub slurp_src
{
	my ($rel) = @_;
	my $path = "$root/$rel";
	open my $fh, '<', $path or die "open $path: $!";
	local $/;
	my $s = <$fh>;
	close $fh;
	return $s;
}

my $grd_c = slurp_src('src/backend/cluster/cluster_grd.c');

# L3 -- D5 self-conflict exclusion is wired in the master conflict scan,
# keyed by {node_id, procno} (NOT the full 4-tuple).
like($grd_c,
	qr/spec-5\.1c D5: same-backend self-conflict exclusion.*?holders\[i\]\.node_id == \(int32\) holder->node_id\s*&&\s*entry->holders\[i\]\.procno == holder->procno/s,
	'L3: master conflict scan excludes the requester\'s own hold by {node_id, procno} (D5)');

my $ges_c = slurp_src('src/backend/cluster/cluster_ges.c');

# L4 -- D1 local BAST delivery is wired: PGPROC resolved, signalled with
# proc->backendId under the deliver-ok guard (not the old counter-only stub).
like($ges_c, qr/spec-5\.1c D1 -- local holder delivery/,
	'L4: local BAST branch is the spec-5.1c delivery (not the spec-2.23 no-op)');
like($ges_c,
	qr/cluster_grd_bast_local_deliver_ok\(.*?SendProcSignal\(target_pid, PROCSIG_CLUSTER_GES_BAST,\s*\(BackendId\) target_backendid\)/s,
	'L4b: signal uses proc->backendId (NOT pgprocno) guarded by deliver-ok (D1, P0 fix)');

# L5 -- D4: BAST_ACK stays release-coupled; opcode-5 handler counter-only.
like($ges_c,
	qr/spec-5\.1c D4 -- BAST_ACK lifecycle.*?cluster_grd_inc_bast_ack\(\);\s*return;/s,
	'L5: BAST_ACK is release-coupled; standalone opcode-5 stays counter-only (D4)');

diag('spec-5.1c: D1 local-delivery e2e + D5 self-conflict e2e are remote-master '
	  . 'paths with no multi-node harness (Q9=B inspection-only); the logic is '
	  . 'unit-covered by test_cluster_grd U9a-c/U10 + test_cluster_ges_mode '
	  . 'downconvert, and the SendProcSignal primitive mirrors the shipped CANCEL '
	  . 'delivery -- the cross-node e2e is deliberately not faked (L341).');


$node->stop;
done_testing();
