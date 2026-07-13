# W3 ownership-generation: grant-install -> finalize vs INVALIDATE (2-node).
#
# SCOPE: exactly ONE defect -- the grant-finalize window mis-ack.  Between a
# requester's grant install and the LockBuffer finalize that flips the local
# mirror to X, pcm_state still reads N (with PCM_OWN_FLAG_GRANT_PENDING set).
# A same-tag INVALIDATE processed in that window used to be acked
# already_invalidated (pre_state==N), letting the master clear this node's
# holder bit and re-grant X elsewhere while the local finalize then installs
# X anyway -- a stale/double X holder (Rule 8.A).  The fix parks the directive
# (gcs_block_invalidate_execute returns false, the A2 park lot retries after
# the finalize) and bumps pcm.invalidate_parked_grant_pending_count.
#
# WHY DELIVERY IS INJECTED, NOT SQL-DRIVEN (audited + verified live by the
# RED subagent): a master INVALIDATE targets S-holders only (the broadcast
# target set comes from cluster_pcm_lock_query_s_holders_bitmap); a node whose
# mirror is N during an in-flight acquire is the X-GRANTEE at the master and
# is served via X-forward / read-image, never INVALIDATE.  The real producers
# of "INVALIDATE meets mirror-N + GRANT_PENDING" are master/mirror asymmetry
# races (e.g. a deferred eviction release racing a fresh re-acquire) -- real
# but not SQL-deterministic.  So this RED drives the REAL handler
# (gcs_block_invalidate_execute) with a synthetic same-tag directive delivered
# at the exact window point via the one-shot
# cluster-pcm-grant-finalize-deliver-invalidate inject (:skip).  The handler's
# park (counter + return-false before any wire send) is the fix contract; an
# already_invalidated ACK there is the W3 defect (surfaces as a WARNING from
# the delivery shim, asserted absent).
#
# FRESH N->X MATTERS: an S->X upgrade keeps mirror==S through the window (the
# S-branch of the handler, not the N-park).  The only clean SQL fresh N->X is
# an INSERT from a node that has NEVER touched the block: heap insert goes
# RelationGetBufferForTuple -> LockBuffer(EXCLUSIVE) with no prior S scan.
# So node1 seeds (master + X holder) and node0's first-ever touch is the
# INSERT under test.
#
#	L1  pair boots, peers connected.
#	L2  node0 INSERT (fresh N->X) hits the finalize window; the delivered
#	    same-tag INVALIDATE is PARKED: invalidate_parked_grant_pending_count
#	    delta >= 1 on node0, and the shim's "ACKed instead of parked"
#	    WARNING is absent (the defect arm did not run).
#	L3  the INSERT commits cleanly and the grant finalized: node0 re-reads
#	    its own row (own-xid, no cross-node resolve).
#	L4  convergence: node1 sees both rows; no lost write, zero TT noise
#	    (53R97 / recycled / unknown) over the whole test.
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-2.36-gcs-block-transfer.md
# Spec: spec-4.7a-hold-until-revoked.md
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep time);
use IPC::Run ();

my ($n0, $n1);

sub state_int {
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'});
	return defined($v) && $v ne '' ? int($v) : 0;
}

# Summed 53R97 / TT-recycled / TT-unknown "noise fired" counters (same set as
# t/394): a non-zero delta means the fixture leaked cross-node low-xid TT
# resolution.
my $TT_SQL = q{
	SELECT COALESCE(SUM(value::bigint), 0) FROM pg_cluster_state
	 WHERE (category = 'cr' AND key IN (
			'vis53r97_leg_invalid_scn_refuse_count',
			'vis53r97_leg_zero_match_refuse_count',
			'vis53r97_leg_srv_other_refuse_count',
			'vis53r97_leg_covers_refuse_count',
			'vis53r97_leg_multi_unresolvable_count',
			'vis53r97_leg_xmax_unprovable_count',
			'cr_xmax_recycled_invisible_count'))
	    OR (category = 'tt_status_hint' AND key = 'drop_unknown_version_count')
	    OR (category = 'tt_recovery'    AND key = 'recycled_liveness_relaxed')
};

sub tt_noise_sum {
	return int($n0->safe_psql('postgres', $TT_SQL))
		+ int($n1->safe_psql('postgres', $TT_SQL));
}

sub arm {
	my ($node, $val) = @_;
	$node->safe_psql('postgres', "ALTER SYSTEM SET cluster.injection_points = '$val'");
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
}

sub disarm {
	my ($node) = @_;
	$node->safe_psql('postgres', 'ALTER SYSTEM RESET cluster.injection_points');
	$node->safe_psql('postgres', 'SELECT pg_reload_conf()');
}

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'pcm_grant_finalize',
	quorum_voting_disks => 3,
	shared_data => 1,
	extra_conf => [
		'autovacuum = off',
		'cluster.grd_max_entries = 1024',
		'cluster.ges_bast = on',
		'cluster.read_scache = on',
		'cluster.crossnode_runtime_visibility = off',
		'cluster.gcs_block_local_cache = on' ]);
$pair->start_pair;
usleep(3_000_000);

$n0 = $pair->node0;
$n1 = $pair->node1;

ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'L1 peers 0->1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'L1 peers 1->0 connected');

# Coinciding-filepath fixture.  node1 seeds and quiesces the page (frozen +
# hint-clean, no ACTIVE ITL); node0 must NOT touch the table before the
# INSERT under test (any read would leave S residency and turn the acquire
# into an S->X upgrade, mirror==S through the window).
my $tbl;
for my $i (1 .. 12) {
	my $t = "gf_t$i";
	$_->safe_psql('postgres', "CREATE TABLE $t (k int, v int) WITH (fillfactor=10)")
		for ($n0, $n1);
	my $p0 = $n0->safe_psql('postgres', "SELECT pg_relation_filepath('$t')");
	my $p1 = $n1->safe_psql('postgres', "SELECT pg_relation_filepath('$t')");
	if (($p0 // '') eq ($p1 // '')) { $tbl = $t; last; }
}
die 'no coinciding filepath found' unless defined $tbl;
diag("table=$tbl");

$n1->safe_psql('postgres', "INSERT INTO $tbl VALUES (1, 10)");
$n1->safe_psql('postgres', "VACUUM (FREEZE) $tbl");
$n1->safe_psql('postgres', 'CHECKPOINT');
$n1->safe_psql('postgres', "SELECT count(*) FROM $tbl");
usleep(300_000);

my $parked_b = state_int($n0, 'pcm', 'invalidate_parked_grant_pending_count');
my $tt_b = tt_noise_sum();

# One-shot delivery: the first fresh N->X acquire on node0 that reaches the
# grant-finalize window gets the synthetic same-tag INVALIDATE.
arm($n0, 'cluster-pcm-grant-finalize-deliver-invalidate:skip');
usleep(400_000);

# node0's FIRST touch of the table: INSERT -> RelationGetBufferForTuple ->
# LockBuffer(EXCLUSIVE) fresh N->X.  Capture stderr: the delivery shim WARNs
# if the directive was ACKed instead of parked (the defect arm).
my ($rc, $out, $err) = $n0->psql('postgres',
	"INSERT INTO $tbl VALUES (2, 22)");
disarm($n0);

my $parked_d = state_int($n0, 'pcm', 'invalidate_parked_grant_pending_count')
	- $parked_b;
diag(sprintf("node0 INSERT rc=%d err=[%s]; parked delta=%d",
		$rc, ($err // '') =~ s/\n.*//sr, $parked_d));

# L2 — the fix contract: the in-window INVALIDATE parked (never mis-acked).
cmp_ok($parked_d, '>=', 1,
	'L2 in-window INVALIDATE was PARKED (invalidate_parked_grant_pending_count '
	  . 'advanced; old code acked already_invalidated here)');
unlike(($err // ''), qr/ACKed instead of parked/,
	'L2 delivery shim saw the park, not the already_invalidated mis-ack');
is($rc, 0, 'L2 the granted INSERT committed cleanly through the delivery');

# L3 — the grant really finalized: node0 owns the block and reads its row.
is($n0->safe_psql('postgres', "SELECT v FROM $tbl WHERE k = 2"), '22',
	'L3 node0 (own-xid read) sees its committed INSERT — grant finalized X');

# L4 — two-node convergence + zero TT noise.
my $n1rows;
for (1 .. 15) {
	my ($r2, $o2, $e2) = $n1->psql('postgres',
		"SELECT count(*) FROM $tbl");
	if ($r2 == 0 && defined $o2 && $o2 ne '') { $n1rows = $o2; last; }
	usleep(300_000);
}
is($n1rows // '', '2', 'L4 node1 converges to both rows (no lost insert)');
is(tt_noise_sum() - $tt_b, 0,
	'L4 zero TT noise (53R97 / recycled / unknown) over the whole test');

$pair->stop_pair;
done_testing();
