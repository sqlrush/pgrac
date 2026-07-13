# Raw-xid collision visibility — resolver-before-self ordering (2-node).
#
# With xid striping off (the default) the two nodes allocate from the SAME
# flat xid value space, so a local transaction's raw xid can equal a remote
# writer's raw xid.  The cluster visibility forks must consult the tuple's
# ITL evidence (physical origin) BEFORE trusting a raw
# TransactionIdIsCurrentTransactionId() match: a fresh remote-origin ITL ref
# routes to the authoritative cross-node verdict even when the raw value
# collides with the reader's own xid; only exact own-origin or no-remote-
# evidence tuples may fall through to the PG-native self/cmin paths.
#
# Each scenario manufactures the collision deterministically: seed a write
# on the node whose xid counter is AHEAD, burn the other node's counter to
# exactly the seed xid (plpgsql exception-block subxacts), then run the
# consumer inside the colliding transaction.  Single-shot assertions — no
# retry may mask the first error.
#
#	L1  xmin collision, UPDATE: node A inserts (xid X, committed, no
#	    read-back), node B updates the row inside its own xact with the
#	    SAME raw xid X.  Broken ordering reads the remote insert as "my
#	    own insert" -> native cmin -> "attempted to update invisible
#	    tuple".  Must succeed and update exactly one row.
#	L2  post-collision reads: both nodes see the updated value.
#	L3  lock-only xmax collision: node A holds SELECT FOR UPDATE open
#	    (xid Y; a lock-only xmax does NOT overwrite the tuple's version
#	    ITL slot, so xmin stays provable), node B runs SELECT FOR UPDATE
#	    NOWAIT inside its own xact with the SAME raw xid Y.  Broken
#	    ordering reads the remote locker as "my own lock" ->
#	    TM_SelfModified nonsense; the verdict path maps it to a remote
#	    in-progress writer -> NOWAIT "could not obtain lock".
#	L4  xmax collision, MVCC read of a remote-DELETED row: the deleter
#	    overwrote the version ITL slot, so below the striping floor the
#	    inserter xid is structurally unprovable -> the contract outcome
#	    is 53R97 fail-closed (TT slot recycled), NEVER a resurrect of
#	    the deleted row and NEVER a silent native fallback -- also on a
#	    raw current-xid match (truth-table row STALE+current).
#	L5  same shape, UPDATE of the remote-deleted row: fail-closed 53R97,
#	    never "already modified" / crash / row resurrect.
#	L6  whole-run invariant: no lost-write detections on either node.
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-3.14-remaining-visibility-paths.md
# Spec: spec-5.22f-shared-catalog-seed-visibility-consumer.md
use strict;
use warnings FATAL => 'all';

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);

sub state_int {
	my ($node, $cat, $key) = @_;

	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state
		   WHERE category='$cat' AND key='$key'});
	return defined($v) && $v ne '' ? int($v) : 0;
}

# Read node $node's next unassigned xid WITHOUT consuming one (snapshot xmax
# is the first xid not yet assigned; read-only).
sub next_xid {
	my ($node) = @_;

	return $node->safe_psql('postgres',
		'SELECT pg_snapshot_xmax(pg_current_snapshot())');
}

# Burn node $node's xid counter so the NEXT assigned top-level xid is exactly
# $target.  A plpgsql exception block assigns a subxact xid only when forced
# (PERFORM txid_current()), so a DO block with N such iterations consumes
# exactly N+1 xids (top + N subxacts).
sub burn_to_xid {
	my ($node, $target) = @_;

	my $needed = $target - next_xid($node);

	die "burn_to_xid: counter already past target (needed=$needed target=$target)"
		if $needed < 0;
	die "burn_to_xid: gap too large (needed=$needed target=$target)"
		if $needed > 20000;
	if ($needed == 1) {
		$node->safe_psql('postgres', 'SELECT txid_current()');
	} elsif ($needed > 1) {
		my $n = $needed - 1;

		$node->safe_psql('postgres', qq{
DO \$\$
DECLARE i int;
BEGIN
  FOR i IN 1..$n LOOP
    BEGIN
      PERFORM txid_current();
      RAISE EXCEPTION 'burn';
    EXCEPTION WHEN OTHERS THEN
    END;
  END LOOP;
END
\$\$});
	}

	my $next = next_xid($node);

	die "burn_to_xid: landed on $next, wanted $target" if $next != $target;
	return;
}

my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'rawxid_collision',
	quorum_voting_disks => 3,
	data_port_span => 2,
	shared_data => 1,
	extra_conf => [
		'autovacuum = off',
		'cluster.gcs_block_starvation_max_retries = 200',
		'cluster.gcs_block_retransmit_max_retries = 8',
		'cluster.crossnode_runtime_visibility = on' ]);
$pair->start_pair;
usleep(3_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'peers 0->1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'peers 1->0 connected');

# Coinciding-filepath fixture (shared storage, per-node catalogs).
my $table;
for my $i (1 .. 8) {
	my $t = "cx_t$i";

	$_->safe_psql('postgres', "CREATE TABLE $t (k int, v int)")
		for ($pair->node0, $pair->node1);
	my $p0 = $pair->node0->safe_psql('postgres', qq{SELECT pg_relation_filepath('$t')});
	my $p1 = $pair->node1->safe_psql('postgres', qq{SELECT pg_relation_filepath('$t')});

	if ($p0 eq $p1) {
		$table = $t;
		last;
	}
}
die 'no coinciding filepath' unless defined $table;
diag("table=$table");

# Command-counter padding scratch table (kept OFF the shared fixture pages so
# the pads never recycle the fixture page's ITL slots).  Created on both nodes
# per the per-node-catalog fixture convention.
$_->safe_psql('postgres', 'CREATE TABLE cx_pad (k int)')
	for ($pair->node0, $pair->node1);

# Pick roles by xid counter: seed on the AHEAD node, collide on the BEHIND
# node (its counter can be burned forward to the seed xid).  Read-only probes
# so neither counter moves.
my $x0 = next_xid($pair->node0);
my $x1 = next_xid($pair->node1);
my ($seed, $coll) = $x0 >= $x1
	? ($pair->node0, $pair->node1)
	: ($pair->node1, $pair->node0);
diag("next xid: node0=$x0 node1=$x1 seed=" . ($x0 >= $x1 ? 'node0' : 'node1'));

my $lw0_b = state_int($pair->node0, 'gcs', 'lost_write_detected_count');
my $lw1_b = state_int($pair->node1, 'gcs', 'lost_write_detected_count');

# ---- L1: xmin collision (remote committed insert vs own raw xid) ----
# Seed: INSERT, capture the xid in the SAME transaction, no read-back.
my $seed_xid = $seed->safe_psql('postgres',
	qq{WITH i AS (INSERT INTO $table SELECT g, 1 FROM generate_series(1, 50) g RETURNING 1)
	   SELECT txid_current()});
diag("L1 seed insert xid=$seed_xid (committed, no read-back, no checkpoint)");

burn_to_xid($coll, $seed_xid);

my ($ret, $out, $err) = $coll->psql('postgres', qq{
BEGIN;
SELECT txid_current();
UPDATE $table SET v = 2 WHERE k = 1 RETURNING v;
COMMIT;});
diag("L1 collision xact: ret=$ret out=[$out] err=[" . ($err // '') . ']');
my @lines = split /\n/, ($out // '');
is($ret, 0, 'L1: collision-xid UPDATE of remote-inserted row succeeds');
is($lines[0] // '', $seed_xid, 'L1: collision xact really got the seed xid');
is($lines[1] // '', '2', 'L1: UPDATE updated exactly the seeded row (v=2)');

# ---- L2: both nodes read the updated value (fresh sessions) ----
is($coll->safe_psql('postgres', "SELECT v FROM $table WHERE k = 1"),
	'2', 'L2: collision node reads v=2');
is($seed->safe_psql('postgres', "SELECT v FROM $table WHERE k = 1"),
	'2', 'L2: seed node reads v=2');

# ---- L3: lock-only xmax collision (remote live locker vs own raw xid) ----
# A lock-only xmax leaves the tuple's version ITL slot (the inserter's) in
# place, so xmin stays provable and the xmax leg is reached.
my $bg = $seed->background_psql('postgres', on_error_stop => 0);
$bg->query_safe('BEGIN');
my $lock_xid = $bg->query_safe('SELECT txid_current()');
$lock_xid =~ s/\s+//g;
$bg->query_safe("SELECT v FROM $table WHERE k = 5 FOR UPDATE");
diag("L3 seed lock xid=$lock_xid (open, FOR UPDATE on k=5)");

burn_to_xid($coll, $lock_xid);

($ret, $out, $err) = $coll->psql('postgres', qq{
BEGIN;
SELECT txid_current();
SELECT v FROM $table WHERE k = 5 FOR UPDATE NOWAIT;
COMMIT;});
diag("L3 collision lock: ret=$ret out=[$out] err=[" . ($err // '') . ']');
@lines = split /\n/, ($out // '');
is($lines[0] // '', $lock_xid, 'L3: collision xact really got the locker xid');
like(($err // ''), qr/could not obtain lock|cross-node/i,
	'L3: remote live locker surfaces as a lock conflict, not as self');
unlike(($err // ''), qr/already modified|invisible tuple/i,
	'L3: remote locker never misread as own lock / own insert');

eval { $bg->query_safe('COMMIT'); };
eval { $bg->quit; };

# ---- L4: MVCC read of a remote-DELETED row under a colliding raw xid ----
# The deleter overwrote the version ITL slot; below the striping floor the
# inserter xid is structurally unprovable -> contract outcome is 53R97
# fail-closed.  Pin the DIRECTION: fail-closed, never resurrect, never a
# silent native fallback on the raw current-xid match.
my $del_xid = $seed->safe_psql('postgres', qq{
BEGIN;
INSERT INTO cx_pad VALUES (101);
INSERT INTO cx_pad VALUES (102);
INSERT INTO cx_pad VALUES (103);
INSERT INTO cx_pad VALUES (104);
INSERT INTO cx_pad VALUES (105);
DELETE FROM $table WHERE k = 2;
SELECT txid_current();
COMMIT;});
diag("L4 seed delete xid=$del_xid (cmax ~5, committed)");

burn_to_xid($coll, $del_xid);

($ret, $out, $err) = $coll->psql('postgres', qq{
BEGIN;
SELECT txid_current();
SELECT count(*) FROM $table WHERE k = 2;
COMMIT;});
diag("L4 collision read: ret=$ret out=[$out] err=[" . ($err // '') . ']');
@lines = split /\n/, ($out // '');
is($lines[0] // '', $del_xid, 'L4: collision xact really got the delete xid');
like(($err // ''), qr/TT slot recycled|TT status unknown/i,
	'L4: unprovable xmin of a remote-deleted row fails closed (53R97 family)');
isnt($lines[1] // '', '1', 'L4: deleted row is never resurrected');

# ---- L5: UPDATE of the remote-deleted row under a colliding raw xid ----
my $del2_xid = $seed->safe_psql('postgres', qq{
BEGIN;
INSERT INTO cx_pad VALUES (111);
INSERT INTO cx_pad VALUES (112);
INSERT INTO cx_pad VALUES (113);
INSERT INTO cx_pad VALUES (114);
INSERT INTO cx_pad VALUES (115);
DELETE FROM $table WHERE k = 3;
SELECT txid_current();
COMMIT;});
diag("L5 seed delete xid=$del2_xid (cmax ~5, committed)");

burn_to_xid($coll, $del2_xid);

($ret, $out, $err) = $coll->psql('postgres', qq{
BEGIN;
SELECT txid_current();
UPDATE $table SET v = 9 WHERE k = 3 RETURNING k;
COMMIT;});
diag("L5 collision update: ret=$ret out=[$out] err=[" . ($err // '') . ']');
@lines = split /\n/, ($out // '');
is($lines[0] // '', $del2_xid, 'L5: collision xact really got the delete xid');
like(($err // ''), qr/TT slot recycled|TT status unknown/i,
	'L5: unprovable xmin of a remote-deleted row fails closed (53R97 family)');
unlike(($err // ''), qr/already modified|invisible tuple/i,
	'L5: remote delete never misread as own modification');

# ---- L6: whole-run invariant — no lost writes ----
is(state_int($pair->node0, 'gcs', 'lost_write_detected_count') - $lw0_b,
	0, 'L6: node0 lost_write_detected_count unchanged');
is(state_int($pair->node1, 'gcs', 'lost_write_detected_count') - $lw1_b,
	0, 'L6: node1 lost_write_detected_count unchanged');

$pair->stop_pair;
done_testing();
