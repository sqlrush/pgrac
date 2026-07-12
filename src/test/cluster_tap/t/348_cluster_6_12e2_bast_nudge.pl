# spec-6.12e2 (㉔) — master->holder BAST nudge on the live-X-holder deny,
# plus the GCS-race round-4c FUNC-1 storage-fallback lost-update guard.
#
# The HG7 conservative deny fires only in the THIRD-PARTY topology
# (master ∉ {requester, holder}): with two nodes the master is always one
# of the parties (path-B / local-master flows), so this is a 3-node TAP.
#
#	off leg   requester writes a block X-held by another live node whose
#	          static PCM home is the third node -> bounded fail-closed
#	          deny (today's behaviour, unchanged).
#	on leg    same topology with cluster.ges_bast=on -> the master sends
#	          the holder a fire-and-forget nudge; the holder's LMON runs
#	          the quiescent X->S self-downgrade; the requester's retry
#	          proceeds through the S-invalidate grant path.  Counters:
#	          nudge_sent on the master, nudge_yield on the holder.
#	          SINGLE-SHOT + final data assertions (round-4c de-masking):
#	          the holder carries a post-CHECKPOINT dirty version, so a
#	          requester that kept its stale pre-read would lose the
#	          holder's update — every converged table must show BOTH the
#	          holder's dirty update AND the requester's insert.
#	refusal   with the holder-side injection armed the nudge is refused:
#	          the deny-retry path must stay intact (advisory contract,
#	          §3.4b: never force the holder) and nudge_refused grows.
#	L6        deterministic lost-update reproduction (round-4c FUNC-1):
#	          holder dirty page + no CHECKPOINT + requester pre-read the
#	          old page in an earlier denied attempt -> the fallback grant
#	          carries the master pi_watermark_scn, the requester proves
#	          its copy stale, re-reads shared storage
#	          (fallback_scn_refresh_count) and BOTH writes survive.
#	L7        cluster-gcs-block-fallback-refresh-stale injection forces
#	          the post-refresh re-verdict to ANOMALY -> requester-side
#	          lost-write detector fail-closes 53R93; disarm -> the retry
#	          heals through the holder re-ack fallback (self-healing).
#
# Topology probing: the tag hash spreads block homes over the three
# declared nodes; eight single-block tables make P(no third-party table)
# = (2/3)^8 ≈ 4%.  The off leg's outcome pattern LOCATES the third-party
# tables (only they fail); the on/refusal legs then drive exactly those.
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterTriple;
use PostgreSQL::Test::Utils;
use Time::HiRes qw(usleep);
use Test::More;

sub state_val
{
	my ($node, $cat, $key) = @_;
	my $v = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='$cat' AND key='$key'});
	return defined($v) && $v ne '' ? $v + 0 : 0;
}

sub write_retry
{
	my ($node, $sql) = @_;
	for my $i (1 .. 10)
	{
		my $ok = eval { $node->safe_psql('postgres', $sql); 1 };
		return 1 if $ok;
		usleep(500_000);
	}
	return 0;
}

# Read-side retry for the DESIGNED-retryable cross-node read errors (TT slot
# recycled / block recovering — the errhint says "retry with a fresh
# snapshot").  Distinct from the banned write_retry masking: the FINAL VALUE
# is still asserted; only the transient snapshot-window error is retried.
sub read_retry
{
	my ($node, $sql) = @_;
	my $out;
	for my $i (1 .. 10)
	{
		my $ok = eval { $out = $node->safe_psql('postgres', $sql); 1 };
		return $out if $ok;
		usleep(500_000);
	}
	return '<read failed>';
}

# ============================================================
# L1: boot the shared-data triple.
# ============================================================
my $triple = PostgreSQL::Test::ClusterTriple->new_triple(
	'e612_bast',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		# t/251 shape: no cluster.grd_max_entries (the GES logical-lock
		# path makes CREATE TABLE DDL time out on triples) and no custom
		# GES/GCS timeouts.
		'autovacuum = off',
	]);
$triple->start_triple;
usleep(3_000_000);

my ($node0, $node1, $node2) =
  ($triple->node(0), $triple->node(1), $triple->node(2));
for my $i (0 .. 2)
{
	is($triple->node($i)->safe_psql('postgres', 'SELECT 1'), '1',
		"L1 node$i alive");
}
# Full IC mesh before any DDL: a GES request sent before the peers are
# connected dead-letters into the 30s timeout (t/251 L0 precedent).
for my $from (0 .. 2)
{
	for my $to (0 .. 2)
	{
		next if $from == $to;
		ok($triple->wait_for_peer_state($from, $to, 'connected', 30),
			"L1 node$from sees node$to connected");
	}
}
is($node0->safe_psql('postgres', 'SHOW cluster.ges_bast'), 'on',
	'L1 cluster.ges_bast default on (round-4 FUNC-1 live-X handoff activation)');

# The L2 leg proves the DISARMED terminal-deny behaviour, so force the
# escape hatch off everywhere first; L3 re-arms it below.
for my $n ($node0, $node1, $node2)
{
	$n->safe_psql('postgres', 'ALTER SYSTEM SET cluster.ges_bast = off');
	$n->safe_psql('postgres', 'SELECT pg_reload_conf()');
}
usleep(1_000_000);
is($node1->safe_psql('postgres', 'SHOW cluster.ges_bast'), 'off',
	'L1b cluster.ges_bast disarmed for the off leg');

# ============================================================
# L2 (off leg): eight probe tables; node2 X-holds each (quiescent after a
# local read-back cleanout); node1 single-shot writes.  Third-party
# tables (home = node0) fail closed; the rest succeed.
# ============================================================
my (@third_party, @other);
for my $i (1 .. 8)
{
	my $t = "e_t$i";
	my $coincide = 1;
	$_->safe_psql('postgres', "CREATE TABLE $t (id int, v int)")
	  for ($node0, $node1, $node2);
	my $p0 = $node0->safe_psql('postgres', qq{SELECT pg_relation_filepath('$t')});
	for my $n ($node1, $node2)
	{
		$coincide = 0
		  if $n->safe_psql('postgres', qq{SELECT pg_relation_filepath('$t')}) ne $p0;
	}
	next unless $coincide;
	next unless write_retry($node2, "INSERT INTO $t VALUES (1, 10), (2, 20)");
	next unless write_retry($node2, 'CHECKPOINT');
	# Round-4c FUNC-1 de-masking: dirty the page AFTER the checkpoint so the
	# holder's X copy is strictly newer than shared storage (v id=1: 10->11).
	# A requester that keeps its stale storage pre-read on the later
	# GRANTED_STORAGE_FALLBACK would silently lose this update — the on-leg
	# final assertions catch exactly that.
	next unless write_retry($node2, "UPDATE $t SET v = v + 1 WHERE id = 1");
	# Local read-back: lazy cleanout stamps the seed+update ITL so the block
	# is QUIESCENT under node2's X (a Fast-Commit unstamped slot would read
	# as ACTIVE and refuse the quiescent downgrade by design).
	$node2->safe_psql('postgres', "SELECT sum(v) FROM $t");

	my ($rc, $out, $err) = $node1->psql('postgres', "INSERT INTO $t VALUES (101, 1)");
	if ($rc != 0) { push @third_party, $t; }
	else          { push @other,       $t; }
}
cmp_ok(scalar(@third_party), '>=', 1,
	'L2 off leg: at least one third-party (master=node0) table fails closed ('
	  . scalar(@third_party) . '/8: ' . join(',', @third_party) . ')');
note('L2 non-third-party tables (succeeded): ' . join(',', @other));

# ============================================================
# L3 (on leg): arm cluster.ges_bast everywhere; the failed tables now
# converge -- nudge_sent on the master, nudge_yield on the holder.
# ============================================================
for my $n ($node0, $node1, $node2)
{
	$n->safe_psql('postgres', 'ALTER SYSTEM SET cluster.ges_bast = on');
	$n->safe_psql('postgres', 'SELECT pg_reload_conf()');
}
usleep(1_000_000);
is($node1->safe_psql('postgres', 'SHOW cluster.ges_bast'), 'on',
	'L3 cluster.ges_bast armed');

my $sent0  = state_val($node0, 'xnode_lever', 'e2_bast_nudge_sent_count');
my $yield0 = state_val($node2, 'xnode_lever', 'e2_bast_nudge_yield_count');
my $refresh0 = state_val($node1, 'gcs', 'fallback_scn_refresh_count');
my $failclosed0 = state_val($node1, 'gcs', 'fallback_scn_failclosed_count');

# Round-4c de-masking: SINGLE-SHOT converge (no write_retry — a retry loop
# would mask a first-attempt lost-update/deny), then assert the FINAL DATA:
# the holder's post-CHECKPOINT dirty update (id=1 -> 11) must coexist with
# the requester's insert on every converged table.
my $converged = 0;
my @on_tables = @third_party;
my $refusal_table = pop @on_tables;    # keep one for the refusal leg
for my $t (@on_tables)
{
	my ($rc, $out, $err) = $node1->psql('postgres', "INSERT INTO $t VALUES (102, 2)");
	$converged++ if $rc == 0;
}
if (@on_tables)
{
	is($converged, scalar(@on_tables),
		'L3 on leg: every nudged third-party table converges single-shot ('
		  . "$converged/" . scalar(@on_tables) . ')');
	my $sent1  = state_val($node0, 'xnode_lever', 'e2_bast_nudge_sent_count');
	my $yield1 = state_val($node2, 'xnode_lever', 'e2_bast_nudge_yield_count');
	cmp_ok($sent1, '>', $sent0, 'L3 master counted nudge_sent');
	cmp_ok($yield1, '>', $yield0, 'L3 holder counted nudge_yield');

	# Read from the HOLDER (node2) side: node1-side reads of node2's aged
	# fixture xids hit the known aged-xid TT wall (t/366 L7 lesson —
	# writer-side read discipline).  The lost-update guard is unaffected:
	# a stale-pre-read clobber is a PAGE-level overwrite, so the holder's
	# own re-read would see its update gone too.
	my $data_ok = 1;
	for my $t (@on_tables)
	{
		my $row = read_retry($node2,
			"SELECT (SELECT v FROM $t WHERE id = 1), (SELECT count(*) FROM $t WHERE id = 102)");
		if ($row ne '11|1')
		{
			$data_ok = 0;
			note("L3 data mismatch on $t (holder view): got '$row' want '11|1'");
		}
	}
	is($data_ok, 1,
		'L3 final data (holder view): dirty update (id=1 v=11) AND requester '
		  . 'insert both survive on every on-leg table (lost-update guard)');
	cmp_ok(state_val($node1, 'gcs', 'fallback_scn_refresh_count'),
		'>=', $refresh0 + scalar(@on_tables),
		'L3 requester re-read shared storage for each stale pre-read '
		  . '(fallback_scn_refresh_count)');
	is(state_val($node1, 'gcs', 'fallback_scn_failclosed_count'), $failclosed0,
		'L3 no fail-closed verdicts on the healthy path');
}
else
{
	note('L3 skipped: only one third-party table found; it is reserved for L4');
}

# ============================================================
# L8 (round-4c P1): yield-notify wire-loss self-heal.  node2 holds the
# quiescent X from the L2 fixture; node1 single-shot INSERTs with the
# notify-drop injection armed on node2's LMON: node2's yield flips local
# S but the master never learns (simulated wire loss).
# Without the renotify self-heal the master nudges forever and the writer
# starves; with it, the nudge-on-already-S re-sends the downgrade notify
# and the SAME statement converges.  node1 drives with an INSERT (no scan
# phase — a node1 read of this table would hit the aged-xid wall).  The
# LMON arm survives reloads (L4 lesson) — harmless afterwards: every
# later drop heals the same way, and the L4 refusal leg denies BEFORE
# the yield path anyway.
# ============================================================
{
	# node2 still holds the quiescent X from the L2 fixture (the page
	# carries ONLY node2 xids, read-back-stamped there) — exactly the
	# state the L3 on-leg proved yieldable.  This leg must run BEFORE
	# L6/L7 pollute the page with node1 inserts: once their xids age past
	# the TT propagation window the page can never be re-quiesced (the
	# pre-existing aged-xid TT-recycled wall, t/366 L7 lesson).
	my ($rc, $out, $err);
	my $yield0   = state_val($node2, 'xnode_lever', 'e2_bast_nudge_yield_count');
	my $refused0 = state_val($node2, 'xnode_lever', 'e2_bast_nudge_refused_count');

	$node2->safe_psql('postgres',
		q{ALTER SYSTEM SET cluster.injection_points = 'cluster-gcs-block-yield-notify-drop:skip'});
	$node2->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(3_000_000);

	($rc, $out, $err) = $node1->psql('postgres', "INSERT INTO $refusal_table VALUES (203, 9)");
	is($rc, 0, 'L8 writer converges single-shot through the renotify self-heal');
	# The drop -> renotify signature on GLOBAL counters (the injection .hits
	# face is per-process — the LMON fire is invisible to a backend query):
	# the holder YIELDED (locally S) yet got nudged AGAIN (master never saw
	# the dropped notify -> the already-S nudge counts as refused and fires
	# the renotify).  The healthy path never re-nudges after a yield, so
	# yield>=1 AND refused>=1 within one converged statement is unique to
	# the wire-loss + self-heal chain.
	cmp_ok(state_val($node2, 'xnode_lever', 'e2_bast_nudge_yield_count'),
		'>', $yield0, 'L8 the holder yielded (notify was then dropped)');
	cmp_ok(state_val($node2, 'xnode_lever', 'e2_bast_nudge_refused_count'),
		'>', $refused0,
		'L8 the master re-nudged the already-S holder (renotify self-heal round)');
	is( read_retry(
			$node2,
			"SELECT (SELECT v FROM $refusal_table WHERE id = 1), "
			  . "(SELECT count(*) FROM $refusal_table WHERE id = 203)"),
		'11|1',
		'L8 final data (holder view): fixture update AND healed insert survive');

	$node2->safe_psql('postgres', 'ALTER SYSTEM RESET cluster.injection_points');
	$node2->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(1_000_000);
}

# ============================================================
# L6 (round-4c FUNC-1): deterministic storage-fallback lost-update guard.
# Fixture state on $refusal_table: node2 X-holds a post-CHECKPOINT dirty
# version (id=1 v=11 unflushed); node1 pre-read the stale storage copy in
# its L2 denied attempt.  Runs BEFORE the refusal leg (the nudge-refusal
# injection arms node2's long-lived LMON and survives reloads).  Pile a
# second dirty update on the holder (id=2 v: 20->120, still NO CHECKPOINT),
# then node1 single-shot INSERTs: the nudge chain yield-flushes the
# holder's version, the fallback grant carries the master watermark, node1
# proves its pre-read stale, re-reads shared storage, and BOTH the
# holder's updates AND node1's insert survive.
# ============================================================
{
	my $refresh0    = state_val($node1, 'gcs', 'fallback_scn_refresh_count');
	my $failclosed0 = state_val($node1, 'gcs', 'fallback_scn_failclosed_count');

	$node2->safe_psql('postgres',
		"UPDATE $refusal_table SET v = v + 100 WHERE id = 2");
	read_retry($node2, "SELECT sum(v) FROM $refusal_table");

	my ($rc, $out, $err) =
	  $node1->psql('postgres', "INSERT INTO $refusal_table VALUES (201, 7)");
	is($rc, 0, 'L6 requester insert converges single-shot (no retry loop)');

	is( read_retry(
			$node2,
			"SELECT (SELECT v FROM $refusal_table WHERE id = 1), "
			  . "(SELECT v FROM $refusal_table WHERE id = 2), "
			  . "(SELECT count(*) FROM $refusal_table WHERE id = 201)"),
		'11|120|1',
		'L6 final data (holder view): both holder dirty updates AND the '
		  . 'requester insert survive');
	cmp_ok(state_val($node1, 'gcs', 'fallback_scn_refresh_count'),
		'>', $refresh0,
		'L6 requester discarded the stale pre-read and re-read shared '
		  . 'storage (fallback_scn_refresh_count)');
	is(state_val($node1, 'gcs', 'fallback_scn_failclosed_count'),
		$failclosed0, 'L6 no fail-closed verdicts on the healthy path');
}

# ============================================================
# L7 (round-4c FUNC-1): requester-side lost-write detector fail-closed +
# self-heal.  Arm cluster-gcs-block-fallback-refresh-stale on node1 (the
# requester): the post-refresh re-verdict is forced to ANOMALY -> 53R93.
# Disarm -> the retry heals through the holder re-ack fallback (master
# already recorded node1 as holder; carrier is 0 -> verdict SKIP) on the
# already-refreshed bytes.
# ============================================================
{
	# node2 re-takes X through its own nudge chain (node1 holds X after L6)
	# and dirties a third version (id=2 v: 120->1120, no CHECKPOINT).
	$node2->safe_psql('postgres',
		"UPDATE $refusal_table SET v = v + 1000 WHERE id = 2");
	read_retry($node2, "SELECT sum(v) FROM $refusal_table");

	my $failclosed0 = state_val($node1, 'gcs', 'fallback_scn_failclosed_count');
	$node1->safe_psql('postgres',
		q{ALTER SYSTEM SET cluster.injection_points = 'cluster-gcs-block-fallback-refresh-stale:skip'});
	$node1->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(1_000_000);

	my ($rc, $out, $err) =
	  $node1->psql('postgres', "INSERT INTO $refusal_table VALUES (202, 8)");
	isnt($rc, 0, 'L7 armed: requester-side lost-write detector fail-closes');
	like($err, qr/stale storage-fallback copy/,
		'L7 armed: 53R93 stale storage-fallback message surfaced');
	cmp_ok(state_val($node1, 'gcs', 'fallback_scn_failclosed_count'),
		'>', $failclosed0, 'L7 fallback_scn_failclosed_count grew');

	$node1->safe_psql('postgres', 'ALTER SYSTEM RESET cluster.injection_points');
	$node1->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(1_000_000);

	($rc, $out, $err) =
	  $node1->psql('postgres', "INSERT INTO $refusal_table VALUES (202, 8)");
	is($rc, 0, 'L7 disarmed: retry heals single-shot (holder re-ack fallback)');
	is( read_retry(
			$node2,
			"SELECT (SELECT v FROM $refusal_table WHERE id = 2), "
			  . "(SELECT count(*) FROM $refusal_table WHERE id = 202)"),
		'1120|1',
		'L7 final data (holder view): third holder update AND healed insert '
		  . 'both survive');
}

# ============================================================
# L4 (refusal leg): holder-side injection refuses the nudge -> the
# bounded deny stays (advisory contract) and nudge_refused grows.
# GUC colon-arm reaches the holder's LMON (SQL arm is per-backend);
# the armed skip survives reloads, so this leg runs LAST.
# ============================================================
{
	# Re-establish the quiescent X hold: after L8 node2 already holds X
	# (covered local write), the read-back cleanout re-quiesces the block.
	$node2->safe_psql('postgres',
		"UPDATE $refusal_table SET v = v + 1 WHERE id = 1");
	read_retry($node2, "SELECT sum(v) FROM $refusal_table");

	my $refused0 = state_val($node2, 'xnode_lever', 'e2_bast_nudge_refused_count');
	$node2->safe_psql('postgres',
		q{ALTER SYSTEM SET cluster.injection_points = 'cluster-gcs-block-bast-nudge:skip'});
	$node2->safe_psql('postgres', 'SELECT pg_reload_conf()');
	usleep(3_000_000);

	my ($rc, $out, $err) =
	  $node1->psql('postgres', "INSERT INTO $refusal_table VALUES (103, 3)");
	isnt($rc, 0, 'L4 refusal: single-shot write still fails closed (deny-retry intact)');
	my $refused1 = state_val($node2, 'xnode_lever', 'e2_bast_nudge_refused_count');
	cmp_ok($refused1, '>', $refused0, 'L4 holder counted nudge_refused');
}

# ============================================================
# L5: lever key surface.
# ============================================================
for my $n ($node0, $node2)
{
	my $rows = $n->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state
		   WHERE category='xnode_lever' AND key IN
		     ('e2_bast_nudge_sent_count','e2_bast_nudge_yield_count',
		      'e2_bast_nudge_refused_count')});
	is($rows, '3', 'L5 wave-e2 lever keys present (' . $n->name . ')');
}

$triple->stop_triple if $triple->can('stop_triple');
done_testing();
