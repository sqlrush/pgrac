# cached-X BAST X->S stale-authorization window (2-node, deterministic).
#
# OWNERSHIP P0 W1 (cached-X no-reverify): a writer takes the
# LockBuffer(EXCLUSIVE) cached-X cover fast path -- it reads pcm_state=X with NO
# lock held and skips the master round-trip -- then races for the buffer content
# lock.  A concurrent BAST X->S self-downgrade (driven by a PEER READ the master
# serves by asking node0 for the quiescent X->S yield) runs in the LMON
# dispatch, takes the content lock first, flips pcm_state X->S, and releases.
# The writer then acquires the content lock and, WITHOUT re-verifying its
# authority, writes the page believing it still holds X -- a write under a
# revoked grant.
#
# SCOPE (deliberately narrowed after adjudication): this RED PROVES the physical
# interleave the ownership-generation fix must close and does so on a fixture
# proven TT-clean.  It asserts, on every run:
#   (a) node0's writer ENTERED the cover window  -- its own-session inject hit
#       counter advanced (the counter is process-local, so it is read in the
#       writer's OWN psql session via a second -c);
#   (b) a REAL X->S downgrade OVERLAPPED the window -- trans_x_to_s_downgrade_
#       count (summed both nodes) advanced by >=1 while the writer was stalled;
#   (c) the fixture is TT-CLEAN -- the summed 53R97 / TT-recycled / TT-unknown
#       counters have ZERO delta across the whole test;
#   (d) the writer really WAITED through the stall (elapsed >= ~2.5s).
# It deliberately does NOT assert the final data value: node0's writer holds a
# pin across the stall, so the peer drop PARKS and the writer typically FLUSHES
# before any drop, which can make "final value survives" PASS even on the buggy
# code (false green).  The data-corruption assertion is fix-coupled and left to
# the fix implementer.
#
# FIXTURE DISCIPLINE (isolates the ORTHOGONAL fresh-cluster low-xid TT bug,
# 53R97 "TT slot recycled for xid ~791" on cross-node heap MVCC reads):
#   * WRITER = a heap UPDATE on node0 -> a real LockBuffer(EXCLUSIVE) on block 0
#     -> the covered-X cover fast path -> the inject arms.  (A cluster SEQUENCE
#     is the WRONG fixture: cluster_sq_nextval serializes cross-node nextval and
#     abstracts away the raw LockBuffer(X), so neither the cover path nor the
#     downgrade ever fires.)
#   * TRIGGER = a node1 READ (count(*)) of the block.  node0 establishes X with
#     a SINGLE committed INSERT (NO update-version-chain: an UPDATE would leave a
#     dead version + xmax that force the peer to TT-resolve node0's low xid ->
#     53R97) and VACUUM + CHECKPOINT + own-xid read-back BEFORE arming, so the
#     one committed tuple carries HEAP_XMIN_COMMITTED and the peer read is served
#     from the hint bit on the native path (crossnode_runtime_visibility OFF) --
#     never the TT resolve path.
#   * The table only ever holds node0's own single committed row until the
#     stalled writer runs (after the trigger), so nothing the peer reads is a
#     version chain.
#
# ORDERING (critical): node0 INSERT -> COMMIT -> VACUUM/CHECKPOINT/own read
# (quiescent, hint-committed, no ACTIVE ITL to reject the downgrade at
# bufmgr.c:7077) -> ARM -> stall the NEXT writer.  The stalled writer has NOT
# written yet, so block 0 is ITL-quiescent AND still a single committed tuple
# throughout the downgrade window.
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-4.7a-hold-until-revoked.md
# Spec: spec-2.36-gcs-block-transfer.md
# Spec: spec-6.12a-quiescent-scache.md
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

sub dg_sum {
	return state_int($n0, 'pcm', 'trans_x_to_s_downgrade_count')
		+ state_int($n1, 'pcm', 'trans_x_to_s_downgrade_count');
}

# Summed 53R97 / TT-recycled / TT-unknown "noise fired" counters, both nodes.
# A non-zero delta means the fixture leaked cross-node low-xid TT resolution.
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
	'pcm_cached_x_bast',
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

# TT-noise baseline for the WHOLE test (setup included) -- proves the entire
# fixture, not just the critical section, is TT-clean.
my $tt_base = tt_noise_sum();

# Coinciding-filepath heap fixture on shared storage.  Tiny fillfactor=10 table
# keeps k=1 on block 0.  node0 establishes cached X with a single committed
# INSERT (NO version chain) and cleans out so the peer read is served off the
# HEAP_XMIN_COMMITTED hint bit (TT-clean) and the page is ITL-quiescent.
my $tbl;
for my $i (1 .. 12) {
	my $t = "cx_t$i";
	$_->safe_psql('postgres', "CREATE TABLE $t (k int, v int) WITH (fillfactor=10)")
		for ($n0, $n1);
	my $p0 = $n0->safe_psql('postgres', "SELECT pg_relation_filepath('$t')");
	my $p1 = $n1->safe_psql('postgres', "SELECT pg_relation_filepath('$t')");
	if (($p0 // '') eq ($p1 // '')) { $tbl = $t; last; }
}
die 'no coinciding filepath found' unless defined $tbl;
diag("table=$tbl");
ok(1, "L1 selected coinciding-filepath table $tbl");

$n0->safe_psql('postgres', "INSERT INTO $tbl VALUES (1, 0)");   # seed + take X
$n0->safe_psql('postgres', "VACUUM (FREEZE) $tbl");             # prune + hint
$n0->safe_psql('postgres', 'CHECKPOINT');
$n0->safe_psql('postgres', "SELECT count(*) FROM $tbl");        # own-xid cleanout
usleep(300_000);

my $dg_base = dg_sum();
my $detect_base = state_int($n0, 'pcm', 'writer_cover_stale_detected_count');
my $reacq_base = state_int($n0, 'pcm', 'writer_reverify_reacquire_count');

# Arm the cached-X stall on node0 (GUC -> every node0 backend). 3s window.
arm($n0, 'cluster-pcm-writer-cached-x-stall:sleep:3000000');
usleep(400_000);

# node0's stalled UPDATE (async).  It covers cached X on block 0 and stalls in
# the cover window (before the content-lock acquire) holding a pin but NO
# content lock and having written nothing.  A second -c in the SAME session
# reads THIS backend's own per-process inject hit counter after the UPDATE
# returns -> deliverable (a) (the counter is process-local).
my ($in, $out, $err) = ('', '', '');
my $t0 = time();
my $h = IPC::Run::start(
	[ 'psql', '-X', '-A', '-t', '-q', '-d', $n0->connstr('postgres'),
		'-c', "UPDATE $tbl SET v = 20 WHERE k = 1",
		'-c', "SELECT 'HITS=' || hits FROM pg_stat_cluster_injections "
			. "WHERE name = 'cluster-pcm-writer-cached-x-stall'" ],
	\$in, \$out, \$err);

usleep(700_000);   # let node0's UPDATE reach the stall holding cached X

# node1 READ of block 0 -> S-request -> node0 holds X, page quiescent -> the
# master serves the read by driving node0's quiescent X->S self-downgrade DURING
# node0's stall.  The single committed tuple is served off its hint bit, so the
# read is TT-free.  Repeat until the downgrade counter advances.
for my $i (1 .. 8) {
	$n1->safe_psql('postgres', "SELECT count(*) FROM $tbl");
	last if dg_sum() - $dg_base >= 1;
	usleep(300_000);
}

my $dg = 0;
for (1 .. 30) { $dg = dg_sum() - $dg_base; last if $dg >= 1; usleep(200_000); }
diag("BAST X->S downgrade delta during stall = $dg");

$h->finish;
my $stall_elapsed = time() - $t0;
my $writer_hits = ($out // '') =~ /HITS=(\d+)/ ? int($1) : -1;
diag(sprintf("node0 stalled writer: elapsed=%.2fs rc=%s own-session inject hits=%d err=[%s]",
		$stall_elapsed, $h->result, $writer_hits, ($err // '') =~ s/\n.*//sr));
disarm($n0);

# (a) node0's writer entered the cover window (its own-session hit counter fired).
cmp_ok($writer_hits, '>=', 1,
	'L2a node0 writer HIT the cached-X cover window (own-session inject hits >= 1)');
# (b) a real X->S downgrade overlapped the writer's stall.
cmp_ok($dg, '>=', 1,
	'L2b a real BAST X->S downgrade fired while node0 writer was stalled');
# (d) the writer really waited through the stall.
cmp_ok($stall_elapsed, '>', 2.5,
	'L2d node0 writer waited through the stall (interleave really happened)');

# FIX-COUPLED assertions (ownership-generation).  Pre-fix, the covered-X writer
# reached the ITL stamp holding stale authority and, on THIS heap sub-interleave,
# was refused by the existing spec-6.12a S-deny backstop (retryable
# CROSS_NODE_WRITE_CONFLICT) -- a liveness cost, and NO backstop at all on the
# non-ITL (index) write path (silent divergence).  The fix makes the writer
# RE-VERIFY under the content lock: it detects the revoked cover and transparently
# re-acquires X before writing.  We prove the mechanism fired via its counters
# (uniform for every PCM-tracked write, ITL or not) and that the writer committed
# cleanly rather than fail-closing.
my $detect_delta = state_int($n0, 'pcm', 'writer_cover_stale_detected_count') - $detect_base;
my $reacq_delta = state_int($n0, 'pcm', 'writer_reverify_reacquire_count') - $reacq_base;
diag(sprintf("writer outcome: rc=%s err=[%s]; cover_stale_detected delta=%d "
		. "reverify_reacquire delta=%d",
		$h->result, ($err // '') =~ s/\n.*//sr, $detect_delta, $reacq_delta));

cmp_ok($detect_delta, '>=', 1,
	'L4a the re-verify DETECTED the stale cover (generation/covers/pending caught '
	  . 'the raced X->S downgrade)');
cmp_ok($reacq_delta, '>=', 1,
	'L4b the writer transparently RE-ACQUIRED X (ownership re-verify action fired)');
like(($err // ''), qr/^\s*$/,
	'L4c the writer committed CLEANLY after re-acquire (no error output; the fix '
	  . 'is transparent, not fail-closed CROSS_NODE_WRITE_CONFLICT)');
unlike(($err // ''), qr/held in X by a remote node|CROSS_NODE_WRITE_CONFLICT/,
	'L4d no stale-authority write-conflict error surfaced');

# (c) the entire fixture stayed TT-clean.
my $tt_delta = tt_noise_sum() - $tt_base;
diag("L3 TT-noise (53R97 / recycled / unknown) delta over whole test = $tt_delta");
is($tt_delta, 0,
	'L3c fixture is TT-clean: zero 53R97 / TT-recycled / TT-unknown delta');

$pair->stop_pair;
done_testing();
