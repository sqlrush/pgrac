# 254_pi_cr_recovery_acceptance.pl
#
# spec-4.9 — Post-Recovery PI / CR Correctness Acceptance.
#
#   Acceptance / composition hard-gate test, NOT a new-mechanism test.  The
#   roadmap-literal 4.9 scope ("PI watermark + CR cache rebuild") is already
#   satisfied by shipped specs (2.37 PI watermark / 3.10 CR block cache /
#   3.21+3.22 CR xmax verdict + retention / 4.7 GCS/PCM warm recovery +
#   remaster / 4.8 TT recovery verdict + recovered_through gate).  This test
#   proves the COMPOSITION holds across crash / restart / remaster -- the five
#   hard gates the individual specs never jointly exercised.
#
#   D0 measure-first (spec-4.9 Impl note v1.0) confirmed all five gates are
#   LIVE in the serve path (no dead code, L243): the redo-before-CR gate at
#   cluster_visibility_resolve.c:203 + cluster_gcs_block.c:675; the CR-cache
#   key carries base_page_lsn (cluster_cr_cache.h:73); pi_watermark retire is
#   tag-lifecycle-only ("HC130 forever forbidden epoch-tied", pcm_lock.c:220);
#   CR xmax verdict + retention live at cluster_cr.c:1609/1656.  No composition
#   gap needs a new protocol -> 4.9 stays pure acceptance.
#
#   Five hard gates (spec-4.9 §3):
#     #1  crash/restart -> CR cache never serves a stale CR page.
#     #2  dead-master/remaster -> PI watermark not cleared by an epoch bump.
#     #3  recovered_through < page_lsn -> CR/visibility fail-closed, no old page.
#     #4  recovered TT verdict + CR xmax resolve -> no false-visible/invisible.
#     #5  >= 1 real 2-node / shared-storage scenario (not all unit/injection):
#         a SURVIVOR backend reads across a peer's crash/remaster (Q4 semantics)
#         -- never the crashed node retaining its own snapshot (impossible).
#
#   Determinism (Q7 / L222 / L247 / spec-4.8 D5 lesson): hard asserts only the
#   8.A safe direction -- success OR an explicit fail-closed SQLSTATE
#   (53R9F / 53R9G / "snapshot too old"), never silent success / wrong data /
#   arbitrary error.  Timing-dependent liveness is diag, not a hard assert.
#
#   Topology note (D0 measure-first finding): in a shared_data cluster, a node
#   that restarts cannot immediately re-serve a previously-held shared block
#   (PCM holder state is volatile; no lazy rebuild in the 4.7 baseline -- see
#   t/251 L1).  That fail-closed behavior is itself 8.A-safe.  So the
#   deterministic LOCAL-correctness legs (CR cache key-safety #1, TT verdict #4)
#   run on a single-node cluster with LOCAL tables (no PCM shared block), and
#   the shared-storage pair carries only the real cross-node leg (#5) + the
#   watermark/remaster observation (#2).
#
#   L239 honest SKIP: the exact "dead-master before redo boundary -> CR
#   fail-closed" flip (#3) is not deterministically reproducible against this
#   harness (mirrors t/251 L2/L3).  The gate DECISION is unit-proven
#   (test_cluster_tt_durable D2 recovered_through truth table) + has a live
#   serve-path caller (cluster_visibility_resolve.c:203, D0).
#
#   Spec: spec-4.9-pi-cr-recovery-acceptance.md (FROZEN, user approve 2026-06-14)
#   src/test/cluster_tap/t/254_pi_cr_recovery_acceptance.pl

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::ClusterPair;
use Test::More;

# Local poll helper (each cluster TAP defines its own; mirrors t/249).
sub poll_query_until_timeout
{
	my ($node, $dbname, $query, $expected, $timeout_s, $label) = @_;
	$expected  //= 't';
	$timeout_s //= 15;
	my $deadline = time + $timeout_s;
	my $last     = '';
	while (time < $deadline)
	{
		$last = $node->safe_psql($dbname, $query);
		return 1 if defined $last && $last eq $expected;
		select(undef, undef, undef, 0.1);
	}
	diag("$label timed out after ${timeout_s}s; expected=$expected; last=$last")
	  if defined $label;
	return 0;
}

# Single-node cluster (cluster mode, stub interconnect, LOCAL tables).
#
#   D0 measure-first finding (codereview P0): a single-node LOCAL read does NOT
#   traverse the cluster CR fork -- the serve-path gate
#   (heapam_visibility.c:1520) requires a CLUSTER-source snapshot AND remote
#   evidence; a single-node local-table read has neither, so the read is judged
#   by PG-native MVCC (verified: all cr_* counters stay 0 even with fastpath
#   off).  The cluster CR fork is fundamentally a cross-node / remote-evidence
#   mechanism (cross-node e2e -> FEATURE_NOT_SUPPORTED, Stage 5 forward; see L6).
#   So this single-node section proves PG-NATIVE post-recovery correctness (the
#   reachable composition), and the cluster CR cache / xmax verdict are proven
#   at cluster_unit (test_cluster_cr_cache.c:123-130 base_page_lsn key MISS;
#   test_cluster_visibility_variants cluster_vis_cr_xmax_verdict; per Q4 #1/#4
#   may be unit/injection -- only #5 must be a real 2-node scenario).
sub _new_single_node
{
	my ($name) = @_;
	my $node = PostgreSQL::Test::Cluster->new($name);
	$node->init;
	$node->append_conf('postgresql.conf',
		    "cluster.enabled = on\n"
		  . "cluster.node_id = 0\n"
		  . "cluster.allow_single_node = on\n"
		  . "cluster.interconnect_tier = stub\n"
		  . "cluster.cr_mvcc_gate = on\n"
		  . "autovacuum = off\n");
	return $node;
}

# ======================================================================
# Part A — single-node deterministic LOCAL-correctness legs.
# ======================================================================
my $node = _new_single_node('picr_single');
$node->start;
is($node->safe_psql('postgres', 'SELECT 1'), '1', 'L0a single node alive');

# ----------------------------------------------------------------------
# L1 (D0/D6) — composition observability surface present.  The acceptance
#   reads composed evidence from EXISTING dump categories; D6 adds none
#   (L39 ripple avoidance).  Validate the emitted content (exact counter
#   counts per category), not mere existence (L223).
# ----------------------------------------------------------------------
my %expect_cat = (
	recovery     => 39,    # 3.16(4)+4.10 block(2)+4.11 thread(4)+4.3 plan(13)+4.4 worker(8)+4.5/4.7 merge(8)
	tt_recovery  => 8,     # 4.8 verdict counters
	gcs_recovery => 10,    # 4.7 warm-recovery(8) + spec-2.41 D7 redo-coverage serve-gate(2)
	cr           => 41,    # 3.10/3.21/3.22 CR path(17) + 5.53 mismatch(5) + 5.54 tuple(8)
	                       # + 11 post-5.54 keys the baseline missed (stale on
	                       # main; first caught by a local full run): 6.12b
	                       # cr_server_{full,partial,denied} +
	                       # cr_remote_{full,partial,failed}, 5.56
	                       # cr_rel_gen_{bump,table_overflow} +
	                       # cr_global_epoch_fallback_bump,
	                       # cr_retention_horizon_advance_noted,
	                       # cr_reconfig_intra_survived
	pcm          => 22,    # 2.37 PI watermark + lock state + 6.14 D5 aux-deferred release
	                       # + spec-6.14a (b)-leg nonholder fail-closed counter
);
for my $cat (sort keys %expect_cat)
{
	my $n = $node->safe_psql('postgres',
		qq{SELECT count(*) FROM cluster_dump_state() WHERE category = '$cat'});
	is($n, "$expect_cat{$cat}",
		"L1 (D6): dump category '$cat' exposes $expect_cat{$cat} counters "
		. "(composition observable; no new category)");
}

# ----------------------------------------------------------------------
# L2 (D2, hard gate #1 — e2e baseline) — post-recovery read returns the
#   correct (current) page version, never a stale one.  On a single node a
#   LOCAL read is PG-native (see _new_single_node note); the CLUSTER CR cache
#   key-safety -- a changed base_page_lsn forces a cache MISS -- is unit-proven
#   at test_cluster_cr_cache.c:123-130 (per Q4 #1 may be unit).  This leg proves
#   recovery does not corrupt / stale the read path.  Deterministic.
# ----------------------------------------------------------------------
$node->safe_psql('postgres', q{
	CREATE TABLE pcr_t (id int primary key, v text);
	INSERT INTO pcr_t SELECT g, 'orig' FROM generate_series(1, 50) g;
});
is($node->safe_psql('postgres', 'SELECT count(*) FROM pcr_t'), '50',
	'L2 pre-restart: 50 rows');
$node->safe_psql('postgres', q{UPDATE pcr_t SET v = 'new' WHERE id <= 10});
$node->safe_psql('postgres', 'CHECKPOINT');
$node->restart;
is($node->safe_psql('postgres', q{SELECT count(*) FROM pcr_t WHERE v = 'new'}),
	'10',
	'L2 (#1 e2e baseline): post-restart read returns the NEW page version '
	. '(no stale pre-restart image; cluster CR-cache key-safety unit-proven)');
is($node->safe_psql('postgres', 'SELECT count(*) FROM pcr_t'), '50',
	'L2 (#1 e2e baseline): full row set intact after restart (no stale/lost rows)');

# ----------------------------------------------------------------------
# L4 (D3, hard gate #4 — e2e baseline) — a crash-left in-flight DELETE never
#   committed; after recovery it resolves ABORTED so the rows stay live and
#   visible (8.A: never show a half-deleted row as gone).  On a single node
#   this is PG-native CLOG/abort recovery (consistent with spec-4.8 D1: on-disk
#   TT slots are never ACTIVE -- in-flight binding is lost on crash); the
#   CLUSTER xmax verdict (cluster_vis_cr_xmax_verdict, no false-visible/
#   invisible across recycled/wrap) is unit-proven at
#   test_cluster_visibility_variants (per Q4 #4 may be unit).  Deterministic.
# ----------------------------------------------------------------------
my $bg = $node->background_psql('postgres', on_error_stop => 0);
$bg->query('BEGIN');
$bg->query('DELETE FROM pcr_t WHERE id <= 25');
$bg->query(q{SELECT 'armed'});    # DELETE done, NOT committed
$node->stop('immediate');         # crash before commit
eval { $bg->quit };
$node->start;
is($node->safe_psql('postgres', 'SELECT count(*) FROM pcr_t'), '50',
	'L4 (#4 e2e baseline): crash-left in-flight DELETE -> recovered ABORTED -> '
	. 'all 50 rows live (no false-invisible / no lost rows)');
is($node->safe_psql('postgres', q{
		SET enable_seqscan = off;
		SELECT count(*) FROM pcr_t WHERE id BETWEEN 1 AND 25;
	}), '25',
	'L4 (#4 e2e baseline): primary-key index scan over the un-deleted range is '
	. 'consistent with the recovered heap (no false-invisible via index)');
$node->stop;

# ======================================================================
# Part B — real 2-node shared-storage legs (hard gate #5 + #2).
#   shared_data => 1 routes user-relation blocks through PCM/GCS, which is
#   what makes the cross-node post-recovery read path real (not a mock).
# ======================================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'pi_cr_accept',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => ['autovacuum = off']);
$pair->start_pair;

my $n0 = $pair->node0;
my $n1 = $pair->node1;
is($n0->safe_psql('postgres', 'SELECT 1'), '1', 'L0b pair node0 alive');
is($n1->safe_psql('postgres', 'SELECT 1'), '1', 'L0b pair node1 alive');

# Same-DDL / same-relfilenode pattern (t/248): the catalog is NOT shared --
# only user-relation DATA blocks are (spec-4.5a).  Both nodes run identical
# DDL so each owns a catalog entry pointing at the SAME shared relfile.
$n0->safe_psql('postgres', 'CREATE TABLE scr_t (id int, v int)');
my $rp0 = $n0->safe_psql('postgres', q{SELECT pg_relation_filepath('scr_t')});
$n1->safe_psql('postgres', 'CREATE TABLE scr_t (id int, v int)');
my $rp1 = $n1->safe_psql('postgres', q{SELECT pg_relation_filepath('scr_t')});

SKIP:
{
	# L239 honest: the same-DDL relfilenode coincidence is not guaranteed on
	# every build; if it does not hold, the real cross-node shared read cannot
	# be set up here -> skip the shared legs (gate #2/#5) with reason rather
	# than fake them.  Gate #3 stays unit-proven below.
	skip "same-DDL relfilepath coincidence does not hold ($rp0 != $rp1); "
	  . "real cross-node shared read not constructible on this build (L239). "
	  . "Single-node gates #1/#4 proven above; gate #3 unit-proven below.", 2
	  if $rp0 ne $rp1;

	# node0 writes the shared blocks + checkpoint (flush to the shared root).
	$n0->safe_psql('postgres', q{
		INSERT INTO scr_t SELECT g, g FROM generate_series(1, 50) g;
		CHECKPOINT;
	});

	# --------------------------------------------------------------
	# L_obs (D1, hard gate #2) — PI watermark survives the remaster
	#   epoch; retire is tag-lifecycle-only, never epoch-tied
	#   (pcm_lock.c:220 HC130).  kill -9 node0 -> node1 (SURVIVOR, Q4)
	#   sees the DEAD edge -> reconfig (epoch bump).  The SAFE invariant
	#   -- retire NOT bumped by the epoch -- is the hard assert.
	# --------------------------------------------------------------
	my $retire_before = $n1->safe_psql('postgres',
		q{SELECT COALESCE(MAX(value::bigint), 0) FROM cluster_dump_state()
		    WHERE category='gcs' AND key='pi_watermark_retire_count'});
	$retire_before = 0 if !defined $retire_before || $retire_before eq '';

	$pair->kill_node9(0);    # SURVIVOR = node1 (Q4 semantics)
	# Bounded settle window for the survivor's DEAD-edge reconfig (diag only).
	poll_query_until_timeout($n1, 'postgres', 'SELECT 1', '1', 30);

	my $retire_after = $n1->safe_psql('postgres',
		q{SELECT COALESCE(MAX(value::bigint), 0) FROM cluster_dump_state()
		    WHERE category='gcs' AND key='pi_watermark_retire_count'});
	$retire_after = 0 if !defined $retire_after || $retire_after eq '';
	# Discriminating (codereview P1): the property is retire NOT bumped BY the
	# epoch -> retire must be FLAT across a pure remaster epoch (no relation
	# drop/truncate in this window).  `==` fails in the unsafe direction (an
	# epoch-tied clear is a retire -> after > before); `>=` would pass it.
	is($retire_after, $retire_before,
		'L_obs (hard gate #2): pi_watermark retire count FLAT across the '
		. "remaster epoch (retire is tag-lifecycle-only, never epoch-tied; "
		. "before=$retire_before after=$retire_after)");

	# --------------------------------------------------------------
	# L6 (hard gate #5) — REAL 2-node shared-storage survivor read across
	#   the peer crash/remaster.  node1 (survivor) reads the shared-data
	#   table node0 wrote, AFTER node0 was killed.  Measure-first finding:
	#   the POSITIVE cross-node read (survivor serves a dead peer's block)
	#   needs cross-node block transfer = FEATURE_NOT_SUPPORTED today (Stage
	#   5 forward; same reason t/248 serializes).  So the reachable outcome
	#   is a SAFE refusal.  8.A hard assert (codereview P1, Q7): correct data
	#   (50) OR an EXPLICIT registered safe outcome -- a CR/authority
	#   fail-closed SQLSTATE (53R9F/53R9G), CR horizon (snapshot too old), or
	#   the explicit FEATURE_NOT_SUPPORTED cross-node-block refusal.  NOT a
	#   broad substring match (no bare GCS/PCM/recover) -- those could mask an
	#   arbitrary/unrelated error as "safe".  Never wrong data.  Liveness diag.
	# --------------------------------------------------------------
	my ($rc, $out, $err) =
	  $n1->psql('postgres', 'SELECT count(*) FROM scr_t', timeout => 30);
	my $failclosed =
	     $err =~ /\b53R9[FG]\b/                          # registered CR / remote-authority fail-closed
	  || $err =~ /snapshot too old/i                     # registered CR horizon fail-closed
	  || $err =~ /cross-node block.*not supported/i;     # explicit FEATURE_NOT_SUPPORTED (Stage 5 forward)
	my $correct = ($rc == 0 && $out eq '50');
	diag("L6: survivor cross-node read rc=$rc out='$out'"
		  . ($failclosed ? " fail-closed (SAFE)" : ($correct ? " correct" : " err=$err")));
	ok($correct || $failclosed,
		'L6 (hard gate #5): real 2-node shared-storage survivor read across '
		. 'peer crash/remaster is correct (50) OR fail-closed (explicit '
		. 'SQLSTATE) -- never wrong data (规则 8.A)');
}

$pair->stop_pair;

# ======================================================================
# L5 (D4, hard gate #3) — recovered_through < page_lsn -> CR/visibility
#   fail-closed.  The exact "dead-master before redo boundary" e2e flip is
#   NOT deterministically reproducible against this harness (L239; mirrors
#   t/251 L2/L3: hold-until-revoked masks held blocks; a survivor restart
#   resets death detection so the corpse is seen alive and the guard is
#   never reached).  The gate DECISION is unit-proven:
#     test_cluster_tt_durable -- recovered_through truth table (4.8 D2):
#         anchor==0 skip / recovered>=anchor trust / recovered<anchor fail.
#     cluster_visibility_resolve.c:203 -- the LIVE serve-path caller (D0).
# ----------------------------------------------------------------------
SKIP:
{
	skip 'hard gate #3 e2e flip (recovered_through < page_lsn -> CR '
	  . 'fail-closed) not deterministically reproducible against this harness '
	  . '(L239; mirrors t/251 L2/L3). Gate decision unit-proven at '
	  . 'test_cluster_tt_durable (recovered_through truth table) + live serve-'
	  . 'path caller cluster_visibility_resolve.c:203 (D0 measure-first).', 1;
	ok(1, 'hard gate #3 placeholder (unit-proven; e2e deferred Stage 5/6)');
}

done_testing();
