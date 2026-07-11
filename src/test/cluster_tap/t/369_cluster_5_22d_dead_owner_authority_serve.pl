#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 369_cluster_5_22d_dead_owner_authority_serve.pl
#	  spec-5.22d D4 + A1 — dead-owner undo verdict SERVE (Route B,
#	  complete-scan prove) end-to-end on a 2-node ClusterPair + shared
#	  cluster_fs root.  This is the positive leg t/359 L6 honestly
#	  forward-linked: there the dead owner's verdict serve-gate DENIED
#	  (fail-closed); here the elected survivor authority SERVES the verdict
#	  from the dead owner's durable shared segment set.
#
#	  2-node scope note (user-ruled, D4-6 approve): with two nodes the
#	  reader IS the survivor IS the elected authority, so this test covers
#	  the SELF-authority leg (route -> SERVE_SELF_BLOCK0 -> complete-scan
#	  prove) only.  The kind-4 PEER-wire serve leg needs a 3+ node topology
#	  (reader != authority) and its e2e evidence is deliberately NOT
#	  claimed here (D7 / 3-node TAP).
#
#	  Substrate (t/359 family): node0 commits NORMAL-xid xacts while undo
#	  GCS coherence is ON, so the durable TT stamps land on the SHARED
#	  cluster_fs root.  The A1 complete-scan prove enumerates the owner's
#	  whole shared segment set, so the TT-slot-cursor / record-cursor
#	  straddle (the D4-8 discovery) no longer strands evidence.  Heap
#	  pages are phantom-shared and checkpointed clean before the crash.
#	  Four SUBJECT tables isolate attribution — each is touched by exactly
#	  ONE owner xact, so no later same-page access can clean the ITLs out
#	  from under the leg that needs a raw ref:
#	    s_t    L1: one committed batch xact (8 rows);
#	    s_ab   L2a: one regular-ROLLBACK xact (id=999) — NO durable
#	           ABORTED stamp exists for regular aborts (A1.5: only 2PC and
#	           the owner's own recovery stamp ABORTED), so under a dead
#	           never-recovered owner this xid is in-doubt: the read must
#	           FAIL CLOSED, never false-visible, never guessed-invisible;
#	    s_ab2  L2b: one 2PC PREPARE + ROLLBACK PREPARED xact (id=888) —
#	           the durable ABORTED stamp (cluster_tt_2pc.c) makes the
#	           terminal state provable from the dead owner's segment set;
#	    s_t2   L3: one committed batch xact, first-touched only under the
#	           armed injection (cold in every cache).
#
#	  PCM warmup (load-bearing): while node0 lives — and BEFORE
#	  crossnode_runtime_visibility is armed — node1 reads the tables once.
#	  The PCM layer works (node0's X grants downgrade for the clean-page S
#	  read under cluster.read_scache; the page bytes land S-held in node1's
#	  pool) but the visibility resolution fails closed 53R97, so NO xid
#	  resolves, NO memo/hint warms: every verdict below is provably
#	  post-mortem.  Without this warmup the owner dies still X-holding the
#	  heap pages and node1's read dies upstream at "could not obtain read
#	  image from X holder" (dead-holder page recovery — out of D4 scope,
#	  registered follow-up).
#
#	  L1   committed batch -> owner DEAD -> node1 reads all 8 rows.  HARD
#	       counter asserts: undo_authority_serve_hit_count moved (Route B
#	       complete-scan prove ran, not overlay/cache/native) AND
#	       materialized_remote_instances == 0 (no WAL-materialization
#	       dependence).
#	  L2a  regular-abort row: read FAILS CLOSED 53R97 persistently +
#	       undo_authority_fail_closed_count moved (in-doubt by design —
#	       an on-disk ACTIVE slot is not proof of non-commit under the
#	       machine-crash model, and there is no live owner to ask).
#	  L2b  2PC-aborted row: NOT visible (never false-visible).  EMPIRICAL
#	       FINDING (documented in spec A1.5): ROLLBACK PREPARED physically
#	       reverts the tuple synchronously — pre-kill warmup already reads
#	       count=0 natively — so the durably-ABORTED SERVE conjunction is
#	       unreachable through this vehicle (serve delta stays 0, printed
#	       honestly).  The ABORTED serve arm is pinned at every pure layer
#	       (scan/fold/fill/map units); its e2e needs a crash window between
#	       the durable abort stamp and the undo apply -> D7 injection.
#	  L3   (L408) injection-forced prove refusal on the cold s_t2:
#	       fail-closed 53R97 + fail_closed counter delta; NEVER a native
#	       CLOG answer.
#	  L3b  disarm control: a fresh backend re-reads s_t2 and succeeds via
#	       the serve — proving L3's refusal was the armed injection.
#
# Spec: spec-5.22d-undo-dead-owner-verdict-serve.md (D4-8, §4.2 + A1)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/369_cluster_5_22d_dead_owner_authority_serve.pl
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

sub poll_until
{
	my ($node, $sql, $want, $secs) = @_;
	my $deadline = time() + $secs;
	while (time() < $deadline)
	{
		my $v = eval { $node->safe_psql('postgres', $sql); };
		return 1 if defined $v && $v eq $want;
		usleep(250_000);
	}
	return 0;
}

sub arm_guc_both
{
	my ($pair, $guc, $val) = @_;
	for my $n ($pair->node0, $pair->node1)
	{
		$n->safe_psql('postgres', "ALTER SYSTEM SET $guc = $val");
		$n->safe_psql('postgres', 'SELECT pg_reload_conf()');
	}
	usleep(1_000_000);
}

# ============================================================
# Boot.
# ============================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'spec_5_22d_authority',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.ges_request_timeout_ms = 30000',
		'cluster.gcs_reply_timeout_ms = 3000',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 5',
		'cluster.undo_segments_max_per_instance = 256',
		'cluster.undo_segment_create_timeout_ms = 5000',
		# L2b needs 2PC (the one regular path with a durable ABORTED stamp).
		'max_prepared_transactions = 10',
		# read_scache (spec-6.12a ㉕): the PCM warmup read must actually
		# DOWNGRADE the owner's X (quiescent X->S) instead of the default
		# one-shot read-image ship (which leaves the dying owner X-holding
		# the heap pages and strands every post-mortem read at "could not
		# obtain read image from X holder", upstream of the verdict).
		'cluster.read_scache = on',
	]);
$pair->start_pair;
usleep(2_000_000);

ok($pair->wait_for_peer_state(0, 1, 'connected', 30), 'boot node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30), 'boot node1 sees node0 connected');

my ($node0, $node1) = ($pair->node0, $pair->node1);

# Phantom-shared heap tables (coincident relfilepath, like t/359/t/346).
# Created BEFORE coherence arms: the first local write must form the local
# pg_undo/instance_N tree (arming before any write hits the cold-formation
# PANIC — the known spec-5.22 gap, out of D4 scope).
for my $n ($node0, $node1)
{
	$n->safe_psql('postgres', 'CREATE TABLE s_t (id int, v int)');
	$n->safe_psql('postgres', 'CREATE TABLE s_ab (id int, v int)');
	$n->safe_psql('postgres', 'CREATE TABLE s_ab2 (id int, v int)');
	$n->safe_psql('postgres', 'CREATE TABLE s_t2 (id int, v int)');
}
my $p0 = $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('s_t')});
my $p1 = $node1->safe_psql('postgres', q{SELECT pg_relation_filepath('s_t')});
is($p0, $p1, 'boot s_t relfilepath coincidence holds (phantom-shared)');
is($node1->safe_psql('postgres', 'SHOW cluster.crossnode_runtime_visibility'),
	'off', 'boot crossnode_runtime_visibility still off (warmup runs fail-closed)');

# Arm coherence, then write the subjects.  The A1 complete-scan prove reads
# the owner's WHOLE shared segment set, so the TT-slot/record cursor straddle
# across the arming boundary no longer matters — the stamps land in SOME
# shared segment file and the scan will find them.
arm_guc_both($pair, 'cluster.undo_gcs_coherence', 'on');
is($node0->safe_psql('postgres', 'SHOW cluster.undo_gcs_coherence'),
	'on', 'undo_gcs_coherence armed on');

# Force a fresh segment under coherence=on (the t/359 recipe position).
# Empirical device with TWO effects this test depends on: the subject
# xacts' records land in a shared-BORN segment, and — observed across the
# D4-8 iteration matrix — the subjects' ITLs stay RAW on the heap pages
# (without it the owner side hints/cleans them and every read below turns
# native + vacuous; the warmup HARD asserts below catch that drift loudly).
ok(write_retry($node0, q{SELECT cluster_undo_test_force_segment_end()}),
	'owner forced fresh undo segment under coherence=on');

# One owner xact per subject table (attribution isolation, see banner).
ok(write_retry($node0, 'INSERT INTO s_t SELECT g, g * 10 FROM generate_series(1, 8) g'),
	'owner committed 8 NORMAL-xid rows into s_t (L1 subject)');
$node0->safe_psql('postgres', q{
	BEGIN;
	INSERT INTO s_ab VALUES (999, -1);
	ROLLBACK;
});
$node0->safe_psql('postgres', q{
	BEGIN;
	INSERT INTO s_ab2 VALUES (888, -2);
	PREPARE TRANSACTION 'spec522d_l2b';
});
$node0->safe_psql('postgres', q{ROLLBACK PREPARED 'spec522d_l2b'});
ok(write_retry($node0, 'INSERT INTO s_t2 SELECT g, g FROM generate_series(1, 5) g'),
	'owner committed 5 rows into s_t2 (L3 cold-cache subject)');
ok(write_retry($node0, 'CHECKPOINT'),
	'owner checkpoint (heap pages clean on shared storage; no page crash-recovery needed)');

# ============================================================
# PCM warmup (see banner): node1 pulls the heap pages S while node0 can
# still yield its X grants; crossnode is OFF so the visibility resolution
# fails closed and NOTHING about the xids is learned or cached.  The
# aborted-subject pages may or may not error depending on rollback physics —
# the read attempt itself pulls the page either way (tolerant legs).
# ============================================================
for my $t (qw(s_t s_t2))
{
	my $warmed = 0;
	my $err_shape = '';
	for my $try (1 .. 20)
	{
		my ($rc, $out, $err) = $node1->psql('postgres', "SELECT count(*) FROM $t");
		if ($rc != 0 && defined $err && $err =~ /cluster TT status unknown/)
		{
			$warmed = 1;
			last;
		}
		$err_shape = ($err // '') . ' out=' . ($out // '');
		usleep(500_000);
	}
	diag("warmup $t last outcome: $err_shape") if !$warmed;
	ok($warmed,
		"warmup: node1 read of $t reached visibility and failed closed 53R97 (page now S-held, zero xid resolution)");
}
for my $t (qw(s_ab s_ab2))
{
	my ($rc, $out, $err) = $node1->psql('postgres', "SELECT count(*) FROM $t");
	diag("warmup $t (tolerant): rc=$rc out=" . ($out // '') . " err=" . ($err // ''));
}

# Arm the crossnode consumer only now — the warmup above must never resolve.
arm_guc_both($pair, 'cluster.crossnode_runtime_visibility', 'on');
is($node1->safe_psql('postgres', 'SHOW cluster.crossnode_runtime_visibility'),
	'on', 'crossnode_runtime_visibility armed on after warmup');

# Counter baselines on the survivor BEFORE the crash.
my $serve_hit0 = state_val($node1, 'cr', 'undo_authority_serve_hit_count');
is(state_val($node1, 'recovery', 'materialized_remote_instances'), 0,
	'baseline: no materialized remote instances before the crash');

# ============================================================
# Owner dies.  node1 (the only survivor) becomes the deterministically
# elected serve authority once node0 is dead-DECIDED (accepted reconfig
# dead set, L419 — CSSD 'suspected' alone keeps the route fail-closed,
# which the read polls below absorb as retries).
# ============================================================
$pair->kill_node9(0);
ok( poll_until($node1,
		q{SELECT state IN ('suspected','dead') FROM pg_cluster_cssd_peers WHERE node_id = 0},
		't', 40),
	'node1 CSSD marks node0 suspected/dead');

# ============================================================
# L1: committed rows served from the dead owner's durable segment set.
# ============================================================
{
	my $ok_rows = 0;
	my $last_err = '';
	for my $try (1 .. 40)
	{
		my ($rc, $out, $err) = $node1->psql('postgres', 'SELECT count(*) FROM s_t');
		if (defined $out && $out =~ /^8$/m) { $ok_rows = 1; last; }
		$last_err = $err // '';
		usleep(1_000_000);
	}

	for my $k (qw(undo_authority_serve_hit_count undo_authority_fail_closed_count
		undo_authority_epoch_stale_reject_count undo_authority_scan_incomplete_reject_count
		undo_authority_multi_match_reject_count rtvis_resolve_committed_count
		rtvis_resolve_aborted_count rtvis_resolve_failclosed_count))
	{
		diag("L1 node1 cr.$k = " . state_val($node1, 'cr', $k));
	}
	diag("L1 node1 undo.smgr_pread_count = " . state_val($node1, 'undo', 'smgr_pread_count'));
	diag("L1 last node1 read error: $last_err") if $last_err;

	ok($ok_rows,
		'L1 POSITIVE LEG: node1 fresh read sees the 8 committed rows after the owner died (authority serve)');

	cmp_ok(state_val($node1, 'cr', 'undo_authority_serve_hit_count'), '>', $serve_hit0,
		'L1 HARD ASSERT: undo_authority_serve_hit_count moved (Route B complete-scan prove ran, not overlay/cache/native)');
	is(state_val($node1, 'recovery', 'materialized_remote_instances'), 0,
		'L1 HARD ASSERT: materialized_remote_instances == 0 (pure Route B, no WAL-materialization dependence)');
}

# ============================================================
# L2a: the regular-abort row is IN-DOUBT under a dead never-recovered owner
#      (no durable ABORTED stamp exists for regular aborts, A1.5): the read
#      must FAIL CLOSED — never false-visible, never guessed-invisible.
# ============================================================
{
	my $failclose_pre = state_val($node1, 'cr', 'undo_authority_fail_closed_count');
	my ($rc, $out, $err) = $node1->psql('postgres', 'SELECT count(*) FROM s_ab');

	isnt($rc, 0, 'L2a regular-abort subject: node1 read fails closed (in-doubt, dead owner)');
	unlike(($out // ''), qr/^\s*[01]\s*$/m,
		'L2a the read returned NO row count at all (neither false-visible 1 nor guessed-invisible 0)');
	like($err, qr/cluster TT status unknown|cluster TT slot recycled/,
		'L2a the failure is the 53R97 fail-closed boundary');
	cmp_ok(state_val($node1, 'cr', 'undo_authority_fail_closed_count'), '>', $failclose_pre,
		'L2a HARD ASSERT: undo_authority_fail_closed_count moved (the refusal came from the prove)');
}

# ============================================================
# L2b: the 2PC-aborted row carries a durable ABORTED stamp — the one
#      regular-path terminal abort a dead owner's segment set can prove.
# ============================================================
{
	my $serve_pre = state_val($node1, 'cr', 'undo_authority_serve_hit_count');
	my $false_visible = 0;
	my $outcome = 'error';
	for my $try (1 .. 15)
	{
		# Empirically this page can wedge PERMANENTLY in the post-reconfig
		# block-protocol rebuild (the dead-X-holder strand family: some
		# deferred 2PC-rollback activity re-takes X after the warmup
		# downgrade, so the owner dies holding it — pre-existing wall,
		# registered follow-up).  A persistent fail-closed ERROR is a SAFE
		# outcome; the ONLY failure mode this leg guards is the row being
		# SERVED VISIBLE (count 1).
		my ($rc, $out, $err) = $node1->psql('postgres', 'SELECT count(*) FROM s_ab2');
		if (defined $out && $out =~ /^1$/m) { $false_visible = 1; last; }
		if (defined $out && $out =~ /^0$/m) { $outcome = 'invisible'; last; }
		usleep(1_000_000);
	}
	my $serve_delta = state_val($node1, 'cr', 'undo_authority_serve_hit_count') - $serve_pre;
	diag("L2b outcome = $outcome, serve_hit delta = $serve_delta (expected 0: ROLLBACK PREPARED "
		. "reverts synchronously, nothing left to resolve — the positive ABORTED-serve e2e is "
		. "D7's crash-window injection)");

	ok(!$false_visible,
		'L2b 2PC-aborted row is NEVER served visible on node1 (invisible or fail-closed are both safe; serve leg honestly vacuous, see banner)');
}

# ============================================================
# L3: injection-forced refusal (L408).  The SKIP point sits at the head of
#     the complete-scan prove core; arming is process-local, and the SELF
#     leg prove runs in the reader backend itself — so arm + first-touch
#     read in ONE session.  s_t2's xid is cold in every cache, so the read
#     MUST reach the prove and MUST refuse fail-closed (53R97), never
#     answer from a native path.
# ============================================================
{
	my $failclose_pre = state_val($node1, 'cr', 'undo_authority_fail_closed_count');
	my ($rc, $out, $err) = $node1->psql('postgres', q{
		SELECT cluster_inject_fault('cluster-undo-authority-block0-prove', 'skip', 0);
		SELECT count(*) FROM s_t2;
	});
	isnt($rc, 0, 'L3 armed prove-refusal: node1 first-touch read of s_t2 errors (fail-closed)');
	unlike(($out // ''), qr/^\s*5\s*$/m,
		'L3 armed read did NOT return the 5 rows (no native/cached bypass of the refused prove)');
	like($err, qr/cluster TT status unknown|cluster TT slot recycled/,
		'L3 the failure is the 53R97 fail-closed boundary (never a native CLOG answer)');
	cmp_ok(state_val($node1, 'cr', 'undo_authority_fail_closed_count'), '>', $failclose_pre,
		'L3 HARD ASSERT: undo_authority_fail_closed_count moved (the refusal came from the prove arm)');
}

# ============================================================
# L3c: enumeration-fault refusal (spec-5.22d Hardening, errno-fragility).
#      A directory read that breaks mid-enumeration must fail closed via the
#      SCAN-INCOMPLETE path (the durable segment set is not provably complete),
#      NEVER a raw error abort and never a truncated "complete" scan that could
#      serve a false-unique verdict.  Arm 'error' models the ReadDir throw; the
#      read of the still-cold s_t2 must reach the prove's enumeration loop and
#      refuse via the scan-incomplete counter, not a generic head refusal.
# ============================================================
{
	my $incpl_pre =
		state_val($node1, 'cr', 'undo_authority_scan_incomplete_reject_count');
	my ($rc, $out, $err) = $node1->psql('postgres', q{
		SELECT cluster_inject_fault('cluster-undo-authority-scan', 'error', 0);
		SELECT count(*) FROM s_t2;
	});
	isnt($rc, 0, 'L3c armed enum-fault: node1 read of s_t2 errors (fail-closed, not served)');
	unlike(($out // ''), qr/^\s*5\s*$/m,
		'L3c armed read did NOT return the 5 rows (an incomplete enumeration never serves a verdict)');
	like($err, qr/cluster TT status unknown|cluster TT slot recycled/,
		'L3c the failure is the 53R97 fail-closed boundary (enumeration folded to a coverage failure, never a raw abort)');
	cmp_ok(state_val($node1, 'cr', 'undo_authority_scan_incomplete_reject_count'),
		'>', $incpl_pre,
		'L3c HARD ASSERT: undo_authority_scan_incomplete_reject_count moved (refusal came from the enum-incomplete path, not a generic head refusal)');
}

# ============================================================
# L3b: disarm control — a fresh (unarmed) backend re-reads s_t2 and the
#      serve succeeds, proving L3's refusal was the armed injection.
# ============================================================
{
	my $ok_rows = 0;
	for my $try (1 .. 20)
	{
		my ($rc, $out, $err) = $node1->psql('postgres', 'SELECT count(*) FROM s_t2');
		if (defined $out && $out =~ /^5$/m) { $ok_rows = 1; last; }
		usleep(500_000);
	}
	ok($ok_rows, 'L3b unarmed control: fresh backend read of s_t2 sees the 5 rows (serve intact)');
}

$pair->stop_pair if $pair->can('stop_pair');
done_testing();
