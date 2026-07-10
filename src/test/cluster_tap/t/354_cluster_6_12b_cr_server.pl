#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 354_cluster_6_12b_cr_server.pl
#	  spec-6.12 wave b -- cross-instance CR-server data plane on a 2-node
#	  ClusterPair (phantom-shared plain-heap table, t/334 pattern).
#
#	  Pre-6.12b, a CR construction whose undo chain lives in a REMOTE
#	  instance fails closed 53R9G (spec-5.57 class-(3) boundary).  With
#	  cluster.crossnode_cr_data_plane = on the origin's CR-server (LMS
#	  constructs, LMON ships) serves the CR page.
#
#	  Why the test SRF, not a natural SELECT (L373/L374 honesty, same as
#	  t/318 for the 5.57 boundary): a natural cross-instance SELECT on this
#	  phantom-shared harness fails UPSTREAM at the visibility-resolution
#	  boundary (a recycled remote ITL slot for a not-yet-materialized origin
#	  -> 53R97) BEFORE CR construction is ever reached, so the data plane is
#	  unreachable from a natural read here (exactly the Stage-6 gap spec-5.57
#	  documents).  cluster_cr_test_image() drives cluster_cr_construct_block
#	  DIRECTLY, so it reaches the data-plane hook deterministically.  The
#	  DIFFERENTIAL master oracle: node1's remote-served CR image must equal
#	  node0's local (all-undo-home) CR image for the same read_scn.
#
#	  L1  pair boots + phantom table coincident + GUC armed
#	  L2  FULL differential: node0 seeds + updates; node1's CR image at the
#	      pre-update read_scn (served by node0's CR-server) EQUALS node0's
#	      local CR image, and the origin counted a FULL serve
#	  L3  data plane OFF at the origin -> requester keeps the unchanged
#	      53R9G fail-closed (Rule 8.A non-degradable)
#	  L4  data plane OFF at the requester -> class-(3) refuse before any
#	      fetch -> 53R9G (the boundary is byte-identical to pre-6.12b)
#	  L5  counter surface: 6 cr-server keys present on both nodes
#
# Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md (wave b)
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# IDENTIFICATION
#	  src/test/cluster_tap/t/354_cluster_6_12b_cr_server.pl
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
		qq{SELECT value FROM pg_cluster_state
		   WHERE category='$cat' AND key='$key'});
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

# (id,v) digest of the CR image of block $b as-of $scn, driven on $node.
# Returns (digest, err): digest defined on success; err holds the SQLSTATE
# text on a fail-closed construction (53R9G etc.).
sub cr_image
{
	my ($node, $b, $scn) = @_;
	my ($rc, $out, $err) = $node->psql('postgres',
		qq{SELECT string_agg(id || ':' || COALESCE(v::text,'NULL'), '|' ORDER BY id)
		   FROM cluster_cr_test_image('cr_t'::regclass, $b, $scn)
		        AS img(cr_off int2, id int, v int)});
	return ($rc == 0 ? $out : undef, $err // '');
}

# Retry cr_image on $node until it constructs (bounded); used on the FULL
# leg where the remote fetch may race the async overlay warm-up.
sub cr_image_retry
{
	my ($node, $b, $scn) = @_;
	for my $i (1 .. 20)
	{
		my ($d, $err) = cr_image($node, $b, $scn);
		return ($d, $err) if defined $d;
		usleep(500_000);
	}
	return (undef, 'retry exhausted');
}

# ============================================================
# L1: boot (wave armed from conf) + phantom-shared table.
# ============================================================
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'b612_crsrv',
	quorum_voting_disks => 3,
	shared_data         => 1,
	data_port_span      => 2,	# spec-7.3: default lms_workers=2 binds data_port+[0,1]
	extra_conf          => [
		'autovacuum = off',
		'cluster.crossnode_cr_data_plane = on',
		# L355: widen heartbeat deathwatch under CI load.
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
	]);
$pair->start_pair;

usleep(2_000_000);
ok($pair->wait_for_peer_state(0, 1, 'connected', 30),
	'L1 node0 sees node1 connected');
ok($pair->wait_for_peer_state(1, 0, 'connected', 30),
	'L1 node1 sees node0 connected');

my ($node0, $node1) = ($pair->node0, $pair->node1);

is($node1->safe_psql('postgres', 'SHOW cluster.crossnode_cr_data_plane'), 'on',
	'L1 crossnode_cr_data_plane armed from boot conf');

$node0->safe_psql('postgres', 'CREATE TABLE cr_t (id int, v int)');
$node1->safe_psql('postgres', 'CREATE TABLE cr_t (id int, v int)');
my $p0 = $node0->safe_psql('postgres', q{SELECT pg_relation_filepath('cr_t')});
my $p1 = $node1->safe_psql('postgres', q{SELECT pg_relation_filepath('cr_t')});
is($p0, $p1, 'L1 cr_t relfilepath coincidence holds');

# One heap block; node0 is the sole writer so every candidate chain is
# node0-home (the FULL split).
ok(write_retry($node0, 'INSERT INTO cr_t SELECT g, g FROM generate_series(1,20) g'),
	'L1 node0 seeded 20 rows (one heap block)');
ok(write_retry($node0, 'CHECKPOINT'), 'L1 checkpoint');

# ============================================================
# L2: FULL differential.  Capture a read_scn AFTER the seed, then update;
# the CR image at that read_scn is the pre-update state on BOTH nodes.
# ============================================================
my $scn_mid = $node0->safe_psql('postgres', 'SELECT cluster_scn_current()');
chomp $scn_mid;
ok(defined $scn_mid && $scn_mid =~ /^\d+$/, "L2 captured read_scn ($scn_mid)");

ok(write_retry($node0, 'UPDATE cr_t SET v = v + 100'),
	'L2 node0 post-read_scn update committed');

# Reference: node0 reconstructs the pre-update image locally (all undo home).
my ($auth, $auth_err) = cr_image($node0, 0, $scn_mid);
ok(defined $auth && $auth ne '',
	'L2 node0 authoritative CR image built (' . (defined $auth ? 'ok' : "err=$auth_err") . ')');

my $sf0 = state_val($node0, 'cr', 'cr_server_full_count');
my $rf0 = state_val($node1, 'cr', 'cr_remote_full_count');

# Subject: node1 reconstructs the same read_scn -> the block's newest chain is
# node0-home, so the data plane fetches from node0's CR-server.
my ($served, $served_err) = cr_image_retry($node1, 0, $scn_mid);
ok(defined $served,
	'L2 node1 remote-served CR image built (' . (defined $served ? 'ok' : "err=$served_err") . ')');
is($served, $auth,
	'L2 remote-served CR image EQUALS the authoritative local reconstruct (differential)');

cmp_ok(state_val($node0, 'cr', 'cr_server_full_count') - $sf0, '>', 0,
	'L2 origin CR-server counted a FULL serve');
cmp_ok(state_val($node1, 'cr', 'cr_remote_full_count') - $rf0, '>', 0,
	'L2 requester counted a remote FULL CR result');

# ============================================================
# L3: origin data plane OFF -> requester keeps 53R9G fail-closed.
# ============================================================
$node0->safe_psql('postgres',
	'ALTER SYSTEM SET cluster.crossnode_cr_data_plane = off');
$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');
usleep(1_000_000);

# node1 still has the data plane on, so it tries to fetch, but node0 refuses
# (submit denied) -> DENIED reply -> requester keeps the unchanged 53R9G.
my $rfail0 = state_val($node1, 'cr', 'cr_remote_failed_count');
my ($off_d, $off_err) = cr_image($node1, 0, $scn_mid);
like($off_err, qr/(53R9G|cross-instance)/,
	'L3 origin-off keeps the requester fail-closed 53R9G (Rule 8.A)');
cmp_ok(state_val($node1, 'cr', 'cr_remote_failed_count') - $rfail0, '>', 0,
	'L3 requester counted the failed remote fetch');

$node0->safe_psql('postgres',
	'ALTER SYSTEM SET cluster.crossnode_cr_data_plane = on');
$node0->safe_psql('postgres', 'SELECT pg_reload_conf()');

# ============================================================
# L4: requester data plane OFF -> class-(3) refuse BEFORE any fetch ->
# byte-identical pre-6.12b 53R9G boundary.
# ============================================================
$node1->safe_psql('postgres',
	'ALTER SYSTEM SET cluster.crossnode_cr_data_plane = off');
$node1->safe_psql('postgres', 'SELECT pg_reload_conf()');
usleep(1_000_000);

my ($req_off_d, $req_off_err) = cr_image($node1, 0, $scn_mid);
like($req_off_err, qr/(53R9G|cross-instance)/,
	'L4 requester-off keeps the class-(3) fail-closed 53R9G (unchanged boundary)');

$node1->safe_psql('postgres',
	'ALTER SYSTEM SET cluster.crossnode_cr_data_plane = on');
$node1->safe_psql('postgres', 'SELECT pg_reload_conf()');

# ============================================================
# L5: counter surface on both nodes.
# ============================================================
for my $n ($node0, $node1)
{
	my $rows = $n->safe_psql('postgres',
		q{SELECT count(*) FROM pg_cluster_state
		   WHERE category='cr' AND key IN
		     ('cr_remote_full_count','cr_remote_partial_count','cr_remote_failed_count',
		      'cr_server_full_count','cr_server_partial_count','cr_server_denied_count')});
	is($rows, '6', 'L5 cr-server keys present (' . $n->name . ')');
}

# ============================================================
# L5b: spec-7.3 D8 — per-worker inline-serve observability.  The L2 remote CR
# above was served inline by the origin's worker[shard], so the origin's
# per-worker serve counters + duration histogram must have moved, and every
# inline serve ships exactly one direct reply (direct_reply >= inline_serve;
# fence/deny ships count as replies too).
# ============================================================
{
	my $srv = state_val($node0, 'lms', 'lms_inline_serve_count');
	my $rep = state_val($node0, 'lms', 'lms_direct_reply_count');
	my $hist_total = $node0->safe_psql('postgres', q{
		SELECT COALESCE(sum(value::bigint), 0) FROM pg_cluster_state
		 WHERE category='lms' AND key LIKE 'lms\_serve\_hist\_us\_%' ESCAPE '\'
		   AND key NOT LIKE '%\_w%' ESCAPE '\'});
	cmp_ok($srv, '>', 0, 'L5b origin inline-serve counter moved (worker-side serve)');
	cmp_ok($rep, '>=', $srv, 'L5b every inline serve shipped a direct reply');
	cmp_ok($hist_total, '>', 0, 'L5b serve-duration histogram recorded the serves');
}

# ============================================================
# L6: spec-7.3 D6 — 8.A envelope on the inline serve.  Forcing the origin's
# CR construction to fail-closed (the cluster-lms-cr-construct skip injection)
# must (a) keep the requester at the unchanged 53R9G, (b) leave the origin's
# serving worker[shard] alive (the inline serve reproduces the drain's
# PG_TRY -> DENIED envelope, so a refused construction never crashes the
# worker), and (c) recover a normal serve once the injection clears.
# ============================================================
SKIP:
{
	skip "ClusterPair inject SKIP helper missing — L6 8.A envelope covered by "
		. "the shared PG_TRY drain path", 3
		unless $pair->can('inject_skip_set');

	$pair->inject_skip_set($node0, 'cluster-lms-cr-construct', 100);
	my ($off_d6, $off_err6) = cr_image($node1, 0, $scn_mid);
	like($off_err6, qr/(53R9G|cross-instance)/,
		'L6 origin construct fail-closed keeps the requester 53R9G (8.A inline)');

	# The origin's serving worker survived the fail-closed serve.
	is($node0->safe_psql('postgres', 'SELECT 1'), '1',
		'L6 origin still serving after a fail-closed inline CR construct');

	$pair->inject_skip_set($node0, 'cluster-lms-cr-construct', 0);
	my ($rec_d6, $rec_err6) = cr_image_retry($node1, 0, $scn_mid);
	is($rec_d6, $auth,
		'L6 remote CR serve recovers after the injection clears '
		. '(' . (defined $rec_d6 ? 'ok' : "err=$rec_err6") . ')');
}

# ============================================================
# L7: spec-7.3 D7 — fence ×N.  A write-fenced node must not ship a CR image on
# the DATA plane.  The fence gate sits ahead of construction in the inline
# serve, so forcing it (cluster-lms-cr-fence-refuse) must (a) keep the requester
# fail-closed 53R9G, (b) bump cr_server_fence_refused_count, (c) NOT construct
# anything (full/partial counters stay put — the RED signal distinguishing the
# gate from a construction that merely failed), (d) leave the origin serving,
# and (e) recover a normal serve once the fence clears.
# ============================================================
SKIP:
{
	skip "ClusterPair inject SKIP helper missing — fence gate covered by "
		. "the shared DENIED reply path", 5
		unless $pair->can('inject_skip_set');

	my $fr_before = state_val($node0, 'cr', 'cr_server_fence_refused_count');
	my $full_before = state_val($node0, 'cr', 'cr_server_full_count');
	my $part_before = state_val($node0, 'cr', 'cr_server_partial_count');

	$pair->inject_skip_set($node0, 'cluster-lms-cr-fence-refuse', 100);
	my ($off_d7, $off_err7) = cr_image($node1, 0, $scn_mid);
	like($off_err7, qr/(53R9G|cross-instance)/,
		'L7 write-fenced origin keeps the requester fail-closed 53R9G (fence ×N, 8.A)');

	# The fence gate fired (refused ship) ...
	my $fr_after = state_val($node0, 'cr', 'cr_server_fence_refused_count');
	ok($fr_after > $fr_before,
		"L7 cr_server_fence_refused_count advanced ($fr_before -> $fr_after)");

	# ... and did so WITHOUT constructing anything (gate is ahead of construct).
	is(state_val($node0, 'cr', 'cr_server_full_count'), $full_before,
		'L7 no FULL construction happened while fenced (gate precedes construct)');
	is(state_val($node0, 'cr', 'cr_server_partial_count'), $part_before,
		'L7 no PARTIAL construction happened while fenced');

	# The origin's serving worker survived the refused serve.
	is($node0->safe_psql('postgres', 'SELECT 1'), '1',
		'L7 origin still serving after a fence-refused inline serve');

	$pair->inject_skip_set($node0, 'cluster-lms-cr-fence-refuse', 0);
	my ($rec_d7, $rec_err7) = cr_image_retry($node1, 0, $scn_mid);
	is($rec_d7, $auth,
		'L7 remote CR serve recovers after the fence clears '
		. '(' . (defined $rec_d7 ? 'ok' : "err=$rec_err7") . ')');
}

$pair->stop_pair if $pair->can('stop_pair');
done_testing();
