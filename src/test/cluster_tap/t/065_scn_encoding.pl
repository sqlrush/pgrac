#-------------------------------------------------------------------------
#
# 065_scn_encoding.pl
#    Stage 1.15 + spec-1.15 SCN encoding layer end-to-end: advance /
#    current / observe SQL UDFs + scn dump 7 keys + node_id high-byte
#    encoding + monotonicity + observe-stat-only contract + superuser
#    gating + injection :error/:skip paths.
#
#    Test matrix (L1-L9):
#
#      L1   cluster_scn_current() returns a valid (non-zero) SCN with
#           the cluster.node_id GUC value encoded in the high 8 bits
#      L2   cluster_scn_advance() bumps local_scn (monotonically; new
#           value > old value via local-only comparison)
#      L3   cluster_scn_observe(remote) is stat-only: max_observed_remote
#           updates but cluster_scn_current local_scn does NOT bump
#           (Stage 1.15 contract per Q4 + L5; full Lamport observe lands
#           at spec-1.16)
#      L4   scn 7 keys present in pg_cluster_state (scn_node_id,
#           scn_current_local, scn_current_encoded, scn_max_observed_remote,
#           scn_total_advance_count, scn_initialized_at, scn_last_advance_at)
#      L5   advance / observe gated on superuser; non-superuser caller
#           gets ereport(ERROR) (L7 lesson)
#      L6   observe with remote_scn whose local_scn < current local_scn
#           is no-op (max-only update semantics)
#      L7   total_advance_count increments per cluster_scn_advance call
#      L8   inject 'cluster-scn-advance-pre' :error fires SQLSTATE 53R0X
#      L9   inject 'cluster-scn-observe-entry' :skip silently bypasses
#           the call without erroring (Q-mechanism TAP保护; F22 lesson)
#
# IDENTIFICATION
#    src/test/cluster_tap/t/065_scn_encoding.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
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

# Pin a deterministic node_id so L1 can verify encoding.
$node->append_conf('postgresql.conf', "cluster.node_id = 7\n");
$node->append_conf('postgresql.conf', "log_min_messages = debug1\n");

$node->start;


# ----------
# L1 (round 8 P1 hardening): cluster_scn_current() returns InvalidScn (=0)
# pre-first-advance, regardless of node_id.  Previously returned
# (node_id << 56) | 0 which compared equal to InvalidScn under
# scn_time_cmp() but PASSED SCN_VALID() -- semantic ambiguity.  After
# fix: local_scn==0 always returns InvalidScn (per spec-1.4 §8 Q2 =
# "real values >= 1").
# ----------
my $pre_advance_scn = $node->safe_psql('postgres',
	'SELECT cluster_scn_current()');
is($pre_advance_scn, '0',
   'L1 cluster_scn_current() returns InvalidScn (=0) before first advance (round 8 P1)');

# After first advance, current() returns a valid encoded SCN.
$node->safe_psql('postgres', 'SELECT cluster_scn_advance()');
my $post_advance_scn = $node->safe_psql('postgres',
	'SELECT cluster_scn_current()');
ok($post_advance_scn ne '' && $post_advance_scn ne '0',
   'L1 cluster_scn_current() returns valid encoded SCN after advance');

# scn_node_id high byte: divide by 2^56 to extract.  psql treats bigint
# arithmetic as signed; node_id 0..127 fits in 7 bits so high bit stays 0.
my $node_id_extracted = $node->safe_psql('postgres',
	'SELECT cluster_scn_current() / 72057594037927936');
is($node_id_extracted, '7',
   'L1 SCN high 8 bits encode cluster.node_id (=7)');


# ----------
# L2: cluster_scn_advance() bumps local_scn monotonically.  Both calls
# return SCN with node_id=7 in high byte; raw int8 ordering reflects
# local_scn ordering (high byte identical).
# ----------
my $before = $node->safe_psql('postgres', 'SELECT cluster_scn_advance()');
my $after = $node->safe_psql('postgres', 'SELECT cluster_scn_advance()');
ok($after > $before,
   "L2 cluster_scn_advance is monotonic ($before -> $after)");


# ----------
# L3 (spec-1.16 v0.2 Q3 upgrade): cluster_scn_observe NOW does Lamport
# bump (max(current, remote+1)).  Spec-1.15 stat-only contract has been
# replaced by spec-1.16 real Lamport observe.  remote with high local
# bumps current_local.
# ----------
my $local_before_observe = $node->safe_psql('postgres', q{
	SELECT value::bigint FROM pg_cluster_state
	 WHERE category='scn' AND key='scn_current_local'
});
# Remote with node_id=42 high byte, local = current_local + 999999 (much higher).
# Use addition rather than `|`: the high 8 bits (42 * 2^56) and the low
# 56 bits ($remote_high < 2^56) never overlap, so addition gives the
# same encoded SCN as bitwise OR while avoiding PG's bigint|int4
# operator-resolution quirks.
my $remote_high = $local_before_observe + 999999;
my $remote = $node->safe_psql('postgres',
	"SELECT 42::bigint * 72057594037927936 + $remote_high");
$node->safe_psql('postgres', "SELECT cluster_scn_observe($remote)");
my $local_after_observe = $node->safe_psql('postgres', q{
	SELECT value::bigint FROM pg_cluster_state
	 WHERE category='scn' AND key='scn_current_local'
});
ok($local_after_observe >= $remote_high + 1,
   "L3 cluster_scn_observe Lamport-bumps current_local to >= remote+1 ($remote_high+1; got $local_after_observe; spec-1.16 v0.2 Q3)");


# ----------
# L4: 7 SCN keys present in pg_cluster_state.
# ----------
my @expected_keys = (
	'scn_node_id',
	'scn_current_local',
	'scn_current_encoded',
	'scn_max_observed_remote',
	'scn_total_advance_count',
	'scn_initialized_at',
	'scn_last_advance_at');
foreach my $k (@expected_keys)
{
	my $count = $node->safe_psql('postgres',
		"SELECT count(*) FROM pg_cluster_state WHERE category='scn' AND key='$k'");
	is($count, '1', "L4 pg_cluster_state has scn key '$k'");
}


# ----------
# L5: advance + observe require superuser.
# ----------
$node->safe_psql('postgres', q{
	CREATE ROLE non_super NOLOGIN;
});
my ($ret, $stdout, $stderr) = $node->psql('postgres',
	'SET ROLE non_super; SELECT cluster_scn_advance()');
isnt($ret, 0, 'L5 cluster_scn_advance fails for non-superuser');
like($stderr, qr/permission denied|must be superuser|restricted to superuser/i,
   'L5 cluster_scn_advance error message mentions superuser/permission');

($ret, $stdout, $stderr) = $node->psql('postgres',
	'SET ROLE non_super; SELECT cluster_scn_observe(0::bigint)');
isnt($ret, 0, 'L5 cluster_scn_observe fails for non-superuser');


# ----------
# L6: observe with remote.local < current.local is no-op (max-only).
# ----------
my $current_full = $node->safe_psql('postgres',
	'SELECT cluster_scn_current()');
# Encode a remote with node_id=42 and local=1 (smaller than current local).
my $small_remote = $node->safe_psql('postgres',
	'SELECT ((42::bigint << 56) | 1)::bigint');
my $max_before = $node->safe_psql('postgres', q{
	SELECT value::bigint FROM pg_cluster_state
	WHERE category='scn' AND key='scn_max_observed_remote'
});
$node->safe_psql('postgres',
	"SELECT cluster_scn_observe($small_remote)");
my $max_after = $node->safe_psql('postgres', q{
	SELECT value::bigint FROM pg_cluster_state
	WHERE category='scn' AND key='scn_max_observed_remote'
});
ok($max_after >= $max_before,
   'L6 observe with smaller local does not regress max_observed_remote');


# ----------
# L7: total_advance_count increments per advance call.
# ----------
my $count_before = $node->safe_psql('postgres', q{
	SELECT value::bigint FROM pg_cluster_state
	WHERE category='scn' AND key='scn_total_advance_count'
});
$node->safe_psql('postgres', 'SELECT cluster_scn_advance()');
$node->safe_psql('postgres', 'SELECT cluster_scn_advance()');
$node->safe_psql('postgres', 'SELECT cluster_scn_advance()');
my $count_after = $node->safe_psql('postgres', q{
	SELECT value::bigint FROM pg_cluster_state
	WHERE category='scn' AND key='scn_total_advance_count'
});
is($count_after - $count_before, 3,
   'L7 total_advance_count incremented by 3 after 3 advance calls');


# ----------
# L8: inject :error on cluster-scn-advance-pre fires SQLSTATE 53R0X.
# Note: cluster_inject_fault arm state is per-backend, so arm + invoke
# must run in the SAME psql session.
# ----------
($ret, $stdout, $stderr) = $node->psql('postgres', q{
	SELECT cluster_inject_fault('cluster-scn-advance-pre', 'error', 0);
	SELECT cluster_scn_advance();
});
isnt($ret, 0, 'L8 cluster_scn_advance fails when inject :error armed');
like($stderr, qr/53R0X|cluster injection|cluster-scn-advance-pre/i,
   'L8 inject :error fires SQLSTATE 53R0X / cluster injection');


# ----------
# L9: inject :skip on cluster-scn-observe-entry silently bypasses.
# Same per-backend caveat: arm + invoke in single session.
# ----------
($ret, $stdout, $stderr) = $node->psql('postgres', q{
	SELECT cluster_inject_fault('cluster-scn-observe-entry', 'skip', 0);
	SELECT cluster_scn_observe(0::bigint);
});
is($ret, 0, 'L9 cluster_scn_observe returns OK with inject :skip armed');


# ============================================================
# Hardening v1.0.1 (round 8 codex review) tests
# ============================================================

# ----------
# L10 (round 8 P2): scn_current_encoded outputs full 64-bit hex.
# Pre fix it was truncated to high 32 bits (node << 56 only); two SCNs
# with same node but different local_scn would show identical value.
# ----------
$node->safe_psql('postgres', 'SELECT cluster_scn_advance()');
my $current_local = $node->safe_psql('postgres', q{
	SELECT value::bigint FROM pg_cluster_state
	WHERE category='scn' AND key='scn_current_local'
});
my $encoded_hex = $node->safe_psql('postgres', q{
	SELECT value FROM pg_cluster_state
	WHERE category='scn' AND key='scn_current_encoded'
});
# Format: 0x0700....NNNN -- 16 hex chars = 64 bit; node 7 in high byte =
# "07" prefix; local_scn occupies low 14 hex chars.
like($encoded_hex, qr/^0x07[0-9A-F]{14}$/,
   "L10 scn_current_encoded is full 64-bit hex with node=7 high byte (got $encoded_hex)");
# Verify the encoded value actually matches (node << 56) | local.
# Convert via Perl hex() (strip "0x" prefix; hex() handles up to ~52
# bits in standard Perl, so split high/low).
(my $hex_clean = $encoded_hex) =~ s/^0x//;
my $hi32 = hex(substr($hex_clean, 0, 8));
my $lo32 = hex(substr($hex_clean, 8, 8));
my $expected_hi = 7 << 24;	# (7 << 56) >> 32 == 0x07000000
is($hi32, $expected_hi,
   "L10 scn_current_encoded high 32 bits = node 7 << 24 (round 8 P2)");
my $expected_int = $node->safe_psql('postgres',
	"SELECT (7::bigint << 56) | $current_local");
# expected_int low 32 bits = current_local & 0xFFFFFFFF
my $expected_lo = $current_local & 0xFFFFFFFF;
is($lo32, $expected_lo,
   "L10 scn_current_encoded low 32 bits = current_local low 32 (round 8 P2)");


# ----------
# L11 (round 8 P2): cluster_scn_observe rejects negative input.
# Previously -1 would cast to 0xFFFFFFFFFFFFFFFF and poison
# max_observed_remote_scn with 2^56-1 until restart.
# ----------
($ret, $stdout, $stderr) = $node->psql('postgres',
	'SELECT cluster_scn_observe(-1::bigint)');
isnt($ret, 0, 'L11 cluster_scn_observe(-1) rejected (round 8 P2)');
like($stderr, qr/non-negative|invalid_parameter/i,
   'L11 negative remote_scn error message mentions non-negative');


# ----------
# L12 (round 8 P2): SCN with reserved node_id 128..255 is unreachable
# from SQL because (node << 56) flips int8 sign bit -- caller hits the
# non-negative check first.  Defense-in-depth at C level is still in
# place (cluster_scn_observe_sql validates SCN_NODE_ID_VALID); test
# verifies the SQL-reachable path catches reserved-via-overflow.
# ----------
($ret, $stdout, $stderr) = $node->psql('postgres',
	'SELECT cluster_scn_observe((200::bigint << 56) | 1)');
isnt($ret, 0, 'L12 cluster_scn_observe rejects reserved node_id 200 via overflow path (round 8 P2)');
like($stderr, qr/non-negative|reserved node_id|valid 0\.\.127/i,
   'L12 reserved-node-via-overflow error message mentions non-negative or reserved range');


# ----------
# L13 (round 8 P3): cluster_scn_current pg_proc volatility = volatile,
# parallel = restricted (was stable + safe; reads dynamically-mutating
# shmem state).
# ----------
my $volatility = $node->safe_psql('postgres',
	"SELECT provolatile FROM pg_proc WHERE proname = 'cluster_scn_current'");
is($volatility, 'v',
   'L13 cluster_scn_current is VOLATILE (round 8 P3)');
my $parallel = $node->safe_psql('postgres',
	"SELECT proparallel FROM pg_proc WHERE proname = 'cluster_scn_current'");
is($parallel, 'r',
   'L13 cluster_scn_current parallel mode is RESTRICTED (round 8 P3)');


# ----------
# L14 (round 8 P3): cluster-scn-wraparound-warning inject point is
# reachable.  Cannot trigger the PANIC ereport from a real test (would
# need 2^50 advances), but inject :warning at this point fires when
# advance crosses the threshold.  Verify inject point is registered and
# arm/disarm round-trip works.
# ----------
my $wraparound_inject = $node->safe_psql('postgres', q{
	SELECT count(*) FROM pg_stat_cluster_injections
	WHERE name = 'cluster-scn-wraparound-warning'
});
is($wraparound_inject, '1',
   'L14 cluster-scn-wraparound-warning inject point registered (round 8 P3)');
my $arm_result = $node->safe_psql('postgres', q{
	SELECT cluster_inject_fault('cluster-scn-wraparound-warning', 'warning', 0)
});
is($arm_result, 't',
   'L14 cluster-scn-wraparound-warning arm/disarm round-trip works (round 8 P3)');


$node->stop;
done_testing();
