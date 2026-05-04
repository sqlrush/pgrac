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
# L1: cluster_scn_current() returns a valid SCN with node_id=7 in high
# 8 bits.  Encoded value = (7 << 56) | local_scn.
# ----------
my $current_scn = $node->safe_psql('postgres',
	'SELECT cluster_scn_current()');
ok($current_scn ne '' && $current_scn ne '0',
   'L1 cluster_scn_current() returns non-zero SCN');

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
# L3: cluster_scn_observe is stat-only; local_scn does NOT bump.
# Use a synthetic remote SCN with node_id=42 + local_scn = current_local + 1000.
# ----------
my $local_before_observe = $node->safe_psql('postgres',
	'SELECT cluster_scn_current()');
# Remote with node_id=42 high byte, local = 999999.
my $remote = $node->safe_psql('postgres',
	'SELECT ((42::bigint << 56) | 999999)::bigint');
$node->safe_psql('postgres',
	"SELECT cluster_scn_observe($remote)");
my $local_after_observe = $node->safe_psql('postgres',
	'SELECT cluster_scn_current()');
is($local_after_observe, $local_before_observe,
   'L3 cluster_scn_observe is stat-only; current SCN does not bump (Q4+L5 contract)');


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


$node->stop;
done_testing();
