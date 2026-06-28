# 316_cluster_5_13_clean_leave_preflight_failclosed.pl
#
# spec-5.13 clean-leave reconfiguration — Hardening v1.0.3 (P1).
#
#   Two cooperative-clean-leave preflight correctness properties that the
#   v1.0.2 preflight handshake did NOT enforce (both 8.A: a silent / version-
#   skewed survivor left holding stale GRD/PCM refs, or a double-drain):
#
#   Part 1 (L1-L8) — preflight fail-CLOSED.  A survivor that receives the F6
#     preflight probe but replies neither ACK nor NAK (silent: version skew /
#     IC loss / slow) must make the leaver REJECT (rejected:preflight_incomplete)
#     rather than time out and fail OPEN into the drain.  The leaver must stay
#     idle and the survivor must NOT have tracked the leave (the probe is side-
#     effect-free), so no enabled survivor drops its refs to the still-present
#     leaver.
#
#   Part 2 (L9-L13) — same-node request serialization.  Two concurrent
#     pg_cluster_clean_leave_request() on the SAME node: exactly one may
#     proceed; the second is rejected:leave_in_progress.  The unlocked
#     phase==IDLE test cannot serialize them — phase stays IDLE through the
#     multi-second preflight window (REQUESTED is set only after preflight) —
#     so without the entry reservation both could enter and double-drain.
#
# The silent survivor is modelled with the v1.0.3 injection point
# cluster-clean-leave-survivor-suppress-preflight-ack (armed :skip on the
# survivor's LMON via the cluster.injection_points GUC).
#
# Author: SqlRush <sqlrush@gmail.com>
#
# IDENTIFICATION
#     src/test/cluster_tap/t/316_cluster_5_13_clean_leave_preflight_failclosed.pl

use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterPair;
use Test::More;
use Time::HiRes qw(usleep);
use IPC::Run;

sub leave_col
{
	my ($node, $col) = @_;
	return $node->safe_psql('postgres',
		"SELECT $col FROM pg_cluster_clean_leave_state");
}

# ----------------------------------------------------------------------
# Part 1: a SILENT survivor must drive the leaver fail-CLOSED.
#   node0 = survivor (armed to drop its preflight ACK), node1 = leaver.
# ----------------------------------------------------------------------
my $pair = PostgreSQL::Test::ClusterPair->new_pair(
	'cl_v103_silent',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.clean_leave_enabled = on',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.cssd_heartbeat_interval_ms = 1000',
		'cluster.cssd_dead_deadband_factor = 8',
	]);

# Arm the SURVIVOR (node0) to stay silent on the preflight probe (neither ACK
# nor NAK).  The GUC arms it in every node0 process, including the LMON that
# dispatches the clean-leave announce handler.
$pair->node0->append_conf('postgresql.conf',
	"cluster.injection_points = 'cluster-clean-leave-survivor-suppress-preflight-ack:skip'"
);
$pair->start_pair;
usleep(3_000_000);

is($pair->node0->safe_psql('postgres', 'SELECT 1'), '1', 'L1 survivor alive');
is($pair->node1->safe_psql('postgres', 'SELECT 1'), '1', 'L1 leaver alive');
is(leave_col($pair->node1, 'phase'), 'idle', 'L2 leaver idle pre-request');

# node1's only survivor (node0) is silent on the probe, so the preflight can
# never gather all-survivor ACKs -> the request must fail CLOSED.
my $req = $pair->node1->safe_psql('postgres',
	'SELECT pg_cluster_clean_leave_request()');
is( $req, 'rejected:preflight_incomplete',
	'L3 silent survivor -> leaver fail-CLOSED rejected:preflight_incomplete (not fail-open accepted)'
);

# The leaver never entered the drain: still idle, nothing escalated.
is(leave_col($pair->node1, 'phase'), 'idle',
	'L4 leaver back to idle (no drain side effect)');
is(leave_col($pair->node1, 'escalate_count'), '0',
	'L5 leaver did not escalate to fail-stop');

# The survivor never tracked the leave (the probe is side-effect-free): it is
# not tracking any leaving node, so it never dropped its refs to node1.
is(leave_col($pair->node0, 'phase'), 'idle',
	'L6 survivor never tracked the leave (idle)');
is(leave_col($pair->node0, 'leaving_node_id'), '-1',
	'L7 survivor is not tracking any leaving node (no ref drop)');
is($pair->node1->safe_psql('postgres', 'SELECT 1'), '1',
	'L8 leaver still serving (it did not leave)');

# ----------------------------------------------------------------------
# Part 2: two concurrent same-node requests — second is rejected.
#   The leaver holds request #1 in the preflight window with a :sleep arm on
#   cluster-clean-leave-request (after the entry reservation); the survivor is
#   silenced so #1 finally aborts preflight_incomplete (no real leave).
# ----------------------------------------------------------------------
my $pair2 = PostgreSQL::Test::ClusterPair->new_pair(
	'cl_v103_concur',
	quorum_voting_disks => 3,
	shared_data         => 1,
	extra_conf          => [
		'autovacuum = off',
		'cluster.clean_leave_enabled = on',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.cssd_heartbeat_interval_ms = 1000',
		'cluster.cssd_dead_deadband_factor = 8',
	]);
$pair2->node0->append_conf('postgresql.conf',
	"cluster.injection_points = 'cluster-clean-leave-survivor-suppress-preflight-ack:skip'"
);
# Hold request #1 for 6s inside the request (AFTER it reserves the in-progress
# flag, at the cluster-clean-leave-request point) so #2 races in deterministically.
$pair2->node1->append_conf('postgresql.conf',
	"cluster.injection_points = 'cluster-clean-leave-request:sleep:6000000'");
$pair2->start_pair;
usleep(3_000_000);

# Fire request #1 asynchronously; it reserves, then sleeps ~6s holding the
# reservation, then fails preflight_incomplete (silent survivor).
my ($in1, $out1, $err1) = ('', '', '');
my $h1 = IPC::Run::start(
	[
		'psql', '-X', '-q', '-t', '-A', '-d',
		$pair2->node1->connstr('postgres'),
		'-c', 'SELECT pg_cluster_clean_leave_request()'
	],
	\$in1, \$out1, \$err1);

# Give #1 time to enter the request and reserve (well inside the 6s sleep window).
usleep(2_500_000);

# Request #2, synchronous, while #1 holds the reservation.
my $req2 = $pair2->node1->safe_psql('postgres',
	'SELECT pg_cluster_clean_leave_request()');
is( $req2, 'rejected:leave_in_progress',
	'L9 second same-node request rejected:leave_in_progress while #1 in preflight'
);
# The second request must NOT have entered a drain.
is(leave_col($pair2->node1, 'phase'), 'idle',
	'L10 node still idle after the rejected second request');

# Reap request #1 — it aborts cleanly (silent survivor) with the same fail-
# closed verdict, proving it held the reservation through preflight, not a drain.
IPC::Run::finish($h1);
$out1 =~ s/\s+$//;
is($out1, 'rejected:preflight_incomplete',
	'L11 request #1 aborted preflight_incomplete (held reservation, did not double-drain)');
is(leave_col($pair2->node1, 'phase'), 'idle',
	'L12 leaver idle after #1 aborts (no leftover drain state)');
is(leave_col($pair2->node1, 'escalate_count'), '0',
	'L13 no escalate from the concurrent attempt');

done_testing();
