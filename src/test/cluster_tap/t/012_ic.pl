#-------------------------------------------------------------------------
#
# 012_ic.pl
#    End-to-end regression for the cluster internal IPC abstraction
#    layer introduced in stage 0.18.
#
#    Stage 0.18 ships only the stub interconnect tier vtable.  The
#    high-level entry points (cluster_msg_send / cluster_rpc_call)
#    are not exposed at the SQL level yet -- exercising them requires
#    a Stage 2+ listener process to feed cluster_ic_recv_bytes.  This
#    TAP test therefore focuses on the surfaces that ARE exercisable
#    on a real PG instance:
#
#      - cluster.interconnect_tier default is 'stub'.
#      - pg_settings exposes the GUC with vartype=enum, context=
#        postmaster, and the four enum values (stub, tier1, tier2,
#        tier3).
#      - Runtime SET is rejected (PGC_POSTMASTER).
#      - Setting tier1 in postgresql.conf prevents the server from
#        starting (cluster_ic_init ereports FEATURE_NOT_SUPPORTED
#        with an errhint pointing to Stage 2).
#      - In a default build without --with-rdma, setting tier2/tier3
#        prevents startup with SQLSTATE 53R22 / RDMA-enabled build hint.
#      - After clearing the bad config, the server starts again on
#        stub.
#      - Baseline regression: spec-0.16 / 0.17 cluster views still
#        return 51 rows each (cluster_ic addition is a pure addition,
#        not a refactor).
#
# IDENTIFICATION
#    src/test/cluster_tap/t/012_ic.pl
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
use Test::More;
use PgracClusterNode;


my $node = PgracClusterNode->new('main');
$node->init;
$node->start;


# ----------
# Default value is 'stub'.  Uses spec-0.22 assert_cluster_guc helper.
# ----------
$node->assert_cluster_guc('cluster.interconnect_tier', 'stub',
	'cluster.interconnect_tier default is stub');


# ----------
# pg_settings metadata: enum / postmaster.
# ----------
my $row = $node->safe_psql(
	'postgres',
	q{SELECT setting, vartype, context
	    FROM pg_settings
	   WHERE name = 'cluster.interconnect_tier'});
is($row, "stub|enum|postmaster",
	'pg_settings rows match: setting=stub, vartype=enum, context=postmaster');


# ----------
# Enum values include stub / tier1 / tier2 / tier3.  enumvals returns
# a text[] array; cast to text and check substring containment for
# resilience against quoting / ordering details.
# ----------
my $enumvals = $node->safe_psql(
	'postgres',
	q{SELECT enumvals::text FROM pg_settings WHERE name = 'cluster.interconnect_tier'});
for my $v ('stub', 'tier1', 'tier2', 'tier3')
{
	like($enumvals, qr/\b$v\b/,
		"cluster.interconnect_tier enum includes '$v'");
}


# ----------
# Runtime SET is rejected (PGC_POSTMASTER).
# ----------
my ($stdout, $stderr);
$node->psql('postgres',
	q{SET "cluster.interconnect_tier" = 'tier1'},
	stdout => \$stdout, stderr => \$stderr);
like($stderr, qr/cannot be changed without restarting the server/i,
	'SET cluster.interconnect_tier at runtime is rejected (PGC_POSTMASTER)');


# ----------
# Baseline regression: spec-0.16 / 0.17 views unaffected.
# ----------
is($node->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_cluster_wait_events'),
	'120',
	'pg_stat_cluster_wait_events returns 120 rows (spec-7.2 LMS data-plane waits)');

is($node->safe_psql('postgres',
		'SELECT count(*) FROM pg_stat_gcluster_wait_events'),
	'120',
	'pg_stat_gcluster_wait_events returns 120 rows (spec-7.2 LMS data-plane waits)');


# ----------
# spec-2.2: tier1 is now implemented but requires pgrac.conf to declare
# self interconnect_addr.  Setting tier=tier1 without pgrac.conf fails at
# listener_bind (cluster_ic_tier1.c) -- different error message than
# pre-spec-2.2 ("tier1 not implemented" -> "cannot parse interconnect_addr").
# (cluster.enabled defaults to ON; cluster_ic_init runs unconditionally.)
# ----------
$node->stop;
$node->append_conf('postgresql.conf', "cluster.interconnect_tier = tier1\n");

# Startup must fail; capture log to verify the new errmsg is emitted.
my $start_failed = !$node->start(fail_ok => 1);
ok($start_failed,
	'server refuses to start with tier=tier1 + missing pgrac.conf (spec-2.2)');

ok(defined $node->wait_for_log_match(qr/tier1.*interconnect_addr|interconnect_addr.*""/i, 5),
	'startup failure log mentions tier1 listener_bind / interconnect_addr');


# ----------
# Switching to tier2/tier3 in a default no-RDMA build fails closed with 53R22.
# ----------
unlink $node->logfile;
$node->adjust_conf('postgresql.conf', 'cluster.interconnect_tier', 'tier2');
$start_failed = !$node->start(fail_ok => 1);
ok($start_failed,
	'server refuses to start with cluster.interconnect_tier = tier2 without --with-rdma');

ok(defined $node->wait_for_log_match(qr/53R22|RDMA-enabled build|with-rdma/i, 5),
	'startup failure for tier2 reports RDMA unavailable / --with-rdma hint');

unlink $node->logfile;
$node->adjust_conf('postgresql.conf', 'cluster.interconnect_tier', 'tier3');
$start_failed = !$node->start(fail_ok => 1);
ok($start_failed,
	'server refuses to start with cluster.interconnect_tier = tier3 without --with-rdma');

ok(defined $node->wait_for_log_match(qr/53R22|RDMA-enabled build|with-rdma|tier3/i, 5),
	'startup failure for tier3 reports RDMA unavailable / --with-rdma hint');


# ----------
# Clearing the bad config restores stub and the server starts again.
# ----------
$node->adjust_conf('postgresql.conf', 'cluster.interconnect_tier', 'stub');
$node->start;
$node->assert_cluster_guc('cluster.interconnect_tier', 'stub',
	'server recovers after reverting to stub tier');


$node->stop;

done_testing();
