#-------------------------------------------------------------------------
#
# PgracClusterNode.pm
#    pgrac TAP test helper: extends PostgreSQL::Test::Cluster for
#    cluster-aware tests.
#
#    Provides convenience methods for spawning multiple independent PG
#    instances (the precursor to a real RAC-style cluster) and
#    asserting the presence of cluster symbols in the postgres binary.
#
# IDENTIFICATION
#    src/test/cluster_tap/lib/PgracClusterNode.pm
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    This is a pgrac-original module (no derivation from PostgreSQL).
#
#    PG 16 renamed the test cluster module from PostgresNode to
#    PostgreSQL::Test::Cluster.  This helper extends the new name.
#
#    Stage 0.5: cluster_init / cluster_shutdown remain stubs.
#    Methods here therefore start "cluster-mode" PG identically to
#    vanilla PG; per-instance configuration that activates cluster
#    behavior lands in stage 0.13+.
#
#    Stage 0.22 added three thin assertion helpers (assert_cluster_guc,
#    assert_pg_cluster_nodes_count, wait_for_log_match) that condense
#    the SHOW cluster.* / pg_cluster_nodes count / log-inspection
#    patterns recurring across cluster_tap test files.  Stage 2+
#    multi-node helpers (start_n_node_cluster, wait_for_membership,
#    fence_node, ...) land here at the same time as their first
#    multi-node spec; see specs/spec-0.22-tap-helpers.md §1.2.
#
#-------------------------------------------------------------------------

package PgracClusterNode;

use strict;
use warnings;

use parent 'PostgreSQL::Test::Cluster';

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep);


#-----------------------------------------------------------------------
# start_cluster_mode -- Start an instance configured for cluster mode.
#
#	Currently equivalent to ->start (no extra setup needed because
#	cluster_init is a stub in stage 0.5).  Provided as a stable hook
#	so test files can write `$node->start_cluster_mode` today and the
#	helper will gain real behavior in stage 0.13+ without test rewrites.
#-----------------------------------------------------------------------
sub start_cluster_mode
{
	my ($self) = @_;

	$self->start;

	# TODO (stage 0.13+):
	#   - SET cluster_enabled = on
	#   - configure cluster_node_id GUC
	#   - configure interconnect endpoint GUC

	return $self;
}


#-----------------------------------------------------------------------
# get_cluster_nodes -- Allocate N independent PG instances.
#
#	Each node gets a unique port and datadir via the standard
#	PostgreSQL::Test::Cluster mechanism.  In stage 0.5 the nodes do
#	NOT communicate with each other; this is purely a process-harness
#	test.  Real interconnect lands in Stage 2.
#
# Args (named):
#	count: number of nodes to allocate (default 2)
#
# Returns:
#	List of started PostgreSQL::Test::Cluster instances.
#-----------------------------------------------------------------------
sub get_cluster_nodes
{
	my (%args) = @_;
	my $count = $args{count} // 2;
	my @nodes;

	for my $i (1 .. $count)
	{
		my $name = "cluster_node_$i";
		my $node = PostgreSQL::Test::Cluster->new($name);
		$node->init;
		$node->start;
		push @nodes, $node;
	}

	return @nodes;
}


#-----------------------------------------------------------------------
# assert_cluster_symbols -- Verify postgres binary contains cluster symbols.
#
#	Uses `nm` to inspect the postgres binary at the given path.
#	Skips with a TAP "skip" message on platforms where nm is not
#	available; otherwise emits Test::More like() assertions for each
#	expected symbol.
#-----------------------------------------------------------------------
sub assert_cluster_symbols
{
	my ($bin) = @_;
	my @symbols = ('cluster_init', 'cluster_shutdown', 'pgrac_version_string');

	my $nm_output = `nm "$bin" 2>/dev/null`;
	if (!$nm_output)
	{
		plan skip_all => "nm not available or postgres binary not found at $bin";
		return;
	}

	for my $sym (@symbols)
	{
		like($nm_output, qr/\b$sym\b/, "symbol $sym present in postgres binary");
	}
}


#-----------------------------------------------------------------------
# assert_cluster_guc -- Run SHOW "$name" and assert the result.
#
#	Wraps the common pattern
#	    is($node->safe_psql('postgres', qq{SHOW "$name"}),
#	       $expected, $label);
#	and quotes the GUC name automatically (cluster.* contains a dot
#	which PG requires double-quoted in SHOW).
#
# Args:
#	$name:     GUC name (e.g. 'cluster.node_id')
#	$expected: expected value as string (safe_psql returns text)
#	$label:    optional Test::More label; defaults to a generated msg
#
# Returns:
#	The Test::More::is result (unused by callers; this is a sink).
#-----------------------------------------------------------------------
sub assert_cluster_guc
{
	my ($self, $name, $expected, $label) = @_;
	$label //= "cluster GUC $name = $expected";

	return is($self->safe_psql('postgres', qq{SHOW "$name"}), $expected, $label);
}


#-----------------------------------------------------------------------
# assert_pg_cluster_nodes_count -- Assert SELECT count(*) FROM pg_cluster_nodes.
#
#	Wraps the common pattern for verifying topology row count after
#	pgrac.conf changes.  The single-node degraded fallback at stage
#	0.19 means the count is 1 unless pgrac.conf has been written.
#
# Args:
#	$expected: row count as string ('1' / '2' / ...)
#	$label:    optional Test::More label; defaults to a generated msg
#-----------------------------------------------------------------------
sub assert_pg_cluster_nodes_count
{
	my ($self, $expected, $label) = @_;
	$label //= "pg_cluster_nodes returns $expected row(s)";

	return is($self->safe_psql('postgres', 'SELECT count(*) FROM pg_cluster_nodes'),
		$expected, $label);
}


#-----------------------------------------------------------------------
# wait_for_log_match -- Poll the node's logfile for a regex.
#
#	Reads $node->logfile every 250ms until $regex matches or $timeout
#	seconds elapse.  Returns the first matched substring (the value
#	of $&) on hit, or undef on timeout.  Does not call ok / fail --
#	the caller decides how to assert.
#
#	Safe to call after $node->stop (reads on-disk file, no live
#	process needed).  Stage 2+ Heartbeat / Reconfig timing tests are
#	the primary intended reader; Stage 0 use is limited to startup
#	failure logs (postmaster ereport(FATAL) -> logfile -> exit).
#
# Args:
#	$regex:   precompiled qr// regex
#	$timeout: seconds to wait (default 60)
#
# Returns:
#	Matched substring on hit; undef on timeout.
#-----------------------------------------------------------------------
sub wait_for_log_match
{
	my ($self, $regex, $timeout) = @_;
	$timeout //= 60;

	my $deadline = time() + $timeout;
	my $logfile = $self->logfile;

	while (time() < $deadline)
	{
		if (-f $logfile)
		{
			my $content = PostgreSQL::Test::Utils::slurp_file($logfile);
			if ($content =~ /$regex/)
			{
				return $&;
			}
		}
		usleep(250_000);    # 250 ms
	}
	return undef;
}


1;
