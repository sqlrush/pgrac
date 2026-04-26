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
#-------------------------------------------------------------------------

package PgracClusterNode;

use strict;
use warnings;

use parent 'PostgreSQL::Test::Cluster';

use PostgreSQL::Test::Cluster;
use Test::More;


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


1;
