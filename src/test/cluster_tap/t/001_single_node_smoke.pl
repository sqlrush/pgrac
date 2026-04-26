#-------------------------------------------------------------------------
#
# 001_single_node_smoke.pl
#    Smoke test: a single PG instance starts, accepts SQL, and the
#    postgres binary contains the pgrac cluster symbols.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/001_single_node_smoke.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    Stage 0.5 baseline: validates the TAP harness wiring.  Real
#    cluster behavior is exercised in later stages (Stage 1+).
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use Test::More;
use PgracClusterNode;


# Allocate and start a single PG instance.
my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;

# Basic SQL works.
is($node->safe_psql('postgres', 'SELECT 1'),
   '1',
   'PG basic SELECT 1 returns 1');

# PG self-reported version is 16.13.
my $version = $node->safe_psql('postgres', 'SELECT version()');
like($version,
     qr/PostgreSQL 16\.13/,
     'PG self-reports version 16.13');

# postgres binary contains the cluster symbols.
my $bindir = $node->config_data('--bindir');
my $bin = "$bindir/postgres";
my $nm_output = `nm "$bin" 2>/dev/null`;

SKIP: {
	skip "nm not available or postgres binary missing", 3 unless $nm_output;
	# nm output uses one symbol per line.  macOS prefixes symbols with
	# '_'; Linux does not.  Match the symbol followed by end-of-line so
	# we don't trip over partial matches.  '_' is a word character so
	# Perl '\b' won't help here -- use a multi-line $ anchor instead.
	like($nm_output, qr/cluster_init$/m,         'cluster_init symbol present');
	like($nm_output, qr/cluster_shutdown$/m,     'cluster_shutdown symbol present');
	like($nm_output, qr/pgrac_version_string$/m, 'pgrac_version_string symbol present');
}

$node->stop;

done_testing();
