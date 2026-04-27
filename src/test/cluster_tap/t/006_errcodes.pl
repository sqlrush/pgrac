#-------------------------------------------------------------------------
#
# 006_errcodes.pl
#    End-to-end regression for the 45 cluster SQLSTATE error codes
#    registered in stage 0.12.
#
#    Cluster errcodes are registered in src/backend/utils/errcodes.txt
#    and become available to plpgsql via PG's auto-generated
#    src/pl/plpgsql/src/plerrcodes.h.  Stage 0.12 wires no ereport()
#    call sites, so the only end-to-end runtime check we can perform is
#    raising an exception in plpgsql with a cluster_* condition name and
#    confirming the resulting SQLSTATE matches the design doc roster.
#
#    What this test proves at runtime:
#      - plerrcodes.h is generated correctly from errcodes.txt
#        (cluster_* friendly names resolve to the expected SQLSTATEs).
#      - PG-native condition names (serialization_failure, etc.) still
#        work after the pgrac sub-sections were inserted -- catches
#        accidental ordering breakage of the .txt file.
#      - Unknown cluster names produce the standard PG error
#        "unrecognized exception condition" so application retry logic
#        does not silently swallow typos.
#
# IDENTIFICATION
#    src/test/cluster_tap/t/006_errcodes.pl
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


my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;


# ----------
# Helper: raise an exception with the given condition name in plpgsql,
# and return the SQLSTATE that bubbles out.  The expected outcome is a
# 5-character string (e.g. '08R01').
# ----------
sub raise_and_get_sqlstate
{
	my ($condname) = @_;

	my $sql = <<"EOF";
DO \$\$
BEGIN
    RAISE EXCEPTION USING ERRCODE = '$condname';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'sqlstate=%', SQLSTATE;
    RETURN;
END
\$\$;
EOF

	my ($stdout, $stderr);
	$node->psql('postgres', $sql, stdout => \$stdout, stderr => \$stderr);
	if ($stderr =~ /sqlstate=([0-9A-Z]{5})/)
	{
		return $1;
	}
	return "(no sqlstate captured: $stderr)";
}


# ----------
# Helper: try to RAISE with an unknown condition name and return the
# error message PG produces (should contain "unrecognized exception
# condition").
# ----------
sub raise_unknown
{
	my ($condname) = @_;

	my $sql = "DO \$\$ BEGIN RAISE EXCEPTION USING ERRCODE = '$condname'; END \$\$;";
	my ($stdout, $stderr);
	$node->psql('postgres', $sql, stdout => \$stdout, stderr => \$stderr);
	return $stderr;
}


# ----------
# Spot-check 5 cluster condition names spanning 5 different classes.
# ----------
is(raise_and_get_sqlstate('cluster_node_unreachable'), '08R01',
	"cluster_node_unreachable -> 08R01");
is(raise_and_get_sqlstate('cluster_reconfig_abort'), '40R01',
	"cluster_reconfig_abort -> 40R01");
is(raise_and_get_sqlstate('cluster_lms_queue_full'), '53R01',
	"cluster_lms_queue_full -> 53R01");
is(raise_and_get_sqlstate('cluster_reconfig_in_progress'), '57R01',
	"cluster_reconfig_in_progress -> 57R01");
is(raise_and_get_sqlstate('cluster_shared_storage_failed'), '58R01',
	"cluster_shared_storage_failed -> 58R01");


# ----------
# PG-native condition names still work (no regression from inserting
# pgrac sub-sections into errcodes.txt).
# ----------
is(raise_and_get_sqlstate('serialization_failure'), '40001',
	"PG-native serialization_failure -> 40001 (no regression)");
is(raise_and_get_sqlstate('connection_failure'), '08006',
	"PG-native connection_failure -> 08006 (no regression)");


# ----------
# Unknown cluster name produces a clear error.
# ----------
my $err = raise_unknown('cluster_nonexistent_xyz');
like($err, qr/unrecognized exception condition/i,
	'unknown cluster_* condition produces "unrecognized exception condition"');


$node->stop;

done_testing();
