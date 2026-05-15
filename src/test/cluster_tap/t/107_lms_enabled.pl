#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 107_lms_enabled.pl
#	  spec-2.20 regression smoke for cluster.lms_enabled.
#
#	  Verifies the startup-only LMS ownership fallback:
#	    L1 default cluster.lms_enabled=on exposes one LMS backend
#	    L2 cluster.lms_enabled=off is visible as a postmaster bool GUC
#	    L3 cluster.lms_enabled=off keeps LMS un-forked after steady state
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


my $node_on = PgracClusterNode->new('lms_on');
$node_on->init;
$node_on->append_conf('postgresql.conf', "cluster.node_id = 0\n");
$node_on->start;

is($node_on->safe_psql('postgres', q{SHOW "cluster.lms_enabled"}),
   'on',
   'L1 cluster.lms_enabled default is on');

ok($node_on->poll_query_until(
	'postgres',
	q{SELECT count(*) = 1 FROM pg_stat_activity WHERE backend_type = 'lms'}),
   'L1a LMS aux process visible when cluster.lms_enabled=on');

$node_on->stop;


my $node_off = PgracClusterNode->new('lms_off');
$node_off->init;
$node_off->append_conf('postgresql.conf',
	"cluster.node_id = 0\ncluster.lms_enabled = off\n");
$node_off->start;

my $lms_guc_meta = $node_off->safe_psql('postgres', q{
	SELECT setting, vartype, context
	  FROM pg_settings
	 WHERE name = 'cluster.lms_enabled'});
is($lms_guc_meta, 'off|bool|postmaster',
   'L2 cluster.lms_enabled=off visible in pg_settings as postmaster bool');

# Give ServerLoop a chance to run its respawn branch; it must still not fork LMS.
sleep 2;

is($node_off->safe_psql(
	'postgres',
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'lms'}),
   '0',
   'L3 cluster.lms_enabled=off keeps LMS aux process un-forked');

$node_off->stop;

done_testing();
