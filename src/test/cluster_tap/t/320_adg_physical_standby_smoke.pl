#-------------------------------------------------------------------------
#
# 320_adg_physical_standby_smoke.pl
#    spec-6.4 ADG physical standby smoke coverage.
#
#    This is intentionally a small two-postmaster physical streaming test:
#    primary emits ADG thread barriers, standby runs B_MRP with
#    cluster.apply_master_election=off (single-node test lease), replays the
#    stream, exposes pg_stat_cluster_adg, and serves read-only queries after
#    standby_consistent_scn is published.
#
# Author: SqlRush <sqlrush@gmail.com>
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Utils;
use Test::More;
use PgracClusterNode;

sub trim
{
	my ($s) = @_;
	$s =~ s/\s+\z//;
	return $s;
}

sub poll_psql_value
{
	my ($node, $sql, $expected, $label) = @_;

	for my $try (1 .. 80)
	{
		my ($ret, $stdout, $stderr) = $node->psql('postgres', $sql);
		my $out = trim($stdout);

		if ($ret == 0 && (!defined $expected || $out eq $expected))
		{
			pass($label);
			return $out;
		}

		select(undef, undef, undef, 0.25);
	}

	fail($label);
	return undef;
}

my $primary = PgracClusterNode->new('adg_primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', qq{
cluster.node_id = 7
cluster.allow_single_node = on
cluster.dg_role = primary
cluster.enable_adg = on
wal_level = replica
max_wal_senders = 5
max_replication_slots = 5
});

$primary->start;
$primary->safe_psql('postgres', q{CREATE TABLE adg_smoke(id int primary key)});
$primary->safe_psql('postgres', q{SELECT pg_create_physical_replication_slot('adg_s1')});

my $backup_name = 'adg_b1';
$primary->backup($backup_name);

my $standby = PgracClusterNode->new('adg_standby');
$standby->init_from_backup($primary, $backup_name, has_streaming => 1);
$standby->append_conf('postgresql.conf', qq{
primary_slot_name = 'adg_s1'
cluster.node_id = 11
cluster.allow_single_node = on
cluster.dg_role = standby
cluster.enable_adg = on
cluster.apply_master_election = off
});
$standby->start;

$primary->wait_for_catchup($standby);

$primary->safe_psql('postgres', q{INSERT INTO adg_smoke SELECT generate_series(1, 3)});
$primary->wait_for_catchup($standby);

poll_psql_value($standby, q{SELECT count(*) FROM adg_smoke}, '3',
	'L1 ADG standby serves read-only query after physical replay');

poll_psql_value(
	$standby,
	q{
	SELECT dg_role, adg_enabled, mrp_status, apply_master_node_id,
	       apply_master_term > 0, standby_consistent_scn > 0, lag_bytes >= 0
	  FROM pg_stat_cluster_adg
	},
	'standby|t|ready|11|t|t|t',
	'L2 pg_stat_cluster_adg reports standby MRP lease and SCN floor');

my ($ret, $stdout, $stderr) = $standby->psql('postgres',
	q{CREATE TABLE adg_should_fail(id int)});
isnt($ret, 0, 'L3 ADG standby rejects writes');
like($stderr, qr/read-only transaction|cannot execute .* in a read-only transaction|recovery is in progress/i,
	'L3 standby write failure is a read-only/recovery error');

$standby->stop;
$primary->stop;

done_testing();
