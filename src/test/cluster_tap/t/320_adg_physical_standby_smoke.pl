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
#    Coverage:
#      L1  read-only service after replay
#      L2  local ADG lag/apply view
#      L3  global ADG view row contract while ADG is enabled
#      L4  2PC PREPARE remains invisible until COMMIT PREPARED redo
#      L5  ROLLBACK PREPARED does not leak prepared changes
#      L6  standby SCN floor is monotonic and barrier-driven
#      L7  standby writes fail closed
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
cluster.dg_mode = max_availability
cluster.enable_adg = on
wal_level = replica
max_wal_senders = 5
max_replication_slots = 5
max_prepared_transactions = 10
});

$primary->start;
$primary->safe_psql('postgres', q{CREATE TABLE adg_smoke(id int primary key)});
$primary->safe_psql('postgres', q{CREATE TABLE adg_2pc(id int primary key, note text)});
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
cluster.dg_mode = max_availability
cluster.enable_adg = on
cluster.apply_master_election = off
max_prepared_transactions = 10
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
	SELECT dg_role, dg_mode, adg_enabled, mrp_status, apply_master_node_id,
	       apply_master_term > 0, standby_consistent_scn > 0, lag_bytes >= 0,
	       lag_seconds >= 0, apply_rate_bytes_per_sec >= 0
	  FROM pg_stat_cluster_adg
	},
	'standby|max_availability|t|ready|11|t|t|t|t|t',
	'L2 pg_stat_cluster_adg reports MRP lease, SCN floor, and lag metrics');

poll_psql_value(
	$standby,
	q{
	SELECT count(*), min(node_id), bool_and(dg_role = 'standby'),
	       bool_and(adg_enabled), bool_and(standby_consistent_scn > 0)
	  FROM pg_stat_gcluster_adg
	},
	'1|11|t|t|t',
	'L3 pg_stat_gcluster_adg exposes enabled ADG standby state');

my $scn_before_2pc = poll_psql_value($standby,
	q{SELECT standby_consistent_scn FROM pg_stat_cluster_adg},
	undef,
	'L6 captured standby SCN floor before prepared commit replay');

$primary->safe_psql('postgres', q{
BEGIN;
INSERT INTO adg_2pc VALUES (1, 'commit prepared');
PREPARE TRANSACTION 'adg_commit_prepared';
});
$primary->wait_for_catchup($standby);
poll_psql_value($standby, q{SELECT count(*) FROM adg_2pc}, '0',
	'L4 prepared transaction remains invisible before COMMIT PREPARED');

$primary->safe_psql('postgres', q{COMMIT PREPARED 'adg_commit_prepared'});
$primary->wait_for_catchup($standby);
poll_psql_value($standby, q{SELECT count(*) FROM adg_2pc}, '1',
	'L4 COMMIT PREPARED redo becomes visible on ADG standby');

$primary->safe_psql('postgres', q{
BEGIN;
INSERT INTO adg_2pc VALUES (2, 'rollback prepared');
PREPARE TRANSACTION 'adg_rollback_prepared';
});
$primary->wait_for_catchup($standby);
poll_psql_value($standby, q{SELECT count(*) FROM adg_2pc}, '1',
	'L5 second prepared transaction remains invisible before rollback');

$primary->safe_psql('postgres', q{ROLLBACK PREPARED 'adg_rollback_prepared'});
$primary->wait_for_catchup($standby);
poll_psql_value($standby, q{SELECT count(*) FROM adg_2pc}, '1',
	'L5 ROLLBACK PREPARED redo does not expose aborted prepared rows');

poll_psql_value(
	$standby,
	qq{
	SELECT standby_consistent_scn >= $scn_before_2pc
	       AND standby_consistent_scn > 0
	  FROM pg_stat_cluster_adg
	},
	't',
	'L6 standby SCN floor is monotonic across prepared commit replay');

my ($ret, $stdout, $stderr) = $standby->psql('postgres',
	q{CREATE TABLE adg_should_fail(id int)});
isnt($ret, 0, 'L7 ADG standby rejects writes');
like($stderr, qr/read-only transaction|cannot execute .* in a read-only transaction|recovery is in progress/i,
	'L7 standby write failure is a read-only/recovery error');

$standby->stop;
$primary->stop;

done_testing();
