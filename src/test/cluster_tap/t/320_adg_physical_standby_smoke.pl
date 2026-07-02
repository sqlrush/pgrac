#-------------------------------------------------------------------------
#
# 320_adg_physical_standby_smoke.pl
#    spec-6.4 ADG physical standby smoke coverage.
#
#    This is intentionally a small two-postmaster physical streaming test:
#    primary emits ADG thread barriers, standby runs B_MRP with a real
#    durable apply-master lease on voting disks, replays the stream, exposes
#    pg_stat_cluster_adg, and serves read-only queries after
#    standby_consistent_scn is published.
#
#    Coverage:
#      L1  read-only service after replay
#      L1b forced cluster-path read resolves replayed regular commit durably
#      L2  local ADG lag/apply view
#      L3  global ADG view row contract while ADG is enabled
#      L4  2PC PREPARE remains invisible until COMMIT PREPARED redo
#      L4b stale 2PC overlay resolves through replayed durable TT fallback
#      L5  ROLLBACK PREPARED does not leak prepared changes
#      L6  standby SCN floor is monotonic and barrier-driven
#      L7  standby writes fail closed
#      L8  injection-only: standby TT overlay lookup tolerates primary epoch
#
# Author: SqlRush <sqlrush@gmail.com>
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use File::Path qw(make_path);
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

my $wal_threads_root = PostgreSQL::Test::Utils::tempdir();
my $standby_thread_dir = "$wal_threads_root/thread_1";
my $primary_thread_dir = "$wal_threads_root/thread_8";
my $standby_pg_wal = $standby->data_dir . '/pg_wal';
make_path($wal_threads_root);
rename $standby_pg_wal, $standby_thread_dir
  or die "rename $standby_pg_wal to $standby_thread_dir: $!";
symlink $standby_thread_dir, $standby_pg_wal
  or die "symlink $standby_pg_wal -> $standby_thread_dir: $!";
make_path($primary_thread_dir);

my $disk_dir = PostgreSQL::Test::Utils::tempdir();
my @voting_disks;
for my $i (0 .. 2)
{
	my $path = "$disk_dir/disk$i";
	open(my $fh, '>', $path) or die "open $path: $!";
	binmode $fh;
	print $fh ("\0" x 262144);
	close $fh;
	push @voting_disks, $path;
}
my $voting_disks_csv = join(',', @voting_disks);
my $primary_connstr = $primary->connstr('postgres');

$standby->append_conf('postgresql.conf', qq{
primary_slot_name = 'adg_s1'
cluster.node_id = 0
cluster.allow_single_node = on
cluster.dg_role = standby
cluster.dg_mode = max_availability
cluster.enable_adg = on
cluster.wal_threads_dir = '$wal_threads_root'
cluster.adg_rfs_conninfos = 'thread_id=8 $primary_connstr'
cluster.apply_master_election = on
cluster.voting_disks = '$voting_disks_csv'
max_prepared_transactions = 10
});
$standby->start;

$primary->wait_for_catchup($standby);

poll_psql_value(
	$standby,
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'remote file server'},
	'1',
	'L0 RFS coordinator aux process is running');

poll_psql_value(
	$standby,
	q{SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'managed recovery process'},
	'1',
	'L0 MRP coordinator aux process is running');

ok($standby->wait_for_log_match(
		qr/ADG RFS upstream 1 for thread 8 connected and streaming/, 30),
	'L0 RFS coordinator opened a thread-bound upstream stream');

$primary->safe_psql('postgres', q{INSERT INTO adg_smoke SELECT generate_series(1, 3)});
$primary->wait_for_catchup($standby);
ok(-s "$primary_thread_dir/000000010000000000000003",
	'RFS lands received WAL into the standby per-thread directory');

poll_psql_value($standby, q{SELECT count(*) FROM adg_smoke}, '3',
	'L1 ADG standby serves read-only query after physical replay');

poll_psql_value(
	$standby,
	q{
	SET cluster.cr_gate_no_peer_fastpath = off;
	SELECT count(*) FROM adg_smoke
	},
	'3',
	'L1b forced standby cluster path resolves replayed regular commit durably');

poll_psql_value(
	$standby,
	q{
	SELECT dg_role, dg_mode, adg_enabled, mrp_status, apply_master_node_id,
	       apply_master_term > 0, receive_lsn IS NOT NULL,
	       apply_lsn IS NOT NULL, standby_consistent_scn > 0, lag_bytes >= 0,
	       lag_seconds >= 0, apply_rate_bytes_per_sec >= 0
	  FROM pg_stat_cluster_adg
	},
	'standby|max_availability|t|ready|0|t|t|t|t|t|t|t',
	'L2 pg_stat_cluster_adg reports MRP lease, SCN floor, and lag metrics');

poll_psql_value(
	$standby,
	q{
	SELECT count(*), min(node_id), bool_and(dg_role = 'standby'),
	       bool_and(adg_enabled), bool_and(standby_consistent_scn > 0)
	  FROM pg_stat_gcluster_adg
	},
	'1|0|t|t|t',
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

$standby->append_conf('postgresql.conf', "cluster.tt_status_overlay_ttl_ms = 1000\n");
$standby->reload;
select(undef, undef, undef, 1.25);
poll_psql_value(
	$standby,
	q{
	SET cluster.cr_gate_no_peer_fastpath = off;
	SELECT count(*) FROM adg_2pc
	},
	'1',
	'L4b stale 2PC overlay falls back to replayed durable TT on standby');
$standby->append_conf('postgresql.conf', "cluster.tt_status_overlay_ttl_ms = 30000\n");
$standby->reload;

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

my $injection_enabled = $standby->safe_psql('postgres', q{
	SELECT count(*) FROM pg_settings
	 WHERE name = 'cluster_test_force_visibility_cluster_path'
});

SKIP: {
	skip "ENABLE_INJECTION not configured (production build)", 5
		unless $injection_enabled == 1;

	$primary->safe_psql('postgres', q{
		CREATE TABLE adg_epoch_visibility(id int primary key, note text);
		INSERT INTO adg_epoch_visibility VALUES (1, 'primary epoch overlay');
	});
	$primary->wait_for_catchup($standby);

	my $epoch_xid = trim($standby->safe_psql('postgres',
		q{SELECT xmin::text::int FROM adg_epoch_visibility WHERE id = 1}));
	like($epoch_xid, qr/^\d+$/, 'L8 captured replayed tuple xid on standby');

	my ($inj_ret, $inj_stdout, $inj_stderr) = $standby->psql('postgres', qq{
		SELECT cluster_test_inject_visibility_tt_ref(
			'$epoch_xid'::xid, 7, 3, 77, 42, 1::int8, false)
	});
	is($inj_ret, 0,
		'L8 installs TT overlay whose record epoch differs from standby live epoch');
	unlike($inj_stderr, qr/53R97|cluster TT status unknown|overlay install verification failed/,
		'L8 install verification did not fail closed on epoch mismatch');

	$standby->append_conf('postgresql.conf',
		"cluster_test_force_visibility_cluster_path = on\n");
	$standby->reload;

	my ($vis_ret, $vis_stdout, $vis_stderr) = $standby->psql('postgres',
		q{SELECT count(*) FROM adg_epoch_visibility});
	chomp $vis_stdout;
	is($vis_ret, 0,
		'L8 forced standby visibility path accepts primary-epoch overlay');
	is($vis_stdout, '1',
		'L8 forced standby visibility path returns the committed row');
}

$standby->stop;
$primary->stop;

done_testing();
