#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 424_wal_retention_reuse.pl
#    RF-ROOT P6 / STOP05 production WAL-retention witnesses.
#
# The fixture helpers emit only canonical production-format carrier bytes.
# Authority is exercised by the server under its real startup, root, and GES
# paths; no SQL mutator or synthetic production grant is installed here.
#
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use Cwd qw(abs_path);
use Digest::SHA qw(sha256_hex);
use File::Copy qw(copy);
use File::Path qw(make_path remove_tree);
use File::Temp qw(tempdir);
use FindBin;
use IPC::Run ();
use Test::More;

use lib "$FindBin::RealBin/../lib";
use PgracClusterNode;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::RecursiveCopy;
use PostgreSQL::Test::ClusterVotingDisk qw(format_voting_file);

my $root = abs_path("$FindBin::RealBin/../../../..");
my $build_root = $ENV{top_builddir} // $root;
my $unit_dir = "$build_root/src/test/cluster_unit";
my $wal_fixture = "$unit_dir/test_cluster_wal_retention";
my $root_fixture = "$unit_dir/test_cluster_control_root";
my ($build_out, $build_err) = ('', '');

# macOS strips DYLD_* while launching the system prove/perl.  Restore the
# temp-install library root before PostgreSQL::Test::Cluster spawns binaries.
$ENV{DYLD_LIBRARY_PATH} = "$build_root/tmp_install/usr/local/pgsql/lib"
	if $^O eq 'darwin';

IPC::Run::run(
	[ 'make', '-C', $unit_dir, 'test_cluster_wal_retention',
	  'test_cluster_control_root' ],
	'>', \$build_out, '2>', \$build_err)
	or BAIL_OUT("cannot build STOP05 WAL fixture helper: $build_err");

my $fixture_dir = tempdir(CLEANUP => 1);
my $wal_path = "$fixture_dir/000000010000000000000003";
my ($fixture_out, $fixture_err) = ('', '');
my $fixture_ok = IPC::Run::run(
	[ $wal_fixture, '--fixture-wal', $wal_path,
	  '1234605616436508552', '1', '1', '3', '16777216' ],
	'>', \$fixture_out, '2>', \$fixture_err);

ok($fixture_ok && -f $wal_path && -s $wal_path == 16 * 1024 * 1024,
	'T05 fixture emits one exact canonical production-format WAL carrier')
	or diag($fixture_out . $fixture_err);

my $wal_root = abs_path(tempdir(CLEANUP => 1));
my $shared_root = abs_path(tempdir(CLEANUP => 1));
my $voting_root = abs_path(tempdir(CLEANUP => 1));
my @voting_disks;
for my $disk_index (0 .. 2)
{
	my $disk_path = "$voting_root/disk$disk_index";
	format_voting_file($disk_path, $disk_index);
	push @voting_disks, $disk_path;
}
my $voting_csv = join(',', @voting_disks);
make_path("$shared_root/global");
my $ic_port = PostgreSQL::Test::Cluster::get_free_port();
my $data_port = PostgreSQL::Test::Cluster::get_free_port();
my $node = PgracClusterNode->new('wal_retention_root_fixture');
$node->init(extra => [ '-X', "$wal_root/thread_1",
	"--pgrac-wal-state-root=$wal_root" ]);
my $empty_wal_state = "$fixture_dir/pgrac_wal_state.empty";
copy("$wal_root/pgrac_wal_state", $empty_wal_state)
	or BAIL_OUT("cannot preserve the initdb-empty WAL registry: $!");
$node->append_conf('postgresql.conf',
	  "cluster.enabled = on\n"
	. "cluster.node_id = 0\n"
	. "cluster.allow_single_node = off\n"
	. "cluster.interconnect_tier = tier1\n"
	. "cluster.lms_workers = 1\n"
	. "cluster.voting_disks = '$voting_csv'\n"
	. "cluster.wal_threads_dir = '$wal_root'\n"
	. "cluster.shared_storage_backend = cluster_fs\n"
	. "cluster.shared_data_dir = '$shared_root'\n"
	. "cluster.shared_storage_uuid = '00112233445566778899aabbccddeeff'\n"
	. "cluster.controlfile_shared_authority = on\n"
	. "cluster.smgr_user_relations = on\n"
	. "cluster.cluster_stats_main_loop_interval = '500ms'\n"
	. "autovacuum = off\n");
PostgreSQL::Test::Utils::append_to_file(
	$node->data_dir . '/pgrac.conf',
	"[cluster]\nname = wal_retention_root_fixture\n\n"
	. "[node.0]\ninterconnect_addr = 127.0.0.1:$ic_port\n"
	. "data_addr = 127.0.0.1:$data_port\n");
my ($controldata_out, $controldata_err) = ('', '');
IPC::Run::run(
	[ 'pg_controldata', $node->data_dir ],
	'>', \$controldata_out, '2>', \$controldata_err)
	or BAIL_OUT("cannot read fixture system identifier: $controldata_err");
$controldata_out =~ /^Database system identifier:\s*(\d+)\s*$/m
	or BAIL_OUT('pg_controldata omitted the system identifier');
my $system_identifier = $1;
my $storage_uuid = '00112233445566778899aabbccddeeff';

($fixture_out, $fixture_err) = ('', '');
$fixture_ok = IPC::Run::run(
	[ $root_fixture, '--fixture-root', $shared_root, $wal_root,
	  $system_identifier, $storage_uuid, 'OPEN', '1',
	  '16777216', '33554432' ],
	'>', \$fixture_out, '2>', \$fixture_err);
ok($fixture_ok
	&& -f "$shared_root/global/pgrac_control_root"
	&& -s "$shared_root/global/pgrac_control_root" == 66048
	&& -f "$shared_root/global/pgrac_control_root.bak"
	&& -s "$shared_root/global/pgrac_control_root.bak" == 66048,
	'T05 fixture binds a canonical active root to real wal-state, claim, sysid, and storage UUID')
	or diag($fixture_out . $fixture_err);

my $root_boot_ok = $node->start(fail_ok => 1);
ok($root_boot_ok,
	'T05 production startup accepts the active root and forms live authority');
if ($root_boot_ok)
{
	my $steady_member_ok = $node->poll_query_until('postgres', q{
		SELECT state = 'member'
			AND presented_incarnation <> 0
			AND presented_incarnation = last_admitted_incarnation
		FROM pg_cluster_membership
		WHERE node_id = 0
	});
	ok($steady_member_ok,
		'T05 foundation reaches exact steady MEMBER authority before clean shutdown');
	my $shutdown_log_offset = -s $node->logfile;
	$node->stop;
	my $shutdown_log = PostgreSQL::Test::Utils::slurp_file(
		$node->logfile, $shutdown_log_offset);
	unlike($shutdown_log,
		qr/PANIC:|terminated by signal 6|abnormal database system shutdown/,
		'T05 foundation shutdown checkpoint exits cleanly without masked PANIC');
}

# Build a real TLI1 -> TLI2 archive-recovery carrier.  The source remains a
# plain PostgreSQL node: the restore targets below add the strict cluster
# authority, external WAL thread, voting disks, and control root independently.
my $timeline_source = PostgreSQL::Test::Cluster->new(
	'wal_retention_timeline_source');
$timeline_source->init(allows_streaming => 1, has_archiving => 1);
$timeline_source->append_conf('postgresql.conf', "wal_keep_size = '512MB'\n");
$timeline_source->start;
my $timeline_backup = 'wal_retention_timeline_backup';
$timeline_source->backup($timeline_backup);
$timeline_source->set_standby_mode;
$timeline_source->restart;
$timeline_source->promote;
$timeline_source->safe_psql('postgres',
	q{CHECKPOINT; SELECT pg_switch_wal(); CHECKPOINT; SELECT pg_switch_wal();});
$timeline_source->stop;

opendir(my $archive_dh, $timeline_source->archive_dir)
	or BAIL_OUT("cannot inspect timeline source archive: $!");
my @archive_names = readdir($archive_dh);
closedir($archive_dh);
ok((grep { $_ eq '00000002.history' } @archive_names)
	&& (grep { /^00000002[0-9A-F]{16}$/ } @archive_names),
	'T05 source archive carries a real TLI1-to-TLI2 replay transition');

my $make_restore_fixture = sub {
	my ($name, $root_start_lsn, $root_tail_lsn) = @_;
	my $restore_wal_root = abs_path(tempdir(CLEANUP => 1));
	my $restore_thread_dir = "$restore_wal_root/thread_1";
	my $restore_shared_root = abs_path(tempdir(CLEANUP => 1));
	my $restore_voting_root = abs_path(tempdir(CLEANUP => 1));
	make_path("$restore_shared_root/global");
	my @restore_voting_disks;
	for my $disk_index (0 .. 2)
	{
		my $disk_path = "$restore_voting_root/disk$disk_index";
		format_voting_file($disk_path, $disk_index);
		push @restore_voting_disks, $disk_path;
	}
	my $restore_voting_csv = join(',', @restore_voting_disks);

	my $bind_wal_thread = sub {
		my ($target) = @_;
		remove_tree($restore_thread_dir) if -e $restore_thread_dir;
		PostgreSQL::Test::RecursiveCopy::copypath(
			$target->data_dir . '/pg_wal', $restore_thread_dir);
		remove_tree($target->data_dir . '/pg_wal');
		symlink($restore_thread_dir, $target->data_dir . '/pg_wal')
			or BAIL_OUT("cannot bind restore pg_wal to thread_1: $!");
	};
	my $configure_restore = sub {
		my ($target, $target_timeline) = @_;
		my $target_ic_port = PostgreSQL::Test::Cluster::get_free_port();
		my $target_data_port = PostgreSQL::Test::Cluster::get_free_port();

		$target->append_conf('postgresql.conf',
			  "cluster.enabled = on\n"
			. "cluster.node_id = 0\n"
			. "cluster.allow_single_node = off\n"
			. "cluster.interconnect_tier = tier1\n"
			. "cluster.lms_workers = 1\n"
			. "cluster.voting_disks = '$restore_voting_csv'\n"
			. "cluster.wal_threads_dir = '$restore_wal_root'\n"
			. "cluster.shared_storage_backend = cluster_fs\n"
			. "cluster.shared_data_dir = '$restore_shared_root'\n"
			. "cluster.shared_storage_uuid = '$storage_uuid'\n"
			. "cluster.controlfile_shared_authority = on\n"
			. "cluster.smgr_user_relations = on\n"
			. "cluster.cluster_stats_main_loop_interval = '500ms'\n"
			. "recovery_target_timeline = '$target_timeline'\n"
			. "recovery_target_action = 'promote'\n"
			. "wal_retrieve_retry_interval = '100ms'\n"
			. "wal_recycle = off\n"
			. "log_error_verbosity = verbose\n"
			. "autovacuum = off\n");
		PostgreSQL::Test::Utils::append_to_file(
			$target->data_dir . '/pgrac.conf',
			"[cluster]\nname = $name\n\n"
			. "[node.0]\ninterconnect_addr = 127.0.0.1:$target_ic_port\n"
			. "data_addr = 127.0.0.1:$target_data_port\n");
	};
	my $read_system_identifier = sub {
		my ($target) = @_;
		my ($control_out, $control_err) = ('', '');

		IPC::Run::run(
			[ 'pg_controldata', $target->data_dir ],
			'>', \$control_out, '2>', \$control_err)
			or BAIL_OUT("cannot read restore system identifier: $control_err");
		$control_out =~ /^Database system identifier:\s*(\d+)\s*$/m
			or BAIL_OUT('restore pg_controldata omitted the system identifier');
		return $1;
	};
	my $install_root = sub {
		my ($target_system_identifier) = @_;

		($fixture_out, $fixture_err) = ('', '');
		$fixture_ok = IPC::Run::run(
			[ $root_fixture, '--fixture-root', $restore_shared_root,
			  $restore_wal_root, $target_system_identifier, $storage_uuid,
			  'OPEN', '1', $root_start_lsn, $root_tail_lsn ],
			'>', \$fixture_out, '2>', \$fixture_err);
		BAIL_OUT("cannot install restore control root: $fixture_out$fixture_err")
			unless $fixture_ok;
	};

	# A new empty voting set has no durable formation and must be rejected as
	# FORMATION_STALE.  Establish the exact same node/storage authority first
	# through a real production boot on TLI1, then restore the immutable backup
	# against that durable formation to exercise the E2 root classifier itself.
	my $formation_seed = PgracClusterNode->new("${name}_formation_seed");
	$formation_seed->init_from_backup(
		$timeline_source, $timeline_backup,
		standby => 0,
		has_restoring => 1);
	$bind_wal_thread->($formation_seed);
	copy($empty_wal_state, "$restore_wal_root/pgrac_wal_state")
		or BAIL_OUT("cannot install initdb-empty restore WAL registry: $!");
	$configure_restore->($formation_seed, 'current');
	# The seed exists only to establish the durable voting formation.  Do not
	# publish its promotion history back into the source archive, which must
	# remain the immutable TLI1 -> TLI2 replay carrier for the actual restore.
	$formation_seed->append_conf('postgresql.conf', "archive_mode = off\n");
	my $restore_system_identifier = $read_system_identifier->($formation_seed);
	$install_root->($restore_system_identifier);
	$formation_seed->start;
	$formation_seed->poll_query_until('postgres', q{
		SELECT state = 'member'
			AND presented_incarnation <> 0
			AND presented_incarnation = last_admitted_incarnation
		FROM pg_cluster_membership
		WHERE node_id = 0
	}) or BAIL_OUT('formation seed did not reach exact steady MEMBER authority');
	$formation_seed->stop;

	my $restore = PgracClusterNode->new($name);
	$restore->init_from_backup(
		$timeline_source, $timeline_backup,
		standby => 0,
		has_restoring => 1);
	$bind_wal_thread->($restore);
	$configure_restore->($restore, 'latest');
	my $actual_system_identifier = $read_system_identifier->($restore);
	BAIL_OUT('formation seed and restore target have different system identifiers')
		unless $actual_system_identifier eq $restore_system_identifier;
	# The seed's clean shutdown populated the registry slot.  The fixture
	# installer deliberately accepts only an empty registry before publishing
	# its exact STOPPED source, so restore that initialization image while
	# retaining the independently durable voting-disk formation.
	copy($empty_wal_state, "$restore_wal_root/pgrac_wal_state")
		or BAIL_OUT("cannot reset restore WAL registry before root install: $!");
	$install_root->($actual_system_identifier);

	my $candidate_segno = 255;
	my $candidate_path = "$restore_thread_dir/0000000100000000000000FF";
	($fixture_out, $fixture_err) = ('', '');
	$fixture_ok = IPC::Run::run(
		[ $wal_fixture, '--fixture-wal', $candidate_path,
		  $actual_system_identifier, '1', '1', $candidate_segno,
		  '16777216' ],
		'>', \$fixture_out, '2>', \$fixture_err);
	BAIL_OUT("cannot install restore WAL candidate: $fixture_out$fixture_err")
		unless $fixture_ok;

	return ($restore, $candidate_path);
};

my $candidate_start_lsn = 255 * 16 * 1024 * 1024;
my $candidate_tail_lsn = 256 * 16 * 1024 * 1024;
my ($negative_restore, $negative_candidate) = $make_restore_fixture->(
	'wal_retention_e2_negative', $candidate_start_lsn, $candidate_tail_lsn);
my $negative_before = sha256_hex(
	PostgreSQL::Test::Utils::slurp_file($negative_candidate));
my $negative_started = $negative_restore->start(fail_ok => 1);
my $negative_log = PostgreSQL::Test::Utils::slurp_file(
	$negative_restore->logfile);
ok(!$negative_started
	&& $negative_log =~ /FATAL:\s+53RBA: cluster WAL retention blocks timeline cleanup/s
	&& $negative_log =~ /Entry 2, thread 1, timeline 1, segment 255, deny reason 5\./s,
	'T05 E2 real timeline switch rejects an intersecting retained root with exact 53RBA')
	or diag($negative_log);
$negative_restore->stop('immediate') if $negative_started;
ok(-f $negative_candidate
	&& -s $negative_candidate == 16 * 1024 * 1024
	&& sha256_hex(PostgreSQL::Test::Utils::slurp_file($negative_candidate))
		eq $negative_before,
	'T05 E2 denial preserves the exact candidate bytes with zero mutation');

done_testing();
