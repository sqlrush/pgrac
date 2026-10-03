#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 430_pcm_grd_resource_reuse_4node.pl
#    Four-node bounded PCM directory reuse under a wide point-update set.
#
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use IPC::Run qw(start finish timeout);
use PostgreSQL::Test::ClusterQuad;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(time usleep);

my $pcm_capacity = 128;
my $shared_buffer_count = 64;
my $working_set = 4 * $pcm_capacity;
my $rows_per_node_round = 32;
my $rounds = $working_set / (4 * $rows_per_node_round);
my $pgrd_voting_file_bytes = (8 * 128 + 3) * 512;

sub state_int
{
	my ($node, $category, $key) = @_;
	my $value = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='$category' AND key='$key'});
	die "missing or non-integer pg_cluster_state key $category.$key: ["
		. (defined($value) ? $value : '<undef>') . "]"
		unless defined($value) && $value =~ /\A\d+\z/;
	return $value + 0;
}

sub write_file
{
	my ($path, $contents) = @_;
	open(my $fh, '>', $path) or die "open $path: $!";
	print {$fh} $contents;
	close($fh) or die "close $path: $!";
}

sub activate_semantic_round
{
	my ($node, $label) = @_;
	my $deadline = time() + 60;
	my ($last_rc, $last_out, $last_err);

	while (time() < $deadline)
	{
		my ($rc, $out, $err) = $node->psql('postgres',
			'ALTER SYSTEM ENABLE RAC TWO_STAGE ROLLING UPDATES ALL',
			timeout => 45);
		($last_rc, $last_out, $last_err) = ($rc, $out, $err);
		return if defined($rc) && $rc == 0;
		die "$label activation returned an unknown timeout outcome: "
			. ($err // '<undef>') unless defined($rc);
		die "$label activation failed outside the retry contract: "
			. ($err // '<undef>')
			unless defined($err)
			&& $err =~ /(?:RF_DEFERRED|CONDITION_NOT_YET_MET|activation request was refused)/;
		usleep(100_000);
	}
	die "$label activation did not reach OPEN_APPLIED: rc="
		. (defined($last_rc) ? $last_rc : '<undef>')
		. ' stdout=[' . ($last_out // '') . '] stderr=['
		. ($last_err // '') . ']';
}

sub wait_for_protocol_drain
{
	my ($quad, $seconds) = @_;
	my $deadline = time() + $seconds;
	my @last;

	do
	{
		@last = ();
		my $drained = 1;
		for my $i (0 .. 3)
		{
			my $node = $quad->node($i);
			my %debt = (
				entry_waiters => state_int($node, 'pcm', 'pcm_grd_wait_refcount'),
				entry_transport => state_int($node, 'pcm', 'pcm_grd_transport_refcount'),
				retained => state_int($node, 'pcm', 'resource_x_retained_debt_count'),
				active_rx => state_int($node, 'pcm', 'resource_x_active_debt_count'),
				local_owner => state_int($node, 'pcm', 'resource_x_local_owner_debt_count'),
				evicting => state_int($node, 'pcm', 'resource_x_evicting_debt_count'),
				invalid => state_int($node, 'pcm', 'resource_x_invalid_debt_count'),
				convert => state_int($node, 'pcm', 'convert_queue_active'),
				outstanding => state_int($node, 'gcs', 'outstanding_count'),
				waiters => state_int($node, 'lmd', 'wait_edge_count'));
			my %residency = (
				live => state_int($node, 'pcm', 'pcm_grd_live_entries'),
				s => state_int($node, 'pcm', 'master_state_s_count'),
				x => state_int($node, 'pcm', 'master_state_x_count'),
				pi => state_int($node, 'pcm', 'pi_holders_total_count'));
			push @last, { debt => \%debt, residency => \%residency };
			$drained = 0 if grep { $_ != 0 } values(%debt);
		}
		return (1, \@last) if $drained;
		usleep(250_000);
	} while (time() < $deadline);
	return (0, \@last);
}

my $quad = PostgreSQL::Test::ClusterQuad->new_quad(
	'pcm_grd_reuse',
	quorum_voting_disks => 3,
	shared_data => 1,
	shared_system_identifier => 1,
	extra_conf => [
		'shared_buffers = 512kB',
		'autovacuum = off',
		'cluster.read_scache = on',
		'cluster.online_join = on',
		'cluster.quorum_poll_interval_ms = 500',
		'cluster.join_convergence_timeout_ms = 30000',
		'cluster.xid_striping = on',
		'cluster.crossnode_runtime_visibility = on',
		'cluster.page_scn_shortcut = on',
		'cluster.past_image = on',
		'cluster.crossnode_write_write = on',
		'cluster.undo_gcs_coherence = on',
		'cluster.crossnode_cr_data_plane = on',
		'cluster.gcs_reply_timeout_ms = 3000',
		'cluster.gcs_block_retransmit_max_retries = 8',
		'cluster.cssd_heartbeat_interval_ms = 2000',
		'cluster.cssd_dead_deadband_factor = 10',
		"cluster.pcm_grd_max_entries = $pcm_capacity",
	]);

my @voting_disks = $quad->voting_disk_paths;
die "expected exactly three voting disks\n" unless @voting_disks == 3;
for my $path (@voting_disks)
{
	truncate($path, $pgrd_voting_file_bytes)
		or die "extend $path to PGRD minimum: $!";
}
for my $node ($quad->nodes)
{
	$node->append_conf('postgresql.conf',
		"cluster.voting_disk_size_bytes = $pgrd_voting_file_bytes\n");
}

$quad->start_quad;
usleep(3_000_000);
for my $from (0 .. 3)
{
	is($quad->node($from)->safe_psql('postgres', 'SELECT 1'), '1',
		"node$from is alive");
	for my $to (0 .. 3)
	{
		next if $from == $to;
		ok($quad->wait_for_peer_state($from, $to, 'connected', 45),
			"node$from sees node$to connected");
	}
}

my $pgrd_root = $quad->shared_data_root . '/pg_undo';
my $pgrd_mirror = "$pgrd_root/pgrac_undo_root.control";
mkdir $pgrd_root or die "mkdir $pgrd_root: $!";
for my $node ($quad->nodes)
{
	$node->poll_query_until('postgres',
		q{SELECT in_quorum FROM pg_cluster_quorum_state}, 't')
		or die "voting-disk majority did not become current\n";
	my ($rc, $out, $err);
	my $deadline = time() + 15;
	while (time() < $deadline)
	{
		($rc, $out, $err) = $node->psql('postgres',
			'ALTER SYSTEM ENABLE RAC TWO_STAGE ROLLING UPDATES ALL',
			timeout => 30);
		last if defined($rc) && $rc != 0 && defined($err)
			&& $err =~ /(?:RF_DEFERRED|CONDITION_NOT_YET_MET)/;
		usleep(100_000);
	}
	die "pre-OPEN PGRD setup did not remain deferred: "
		. ($err // '<undef>')
		unless defined($rc) && $rc != 0 && defined($err)
		&& $err =~ /(?:RF_DEFERRED|CONDITION_NOT_YET_MET)/;
}
ok(-f $pgrd_mirror, 'pre-OPEN PGRD mirror is present');

activate_semantic_round($quad->node0, 'R4 bit0');
activate_semantic_round($quad->node0, 'Resource-X bit10');
for my $i (0 .. 3)
{
	is($quad->node($i)->safe_psql('postgres', q{
		SELECT setting FROM pg_settings WHERE name='shared_buffers'}),
		"$shared_buffer_count",
		"node$i cache replacement starts before PCM directory exhaustion");
	is($quad->node($i)->safe_psql('postgres', q{
		SELECT value FROM pg_cluster_state
		WHERE category='pcm' AND key='resource_x_gate_phase'}),
		'open', "node$i Resource-X gate is open");
	is(state_int($quad->node($i), 'pcm', 'pcm_grd_max_entries'),
		$pcm_capacity, "node$i uses the bounded PCM directory");
}

my @ddl_nodes = ($ENV{PGRAC_TEST_TWO_STAGE_VOTING_LOOP} // '') eq '1'
  ? ($quad->node0)
  : $quad->nodes;
for my $node (@ddl_nodes)
{
	$node->safe_psql('postgres', q{
		CREATE TABLE pcm_grd_reuse (
			id integer NOT NULL,
			v bigint NOT NULL,
			p01 name NOT NULL, p02 name NOT NULL,
			p03 name NOT NULL, p04 name NOT NULL,
			p05 name NOT NULL, p06 name NOT NULL,
			p07 name NOT NULL, p08 name NOT NULL,
			p09 name NOT NULL, p10 name NOT NULL,
			p11 name NOT NULL, p12 name NOT NULL
		) WITH (fillfactor = 10)
	});
}

# Retain each catalog identity when diagnosing cross-node accounting failures.
for my $i (0 .. 3)
{
	diag("node$i relation=" . $quad->node($i)->safe_psql('postgres',
		q{SELECT pg_relation_filepath('pcm_grd_reuse')}));
}

# Seed one resource per autocommit transaction so preparation does not create
# an unrelated multi-resource authority round.  The measured reuse delta below
# starts only after all physical rows exist.
my @seed_retire_before = map {
	state_int($quad->node($_), 'pcm', 'pcm_grd_reclaim_success_count')
} (0 .. 3);
my @seed_capacity_fail_before = map {
	state_int($quad->node($_), 'pcm', 'pcm_grd_capacity_fail_count')
} (0 .. 3);
my $seed_buffer_alloc_before = 0 + $quad->node0->safe_psql('postgres', q{
	SELECT buffers_alloc FROM pg_stat_bgwriter
});
my $seed_sql = '';
for my $id (1 .. $working_set)
{
	$seed_sql .= qq{
		INSERT INTO pcm_grd_reuse(
			id, v, p01, p02, p03, p04, p05, p06,
			p07, p08, p09, p10, p11, p12)
		VALUES ($id, 0,
			repeat('x', 63)::name, repeat('x', 63)::name,
			repeat('x', 63)::name, repeat('x', 63)::name,
			repeat('x', 63)::name, repeat('x', 63)::name,
			repeat('x', 63)::name, repeat('x', 63)::name,
			repeat('x', 63)::name, repeat('x', 63)::name,
			repeat('x', 63)::name, repeat('x', 63)::name)
		RETURNING ctid::text AS seeded_ctid \\gset
		SELECT id FROM pcm_grd_reuse WHERE ctid = :'seeded_ctid'::tid;
	};
}
$quad->node0->safe_psql('postgres', $seed_sql, timeout => 180);

# Fresh TAP clusters begin near the xid-stripe floor, so the 512 one-row seed
# transactions can recycle their TT slots before a peer first visits the page.
# The ctid read after each INSERT commit applies the established owner-side
# hint recipe one resource at a time, preserving the seed's single-resource
# authority rounds.  This is setup hygiene only and leaves the measured
# UPDATE+COMMIT workload, capacity, timeout, and exact-once accounting intact.
for my $node ($quad->nodes)
{
	$node->safe_psql('postgres', 'CHECKPOINT');
}

my $seed_retire_delta = 0;
my $seed_buffer_alloc_after = 0 + $quad->node0->safe_psql('postgres', q{
	SELECT buffers_alloc FROM pg_stat_bgwriter
});
cmp_ok($seed_buffer_alloc_after - $seed_buffer_alloc_before, '>',
	$shared_buffer_count,
	'seed workload performed more buffer allocations than cache slots');
for my $i (0 .. 3)
{
	$seed_retire_delta += state_int($quad->node($i), 'pcm',
		'pcm_grd_reclaim_success_count') - $seed_retire_before[$i];
	is(state_int($quad->node($i), 'pcm', 'pcm_grd_capacity_fail_count'),
		$seed_capacity_fail_before[$i],
		"node$i seed replacement preceded directory exhaustion");
}
cmp_ok($seed_retire_delta, '>', 0,
	'seed workload produced terminal entries through cache replacement');

my $distinct_heap_pages = $quad->node0->safe_psql('postgres', q{
	SELECT count(DISTINCT split_part(trim(both '()' from ctid::text), ',', 1))
	FROM pcm_grd_reuse
});
cmp_ok($distinct_heap_pages, '>=', $working_set,
	"working set spans at least $working_set distinct heap resources");
my $ctid_rows = $quad->node0->safe_psql('postgres', q{
	SELECT id::text || E'\t' || ctid::text
	FROM pcm_grd_reuse ORDER BY id
});
my %ctid_by_id;
for my $row (split(/\n/, $ctid_rows))
{
	my ($id, $ctid) = split(/\t/, $row, 2);
	die "invalid frozen ctid row [$row]"
		unless defined($id) && $id =~ /\A\d+\z/
		&& defined($ctid) && $ctid =~ /\A\(\d+,\d+\)\z/;
	$ctid_by_id{$id + 0} = $ctid;
}
is(scalar(keys(%ctid_by_id)), $working_set,
	'all point-update ctids were frozen before the workload');

my @reuse_before = map {
	state_int($quad->node($_), 'pcm', 'pcm_grd_reclaim_reuse_count')
} (0 .. 3);
my @retire_before = map {
	state_int($quad->node($_), 'pcm', 'pcm_grd_reclaim_success_count')
} (0 .. 3);
my @capacity_fail_before = map {
	state_int($quad->node($_), 'pcm', 'pcm_grd_capacity_fail_count')
} (0 .. 3);
my @log_offsets = map { (-s $quad->node($_)->logfile) // 0 } (0 .. 3);
my @node_commits = (0, 0, 0, 0);
my $client_failures = 0;
my $script_dir = PostgreSQL::Test::Utils::tempdir();

for my $round (0 .. $rounds - 1)
{
	my @runs;
	for my $i (0 .. 3)
	{
		my $first = $round * 4 * $rows_per_node_round
			+ $i * $rows_per_node_round + 1;
		my $last = $first + $rows_per_node_round - 1;
		my $script = "$script_dir/round${round}_node${i}.sql";
		my $sql = "\\set ON_ERROR_STOP on\n";
		for my $id ($first .. $last)
		{
			my $ctid = $ctid_by_id{$id};
			die "missing frozen ctid for id $id" unless defined($ctid);
			$sql .= "BEGIN;\nSET LOCAL enable_seqscan = off;\n"
				. "UPDATE pcm_grd_reuse SET v = v + 1 "
				. "WHERE ctid = '$ctid'::tid AND id = $id;\n"
				. "COMMIT;\n";
		}
		write_file($script, $sql);
		my %run = (stdout => '', stderr => '', timed_out => 0);
		my @cmd = (
			$quad->node($i)->installed_command('psql'), '-X', '-d', 'postgres',
			'-h', $quad->node($i)->host, '-p', $quad->node($i)->port,
			'-f', $script);
		$run{handle} = start(\@cmd, '<', \undef, '>', \$run{stdout},
			'2>', \$run{stderr}, timeout(120));
		push @runs, \%run;
	}
	for my $i (0 .. 3)
	{
		my $run = $runs[$i];
		my $finished = eval { finish($run->{handle}); 1 };
		unless ($finished)
		{
			$run->{timed_out} = 1;
			eval { $run->{handle}->kill_kill; };
		}
		my $rc = eval { $run->{handle}->result(0) };
		$rc = -1 unless defined($rc);
		my $commits = () = $run->{stdout} =~ /^COMMIT$/mg;
		my $updates_zero = () = $run->{stdout} =~ /^UPDATE 0$/mg;
		my $updates_one = () = $run->{stdout} =~ /^UPDATE 1$/mg;
		my $errors = () = $run->{stderr} =~ /(?:ERROR|FATAL|PANIC):/g;
		$node_commits[$i] += $commits;
		$client_failures++ if $run->{timed_out} || $rc != 0
			|| $errors != 0 || $commits != $rows_per_node_round;
		diag("round=$round node=$i rc=$rc timeout=$run->{timed_out} "
			. "commits=$commits updates_zero=$updates_zero updates_one=$updates_one "
			. "errors=$errors stderr=[$run->{stderr}]");
	}
}

is($client_failures, 0, 'all wide-set clients completed without error or timeout');
for my $i (0 .. 3)
{
	is($node_commits[$i], $rounds * $rows_per_node_round,
		"node$i committed every point update");
	$quad->node($i)->safe_psql('postgres', 'CHECKPOINT');
}
is($quad->node0->safe_psql('postgres',
	'SELECT sum(v) FROM pcm_grd_reuse'), "$working_set",
	'all point updates committed exactly once');

my ($drained, $terminal) = wait_for_protocol_drain($quad, 60);
ok($drained, 'waiter, retained, EVICTING, transport and active Resource-X debt drained');
unless ($drained)
{
	for my $i (0 .. 3)
	{
		diag("node$i debt=" . join(',', map { "$_=$terminal->[$i]{debt}{$_}" }
			sort keys %{$terminal->[$i]{debt}})
			. " residency=" . join(',', map { "$_=$terminal->[$i]{residency}{$_}" }
			sort keys %{$terminal->[$i]{residency}}));
	}
}

my $reuse_delta = 0;
my $retire_delta = 0;
for my $i (0 .. 3)
{
	my $node = $quad->node($i);
	my $reuse = state_int($node, 'pcm', 'pcm_grd_reclaim_reuse_count');
	my $retire = state_int($node, 'pcm', 'pcm_grd_reclaim_success_count');
	my $capacity_fail = state_int($node, 'pcm', 'pcm_grd_capacity_fail_count');
	my $peak = state_int($node, 'pcm', 'pcm_grd_peak_live_entries');
	$reuse_delta += $reuse - $reuse_before[$i];
	$retire_delta += $retire - $retire_before[$i];
	is($capacity_fail, $capacity_fail_before[$i],
		"node$i had no terminal directory-capacity failure");
	cmp_ok($peak, '<=', $pcm_capacity,
		"node$i peak live directory entries stayed within capacity");
	my $log = substr(slurp_file($node->logfile), $log_offsets[$i]);
	unlike($log, qr/cluster PCM-X runtime fail-closed/,
		"node$i Resource-X gate did not fail closed");
}
cmp_ok($retire_delta, '>', 0, 'wide-set workload retired terminal entries');
cmp_ok($reuse_delta, '>', 0, 'wide-set workload reused retired directory slots');

done_testing();
