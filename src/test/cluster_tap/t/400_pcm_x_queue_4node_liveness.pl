#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 400_pcm_x_queue_4node_liveness.pl
#    spec-2.36a S3-core RED/GREEN: four nodes concurrently update four
#    different tuples that occupy the same heap BufferTag.  Every writer
#    must make progress without surfacing a client error.  This fixture owns
#    the retained finish-Flush positive path; exact destructive containment
#    belongs to t/406_resource_x_finish_flush_failclosed_4node.pl.
#
# Author: SqlRush <sqlrush@gmail.com>
#
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use Cwd qw(abs_path);
use FindBin;
use IPC::Run qw(start finish timeout);
use PostgreSQL::Test::ClusterQuad;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(time usleep);

my $pgrd_voting_file_bytes = (8 * 128 + 3) * 512;
my $source_root = abs_path("$FindBin::RealBin/../../../..");

# The deleted ticket family is proved absent by the focused source-removal
# contract, not by runtime counters that survived only as constant-zero rows.
# Run its seven independent static checks once and retain the named results as
# immutable witnesses for the one-for-one judge migration below.
my ($source_removal_stdout, $source_removal_stderr) = ('', '');
IPC::Run::run(
	[ 'python3', "$source_root/src/test/cluster_unit/test_r11_legacy_source_removed.py", '-v' ],
	'>', \$source_removal_stdout, '2>', \$source_removal_stderr)
	or die "R11 source-removal contract failed: "
		. $source_removal_stdout . $source_removal_stderr;
my $source_removal_output = $source_removal_stdout . $source_removal_stderr;
my @source_removal_test_names = qw(
	test_convert_source_header_and_unit_are_absent
	test_build_manifests_do_not_name_convert_object_or_unit
	test_production_has_no_legacy_include_or_family_identity
	test_old_payload_semantics_are_absent
	test_source_wrapper_fallback_and_legacy_ticks_are_absent
	test_target_native_replacements_remain_positive
	test_legacy_message_values_remain_reserved
);
my %source_removal_fact = map {
	$_ => ($source_removal_output =~ /^\Q$_\E .* \b(?:ok|OK)\b/m ? 1 : 0)
} @source_removal_test_names;
die "R11 source-removal contract omitted a named witness: $source_removal_output"
	if grep { !$source_removal_fact{$_} } @source_removal_test_names;

sub state_int
{
	my ($node, $category, $key) = @_;
	my $value = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state WHERE category='$category' AND key='$key'});
	unless (defined($value) && $value =~ /\A\d+\z/)
	{
		my $shown = defined($value) ? $value : '<undef>';
		die "missing or non-integer pg_cluster_state key $category.$key: [$shown]";
	}
	return $value + 0;
}

my @retired_pcm_debug_keys = qw(
	pcm_x_runtime_state
	pcm_x_runtime_generation
	pcm_x_runtime_fail_closed_site
	pcm_x_queue_enqueue_count
	pcm_x_queue_admit_count
	pcm_x_queue_confirm_count
	pcm_x_queue_promotion_count
	pcm_x_queue_transfer_count
	pcm_x_queue_complete_count
	pcm_x_queue_cancel_count
	pcm_x_queue_revoke_count
	pcm_x_queue_coalesced_count
	pcm_x_queue_wait_count
	pcm_x_queue_full_count
	pcm_x_queue_stale_count
	pcm_x_queue_miss_count
	pcm_x_queue_recovery_blocked_count
	pcm_x_queue_activating_reset_count
	pcm_x_queue_depth
	pcm_x_queue_depth_high_water
	pcm_x_queue_active_tags
	pcm_x_queue_live_tickets
	pcm_x_queue_live_slots
	pcm_x_local_retire_gate
	pcm_x_local_retire_marker_count
	pcm_x_local_retire_marker_ticket_id
	pcm_x_own_begin_count
	pcm_x_own_commit_count
	pcm_x_own_abort_count
	pcm_x_own_busy_count
	pcm_x_own_corrupt_count
);

my @retired_gcs_debug_keys = qw(
	dedup_pcm_x_stage_count
	dedup_pcm_x_replay_count
	dedup_pcm_x_release_count
	dedup_pcm_x_failclosed_count
);

my @resource_x_lmd_keys = qw(wait_edge_count);

my @resource_x_o1_keys = qw(
	remote_install_observed_count
	remote_grant_after_image_count
	remote_image_at_or_after_grant_count
	remote_episode_excluded_no_install
	remote_episode_excluded_missing_grant
	remote_episode_excluded_missing_image
	last_remote_t_image_us
	last_remote_t_grant_us
	last_remote_t_install_us
);

sub exact_key_count
{
	my ($node, $category, $keys) = @_;
	my $quoted = join(',', map { "'$_'" } @{$keys});
	return $node->safe_psql('postgres',
		qq{SELECT count(*) FROM pg_cluster_state WHERE category='$category' AND key IN ($quoted)});
}

sub state_snapshot
{
	my ($node, $category, $keys) = @_;
	my $quoted = join(',', map { "'$_'" } @{$keys});
	my $rows = $node->safe_psql('postgres',
		qq{SELECT key || E'\\t' || value FROM pg_cluster_state }
		. qq{WHERE category='$category' AND key IN ($quoted) ORDER BY key},
		timeout => 5);
	my %expected = map { $_ => 1 } @{$keys};
	my %snapshot;

	for my $row (grep { $_ ne '' } split(/\n/, $rows))
	{
		my ($key, $value) = split(/\t/, $row, 2);
		my $shown_key = defined($key) ? $key : '<undef>';

		die "unexpected pg_cluster_state key $category.$shown_key"
			unless defined($key) && exists($expected{$key});
		die "duplicate pg_cluster_state key $category.$key"
			if exists($snapshot{$key});
		my $shown = defined($value) ? $value : '<undef>';
		die "missing or non-integer pg_cluster_state key $category.$key: [$shown]"
			unless defined($value) && $value =~ /\A\d+\z/;
		$snapshot{$key} = $value + 0;
	}
	for my $key (@{$keys})
	{
		die "missing pg_cluster_state key $category.$key"
			unless exists($snapshot{$key});
	}
	return \%snapshot;
}

sub aggregate_snapshots
{
	my ($snapshots, $keys) = @_;
	my %aggregate = map { $_ => 0 } @{$keys};

	for my $snapshot (@{$snapshots})
	{
		$aggregate{$_} += $snapshot->{$_} for @{$keys};
	}
	return \%aggregate;
}

sub wait_for_resource_x_terminal_drain
{
	my ($quad, $wait_edge_before, $timeout_seconds) = @_;
	my $deadline = time() + $timeout_seconds;
	my (@outstanding, @wait_edges);

	do
	{
		@outstanding = map {
			state_int($quad->node($_), 'gcs', 'outstanding_count')
		} (0 .. 3);
		@wait_edges = map {
			state_int($quad->node($_), 'lmd', 'wait_edge_count')
		} (0 .. 3);
		my $drained = 1;
		for my $i (0 .. 3)
		{
			$drained = 0 if $outstanding[$i] != 0
				|| $wait_edges[$i] != $wait_edge_before->[$i];
		}
		return (1, \@outstanding, \@wait_edges) if $drained;
		usleep(250_000);
	} while (time() < $deadline);

	return (0, \@outstanding, \@wait_edges);
}

sub wait_for_node_state_gt
{
	my ($node, $category, $key, $before, $timeout_seconds) = @_;
	my $deadline = time() + $timeout_seconds;
	my $last_value;

	do
	{
		my ($rc, $out, $err) = $node->psql('postgres',
			qq{SELECT value FROM pg_cluster_state WHERE category='$category' AND key='$key'},
			timeout => 3);
		if (defined($rc) && $rc == 0 && defined($out) && $out =~ /\A\d+\z/)
		{
			$last_value = $out + 0;
			return (1, $last_value) if $last_value > $before;
		}
		usleep(100_000);
	} while (time() < $deadline);

	return (0, $last_value);
}

sub pcm_xq_dirty_retain_block0_tag
{
	my ($node, $shared_root) = @_;
	die "L2F shared data root is unavailable\n"
		unless defined($shared_root) && $shared_root ne '';
	my $row = $node->safe_psql('postgres', q{
		SELECT c.relname || '|' ||
			COALESCE(NULLIF(c.reltablespace, 0), d.dattablespace)::text || '|' ||
			d.oid::text || '|' || pg_relation_filenode(c.oid)::text || '|' ||
			0::text || '|' || 0::text || '|' ||
			pg_relation_filepath(c.oid)
		FROM pg_class AS c
		JOIN pg_database AS d ON d.datname = current_database()
		WHERE c.oid = 'pcm_xq_dirty_retain'::regclass
	});
	my @fields = split(/\|/, $row, -1);

	die "L2F catalog tag projection returned [$row]\n"
		unless @fields == 7;
	my ($relname, $spc_oid, $db_oid, $rel_number, $fork_number,
		$block_number, $relation_path) = @fields;
	die "L2F catalog tag relation mismatch: [$relname]\n"
		unless $relname eq 'pcm_xq_dirty_retain';
	die "L2F catalog tag contains a non-decimal identity: [$row]\n"
		unless $spc_oid =~ /\A\d+\z/
			&& $db_oid =~ /\A\d+\z/
			&& $rel_number =~ /\A\d+\z/
			&& $fork_number =~ /\A\d+\z/
			&& $block_number =~ /\A\d+\z/;
	die "L2F catalog tag contains an out-of-range identity: [$row]\n"
		unless $spc_oid > 0 && $spc_oid <= 4_294_967_295
			&& $db_oid > 0 && $db_oid <= 4_294_967_295
			&& $rel_number > 0 && $rel_number <= 4_294_967_295
			&& $fork_number == 0 && $block_number == 0;
	die "L2F catalog path is not a safe main-fork path: [$relation_path]\n"
		unless defined($relation_path) && $relation_path ne ''
			&& $relation_path !~ m{(?:\A|/)\.\.(?:/|\z)}
			&& $relation_path =~ m{(?:\A|/)\Q$rel_number\E\z};
	my $physical_path = "$shared_root/$relation_path";
	my $physical_bytes = -s $physical_path;
	die "L2F shared main fork has no physical block 0: [$physical_path]\n"
		unless defined($physical_bytes) && $physical_bytes > 0;
	return join('/', $spc_oid, $db_oid, $rel_number, $fork_number,
		$block_number);
}

my $warmup_error_count = 0;

sub write_retry
{
	my ($node, $sql, $attempts) = @_;
	$attempts //= 20;
	my ($last_rc, $last_out, $last_err);
	for (1 .. $attempts)
	{
		my ($rc, $out, $err) = $node->psql('postgres', $sql, timeout => 30);
		($last_rc, $last_out, $last_err) = ($rc, $out, $err);
		return 1 if defined($rc) && $rc == 0;
		$warmup_error_count++;
		diag('warmup retry error: rc=' . (defined($rc) ? $rc : 'undef')
			. ' stdout=[' . ($out // '') . '] stderr=[' . ($err // '') . ']');
		usleep(300_000);
	}
	diag('write_retry exhausted: rc=' . (defined($last_rc) ? $last_rc : 'undef')
		. ' stdout=[' . ($last_out // '') . '] stderr=[' . ($last_err // '') . ']');
	return 0;
}

sub wait_for_lms_finish_flush_reload
{
	my ($node, $log_offset, $expected_workers, $expected_armed,
		$expected_value, $expected_target, $timeout_seconds) = @_;
	my $deadline = time() + $timeout_seconds;
	my $last_log = '';

	do
	{
		$last_log = substr(slurp_file($node->logfile), $log_offset);
		my %ready_workers;
		while ($last_log =~ /cluster_lms: DATA worker=(\d+) applied PCM-X finish Flush injection config: pid=\d+ armed=(true|false) value="([^"]*)" target="([^"]*)"/g)
		{
			my ($worker_id, $armed, $value, $target) = ($1, $2, $3, $4);
			next unless $armed eq $expected_armed && $value eq $expected_value
				&& $target eq $expected_target;
			$ready_workers{$worker_id} = 1;
		}
		return (1, $last_log) if scalar(keys %ready_workers) == $expected_workers;
		usleep(100_000);
	} while (time() < $deadline);

	return (0, $last_log);
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
	my ($node, $round_name, $timeout_seconds) = @_;
	my $deadline = time() + $timeout_seconds;
	my ($last_rc, $last_out, $last_err);

	while (time() < $deadline)
	{
		my ($rc, $out, $err) = $node->psql('postgres',
			'ALTER SYSTEM ENABLE RAC TWO_STAGE ROLLING UPDATES ALL',
			timeout => 45);
		($last_rc, $last_out, $last_err) = ($rc, $out, $err);
		return if defined($rc) && $rc == 0;
		die "$round_name activation result is unknown after SQL timeout: "
			. ($err // '<undef>')
			unless defined($rc);
		die "$round_name activation failed with an untyped error: "
			. ($err // '<undef>')
			unless defined($err)
			&& $err =~ /(?:RF_DEFERRED|CONDITION_NOT_YET_MET|activation request was refused)/;
		usleep(100_000);
	}

	die "$round_name activation did not reach OPEN_APPLIED: rc="
		. (defined($last_rc) ? $last_rc : '<undef>')
		. ' stdout=[' . ($last_out // '') . '] stderr=['
		. ($last_err // '') . "]";
}

my $quad = PostgreSQL::Test::ClusterQuad->new_quad(
	'pcm_xq_liveness',
	quorum_voting_disks => 3,
	shared_data         => 1,
	shared_system_identifier => 1,
	extra_conf          => [
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
	]);

my @pgrd_voting_disks = $quad->voting_disk_paths;
die "D10-11 requires exactly three voting disks\n"
	unless scalar(@pgrd_voting_disks) == 3;
for my $path (@pgrd_voting_disks)
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
		"L1 node$from is alive");
	for my $to (0 .. 3)
	{
		next if $from == $to;
		ok($quad->wait_for_peer_state($from, $to, 'connected', 45),
			"L1 node$from sees node$to connected");
	}
}

# Establish the already-frozen pre-OPEN PGRD authority independently in
# every postmaster.  The utility must remain RF_DEFERRED: this episode may
# publish only the strict-majority descriptor plus its exact shared mirror,
# never open ordinary R4 TARGET or change the workload below.
my $pgrd_mirror
	= $quad->shared_data_root . '/pg_undo/pgrac_undo_root.control';
my $pgrd_root = $quad->shared_data_root . '/pg_undo';
mkdir $pgrd_root or die "mkdir $pgrd_root: $!";
for my $node ($quad->nodes)
{
	$node->poll_query_until('postgres',
		q{SELECT in_quorum FROM pg_cluster_quorum_state}, 't')
		or die "pre-OPEN M4 voting-disk majority did not become current\n";
	my ($activation_rc, $activation_stdout, $activation_stderr);
	my $activation_deadline = time() + 15;
	while (time() < $activation_deadline)
	{
		($activation_rc, $activation_stdout, $activation_stderr)
			= $node->psql('postgres',
				'ALTER SYSTEM ENABLE RAC TWO_STAGE ROLLING UPDATES ALL',
				timeout => 30);
		last if $activation_rc != 0
			&& $activation_stderr
				=~ /(?:RF_DEFERRED|CONDITION_NOT_YET_MET)/;
		usleep(100_000);
	}
	die "pre-OPEN M4 PGRD setup did not remain RF_DEFERRED: "
		. ($activation_stderr // '<undef>')
		unless defined($activation_rc) && $activation_rc != 0
		&& defined($activation_stderr)
		&& $activation_stderr =~ /(?:RF_DEFERRED|CONDITION_NOT_YET_MET)/;
}
die "pre-OPEN M4 PGRD mirror was not published\n" unless -f $pgrd_mirror;

# Drive the two real pre-removal activation rounds through ProcessUtility and
# the four-node R4 carrier.  The first round opens R4 bit0; the second opens
# the Resource-X target-only bit10 after its exact same-T R8/R10 and L3
# readiness checks.  This setup is deliberately count-neutral so the frozen
# 236-item L3 workload and judge remain byte-for-byte unchanged.
activate_semantic_round($quad->node0, 'R4 bit0', 60);
my @bit10_log_offsets = map { (-s $quad->node($_)->logfile) // 0 } (0 .. 3);
activate_semantic_round($quad->node0, 'Resource-X bit10', 60);
my @bit10_open_logs = map {
	substr(slurp_file($quad->node($_)->logfile), $bit10_log_offsets[$_])
} (0 .. 3);
my @bit10_full_open_applied_by_node;

for my $i (0 .. 3)
{
	my $node = $quad->node($i);
	my @remote_nodes = grep { $_ != $i } (0 .. 3);
	my $full_open_applied = 1;
	$full_open_applied &&=
		$bit10_open_logs[$i]
			=~ /bit22 cutover \(node $i\): coordinator applied member ACK stage=5 src=$_ result=2/
		for @remote_nodes;
	$bit10_full_open_applied_by_node[$i] = $full_open_applied;
	is($node->safe_psql('postgres', 'SHOW cluster.xid_striping'), 'on',
		'L1 xid striping is active on the writer topology');
	is($node->safe_psql('postgres', 'SHOW cluster.crossnode_runtime_visibility'), 'on',
		'L1 runtime visibility is active on the writer topology');
	is($node->safe_psql('postgres', 'SHOW cluster.gcs_block_local_cache'), 'on',
		'L1 hold-until-revoked cache is active by default');
		cmp_ok(state_int($node, 'xid_stripe', 'xid_stripe_activated_floor'), '>', 0,
			'L1 xid stripe activation floor is published');
		is(exact_key_count($node, 'pcm', \@resource_x_o1_keys),
			scalar(@resource_x_o1_keys),
			'L1 native Resource-X O1 witness set is complete');
		is(exact_key_count($node, 'lmd', \@resource_x_lmd_keys),
			scalar(@resource_x_lmd_keys),
			'L1 native WFG terminal witness is present');
		ok($full_open_applied,
			"L1 node$i observed exact full-member bit10 OPEN_APPLIED");
		is(exact_key_count($node, 'pcm', \@retired_pcm_debug_keys)
			+ exact_key_count($node, 'gcs', \@retired_gcs_debug_keys), 0,
			'L1 retired legacy compatibility diagnostics are absent');
		$node->safe_psql('postgres', q{
		CREATE TABLE pcm_xq_hot (
			id integer,
			v bigint NOT NULL
		) WITH (fillfactor = 100);
		CREATE TABLE pcm_xq_self (
			id integer,
			v bigint NOT NULL
		) WITH (fillfactor = 100);
		CREATE TABLE pcm_xq_dirty_retain (
			id integer,
			v bigint NOT NULL
		) WITH (fillfactor = 100)
	});
}

my @paths = map {
	$quad->node($_)->safe_psql('postgres',
		q{SELECT pg_relation_filepath('pcm_xq_hot')})
} (0 .. 3);
is(scalar(grep { $_ eq $paths[0] } @paths), 4,
	'L2 all nodes map the test table to the same relation file');

ok(write_retry($quad->node0,
	q{INSERT INTO pcm_xq_hot(id, v) SELECT g, 0 FROM generate_series(1, 4) g}),
	'L2 seeded four distinct tuples');
my ($self_seed_rc, $self_seed_out, $self_seed_err) = $quad->node0->psql('postgres', q{
	SET cluster.gcs_block_local_cache = off;
	INSERT INTO pcm_xq_self(id, v) VALUES (1, 0)
}, timeout => 30);
is($self_seed_rc, 0,
	'L2 seeded the sole-S tuple with X released to N at unlock')
	or diag("L2 sole-S seed stdout=[$self_seed_out] stderr=[$self_seed_err]");
ok(write_retry($quad->node0,
	q{VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) pcm_xq_hot}),
	'L2 seed frozen so the queue test begins from a stable committed page image');
ok(write_retry($quad->node0,
	q{SELECT count(*) FROM pcm_xq_hot WHERE id BETWEEN 1 AND 4}),
	'L2 seed owner installed committed visibility hints');
my @l2s_checkpoint_log_offsets = map { (-s $quad->node($_)->logfile) // 0 } (0 .. 3);
# The hot main page is guaranteed dirty by seed/VACUUM and hashes to remote
# master node1 on shard1.  That single consumed note deterministically proves
# the P0-24 DATA-plane route.  The VM page also hashes to node3, but its
# checkpoint write-note is conditional on still holding PCM S/X at flush; N
# is legal, so waiting for node3 would assert a non-contractual side effect.
my %l2s_pi_durable_applied_before = map {
	$_ => state_int($quad->node($_), 'gcs', 'pi_durable_note_apply_count')
} (1);
my @l2s_pi_route_before_by_node = map {
	{
		misroute => state_int($quad->node($_), 'gcs',
			'dedup_misroute_failclosed_count'),
		requeue_drop => state_int($quad->node($_), 'lms',
			'lms_outbound_requeue_drop_count'),
	}
} (0 .. 3);
ok(write_retry($quad->node0, 'CHECKPOINT'), 'L2 seed checkpointed');
for my $target (1)
{
	my ($applied, $after) = wait_for_node_state_gt(
		$quad->node($target), 'gcs', 'pi_durable_note_apply_count',
		$l2s_pi_durable_applied_before{$target}, 15);
	ok($applied,
		"L2S target node$target DATA worker consumed its checkpoint status-3 PI durable note")
		or diag("L2S node$target pi_durable_note_apply_count before="
			. $l2s_pi_durable_applied_before{$target} . ' after='
			. (defined($after) ? $after : '<unavailable>'));
}
for my $i (0 .. 3)
{
	my ($checkpoint_probe_rc, $checkpoint_probe_out, $checkpoint_probe_err)
		= $quad->node($i)->psql('postgres', 'SELECT 1', timeout => 5);
	ok(defined($checkpoint_probe_rc) && $checkpoint_probe_rc == 0
		&& defined($checkpoint_probe_out) && $checkpoint_probe_out eq '1',
		"L2S node$i survived checkpoint PI durable-note routing")
		or diag("L2S node$i checkpoint probe rc="
			. (defined($checkpoint_probe_rc) ? $checkpoint_probe_rc : '<undef>')
			. " stdout=[" . ($checkpoint_probe_out // '')
			. "] stderr=[" . ($checkpoint_probe_err // '') . "]");
	my $checkpoint_log = substr(slurp_file($quad->node($i)->logfile),
		$l2s_checkpoint_log_offsets[$i]);
	unlike($checkpoint_log,
		qr/gcs block invalidate-ack misrouted|failed Assert\("tag_shard == recv_worker"\)/,
		"L2S node$i checkpoint emitted no invalidate-ack shard violation");
	is(state_int($quad->node($i), 'gcs', 'dedup_misroute_failclosed_count'),
		$l2s_pi_route_before_by_node[$i]{misroute},
		"L2S node$i checkpoint misroute counter stayed exact");
	is(state_int($quad->node($i), 'lms', 'lms_outbound_requeue_drop_count'),
		$l2s_pi_route_before_by_node[$i]{requeue_drop},
		"L2S node$i checkpoint outbound requeue-drop counter stayed exact");
}

# Build a deterministic sole-requester S source before the four-writer leg.
# The cache-off INSERT above releases node0's X to N at content-lock unlock.
# No other node has touched this relation, so this cache-on node0 read is the
# unique N->S grant.  The following node0 UPDATE must therefore exercise the
# Resource-X sole-S conversion without falling back to the legacy fused
# handoff/A-record path.
my ($self_read_rc, $self_read_out, $self_read_err) = $quad->node0->psql(
	'postgres', q{SELECT v FROM pcm_xq_self WHERE id = 1}, timeout => 30);
is($self_read_rc, 0, 'L2S sole requester acquired the only S copy');
is($self_read_out, '0', 'L2S sole requester saw the seeded image')
	or diag("L2S sole requester read stderr=[$self_read_err]");

my @self_conversion_log_offsets = map {
	(-s $quad->node($_)->logfile) // 0
} (0 .. 3);

my ($self_write_rc, $self_write_out, $self_write_err) = $quad->node0->psql(
	'postgres', q{UPDATE pcm_xq_self SET v = v + 1 WHERE id = 1}, timeout => 30);
is($self_write_rc, 0, 'L2S sole-S requester completed S-to-X conversion')
	or diag("L2S sole-S write stdout=[$self_write_out] stderr=[$self_write_err]");

ok($source_removal_fact{test_target_native_replacements_remain_positive},
	'L2S sole-S conversion is rooted only in native Resource-X targets');
ok($source_removal_fact{test_source_wrapper_fallback_and_legacy_ticks_are_absent},
	'L2S sole-S conversion has no legacy wrapper or fallback root');
for my $i (0 .. 3)
{
	ok($bit10_full_open_applied_by_node[$i],
		"L2S node$i retained exact full-member bit10 OPEN_APPLIED");
	my $conversion_log = substr(slurp_file($quad->node($i)->logfile),
		$self_conversion_log_offsets[$i]);
	unlike($conversion_log, qr/cluster PCM-X runtime fail-closed/,
		"L2S node$i Resource-X gate remained open across the conversion");
}
is($quad->node0->safe_psql('postgres',
	q{SELECT v FROM pcm_xq_self WHERE id = 1}), '1',
	'L2S sole-requester conversion preserved exact page contents');

# Seed and vacuum one target page so node1's later INSERT obtains that exact
# heap page from the FSM and asks for X directly.  An UPDATE would first read
# the row into a requester-local S mirror, making the authority selector take
# the intentional self-source path instead of the remote finish path tested
# here.  The node0 UPDATE after this checkpoint is the sole dirty source.
ok(write_retry($quad->node0,
	q{INSERT INTO pcm_xq_dirty_retain(id, v) VALUES (1, 0)}),
	'L2F seeded the direct-X target page');
ok(write_retry($quad->node0, 'VACUUM pcm_xq_dirty_retain'),
	'L2F published target-page free space before the direct-X request');
ok(write_retry($quad->node0, 'CHECKPOINT'),
	'L2F baseline checkpointed before the dirty retain leg');
my @dirty_retain_tags = map {
	pcm_xq_dirty_retain_block0_tag(
		$quad->node($_), $quad->shared_data_root)
} (0 .. 3);
is(scalar(grep { $_ eq $dirty_retain_tags[0] } @dirty_retain_tags), 4,
	'L2F all nodes resolve the same catalog-authoritative block-0 target');
my $dirty_retain_target = $dirty_retain_tags[0];

# The source INSERT leaves an uncheckpointed dirty X image on node0.  A
# different node must install that exact Resource-X image only after the
# holder-side FlushBuffer contract is satisfied.  This is a behavioral gate:
# the legacy A-record counter must stay unchanged, while
# h_pi_write_note_count increments only after FlushBuffer's smgrwrite returned,
# proving the holder-side physical flush completed before ownership commit.
my $dirty_flush_before = state_int($quad->node0, 'xnode_lever',
	'h_pi_write_note_count');
my $dirty_flush_log_offset = (-s $quad->node0->logfile) // 0;
my $dirty_requester_log_offset = (-s $quad->node1->logfile) // 0;
my $dirty_lms_workers = $quad->node0->safe_psql('postgres',
	'SHOW cluster.lms_workers') + 0;
my $dirty_requester_lms_workers = $quad->node1->safe_psql('postgres',
	'SHOW cluster.lms_workers') + 0;
$quad->node0->safe_psql('postgres', q{
	ALTER SYSTEM SET cluster.pcm_x_retain_flush_error_target =
		'} . $dirty_retain_target . q{';
	ALTER SYSTEM SET cluster.injection_points = 'cluster-pcm-x-retain-flush-error';
	SELECT pg_reload_conf()
});
$quad->node1->safe_psql('postgres', q{
	ALTER SYSTEM SET cluster.pcm_x_retain_flush_error_target =
		'} . $dirty_retain_target . q{';
	ALTER SYSTEM SET cluster.injection_points = 'cluster-pcm-x-retain-flush-error';
	SELECT pg_reload_conf()
});
my ($dirty_reload_ready, $dirty_reload_log) = wait_for_lms_finish_flush_reload(
	$quad->node0, $dirty_flush_log_offset, $dirty_lms_workers, 'true',
	'cluster-pcm-x-retain-flush-error', $dirty_retain_target, 15);
ok($dirty_reload_ready,
	'L2F every node0 DATA worker applied the finish-Flush injection arm')
	or diag("L2F DATA-worker reload log=[$dirty_reload_log]");
my ($dirty_requester_reload_ready, $dirty_requester_reload_log)
	= wait_for_lms_finish_flush_reload(
		$quad->node1, $dirty_requester_log_offset, $dirty_requester_lms_workers, 'true',
		'cluster-pcm-x-retain-flush-error', $dirty_retain_target, 15);
ok($dirty_requester_reload_ready,
	'L2F every node1 DATA worker applied the transfer-boundary diagnostic arm')
	or diag("L2F requester DATA-worker reload log=[$dirty_requester_reload_log]");
my ($dirty_seed_rc, $dirty_seed_out, $dirty_seed_err) = $quad->node0->psql(
	'postgres', q{UPDATE pcm_xq_dirty_retain SET v = 1 WHERE id = 1}, timeout => 30);
is($dirty_seed_rc, 0, 'L2F node0 created an uncheckpointed dirty X source')
	or diag("L2F dirty seed stdout=[$dirty_seed_out] stderr=[$dirty_seed_err]");
my ($dirty_retain_rc, $dirty_retain_out, $dirty_retain_err) = $quad->node1->psql(
	'postgres', q{INSERT INTO pcm_xq_dirty_retain(id, v) VALUES (2, 1)}, timeout => 30);
is($dirty_retain_rc, 0,
	'L2F remote writer completed the dirty retain/flush lifecycle')
	or diag("L2F dirty retain stdout=[$dirty_retain_out] stderr=[$dirty_retain_err]");
my $dirty_flush_log = substr(slurp_file($quad->node0->logfile),
	$dirty_flush_log_offset);
like($dirty_flush_log,
	qr/Resource-X frame ingress diagnostic\n.*?kind=2 msg_type=17 source=0 requester=1 attempt=\d+ result=0/s,
	'L2F source DATA worker published the exact native type-17 transition');
cmp_ok(state_int($quad->node0, 'xnode_lever', 'h_pi_write_note_count')
		- $dirty_flush_before, '>', 0,
	'L2F source FlushBuffer completed smgrwrite before ownership commit');
like($dirty_flush_log,
	qr/cluster injection point "cluster-pcm-x-retain-flush-error" armed with WARNING/,
	'L2F DATA worker reached the finish-exclusive FlushBuffer injection seam');
like($dirty_flush_log,
	qr/cluster PCM-X retained-image finish FlushBuffer succeeded:/,
	'L2F finish-exclusive FlushBuffer returned after the physical write');
is($quad->node1->safe_psql('postgres',
	q{SELECT string_agg(id::text || ':' || v::text, ',' ORDER BY id)
		FROM pcm_xq_dirty_retain}), '1:1,2:1',
	'L2F dirty retain preserved the exact page contents');
$quad->node0->safe_psql('postgres', q{
	ALTER SYSTEM SET cluster.injection_points =
		'cluster-pcm-x-retain-flush-error:none:0';
	ALTER SYSTEM SET cluster.pcm_x_retain_flush_error_target = '';
	SELECT pg_reload_conf()
});
$quad->node1->safe_psql('postgres', q{
	ALTER SYSTEM SET cluster.injection_points =
		'cluster-pcm-x-retain-flush-error:none:0';
	ALTER SYSTEM SET cluster.pcm_x_retain_flush_error_target = '';
	SELECT pg_reload_conf()
});
my ($dirty_reset_ready, $dirty_reset_log) = wait_for_lms_finish_flush_reload(
	$quad->node0, $dirty_flush_log_offset, $dirty_lms_workers, 'false',
	'cluster-pcm-x-retain-flush-error:none:0', '', 15);
ok($dirty_reset_ready,
	'L2F every node0 DATA worker applied the finish-Flush injection disarm')
	or diag("L2F DATA-worker reset log=[$dirty_reset_log]");
my ($dirty_requester_reset_ready, $dirty_requester_reset_log)
	= wait_for_lms_finish_flush_reload(
		$quad->node1, $dirty_requester_log_offset, $dirty_requester_lms_workers,
		'false', 'cluster-pcm-x-retain-flush-error:none:0', '', 15);
ok($dirty_requester_reset_ready,
	'L2F every node1 DATA worker applied the transfer-boundary diagnostic disarm')
	or diag("L2F requester DATA-worker reset log=[$dirty_requester_reset_log]");

for my $i (0 .. 3)
{
	ok(write_retry($quad->node($i),
		q{SELECT count(*) FROM pcm_xq_hot WHERE id BETWEEN 1 AND 4}),
		"L2 node$i can read the seed");
}
is($warmup_error_count, 0,
	'L2 warmup completed without any transient or terminal client error');

my $block_count = $quad->node0->safe_psql('postgres', q{
	SELECT count(DISTINCT split_part(trim(both '()' from ctid::text), ',', 1))
	FROM pcm_xq_hot WHERE id BETWEEN 1 AND 4
});
is($block_count, '1',
	'L2 four different tuple ids occupy one heap block/BufferTag');

my $tuple_map = $quad->node0->safe_psql('postgres', q{
	SELECT string_agg(id::text || ':' || ctid::text, ',' ORDER BY id)
	FROM pcm_xq_hot WHERE id BETWEEN 1 AND 4
});
diag("L2 fixed hot-block tuple map: rel=$paths[0] tuples=$tuple_map");

my @workload_log_offsets = map { (-s $quad->node($_)->logfile) // 0 } (0 .. 3);
my @lmd_before_by_node = map {
	state_snapshot($quad->node($_), 'lmd', \@resource_x_lmd_keys)
} (0 .. 3);
my @resource_x_o1_before_by_node = map {
	state_snapshot($quad->node($_), 'pcm', \@resource_x_o1_keys)
} (0 .. 3);
my %lmd_before = %{aggregate_snapshots(\@lmd_before_by_node, \@resource_x_lmd_keys)};
my %resource_x_o1_before = %{
	aggregate_snapshots(\@resource_x_o1_before_by_node, \@resource_x_o1_keys)
};
my @lms_transport_before_by_node = map {
	{
		not_admitted => state_int($quad->node($_), 'lms',
			'lms_outbound_not_admitted_count'),
		requeue_drop => state_int($quad->node($_), 'lms',
			'lms_outbound_requeue_drop_count'),
		cap_guard_drop => state_int($quad->node($_), 'lms',
			'lms_outbound_cap_guard_drop_count'),
	}
} (0 .. 3);
my @holder_evicted_before_by_node = map {
	state_int($quad->node($_), 'gcs', 'block_forward_holder_evicted_count')
} (0 .. 3);
my $start_at = $quad->node0->safe_psql('postgres',
	q{SELECT (clock_timestamp() + interval '5 seconds')::text});
my $script_dir = PostgreSQL::Test::Utils::tempdir();
my @runs;
my $data_worker_buffer_content_wait_samples = 0;

for my $i (0 .. 3)
{
	my $id = $i + 1;
	my $script = "$script_dir/node$i.sql";
	write_file($script,
		"SELECT pg_sleep(GREATEST(0.0, EXTRACT(EPOCH FROM "
		. "(TIMESTAMPTZ '$start_at' - clock_timestamp()))));\n"
		. "UPDATE pcm_xq_hot SET v = v + 1 WHERE id = $id;\n");

	my %run = (stdout => '', stderr => '', timed_out => 0);
	my @cmd = (
		$quad->node($i)->installed_command('pgbench'),
		'-n', '-c', '1', '-j', '1', '-T', '15', '--max-tries=1',
		'-f', $script, '-h', $quad->node($i)->host,
		'-p', $quad->node($i)->port, 'postgres');
	$run{handle} = start(\@cmd, '<', \undef, '>', \$run{stdout},
		'2>', \$run{stderr}, timeout(45));
	push @runs, \%run;
}

# Mid-leg probe: while the four writers are (potentially) stalled, capture
# each node's writer wait state and the live queue gauges.  A post-mortem
# probe cannot see this — a wedged writer holds no cluster state after
# kill_kill.  Diagnostic only; every query is bounded and failure-tolerant.
{
	for my $offset (1, 3, 8)
	{
		my $probe_at = $quad->node0->safe_psql('postgres',
			"SELECT GREATEST(0.0, EXTRACT(EPOCH FROM "
			. "(TIMESTAMPTZ '$start_at' + interval '$offset seconds' - clock_timestamp())))");
		sleep($probe_at) if $probe_at > 0;
		for my $i (0 .. 3)
		{
			my $waits = eval {
				$quad->node($i)->safe_psql('postgres',
					q{SELECT pid || ':' || state || ':' || coalesce(wait_event_type, '-')
						|| '/' || coalesce(wait_event, '-')
					  FROM pg_stat_activity
					  WHERE query LIKE 'UPDATE pcm_xq_hot%'},
					timeout => 10);
			} // 'probe-failed';
			$waits =~ s/\n/ | /g;
			my $aux = eval {
				$quad->node($i)->safe_psql('postgres',
					q{SELECT backend_type || ':' || coalesce(wait_event, '-')
					  FROM pg_stat_activity
					  WHERE backend_type IN ('lmon', 'lms', 'lms worker', 'lck', 'lmd',
						'cssd', 'diag', 'cluster stats', 'qvotec', 'interconnect listener')
					  ORDER BY backend_type},
					timeout => 10);
			} // 'probe-failed';
			$aux =~ s/\n/ | /g;
			my $data_content_waits = eval {
				$quad->node($i)->safe_psql('postgres',
					q{SELECT count(*) FROM pg_stat_activity
					  WHERE backend_type = 'lms worker'
					    AND wait_event = 'BufferContent'},
					timeout => 10);
			} // 0;
			$data_worker_buffer_content_wait_samples += $data_content_waits
				if $data_content_waits =~ /\A\d+\z/;
			my $wire = eval {
				$quad->node($i)->safe_psql('postgres',
					q{SELECT string_agg('peer' || node_id || ':s' || msg_send_count
						|| ':r' || msg_recv_count, ' ' ORDER BY node_id)
					  FROM pg_cluster_ic_peers},
					timeout => 10);
			} // 'probe-failed';
			my $debt = eval {
				$quad->node($i)->safe_psql('postgres',
					q{SELECT string_agg(category || '.' || key || '=[' || value || ']',
						' ' ORDER BY category, key)
					  FROM pg_cluster_state
					  WHERE (category = 'gcs' AND key = 'outstanding_count')
						 OR (category = 'lmd' AND key = 'wait_edge_count')
						 OR (category = 'lms' AND key IN (
							'lms_outbound_not_admitted_count',
							'lms_outbound_requeue_drop_count',
							'lms_outbound_cap_guard_drop_count'))},
					timeout => 10);
			} // 'probe-failed';
			$debt =~ s/\n/ | /g;
			diag("L3 mid-leg t+$offset node$i waits=[$waits]");
			diag("L3 mid-leg t+$offset node$i aux=[$aux]");
			diag("L3 mid-leg t+$offset node$i DATA BufferContent waits="
				. $data_content_waits);
			diag("L3 mid-leg t+$offset node$i wire=[$wire]");
			diag("L3 mid-leg t+$offset node$i native-debt=[$debt]");
		}
	}
}

for my $i (0 .. 3)
{
	my $run = $runs[$i];
	my $finished = eval { finish($run->{handle}); 1 };
	unless ($finished)
	{
		$run->{timed_out} = 1;
		$run->{finish_error} = $@;
		eval { $run->{handle}->kill_kill; };
	}
	$run->{result} = eval { $run->{handle}->result(0) };
	$run->{result} = -1 unless defined($run->{result});
	($run->{transactions}) =
		$run->{stdout} =~ /number of transactions actually processed:\s+(\d+)/;
	$run->{transactions} //= 0;
	my @errors = $run->{stderr} =~ /^.*(?:ERROR|FATAL|PANIC):.*$/mg;
	$run->{errors} = scalar(@errors);
	diag("L3 node$i result=$run->{result} timed_out=$run->{timed_out} "
		. "transactions=$run->{transactions} errors=$run->{errors} "
		. 'finish_error=[' . ($run->{finish_error} // '') . '] '
		. "stderr=[$run->{stderr}]");
}

my @wait_edge_before = map {
	$lmd_before_by_node[$_]{wait_edge_count}
} (0 .. 3);
my ($terminal_drained, $gcs_outstanding_after_ref, $wait_edges_after_ref)
	= wait_for_resource_x_terminal_drain($quad, \@wait_edge_before, 30);
my @gcs_outstanding_after_by_node = @{$gcs_outstanding_after_ref};
my @wait_edges_after_by_node = @{$wait_edges_after_ref};
my @lmd_after_by_node = map {
	state_snapshot($quad->node($_), 'lmd', \@resource_x_lmd_keys)
} (0 .. 3);
my @resource_x_o1_after_by_node = map {
	state_snapshot($quad->node($_), 'pcm', \@resource_x_o1_keys)
} (0 .. 3);
my %lmd_after = %{
	aggregate_snapshots(\@lmd_after_by_node, \@resource_x_lmd_keys)
};
my %resource_x_o1_after = %{
	aggregate_snapshots(\@resource_x_o1_after_by_node, \@resource_x_o1_keys)
};
my @lms_transport_after_by_node = map {
	{
		not_admitted => state_int($quad->node($_), 'lms',
			'lms_outbound_not_admitted_count'),
		requeue_drop => state_int($quad->node($_), 'lms',
			'lms_outbound_requeue_drop_count'),
		cap_guard_drop => state_int($quad->node($_), 'lms',
			'lms_outbound_cap_guard_drop_count'),
	}
} (0 .. 3);
my @holder_evicted_after_by_node = map {
	state_int($quad->node($_), 'gcs', 'block_forward_holder_evicted_count')
} (0 .. 3);
my @workload_logs = map {
	my $log = slurp_file($quad->node($_)->logfile);
	substr($log, $workload_log_offsets[$_]);
} (0 .. 3);
my (@native_round_seen, @native_round_formation_exact,
	@native_round_formations_by_node, @all_native_round_formations);
for my $i (0 .. 3)
{
	my @logged_formations = $workload_logs[$i] =~
		/Resource-X kind-9 request diagnostic\n[^\n]*DETAIL:\s+source=\d+ requester=\d+ attempt=\d+ result=\d+ ack_base=\d+ formation=(\d+)/g;
	$native_round_formations_by_node[$i] = \@logged_formations;
	push @all_native_round_formations, @logged_formations;
	$native_round_seen[$i] = $workload_logs[$i] =~
		/Resource-X kind-9 (?:request|ACK) diagnostic/;
}
my %native_formation_set = map { $_ => 1 } @all_native_round_formations;
my $canonical_resource_x_formation
	= scalar(keys(%native_formation_set)) == 1
	? $all_native_round_formations[0] : 0;
for my $i (0 .. 3)
{
	$native_round_formation_exact[$i] = $native_round_seen[$i]
		&& $canonical_resource_x_formation > 0
		&& !grep { $_ != $canonical_resource_x_formation }
			@{$native_round_formations_by_node[$i]};
}
diag('L3 native drain: outstanding=['
	. join(',', @gcs_outstanding_after_by_node) . '] wait_edges=['
	. join(',', @wait_edges_after_by_node) . '] Resource-X formation='
	. $canonical_resource_x_formation);
for my $i (0 .. 3)
{
	diag("L3 node$i holder copy refusal baseline=$holder_evicted_before_by_node[$i]"
		. " final=$holder_evicted_after_by_node[$i] delta="
		. ($holder_evicted_after_by_node[$i] - $holder_evicted_before_by_node[$i]));
}

# Per-node truth table: current R4/formation plus native Resource-X traffic
# proves the gate stayed open.  Terminal truth comes from native GCS slots,
# WFG edges, and transport refusal counters.  Deleted PCM-X debug keys must
# remain absent rather than being recreated as aliases or constant-zero rows.
for my $i (0 .. 3)
{
	ok($bit10_full_open_applied_by_node[$i],
		"L3 node$i retained the exact full-member bit10 OPEN_APPLIED witness");
	ok($canonical_resource_x_formation > 0,
		"L3 node$i observed one exact nonzero Resource-X formation");
	ok($native_round_seen[$i],
		"L3 node$i observed a native kind-9 Resource-X round");
	ok($native_round_formation_exact[$i],
		"L3 node$i native Resource-X rounds used the current formation");
	is($lmd_after_by_node[$i]{wait_edge_count},
		$lmd_before_by_node[$i]{wait_edge_count},
		"L3 node$i native WFG live edge count returned to baseline");
	is($gcs_outstanding_after_by_node[$i], 0,
		"L3 node$i native GCS request slots drained");
	is($lms_transport_after_by_node[$i]{not_admitted},
		$lms_transport_before_by_node[$i]{not_admitted},
		"L3 node$i transport admission refusal count stayed exact");
	is($lms_transport_after_by_node[$i]{requeue_drop},
		$lms_transport_before_by_node[$i]{requeue_drop},
		"L3 node$i transport requeue-drop count stayed exact");
	is($lms_transport_after_by_node[$i]{cap_guard_drop},
		$lms_transport_before_by_node[$i]{cap_guard_drop},
		"L3 node$i transport capability-guard drop count stayed exact");
	is(exact_key_count($quad->node($i), 'pcm', \@retired_pcm_debug_keys)
		+ exact_key_count($quad->node($i), 'gcs', \@retired_gcs_debug_keys), 0,
		"L3 node$i exposes no retired legacy compatibility diagnostics");
	unlike($workload_logs[$i], qr/cluster PCM-X runtime fail-closed/,
		"L3 node$i Resource-X gate stayed open across the workload");
}
for my $i (0 .. 3)
{
	my $detail = $quad->node($i)->safe_psql('postgres', q{
		SELECT string_agg(key || '=' || value, ', ' ORDER BY key)
		FROM pg_cluster_state
		WHERE category = 'gcs'
		  AND key IN (
			'block_master_not_holder_count',
			'block_forward_holder_evicted_count',
			'block_x_self_ship_count',
			'drop_pinned_deny_count',
			'x_vs_s_no_carrier_denied_count',
			'block_invalidate_broadcast_count',
			'block_invalidate_ack_received_count',
			'invalidate_send_queued_count',
			'invalidate_passive_s_release_count',
			'invalidate_parked_count',
			'invalidate_busy_sent_count',
			'invalidate_busy_received_count',
			'invalidate_park_expired_count',
			'invalidate_park_overflow_count',
			'stale_reply_drop_count',
			'block_checksum_fail_count',
			'block_forward_received_count',
			'block_from_holder_ship_count',
			'cf_xheld_read_ship_count',
			'invalidate_send_not_admitted_count',
			'forward_send_not_admitted_count',
			'reply_send_not_admitted_count')
	});
		diag("L3 node$i GCS branch state: $detail");
		my $visibility = $quad->node($i)->safe_psql('postgres', q{
			SELECT string_agg(category || '.' || key || '=' || value,
				', ' ORDER BY category, key)
			FROM pg_cluster_state
			WHERE (category = 'cr' AND key IN (
				'rtvis_undo_fetch_wire_count',
				'rtvis_undo_fetch_cache_hit_count',
				'rtvis_undo_fetch_failclosed_count',
				'rtvis_resolve_committed_count',
				'rtvis_resolve_aborted_count',
				'rtvis_resolve_failclosed_count',
				'rtvis_verdict_wire_count',
				'rtvis_verdict_failclosed_count',
				'rtvis_verdict_exact_count',
				'rtvis_verdict_below_horizon_count',
				'rtvis_verdict_inadmissible_count',
				'rtvis_underivable_failclosed_count',
				'cr_server_verdict_served_count',
				'cr_server_verdict_denied_count',
				'cr_server_fence_refused_count',
				'undo_authority_serve_hit_count',
				'undo_authority_fail_closed_count',
				'undo_authority_epoch_stale_reject_count',
				'undo_authority_scan_incomplete_reject_count',
				'undo_authority_multi_match_reject_count',
				'vis53r97_leg_invalid_scn_refuse_count',
				'vis53r97_leg_zero_match_refuse_count',
				'vis53r97_leg_srv_other_refuse_count',
				'vis53r97_leg_covers_refuse_count',
				'vis53r97_leg_multi_unresolvable_count',
				'vis53r97_leg_xmax_unprovable_count',
				'vis53r97_leg_xmin_overlay_verdict_ask_count',
				'vis53r97_leg_xmin_overlay_verdict_hit_count',
				'vis53r97_leg_multi_member_serve_ask_count',
				'vis53r97_leg_multi_member_serve_hit_count',
				'vis53r97_leg_live_upgrade_hit_count'))
			   OR (category = 'xnode' AND key IN (
				'c_resolve_count', 'c_tt_lookup_count',
				'c_memo_hit_count', 'c_memo_install_count'))
		});
		diag("L3 node$i visibility state: $visibility");
	}

my @node_target_ok = map {
	$runs[$_]->{result} == 0
		&& $runs[$_]->{errors} == 0
		&& $runs[$_]->{transactions} > 0
} (0 .. 3);
my $all_nodes_target_ok = 1;
$all_nodes_target_ok &&= $_ for @node_target_ok;
my $remote_install_delta
	= $resource_x_o1_after{remote_install_observed_count}
	- $resource_x_o1_before{remote_install_observed_count};
my $grant_after_image_delta
	= $resource_x_o1_after{remote_grant_after_image_count}
	- $resource_x_o1_before{remote_grant_after_image_count};
my $image_at_or_after_grant_delta
	= $resource_x_o1_after{remote_image_at_or_after_grant_count}
	- $resource_x_o1_before{remote_image_at_or_after_grant_count};
my @resource_x_excluded_keys = qw(
	remote_episode_excluded_no_install
	remote_episode_excluded_missing_grant
	remote_episode_excluded_missing_image
);
my @resource_x_excluded_deltas = map {
	$resource_x_o1_after{$_} - $resource_x_o1_before{$_}
} @resource_x_excluded_keys;
my $remote_install_cohort_complete = $remote_install_delta > 0
	&& !grep { $_ != 0 } @resource_x_excluded_deltas;
my $all_workload_log = join("\n", @workload_logs);
my $kind9_request_count = () =
	$all_workload_log =~ /Resource-X kind-9 request diagnostic/g;
my $native_type17_success = $all_workload_log =~
	/Resource-X frame ingress diagnostic\n[^\n]*DETAIL:\s+kind=2 msg_type=17 [^\n]*result=0/;
my $native_authority_grant_success = $all_workload_log =~
	/Resource-X frame ingress diagnostic\n[^\n]*DETAIL:\s+kind=5 msg_type=15 [^\n]*result=0/;
my $native_install_settled = $all_workload_log =~
	/Resource-X install settlement diagnostic\n[^\n]*DETAIL:\s+source=\d+ requester=\d+ attempt=\d+ result=0 phase=5/;
my $native_source_settled = $all_workload_log =~
	/Resource-X source settlement ACK diagnostic\n[^\n]*DETAIL:\s+master=\d+ requester=\d+ attempt=\d+ result=0/;
my $all_native_formations_exact = !grep { !$_ }
	@native_round_formation_exact;
my $all_wait_edges_drained = !grep {
	$wait_edges_after_by_node[$_] != $wait_edge_before[$_]
} (0 .. 3);
my $all_gcs_slots_drained = !grep {
	$gcs_outstanding_after_by_node[$_] != 0
} (0 .. 3);
my $all_transport_not_admitted_exact = !grep {
	$lms_transport_after_by_node[$_]{not_admitted}
		!= $lms_transport_before_by_node[$_]{not_admitted}
} (0 .. 3);
my $all_transport_requeue_exact = !grep {
	$lms_transport_after_by_node[$_]{requeue_drop}
		!= $lms_transport_before_by_node[$_]{requeue_drop}
} (0 .. 3);
my $all_transport_cap_guard_exact = !grep {
	$lms_transport_after_by_node[$_]{cap_guard_drop}
		!= $lms_transport_before_by_node[$_]{cap_guard_drop}
} (0 .. 3);
my $all_retired_debug_absent = !grep {
	exact_key_count($quad->node($_), 'pcm', \@retired_pcm_debug_keys)
		+ exact_key_count($quad->node($_), 'gcs', \@retired_gcs_debug_keys) != 0
} (0 .. 3);
my $all_source_removal_facts = !grep { !$source_removal_fact{$_} }
	@source_removal_test_names;

for my $i (0 .. 3)
{
	is($runs[$i]->{timed_out}, 0, "L3 node$i writer met the hard deadline");
	is($runs[$i]->{result}, 0, "L3 node$i writer exited successfully");
	is($runs[$i]->{errors}, 0, "L3 node$i writer surfaced zero client errors");
	cmp_ok($runs[$i]->{transactions}, '>', 0,
		"L3 node$i writer made progress");
	ok($node_target_ok[$i]
		&& $native_round_seen[$i],
		"L3 node$i completed through its native Resource-X round");
}

# Resource-X target-only admission is witnessed by real writers, native wire
# episodes, O1 ordering, and terminal drain.  Source removal is independently
# established by the focused static contract; no retired runtime counter is
# permitted to stand in for any of these facts.
ok($all_nodes_target_ok
	&& $source_removal_fact{test_source_wrapper_fallback_and_legacy_ticks_are_absent},
	'L3 all four nodes completed through the source-removed target-only root');
ok($remote_install_cohort_complete,
	'L3 Resource-X remote-install cohort advanced with no excluded episode');
ok($native_type17_success,
	'L3 Resource-X holder transition used authenticated type-17');
cmp_ok($kind9_request_count, '>', 0,
	'L3 Resource-X pre-ASSERT bootstrap produced native kind-9 traffic');
ok($native_authority_grant_success,
	'L3 Resource-X T1 authority grant completed on native type-15');
for my $name (@source_removal_test_names)
{
	ok($source_removal_fact{$name},
		"L3 source-removal witness $name is exact");
}
ok($native_source_settled,
	'L3 Resource-X source transfer reached exact settlement ACK');
unlike($all_workload_log, qr/cluster PCM-X runtime fail-closed/,
	'L3 Resource-X gate emitted no fail-closed transition');
ok($all_retired_debug_absent,
	'L3 deleted legacy diagnostic keys remain absent on all nodes');
ok($canonical_resource_x_formation > 0,
	'L3 native master traffic reports one exact current Resource-X formation');
ok(!(grep { !$_ } @bit10_full_open_applied_by_node),
	'L3 all nodes retain exact full-member bit10 OPEN_APPLIED');
ok($all_transport_not_admitted_exact,
	'L3 admitted native traffic added no LMS admission refusal');
ok($all_transport_cap_guard_exact,
	'L3 admitted native traffic added no LMS capability-guard drop');
ok($all_wait_edges_drained,
	'L3 native Resource-X WFG edges returned to their exact baselines');
ok($grant_after_image_delta >= 0 && $image_at_or_after_grant_delta >= 0,
	'L3 Resource-X remote install ordering counters remained monotone');
is($grant_after_image_delta + $image_at_or_after_grant_delta,
	$remote_install_delta,
	'L3 every remote install entered exactly one Resource-X ordering bucket');
for my $key (@resource_x_excluded_keys)
{
	is($resource_x_o1_after{$key} - $resource_x_o1_before{$key}, 0,
		"L3 Resource-X happy path left $key unchanged");
}
for my $key (qw(
	last_remote_t_image_us
	last_remote_t_grant_us
	last_remote_t_install_us))
{
	cmp_ok($resource_x_o1_after{$key}, '>', 0,
		"L3 Resource-X happy path published nonzero $key");
}
ok($all_gcs_slots_drained,
	'L3 native Resource-X GCS request slots drained to zero');
ok($all_native_formations_exact,
	'L3 every observed kind-9 round used the current formation');
is($data_worker_buffer_content_wait_samples, 0,
	'L3 DATA workers never waited on BufferContent while receive progress depended on them');
ok($terminal_drained,
	'L3 Resource-X slot and WFG terminal state drained within 30 seconds');
ok($native_install_settled,
	'L3 Resource-X T2/T3 install reached exact phase-5 settlement');
ok($native_source_settled,
	'L3 Resource-X terminal source settlement was observed');
ok($native_authority_grant_success,
	'L3 Resource-X remote authority grant was observed');
ok($native_type17_success,
	'L3 Resource-X terminal holder transition was observed');
ok($all_transport_requeue_exact,
	'L3 terminal drain added no LMS transport requeue drop');
ok($all_source_removal_facts,
	'L3 all seven source-removal static facts remain true');
is(scalar(grep { $_ } @node_target_ok), 4,
	'L3 four real node-local writers committed through Resource-X');
ok($kind9_request_count > 0 && $terminal_drained,
	'L3 native kind-9 traffic reached exact slot/WFG terminal drain');

for my $i (0 .. 3)
{
	my $log = slurp_file($quad->node($i)->logfile);
	my $workload_log = substr($log, $workload_log_offsets[$i]);
	unlike($workload_log, qr/\b(?:FATAL|PANIC):/,
		"L3 node$i log contains no FATAL or PANIC");
	unlike($workload_log, qr/owner-plane[^\n]*(?:violation|corrupt|fail(?:ed|ure)?)/i,
		"L3 node$i log contains no owner-plane violation");
}

my ($advanced_rc, $advanced, $advanced_err) = $quad->node0->psql('postgres', q{
	SELECT string_agg(id::text || ':' || row_count::text || ':' || total_v::text,
		',' ORDER BY id)
	FROM (
		SELECT id, count(*) AS row_count, sum(v) AS total_v
		FROM pcm_xq_hot
		GROUP BY id
	) exact_rows
}, timeout => 30);
my $expected_exact = join(',', map {
	my $id = $_ + 1;
	$id . ':1:' . $runs[$_]->{transactions}
} (0 .. 3));
is($advanced_rc, 0, 'L4 final exact-conservation query completed');
is($advanced, $expected_exact,
	'L4 every logical id is unique and its value equals that node processed transactions')
	or diag("L4 expected=[$expected_exact] stderr=[$advanced_err]");

my ($sum_rc, $sum_v, $sum_err) = $quad->node0->psql('postgres',
	q{SELECT coalesce(sum(v), 0) FROM pcm_xq_hot}, timeout => 30);
my $expected_sum = 0;
$expected_sum += $runs[$_]->{transactions} for (0 .. 3);
is($sum_rc, 0, 'L4 aggregate conservation query completed');
is($sum_v, "$expected_sum",
	'L4 aggregate value equals total committed pgbench transactions')
	or diag("L4 expected_sum=$expected_sum stderr=[$sum_err]");

$quad->stop_quad;
done_testing();
