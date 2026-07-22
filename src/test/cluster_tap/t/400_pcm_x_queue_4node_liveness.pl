#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 400_pcm_x_queue_4node_liveness.pl
#    spec-2.36a S3-core RED/GREEN: four nodes concurrently update four
#    different tuples that occupy the same heap BufferTag.  Every writer
#    must make progress without surfacing a client error.
#
# Author: SqlRush <sqlrush@gmail.com>
#
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use IPC::Run qw(start finish timeout);
use PostgreSQL::Test::ClusterQuad;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(time usleep);

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

my @pcm_x_pcm_keys = qw(
	pcm_x_runtime_state
	pcm_x_runtime_generation
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

my @pcm_x_lmd_keys = qw(
	wait_edge_count
	pcm_convert_wfg_replace_count
	pcm_convert_wfg_remove_count
	pcm_convert_wfg_replace_fail_count
	pcm_convert_wfg_exact_remove_stale_count
);

my @pcm_x_final_gauge_keys = qw(
	pcm_x_queue_depth
	pcm_x_queue_active_tags
	pcm_x_queue_live_tickets
	pcm_x_queue_live_slots
	pcm_x_local_retire_gate
	pcm_x_local_retire_marker_count
	pcm_x_local_retire_marker_ticket_id
);

my @positive_pcm_lifecycle_keys = qw(
	pcm_x_queue_enqueue_count
	pcm_x_queue_admit_count
	pcm_x_queue_confirm_count
	pcm_x_queue_promotion_count
	pcm_x_queue_transfer_count
	pcm_x_queue_complete_count
	pcm_x_queue_revoke_count
	pcm_x_queue_wait_count
	pcm_x_own_begin_count
	pcm_x_own_commit_count
);

my @zero_pcm_failure_keys = qw(
	pcm_x_queue_cancel_count
	pcm_x_queue_full_count
	pcm_x_queue_recovery_blocked_count
	pcm_x_queue_activating_reset_count
	pcm_x_own_abort_count
	pcm_x_own_corrupt_count
);

my @positive_wfg_keys = qw(
	pcm_convert_wfg_replace_count
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

sub wait_for_pcm_gauges_zero
{
	my ($quad, $all_keys, $gauge_keys, $timeout_seconds) = @_;
	my $gauge_deadline = time() + $timeout_seconds;
	my @snapshots;

	do
	{
		@snapshots = map {
			state_snapshot($quad->node($_), 'pcm', $all_keys)
		} (0 .. 3);
		my $all_zero = 1;

		for my $snapshot (@snapshots)
		{
			for my $key (@{$gauge_keys})
			{
				$all_zero = 0 if $snapshot->{$key} != 0;
			}
		}
		return (1, \@snapshots) if $all_zero;
		usleep(250_000);
	} while (time() < $gauge_deadline);

	return (0, \@snapshots);
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
		$expected_value, $timeout_seconds) = @_;
	my $deadline = time() + $timeout_seconds;
	my $last_log = '';

	do
	{
		$last_log = substr(slurp_file($node->logfile), $log_offset);
		my %ready_workers;
		while ($last_log =~ /cluster_lms: DATA worker=(\d+) applied PCM-X finish Flush injection config: pid=\d+ armed=(true|false) value="([^"]*)"/g)
		{
			my ($worker_id, $armed, $value) = ($1, $2, $3);
			next unless $armed eq $expected_armed && $value eq $expected_value;
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

my $quad = PostgreSQL::Test::ClusterQuad->new_quad(
	'pcm_xq_liveness',
	quorum_voting_disks => 3,
	shared_data         => 1,
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

for my $node ($quad->nodes)
{
	is($node->safe_psql('postgres', 'SHOW cluster.xid_striping'), 'on',
		'L1 xid striping is active on the writer topology');
	is($node->safe_psql('postgres', 'SHOW cluster.crossnode_runtime_visibility'), 'on',
		'L1 runtime visibility is active on the writer topology');
	is($node->safe_psql('postgres', 'SHOW cluster.gcs_block_local_cache'), 'on',
		'L1 hold-until-revoked cache is active by default');
		cmp_ok(state_int($node, 'xid_stripe', 'xid_stripe_activated_floor'), '>', 0,
			'L1 xid stripe activation floor is published');
		is(exact_key_count($node, 'pcm', \@pcm_x_pcm_keys), scalar(@pcm_x_pcm_keys),
			'L1 PCM-X PCM observability key set is complete');
		is(exact_key_count($node, 'lmd', \@pcm_x_lmd_keys), scalar(@pcm_x_lmd_keys),
			'L1 PCM-X LMD observability key set is complete');
		state_int($node, 'pcm', $_) for @pcm_x_pcm_keys;
		state_int($node, 'lmd', $_) for @pcm_x_lmd_keys;
		is(state_int($node, 'pcm', 'pcm_x_runtime_state'), 1,
			'L1 PCM-X runtime is ACTIVE before the workload');
		cmp_ok(state_int($node, 'pcm', 'pcm_x_runtime_generation'), '>', 0,
			'L1 PCM-X runtime generation is published before the workload');
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
		) WITH (fillfactor = 100);
		CREATE TABLE pcm_xq_flush_error (
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
# unique N->S grant.  The following node0 UPDATE must therefore take the
# sole-requester S->X handoff, not a remote-source transfer or cached-X path.
my ($self_read_rc, $self_read_out, $self_read_err) = $quad->node0->psql(
	'postgres', q{SELECT v FROM pcm_xq_self WHERE id = 1}, timeout => 30);
is($self_read_rc, 0, 'L2S sole requester acquired the only S copy');
is($self_read_out, '0', 'L2S sole requester saw the seeded image')
	or diag("L2S sole requester read stderr=[$self_read_err]");

my $self_handoff_before = state_int($quad->node0, 'gcs',
	'pcm_x_self_handoff_count');
my $self_handoff_drain_before = state_int($quad->node0, 'gcs',
	'pcm_x_self_handoff_drain_count');
my @self_runtime_before_by_node = map {
	{
		runtime_state => state_int($quad->node($_), 'pcm',
			'pcm_x_runtime_state'),
		recovery_blocked => state_int($quad->node($_), 'pcm',
			'pcm_x_queue_recovery_blocked_count'),
	}
} (0 .. 3);

my ($self_write_rc, $self_write_out, $self_write_err) = $quad->node0->psql(
	'postgres', q{UPDATE pcm_xq_self SET v = v + 1 WHERE id = 1}, timeout => 30);
is($self_write_rc, 0, 'L2S sole-S requester completed S-to-X conversion')
	or diag("L2S sole-S write stdout=[$self_write_out] stderr=[$self_write_err]");

my ($self_handoff_after, $self_handoff_drain_after);
my $self_drain_deadline = time() + 15;
# NB: a do{}while body is not a loop block in Perl -- "last" inside one is a
# runtime error that fired exactly when the polled counters advanced.
while (1)
{
	$self_handoff_after = state_int($quad->node0, 'gcs',
		'pcm_x_self_handoff_count');
	$self_handoff_drain_after = state_int($quad->node0, 'gcs',
		'pcm_x_self_handoff_drain_count');
	last if $self_handoff_after > $self_handoff_before
		&& $self_handoff_drain_after > $self_handoff_drain_before;
	last if time() >= $self_drain_deadline;
	usleep(100_000);
}

cmp_ok($self_handoff_after - $self_handoff_before, '>', 0,
	'L2S sole-requester S source exercised the fused revoke-to-grant handoff');
cmp_ok($self_handoff_drain_after - $self_handoff_drain_before, '>', 0,
	'L2S sole-requester S source released its immutable record at DRAIN');
for my $i (0 .. 3)
{
	is(state_int($quad->node($i), 'pcm', 'pcm_x_runtime_state'), 1,
		"L2S node$i PCM-X runtime remained ACTIVE across DRAIN/RETIRE");
	is(state_int($quad->node($i), 'pcm',
			'pcm_x_queue_recovery_blocked_count'),
		$self_runtime_before_by_node[$i]{recovery_blocked},
		"L2S node$i recovery-blocked count did not advance at RETIRE");
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

# The source INSERT leaves an uncheckpointed dirty X image on node0.  A
# different node must materialize that exact image and retain it only after
# FlushBuffer's caller-owned pin contract is satisfied.  This is a behavioral
# gate: the immutable staging counter proves the DATA worker reached the dirty
# remote-source leg, while h_pi_write_note_count increments only after
# FlushBuffer's smgrwrite returned, proving the real flush completed.
my $dirty_stage_before = state_int($quad->node0, 'gcs',
	'dedup_pcm_x_stage_count');
my $dirty_flush_before = state_int($quad->node0, 'xnode_lever',
	'h_pi_write_note_count');
my $dirty_flush_log_offset = (-s $quad->node0->logfile) // 0;
my $dirty_requester_log_offset = (-s $quad->node1->logfile) // 0;
my $dirty_lms_workers = $quad->node0->safe_psql('postgres',
	'SHOW cluster.lms_workers') + 0;
my $dirty_requester_lms_workers = $quad->node1->safe_psql('postgres',
	'SHOW cluster.lms_workers') + 0;
$quad->node0->safe_psql('postgres', q{
	ALTER SYSTEM SET cluster.injection_points = 'cluster-pcm-x-retain-flush-error';
	SELECT pg_reload_conf()
});
$quad->node1->safe_psql('postgres', q{
	ALTER SYSTEM SET cluster.injection_points = 'cluster-pcm-x-retain-flush-error';
	SELECT pg_reload_conf()
});
my ($dirty_reload_ready, $dirty_reload_log) = wait_for_lms_finish_flush_reload(
	$quad->node0, $dirty_flush_log_offset, $dirty_lms_workers, 'true',
	'cluster-pcm-x-retain-flush-error', 15);
ok($dirty_reload_ready,
	'L2F every node0 DATA worker applied the finish-Flush injection arm')
	or diag("L2F DATA-worker reload log=[$dirty_reload_log]");
my ($dirty_requester_reload_ready, $dirty_requester_reload_log)
	= wait_for_lms_finish_flush_reload(
		$quad->node1, $dirty_requester_log_offset, $dirty_requester_lms_workers, 'true',
		'cluster-pcm-x-retain-flush-error', 15);
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
cmp_ok(state_int($quad->node0, 'gcs', 'dedup_pcm_x_stage_count')
		- $dirty_stage_before, '>', 0,
	'L2F source DATA worker materialized the immutable dirty image');
cmp_ok(state_int($quad->node0, 'xnode_lever', 'h_pi_write_note_count')
		- $dirty_flush_before, '>', 0,
	'L2F source FlushBuffer completed smgrwrite before ownership commit');
my $dirty_flush_log = substr(slurp_file($quad->node0->logfile),
	$dirty_flush_log_offset);
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
	ALTER SYSTEM RESET cluster.injection_points;
	SELECT pg_reload_conf()
});
$quad->node1->safe_psql('postgres', q{
	ALTER SYSTEM RESET cluster.injection_points;
	SELECT pg_reload_conf()
});
my ($dirty_reset_ready, $dirty_reset_log) = wait_for_lms_finish_flush_reload(
	$quad->node0, $dirty_flush_log_offset, $dirty_lms_workers, 'false', '', 15);
ok($dirty_reset_ready,
	'L2F every node0 DATA worker applied the finish-Flush injection disarm')
	or diag("L2F DATA-worker reset log=[$dirty_reset_log]");
my ($dirty_requester_reset_ready, $dirty_requester_reset_log)
	= wait_for_lms_finish_flush_reload(
		$quad->node1, $dirty_requester_log_offset, $dirty_requester_lms_workers, 'false', '', 15);
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
my @pcm_before_by_node = map {
	state_snapshot($quad->node($_), 'pcm', \@pcm_x_pcm_keys)
} (0 .. 3);
my @lmd_before_by_node = map {
	state_snapshot($quad->node($_), 'lmd', \@pcm_x_lmd_keys)
} (0 .. 3);
my %pcm_before = %{aggregate_snapshots(\@pcm_before_by_node, \@pcm_x_pcm_keys)};
my %lmd_before = %{aggregate_snapshots(\@lmd_before_by_node, \@pcm_x_lmd_keys)};
my $queue_before = $pcm_before{pcm_x_queue_enqueue_count};
my $denied_before = 0;
$denied_before += state_int($quad->node($_), 'gcs',
	'starvation_denied_pending_x_count') for (0 .. 3);
my $passive_s_before = 0;
$passive_s_before += state_int($quad->node($_), 'gcs',
	'invalidate_passive_s_release_count') for (0 .. 3);
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
	my @samples;

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
			my $slots = eval {
				$quad->node($i)->safe_psql('postgres',
					q{SELECT string_agg(key || '=[' || value || ']', ' ' ORDER BY key)
					  FROM pg_cluster_state
					  WHERE category = 'pcm'
						AND (key LIKE 'pcm_x_tag_%' OR key LIKE 'pcm_x_ticket_%'
							OR key LIKE 'pcm_x_ltag_%' OR key = 'pcm_x_terminal_last_note')},
					timeout => 10);
			} // 'probe-failed';
			$slots =~ s/\n/ | /g;
			$samples[$offset][$i] = eval {
				state_snapshot($quad->node($i), 'pcm', \@pcm_x_pcm_keys);
			};
			diag("L3 mid-leg t+$offset node$i waits=[$waits]"
				. ($samples[$offset][$i] ? '' : ' snapshot-failed'));
			diag("L3 mid-leg t+$offset node$i aux=[$aux]");
			diag("L3 mid-leg t+$offset node$i DATA BufferContent waits="
				. $data_content_waits);
			diag("L3 mid-leg t+$offset node$i wire=[$wire]");
			diag("L3 mid-leg t+$offset node$i slots=[$slots]");
		}
	}

	# Early samples catch the terminal handoff mid-flight; the late pair
	# separates the live retry loop (still ticking) from the wedged stages
	# (frozen).
	for my $pair ([1, 3], [3, 8])
	{
		my ($from, $to) = @{$pair};
		for my $i (0 .. 3)
		{
			next unless $samples[$from][$i] && $samples[$to][$i];
			my @moved;
			for my $key (@pcm_x_pcm_keys)
			{
				my $d = $samples[$to][$i]{$key} - $samples[$from][$i]{$key};
				push @moved, "$key:+$d" if $d != 0;
			}
			diag("L3 mid-leg node$i t+$from..t+$to moved: "
				. (@moved ? join(' ', @moved) : '(all frozen)'));
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

my ($gauges_drained, $pcm_after_by_node_ref) = wait_for_pcm_gauges_zero(
	$quad, \@pcm_x_pcm_keys, \@pcm_x_final_gauge_keys, 30);
my @pcm_after_by_node = @{$pcm_after_by_node_ref};
my @lmd_after_by_node = map {
	state_snapshot($quad->node($_), 'lmd', \@pcm_x_lmd_keys)
} (0 .. 3);
my %pcm_after = %{aggregate_snapshots(\@pcm_after_by_node, \@pcm_x_pcm_keys)};
my %lmd_after = %{aggregate_snapshots(\@lmd_after_by_node, \@pcm_x_lmd_keys)};
my $queue_after = $pcm_after{pcm_x_queue_enqueue_count};
my $denied_after = 0;
$denied_after += state_int($quad->node($_), 'gcs',
	'starvation_denied_pending_x_count') for (0 .. 3);
my $passive_s_after = 0;
$passive_s_after += state_int($quad->node($_), 'gcs',
	'invalidate_passive_s_release_count') for (0 .. 3);
my @holder_evicted_after_by_node = map {
	state_int($quad->node($_), 'gcs', 'block_forward_holder_evicted_count')
} (0 .. 3);
diag('L3 path probes: pcm_x_queue_enqueue_delta='
	. ($queue_after - $queue_before)
	. ' legacy_denied_pending_x_delta='
	. ($denied_after - $denied_before)
	. ' passive_s_release_delta='
	. ($passive_s_after - $passive_s_before));
for my $i (0 .. 3)
{
	diag("L3 node$i holder copy refusal baseline=$holder_evicted_before_by_node[$i]"
		. " final=$holder_evicted_after_by_node[$i] delta="
		. ($holder_evicted_after_by_node[$i] - $holder_evicted_before_by_node[$i]));
}

# Per-node runtime probe: the aggregate sums above cannot distinguish which
# node fused.  Name the fused node and its fail-closed arm (file:line).
for my $i (0 .. 3)
{
	my $site = $quad->node($i)->safe_psql('postgres',
		q{SELECT value FROM pg_cluster_state
		  WHERE category = 'pcm' AND key = 'pcm_x_runtime_fail_closed_site'});
	diag("L3 node$i runtime probe:"
		. " state=$pcm_after_by_node[$i]{pcm_x_runtime_state}"
		. " generation=$pcm_after_by_node[$i]{pcm_x_runtime_generation}"
		. ' generation_delta='
		. ($pcm_after_by_node[$i]{pcm_x_runtime_generation}
			- $pcm_before_by_node[$i]{pcm_x_runtime_generation})
		. ' recovery_blocked_delta='
		. ($pcm_after_by_node[$i]{pcm_x_queue_recovery_blocked_count}
			- $pcm_before_by_node[$i]{pcm_x_queue_recovery_blocked_count})
		. " fail_closed_site=[$site]");
	is($pcm_after_by_node[$i]{pcm_x_runtime_state}, 1,
		"L3 node$i PCM-X runtime remained ACTIVE");
	is($pcm_after_by_node[$i]{pcm_x_runtime_generation},
		$pcm_before_by_node[$i]{pcm_x_runtime_generation},
		"L3 node$i PCM-X runtime generation stayed exact");
	is($site, '(none)', "L3 node$i PCM-X fail-closed site stayed empty");
	is($lmd_after_by_node[$i]{wait_edge_count},
		$lmd_before_by_node[$i]{wait_edge_count},
		"L3 node$i WFG live edge count returned to baseline");
	for my $key (@pcm_x_final_gauge_keys)
	{
		is($pcm_after_by_node[$i]{$key}, 0,
			"L3 node$i final PCM-X gauge $key is zero");
	}
}
for my $key (@pcm_x_pcm_keys)
{
	diag("L3 PCM-X state $key=$pcm_after{$key} delta="
		. ($pcm_after{$key} - $pcm_before{$key}));
}
for my $key (@pcm_x_lmd_keys)
{
	diag("L3 PCM-X LMD state $key=$lmd_after{$key} delta="
		. ($lmd_after{$key} - $lmd_before{$key}));
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
			'pcm_x_self_handoff_count',
			'pcm_x_self_handoff_drain_count',
			'dedup_pcm_x_stage_count',
			'dedup_pcm_x_replay_count',
			'dedup_pcm_x_release_count',
			'dedup_pcm_x_failclosed_count',
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

for my $i (0 .. 3)
{
	is($runs[$i]->{timed_out}, 0, "L3 node$i writer met the hard deadline");
	is($runs[$i]->{result}, 0, "L3 node$i writer exited successfully");
	is($runs[$i]->{errors}, 0, "L3 node$i writer surfaced zero client errors");
	cmp_ok($runs[$i]->{transactions}, '>', 0,
		"L3 node$i writer made progress");
	cmp_ok($pcm_after_by_node[$i]{pcm_x_queue_wait_count}
			- $pcm_before_by_node[$i]{pcm_x_queue_wait_count}, '>', 0,
		"L3 node$i requester identity entered the queue wait lifecycle");
}

# Queue counters live on the static tag master, not on each requester.  The
# aggregate lifecycle plus every writer's committed progress proves that all
# four requesters traversed the protocol; requiring a local enqueue delta on
# non-master nodes is a false topology assumption.
cmp_ok($queue_after - $queue_before, '>=', 4,
	'L3 aggregate PCM-X queue admission floor reached four');
cmp_ok($denied_after - $denied_before, '>', 0,
	'L3 Shape-B arbitration denied an in-flight legacy reader without a client error');
cmp_ok($passive_s_after - $passive_s_before, '>', 0,
	'L3 exact queue INVALIDATE exercised passive-pinned S release');
for my $key (@positive_pcm_lifecycle_keys)
{
	cmp_ok($pcm_after{$key} - $pcm_before{$key}, '>', 0,
		"L3 PCM-X lifecycle $key advanced");
}
for my $key (@zero_pcm_failure_keys)
{
	is($pcm_after{$key} - $pcm_before{$key}, 0,
		"L3 PCM-X failure counter $key stayed zero");
}
# STALE/NOT_FOUND are retry classifications used by generation-exact role
# refresh and idempotent replay.  They are observable churn, not terminal
# failures; zero terminal gauges, zero client errors, and L4 conservation are
# the authoritative safety/liveness verdicts.
for my $key (@positive_wfg_keys)
{
	cmp_ok($lmd_after{$key} - $lmd_before{$key}, '>', 0,
		"L3 PCM-X WFG lifecycle $key advanced");
}
# Atomic replacement with a zero-blocker set removes the old waiter edges but
# intentionally counts as a replace, so an explicit remove call is not
# required on every successful run.
is($lmd_after{pcm_convert_wfg_replace_fail_count}
		- $lmd_before{pcm_convert_wfg_replace_fail_count},
	0, 'L3 PCM-X WFG atomic replace failures stayed zero');
is($lmd_after{pcm_convert_wfg_exact_remove_stale_count}
		- $lmd_before{pcm_convert_wfg_exact_remove_stale_count},
	0, 'L3 PCM-X WFG exact-remove stale identities stayed zero');
is($data_worker_buffer_content_wait_samples, 0,
	'L3 DATA workers never waited on BufferContent while receive progress depended on them');
ok($gauges_drained, 'L3 PCM-X terminal gauges drained within 30 seconds');
for my $key (@pcm_x_final_gauge_keys)
{
	is($pcm_after{$key}, 0, "L3 final aggregate PCM-X gauge $key is zero");
}

my $successful_transactions = 0;
$successful_transactions += $runs[$_]->{transactions} for (0 .. 3);
my $stale_miss_delta
	= ($pcm_after{pcm_x_queue_stale_count} - $pcm_before{pcm_x_queue_stale_count})
	+ ($pcm_after{pcm_x_queue_miss_count} - $pcm_before{pcm_x_queue_miss_count});
cmp_ok($stale_miss_delta, '<=', 4 * $successful_transactions + 16,
	'L3 retryable stale/miss churn stayed within the per-transaction budget');

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

# The destructive leg is deliberately last: its expected outcome is a
# fail-closed runtime, so no later assertion may depend on ACTIVE or drained
# gauges.  GUC+reload is required because injection state is process-local;
# the DATA worker, not this SQL backend, executes the finish boundary.
ok(write_retry($quad->node0,
	q{INSERT INTO pcm_xq_flush_error(id, v) VALUES (1, 0)}),
	'L5F seeded the direct-X target page');
ok(write_retry($quad->node0, 'VACUUM pcm_xq_flush_error'),
	'L5F published target-page free space before the direct-X request');
ok(write_retry($quad->node0, 'CHECKPOINT'),
	'L5F checkpointed before the destructive finish-Flush test');
my $flush_error_relfilenode = $quad->node0->safe_psql('postgres',
	q{SELECT pg_relation_filenode('pcm_xq_flush_error'::regclass)}) + 0;
my $flush_error_stage_before = state_int($quad->node0, 'gcs',
	'dedup_pcm_x_stage_count');
my $flush_error_release_before = state_int($quad->node0, 'gcs',
	'dedup_pcm_x_release_count');
my $flush_error_blocked_before = state_int($quad->node0, 'pcm',
	'pcm_x_queue_recovery_blocked_count');
my $flush_error_log_offset = (-s $quad->node0->logfile) // 0;

$quad->node0->safe_psql('postgres', q{
	ALTER SYSTEM SET cluster.injection_points =
		'cluster-pcm-x-retain-flush-error:skipn:1';
	SELECT pg_reload_conf()
});
my ($flush_error_reload_ready, $flush_error_reload_log)
	= wait_for_lms_finish_flush_reload(
		$quad->node0, $flush_error_log_offset, $dirty_lms_workers, 'true',
		'cluster-pcm-x-retain-flush-error:skipn:1', 15);
ok($flush_error_reload_ready,
	'L5F every node0 DATA worker applied the one-shot finish-Flush fault arm')
	or diag("L5F DATA-worker reload log=[$flush_error_reload_log]");

my ($flush_seed_rc, $flush_seed_out, $flush_seed_err) = $quad->node0->psql(
	'postgres', q{UPDATE pcm_xq_flush_error SET v = 1 WHERE id = 1}, timeout => 30);
is($flush_seed_rc, 0, 'L5F node0 created the destructive-leg X source')
	or diag("L5F seed stdout=[$flush_seed_out] stderr=[$flush_seed_err]");
my ($flush_error_rc, $flush_error_out, $flush_error_err) = $quad->node1->psql(
	'postgres', q{
		SET statement_timeout = '5s';
		INSERT INTO pcm_xq_flush_error(id, v) VALUES (2, 1)
	}, timeout => 30);
isnt($flush_error_rc, 0,
	'L5F remote writer failed when finish FlushBuffer raised the injected ERROR')
	or diag("L5F unexpected success stdout=[$flush_error_out] stderr=[$flush_error_err]");

my ($flush_error_runtime_after, $flush_error_blocked_after);
my $flush_error_deadline = time() + 15;
while (1)
{
	$flush_error_runtime_after = state_int($quad->node0, 'pcm',
		'pcm_x_runtime_state');
	$flush_error_blocked_after = state_int($quad->node0, 'pcm',
		'pcm_x_queue_recovery_blocked_count');
	last if $flush_error_runtime_after == 0
		&& $flush_error_blocked_after > $flush_error_blocked_before;
	last if time() >= $flush_error_deadline;
	usleep(100_000);
}

is($quad->node0->safe_psql('postgres', 'SELECT 1'), '1',
	'L5F node0 postmaster remained alive after the DATA-worker ERROR');
is($flush_error_runtime_after, 0,
	'L5F node0 PCM-X runtime moved to RECOVERY_BLOCKED');
cmp_ok($flush_error_blocked_after - $flush_error_blocked_before, '>', 0,
	'L5F recovery-blocked counter recorded the finish error');
cmp_ok(state_int($quad->node0, 'gcs', 'dedup_pcm_x_stage_count')
		- $flush_error_stage_before, '>', 0,
	'L5F immutable A-record reached MATERIALIZED_UNCOMMITTED before ERROR');
my $flush_error_log = substr(slurp_file($quad->node0->logfile),
	$flush_error_log_offset);
like($flush_error_log,
	qr/PCM-X finish-error evidence exact.*?preserve_result=12 retained=true worker=\d+ tag=\d+\/\d+\/\Q$flush_error_relfilenode\E\/0\/0 requester=1 backend=\d+ request_id=\d+ ticket=\d+ queue_generation=\d+ grant_generation=\d+ image_id=\d+ reservation_token=\d+ source_state=\d+/s,
	'L5F exact immutable A-record remains retained after ERROR');
is(state_int($quad->node0, 'gcs', 'dedup_pcm_x_release_count'),
	$flush_error_release_before,
	'L5F ERROR did not release immutable evidence or its REVOKING fence');
like($flush_error_log,
	qr/injected PCM-X retained-image FlushBuffer failure/,
	'L5F exact pre-smgrwrite finish FlushBuffer ERROR reached the DATA worker');
like($flush_error_log,
	qr/cluster PCM-X runtime fail-closed \(recovery blocked\)/,
	'L5F GCS finish catch preserved evidence and fused the runtime');

$quad->stop_quad;
my $flush_error_shutdown_log = substr(slurp_file($quad->node0->logfile),
	$flush_error_log_offset);
unlike($flush_error_shutdown_log, qr/lost track of buffer IO/,
	'L5F absorbed FlushBuffer ERROR left no ResourceOwner BufferIO at shutdown');
done_testing();
