#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 406_resource_x_finish_flush_failclosed_4node.pl
#    Four-node exact-target Resource-X retained-source FlushBuffer failure.
#    A nonmatching decoy must complete without consuming the one-shot; the
#    matching target must fail before smgrwrite and retain fail-closed proof.
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use PostgreSQL::Test::ClusterQuad;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(time usleep);

my $pgrd_voting_file_bytes = (8 * 128 + 3) * 512;
my $quad;
my $quad_started = 0;
my $quad_stopped = 0;

END
{
	if (defined($quad) && $quad_started && !$quad_stopped)
	{
		eval {
			$quad->node0->psql('postgres', q{
				ALTER SYSTEM SET cluster.injection_points =
					'cluster-pcm-x-retain-flush-error:none:0';
				ALTER SYSTEM SET cluster.pcm_x_retain_flush_error_target = '';
				SELECT pg_reload_conf()
			}, timeout => 15);
		};
		eval { $quad->stop_quad; };
	}
}

sub state_int
{
	my ($node, $category, $key) = @_;
	my $value = $node->safe_psql('postgres',
		qq{SELECT value FROM pg_cluster_state
		   WHERE category='$category' AND key='$key'});
	die "missing or non-integer pg_cluster_state key $category.$key: ["
		. (defined($value) ? $value : '<undef>') . "]\n"
		unless defined($value) && $value =~ /\A\d+\z/;
	return $value + 0;
}

sub write_retry
{
	my ($node, $sql, $attempts) = @_;
	$attempts //= 20;
	my ($last_rc, $last_out, $last_err);

	for (1 .. $attempts)
	{
		my ($rc, $out, $err) =
			$node->psql('postgres', $sql, timeout => 30);
		($last_rc, $last_out, $last_err) = ($rc, $out, $err);
		return 1 if defined($rc) && $rc == 0;
		usleep(300_000);
	}
	diag('setup retry exhausted: rc='
		. (defined($last_rc) ? $last_rc : '<undef>')
		. ' stdout=[' . ($last_out // '') . '] stderr=['
		. ($last_err // '') . ']');
	return 0;
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
		die "$label activation result is unknown after SQL timeout: "
			. ($err // '<undef>') . "\n"
			unless defined($rc);
		die "$label activation failed outside the retry contract: "
			. ($err // '<undef>') . "\n"
			unless defined($err)
				&& $err =~ /(?:RF_DEFERRED|CONDITION_NOT_YET_MET|activation request was refused)/;
		usleep(100_000);
	}
	die "$label activation did not reach OPEN_APPLIED: rc="
		. (defined($last_rc) ? $last_rc : '<undef>')
		. ' stdout=[' . ($last_out // '') . '] stderr=['
		. ($last_err // '') . "]\n";
}

sub wait_for_preopen_rf_deferred
{
	my ($node) = @_;
	my $deadline = time() + 15;
	my ($last_rc, $last_out, $last_err);

	while (time() < $deadline)
	{
		($last_rc, $last_out, $last_err) = $node->psql(
			'postgres',
			'ALTER SYSTEM ENABLE RAC TWO_STAGE ROLLING UPDATES ALL',
			timeout => 30);
		return (1, $last_err)
			if defined($last_rc) && $last_rc != 0
				&& defined($last_err)
				&& $last_err =~ /(?:RF_DEFERRED|CONDITION_NOT_YET_MET)/;
		return (0, 'unexpected activation success')
			if defined($last_rc) && $last_rc == 0;
		return (0, 'unknown timeout outcome')
			unless defined($last_rc);
		return (0, $last_err // '<undef>')
			if defined($last_err)
				&& $last_err !~ /cluster semantic activation request was refused/;
		usleep(100_000);
	}
	return (0, 'deadline expired: ' . ($last_err // '<undef>'));
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
		return (1, $last_log)
			if scalar(keys %ready_workers) == $expected_workers;
		usleep(100_000);
	} while (time() < $deadline);

	return (0, $last_log);
}

sub exact_finish_flush_tags
{
	my ($node, $shared_root) = @_;
	die "finish-Flush shared data root is unavailable\n"
		unless defined($shared_root) && $shared_root ne '';
	my $rows = $node->safe_psql('postgres', q{
		SELECT c.relname || '|' ||
			COALESCE(NULLIF(c.reltablespace, 0), d.dattablespace)::text || '|' ||
			d.oid::text || '|' || pg_relation_filenode(c.oid)::text || '|' ||
			0::text || '|' || 0::text || '|' ||
			pg_relation_filepath(c.oid)
		FROM pg_class AS c
		JOIN pg_database AS d ON d.datname = current_database()
		WHERE c.oid IN ('pcm_x_flush_decoy'::regclass,
						'pcm_x_flush_target'::regclass)
		ORDER BY c.relname
	});
	my @rows = split(/\n/, $rows, -1);
	pop @rows if @rows && $rows[-1] eq '';
	die "finish-Flush tag projection returned " . scalar(@rows)
		. " rows: [$rows]\n" unless @rows == 2;

	my %tags;
	for my $row (@rows)
	{
		my @fields = split(/\|/, $row, -1);
		die "finish-Flush catalog tag projection malformed: [$row]\n"
			unless @fields == 7;
		my ($relname, $spc_oid, $db_oid, $rel_number, $fork_number,
			$block_number, $relation_path) = @fields;
		die "finish-Flush catalog tag relation is unexpected: [$relname]\n"
			unless $relname eq 'pcm_x_flush_decoy'
				|| $relname eq 'pcm_x_flush_target';
		die "finish-Flush catalog tag relation is duplicated: [$relname]\n"
			if exists($tags{$relname});
		die "finish-Flush catalog tag contains a non-decimal identity: [$row]\n"
			unless $spc_oid =~ /\A\d+\z/
				&& $db_oid =~ /\A\d+\z/
				&& $rel_number =~ /\A\d+\z/
				&& $fork_number =~ /\A\d+\z/
				&& $block_number =~ /\A\d+\z/;
		die "finish-Flush catalog tag contains an out-of-range identity: [$row]\n"
			unless $spc_oid > 0 && $spc_oid <= 4_294_967_295
				&& $db_oid > 0 && $db_oid <= 4_294_967_295
				&& $rel_number > 0 && $rel_number <= 4_294_967_295
				&& $fork_number == 0 && $block_number == 0;
		die "finish-Flush catalog path is not a safe main-fork path: "
			. "[$relation_path]\n"
			unless defined($relation_path) && $relation_path ne ''
				&& $relation_path !~ m{(?:\A|/)\.\.(?:/|\z)}
				&& $relation_path =~ m{(?:\A|/)\Q$rel_number\E\z};
		my $physical_path = "$shared_root/$relation_path";
		my $physical_bytes = -s $physical_path;
		die "finish-Flush shared main fork has no physical block 0: "
			. "[$physical_path]\n"
			unless defined($physical_bytes) && $physical_bytes > 0;
		$tags{$relname} = join('/', $spc_oid, $db_oid, $rel_number,
			$fork_number, $block_number);
	}
	die "finish-Flush catalog tag relation set is incomplete\n"
		unless exists($tags{pcm_x_flush_decoy})
			&& exists($tags{pcm_x_flush_target});
	return \%tags;
}

$quad = PostgreSQL::Test::ClusterQuad->new_quad(
	'rx_finish_flush_fail',
	quorum_voting_disks => 3,
	shared_data => 1,
	shared_system_identifier => 1,
	extra_conf => [
		'autovacuum = off',
		'cluster.lms_workers = 1',
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

my @voting_disks = $quad->voting_disk_paths;
die "t/406 requires exactly three voting disks\n"
	unless @voting_disks == 3;
for my $path (@voting_disks)
{
	truncate($path, $pgrd_voting_file_bytes)
		or die "extend $path to PGRD minimum: $!\n";
}
for my $node ($quad->nodes)
{
	$node->append_conf('postgresql.conf',
		"cluster.voting_disk_size_bytes = $pgrd_voting_file_bytes\n");
}

$quad->start_quad;
$quad_started = 1;
usleep(3_000_000);

for my $from (0 .. 3)
{
	is($quad->node($from)->safe_psql('postgres', 'SELECT 1'), '1',
		"F1 node$from is alive");
	for my $to (0 .. 3)
	{
		next if $from == $to;
		ok($quad->wait_for_peer_state($from, $to, 'connected', 45),
			"F1 node$from sees node$to connected");
	}
	ok($quad->node($from)->poll_query_until('postgres',
			q{SELECT in_quorum FROM pg_cluster_quorum_state}, 't'),
		"F1 node$from observes current quorum");
	is($quad->node($from)->safe_psql('postgres',
			'SHOW cluster.pcm_x_retain_flush_error_target'), '',
		"F1 node$from has the match-none finish-Flush target default");
}

my $pgrd_root = $quad->shared_data_root . '/pg_undo';
my $pgrd_mirror = "$pgrd_root/pgrac_undo_root.control";
mkdir $pgrd_root or die "mkdir $pgrd_root: $!\n";
for my $node_id (0 .. 3)
{
	my ($deferred, $deferred_detail) =
		wait_for_preopen_rf_deferred($quad->node($node_id));
	ok($deferred, "F1 node$node_id pre-OPEN setup returned typed RF_DEFERRED")
		or BAIL_OUT("node$node_id pre-OPEN result: $deferred_detail");
}
ok(-f $pgrd_mirror, 'F1 pre-OPEN PGRD mirror is present')
	or BAIL_OUT('pre-OPEN PGRD mirror was not published');

activate_semantic_round($quad->node0, 'R4 bit0');
activate_semantic_round($quad->node0, 'Resource-X bit10');
for my $node_id (0 .. 3)
{
	is($quad->node($node_id)->safe_psql('postgres', q{
		SELECT value FROM pg_cluster_state
		WHERE category='pcm' AND key='resource_x_gate_phase'
	}), 'open', "F1 node$node_id Resource-X gate is OPEN_APPLIED");
}

for my $node_id (0 .. 3)
{
	$quad->node($node_id)->safe_psql('postgres', q{
		CREATE TABLE pcm_x_flush_decoy (
			id integer,
			v bigint NOT NULL
		) WITH (fillfactor = 100);
		CREATE TABLE pcm_x_flush_target (
			id integer,
			v bigint NOT NULL
		) WITH (fillfactor = 100)
	});
}

for my $relation (qw(pcm_x_flush_decoy pcm_x_flush_target))
{
	my @paths = map {
		$quad->node($_)->safe_psql('postgres',
			"SELECT pg_relation_filepath('$relation')")
	} (0 .. 3);
	is(scalar(grep { $_ eq $paths[0] } @paths), 4,
		"F2 all nodes map $relation to the same relation file");
}

ok(write_retry($quad->node0,
	q{INSERT INTO pcm_x_flush_decoy(id, v) VALUES (1, 0)}),
	'F2 seeded the decoy block-0 page');
ok(write_retry($quad->node0,
	q{INSERT INTO pcm_x_flush_target(id, v) VALUES (1, 0)}),
	'F2 seeded the target block-0 page');
ok(write_retry($quad->node0, 'VACUUM pcm_x_flush_decoy'),
	'F2 published decoy block-0 free space');
ok(write_retry($quad->node0, 'VACUUM pcm_x_flush_target'),
	'F2 published target block-0 free space');
ok(write_retry($quad->node0, 'CHECKPOINT'),
	'F2 checkpointed both baseline pages');

my @tag_maps = map {
	exact_finish_flush_tags($quad->node($_), $quad->shared_data_root)
} (0 .. 3);
my @tag_projections = map {
	my $node_id = $_;
	join(',', map { $_ . '=' . $tag_maps[$node_id]->{$_} }
		qw(pcm_x_flush_decoy pcm_x_flush_target))
} (0 .. 3);
is(scalar(grep { $_ eq $tag_projections[0] } @tag_projections), 4,
	'F2 all nodes resolve the same two catalog-authoritative block-0 tags');
my $decoy_tag = $tag_maps[0]->{pcm_x_flush_decoy};
my $target_tag = $tag_maps[0]->{pcm_x_flush_target};
ok($decoy_tag ne $target_tag,
	'F2 decoy and fault target have distinct physical BufferTags');

my $arm_log_offset = (-s $quad->node0->logfile) // 0;
$quad->node0->safe_psql('postgres', q{
	ALTER SYSTEM SET cluster.pcm_x_retain_flush_error_target =
		'} . $target_tag . q{';
	ALTER SYSTEM SET cluster.injection_points =
		'cluster-pcm-x-retain-flush-error:skipn:1';
	SELECT pg_reload_conf()
});
my ($arm_ready, $arm_log) = wait_for_lms_finish_flush_reload(
	$quad->node0, $arm_log_offset, 1, 'true',
	'cluster-pcm-x-retain-flush-error:skipn:1', $target_tag, 15);
ok($arm_ready,
	'F3 node0 DATA worker applied the exact target one-shot fault arm')
	or diag("F3 reload log=[$arm_log]");

my $decoy_pi_before = state_int($quad->node0, 'xnode_lever',
	'h_pi_write_note_count');
my ($decoy_update_rc, $decoy_update_out, $decoy_update_err) =
	$quad->node0->psql('postgres',
		q{UPDATE pcm_x_flush_decoy SET v = 1 WHERE id = 1}, timeout => 30);
is($decoy_update_rc, 0, 'F3 node0 established the nonmatching decoy X source')
	or diag("F3 decoy update stdout=[$decoy_update_out] stderr=[$decoy_update_err]");
my ($decoy_insert_rc, $decoy_insert_out, $decoy_insert_err) =
	$quad->node1->psql('postgres',
		q{INSERT INTO pcm_x_flush_decoy(id, v) VALUES (2, 1)}, timeout => 30);
is($decoy_insert_rc, 0,
	'F3 remote decoy writer completed without consuming the one-shot')
	or diag("F3 decoy insert stdout=[$decoy_insert_out] stderr=[$decoy_insert_err]");

my ($decoy_log, $decoy_pi_after, $decoy_non_target, $decoy_type17);
my $decoy_deadline = time() + 15;
while (time() < $decoy_deadline)
{
	$decoy_log = substr(slurp_file($quad->node0->logfile), $arm_log_offset);
	$decoy_pi_after = state_int($quad->node0, 'xnode_lever',
		'h_pi_write_note_count');
	$decoy_non_target = $decoy_log =~
		qr/cluster PCM-X retained-image finish fault skipped non-target\n.*?DETAIL:\s+actual=\Q$decoy_tag\E target="\Q$target_tag\E"/s;
	$decoy_type17 = $decoy_log =~
		qr/Resource-X frame ingress diagnostic\n.*?DETAIL:\s+kind=2 msg_type=17 source=[0-3] requester=1 attempt=\d+ result=0/s;
	last if $decoy_pi_after > $decoy_pi_before
		&& $decoy_non_target && $decoy_type17;
	usleep(100_000);
}
cmp_ok($decoy_pi_after - $decoy_pi_before, '>', 0,
	'F3 decoy source completed physical PI write before transfer');
ok($decoy_non_target,
	'F3 exact diagnostic names the decoy and configured target tags');
ok($decoy_type17, 'F3 decoy completed native type-17 with result 0');
unlike($decoy_log,
	qr/injected PCM-X retained-image FlushBuffer failure/,
	'F3 decoy did not dispatch the destructive finish-Flush fault');
unlike($decoy_log,
	qr/\Qcluster PCM-X runtime fail-closed (recovery blocked)\E/,
	'F3 decoy did not fuse the Resource-X runtime');
is($quad->node0->safe_psql('postgres', 'SELECT 1'), '1',
	'F3 node0 remains alive after the nonmatching decoy');

my $target_log_offset = (-s $quad->node0->logfile) // 0;
my ($target_update_rc, $target_update_out, $target_update_err) =
	$quad->node0->psql('postgres',
		q{UPDATE pcm_x_flush_target SET v = 1 WHERE id = 1}, timeout => 30);
is($target_update_rc, 0, 'F4 node0 established the exact target X source')
	or diag("F4 target update stdout=[$target_update_out] stderr=[$target_update_err]");
my ($target_insert_rc, $target_insert_out, $target_insert_err) =
	$quad->node1->psql('postgres', q{
		SET statement_timeout = '5s';
		INSERT INTO pcm_x_flush_target(id, v) VALUES (2, 1)
	}, timeout => 30);
isnt($target_insert_rc, 0,
	'F4 remote target writer failed at the injected finish-Flush boundary')
	or diag("F4 unexpected success stdout=[$target_insert_out] stderr=[$target_insert_err]");

my ($target_log, $target_fail_closed, $target_finish_exact,
	$target_pair_retained, $target_assertion_sequence, $target_injected);
my $target_deadline = time() + 15;
while (time() < $target_deadline)
{
	$target_log = substr(slurp_file($quad->node0->logfile),
		$target_log_offset);
	$target_fail_closed = $target_log =~
		qr/\Qcluster PCM-X runtime fail-closed (recovery blocked)\E/;
	$target_finish_exact = $target_log =~
		qr/Resource-X type-17 finish diagnostic\n[^\n]*DETAIL:\s+result=5 [^\n]*tagless=true/;
	$target_pair_retained = $target_log =~
		qr/PCM-X Resource-X finish-error evidence exact.*?retained=true tag=\Q$target_tag\E requester=1 assertion_sequence=(\d+) base=\d+ formation=\d+ master_session=\d+ source_generation=\d+ reservation_token=\d+ source_state=\d+/s;
	$target_assertion_sequence = $1 if $target_pair_retained;
	$target_injected = $target_log =~
		qr/injected PCM-X retained-image FlushBuffer failure/;
	last if $target_fail_closed && $target_finish_exact
		&& $target_pair_retained && $target_injected;
	usleep(100_000);
}

like($target_log,
	qr/Resource-X source finish FlushBuffer failed; preserved pending pair and blocked recovery/,
	'F4 native source-finish boundary recorded blocked recovery');
ok($target_finish_exact,
	'F4 native type-17 finish retained the exact fail-closed terminal state');
ok($target_pair_retained,
	'F4 exact target Resource-X pair remains pending after ERROR');
my $target_ack_for_failed_attempt = defined($target_assertion_sequence)
	&& $target_log =~
		qr/Resource-X source settlement ACK diagnostic\n.*?DETAIL:\s+master=\d+ requester=1 attempt=\Q$target_assertion_sequence\E result=/s;
ok(!$target_ack_for_failed_attempt,
	'F4 failed target emitted no Resource-X source settlement ACK diagnostic');
ok($target_injected,
	'F4 exact target dispatched the pre-smgrwrite injected ERROR');
ok($target_fail_closed, 'F4 Resource-X runtime moved to fail closed');
is($quad->node0->safe_psql('postgres', 'SELECT 1'), '1',
	'F4 node0 postmaster remains alive after the DATA-worker ERROR');

my $disarm_log_offset = (-s $quad->node0->logfile) // 0;
my ($disarm_rc, $disarm_out, $disarm_err) =
	$quad->node0->psql('postgres', q{
		ALTER SYSTEM SET cluster.injection_points =
			'cluster-pcm-x-retain-flush-error:none:0';
		ALTER SYSTEM SET cluster.pcm_x_retain_flush_error_target = '';
		SELECT pg_reload_conf()
	}, timeout => 30);
is($disarm_rc, 0, 'F5 explicit fault disarm and target clear reloaded')
	or diag("F5 disarm stdout=[$disarm_out] stderr=[$disarm_err]");
my ($disarm_ready, $disarm_log) = wait_for_lms_finish_flush_reload(
	$quad->node0, $disarm_log_offset, 1, 'false',
	'cluster-pcm-x-retain-flush-error:none:0', '', 15);
ok($disarm_ready,
	'F5 node0 DATA worker acknowledged exact disarm before shutdown')
	or diag("F5 disarm reload log=[$disarm_log]");

$quad->stop_quad;
$quad_stopped = 1;
my $shutdown_log = substr(slurp_file($quad->node0->logfile),
	$target_log_offset);
unlike($shutdown_log, qr/lost track of buffer IO/,
	'F5 absorbed FlushBuffer ERROR left no ResourceOwner BufferIO at shutdown');
done_testing();
