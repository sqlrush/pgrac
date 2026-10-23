#!/usr/bin/env perl
#-------------------------------------------------------------------------
#
# 462_e1_crash_reconciliation.pl
#    M5 1.16 E1 real-path crash/reconciliation contract.
#
#    The positive leg deliberately uses the shared two-node fixture: both
#    members commit rows, both members hold an uncommitted transaction, and
#    both postmasters are SIGKILLed before the cold RECOVERED -> OPEN start.
#    The rows which survive and the rows which disappear are checked from a
#    live member after recovery.  The two negative legs corrupt identity or
#    remove the durable shared control record and require startup to refuse
#    the pair; neither leg is allowed to become a SKIP or a stub.
#
#    Product contract baseline: fix/pre2-a-e1-20261014 at
#    cf06d84f50e2f1b697731035c41072f236d21ce4.
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PgracColdPair qw(new_cold_pair);
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(time usleep);

my @PAIR_CONF = (
	'autovacuum = off',
	'restart_after_crash = off',
	'cluster.merged_recovery = on',
	'cluster.recovery_workers_max = 0',
	'cluster.recovery_stale_active_ms = 1000',
);

sub log_offset
{
	my ($node) = @_;
	my $path = $node->logfile;
	return 0 unless defined $path && -e $path;
	return -s $path;
}

sub log_since
{
	my ($node, $offset) = @_;
	return '' unless defined $node->logfile && -e $node->logfile;
	return PostgreSQL::Test::Utils::slurp_file($node->logfile, $offset // 0);
}

sub wait_log_match
{
	my ($node, $offset, $regex, $timeout_s) = @_;
	my $deadline = time() + ($timeout_s // 30);
	while (time() < $deadline)
	{
		my $text = eval { log_since($node, $offset) } // '';
		return 1 if $text =~ $regex;
		usleep(100_000);
	}
	return 0;
}

sub no_panic_since
{
	my ($node, $offset) = @_;
	my $text = eval { log_since($node, $offset) } // '';
	return $text !~ /\bPANIC\b/i;
}

sub start_pair_checked
{
	my ($pair, $label) = @_;
	my @offsets = map { log_offset($_) } ($pair->node0, $pair->node1);
	my ($started, $eval_ok);
	$eval_ok = eval {
		$started = $pair->start_pair(fail_ok => 1);
		1;
	};
	diag("$label startup raised: $@") unless $eval_ok;
	$started = 0 unless $eval_ok && $started;
	return ($started, \@offsets);
}

sub wait_open
{
	my ($pair, $offsets, $label, $timeout_s) = @_;
	my $deadline = time() + ($timeout_s // 45);
	while (time() < $deadline)
	{
		my $all_open = 1;
		for my $i (0, 1)
		{
			my $node = $i == 0 ? $pair->node0 : $pair->node1;
			my $published = wait_log_match($node, $offsets->[$i],
				qr/OPEN_APPLIED/, 0.2);
			my $pcm = eval {
				$node->safe_psql('postgres',
					q{SELECT value FROM pg_cluster_state }
					. q{WHERE category='pcm' AND key='pcm_x_runtime_state'});
			};
			$all_open = 0 unless $published && defined($pcm) && $pcm eq '1';
		}
		return 1 if $all_open;
		usleep(100_000);
	}
	diag("$label did not publish OPEN_APPLIED with pcm_x_runtime_state=1");
	return 0;
}

sub write_committed
{
	my ($node, $lo, $hi) = @_;
	$node->safe_psql('postgres', qq{
		BEGIN;
		INSERT INTO e1_reconcile(id, committed, payload)
		SELECT g, true, 'committed-' || g FROM generate_series($lo, $hi) AS g;
		COMMIT;
	});
	return 1;
}

sub open_uncommitted
{
	my ($node, $id) = @_;
	my $session = $node->background_psql('postgres', on_error_die => 1);
	$session->query_safe('BEGIN');
	$session->query_safe(qq{
		INSERT INTO e1_reconcile(id, committed, payload)
		VALUES ($id, false, 'uncommitted-$id')});
	return $session;
}

sub kill_all
{
	my ($pair) = @_;
	for my $i (0, 1)
	{
		eval { $pair->kill_node9($i); 1 }
			or diag("SIGKILL node$i raised: $@");
	}
}

sub stop_pair_safely
{
	my ($pair) = @_;
	eval { $pair->stop_pair; 1 }
		or diag("pair stop raised: $@");
}

# A raw strict start is used only for negative fixtures.  ClusterPair's
# normal start_pair deliberately BAIL_OUTs when its post-start shared-control
# assertion cannot be evaluated; these cases must instead report a normal
# failed admission and keep the TAP alive for the fail-closed assertion.
sub raw_strict_start
{
	my ($pair) = @_;
	my ($node0, $node1) = ($pair->node0, $pair->node1);
	my @offsets = map { log_offset($_) } ($node0, $node1);
	my $rc;
	{
		local %ENV = $node1->_get_env(PGAPPNAME => undef);
		$rc = PostgreSQL::Test::Utils::system_log(
			'pg_ctl', '-W', '-D', $node1->data_dir,
			'-l', $node1->logfile, '-o',
			'--cluster-name=' . $node1->name, 'start');
	}
	return 0 if $rc != 0;
	# pg_ctl -W returns before the node is SQL-ready, but a successful
	# launch still owns a postmaster PID.  Track it before starting node0 so
	# every failed admission path remains cleanable by stop_pair_safely().
	my $pidfile = $node1->data_dir . '/postmaster.pid';
	my $pid_deadline = time() + 45;
	while (!-e $pidfile && time() < $pid_deadline)
	{
		usleep(100_000);
	}
	return 0 unless -e $pidfile;
	my $node1_pid_known = eval { $node1->_update_pid(1); 1 };
	return 0 unless $node1_pid_known;
	my $node0_started = eval { $node0->start(fail_ok => 1) };
	return 0 unless $node0_started;
	my $deadline = time() + 45;
	my $node1_ready = 0;
	while (time() < $deadline)
	{
		my ($psql_rc) = eval { $node1->psql('postgres', 'SELECT 1', timeout => 2) };
		if (defined($psql_rc) && $psql_rc == 0)
		{
			$node1_ready = 1;
			last;
		}
		usleep(250_000);
	}
	return 0 unless $node1_ready;
	# A SQL-ready postmaster is not an admitted E1 member.  Report success
	# only after both logs publish OPEN and both pcm-X state readers agree.
	return wait_open($pair, \@offsets, 'strict E1 admission', 45);
}

sub new_fixture
{
	my ($name) = @_;
	return eval {
		new_cold_pair(
			$name,
			extra_conf => \@PAIR_CONF);
	};
}

sub create_schema_after_open
{
	my ($pair) = @_;
	return eval {
		$pair->node0->safe_psql('postgres', q{
			CREATE TABLE e1_reconcile (
				id integer PRIMARY KEY,
				committed boolean NOT NULL,
				payload text NOT NULL)});
		1;
	};
}

my $pair = new_fixture('e1_crash_reconcile_462');
ok(defined($pair), 'E1 positive fixture provisions a real shared two-node pair');

my ($started, $initial_offsets) = (0, [0, 0]);
if ($pair)
{
	($started, $initial_offsets) = start_pair_checked($pair, 'positive');
}
ok($started, 'normal multi-node path starts');

my $initial_open = $started && wait_open($pair, $initial_offsets, 'initial');
ok($initial_open, 'normal multi-node path reaches OPEN');

my $peer_connected = 0;
my $pcm_active = 0;
if ($started)
{
	$peer_connected = eval { $pair->wait_for_peer_state(0, 1, 'connected', 30) } ? 1 : 0;
	$pcm_active = eval { $pair->wait_for_pcm_x_active(30) } ? 1 : 0;
}
ok($peer_connected, 'both live members report the peer connected');
ok($pcm_active, 'both live members report PCM-X active');

my $schema_created = 0;
if ($started && $initial_open && $peer_connected && $pcm_active)
{
	$schema_created = create_schema_after_open($pair) ? 1 : 0;
}
ok($schema_created, 'positive schema is created after recovered OPEN');

my ($committed0, $committed1) = (0, 0);
if ($started && $initial_open && $peer_connected && $pcm_active && $schema_created)
{
	$committed0 = eval { write_committed($pair->node0, 1, 32); 1 } ? 1 : 0;
	$committed1 = eval { write_committed($pair->node1, 33, 64); 1 } ? 1 : 0;
}
ok($committed0, 'node0 commits its half of the reconciliation workload');
ok($committed1, 'node1 commits its half of the reconciliation workload');

my ($uncommitted0, $uncommitted1);
if ($committed0 && $committed1)
{
	$uncommitted0 = eval { open_uncommitted($pair->node0, 1001) };
	$uncommitted1 = eval { open_uncommitted($pair->node1, 1002) };
}
ok(defined($uncommitted0), 'node0 holds an uncommitted row before the crash');
ok(defined($uncommitted1), 'node1 holds an uncommitted row before the crash');

if ($pair)
{
	# Deliberately discard handles only after SIGKILL: normal handle cleanup
	# would send a client disconnect and roll the transactions back first.
	kill_all($pair);
}
undef $uncommitted0;
undef $uncommitted1;

my ($restarted, $restart_offsets) = (0, [0, 0]);
if ($pair)
{
	($restarted, $restart_offsets) = start_pair_checked($pair, 'cold E1 recovery');
}
ok($restarted, 'cold E1 recovery starts after both postmasters are killed');

my $recovered_open = $restarted && wait_open($pair, $restart_offsets, 'recovery');
ok($recovered_open, 'RECOVERED startup reaches OPEN');

my ($committed_rows, $uncommitted_rows);
if ($recovered_open)
{
	$committed_rows = eval {
		$pair->node0->safe_psql('postgres',
			q{SELECT count(*) FROM e1_reconcile
			  WHERE id BETWEEN 1 AND 64 AND committed})
	};
	$uncommitted_rows = eval {
		$pair->node0->safe_psql('postgres',
			q{SELECT count(*) FROM e1_reconcile WHERE id >= 1000})
	};
}
is($committed_rows // 'unavailable', '64',
	'after recovery every committed row is present');
is($uncommitted_rows // 'unavailable', '0',
	'after recovery every uncommitted row is absent');

my $no_recovery_panic = 0;
if ($pair && $restarted)
{
	$no_recovery_panic = no_panic_since($pair->node0, $restart_offsets->[0])
		&& no_panic_since($pair->node1, $restart_offsets->[1]);
}
ok($no_recovery_panic, 'E1 recovery emits no PANIC after the cold restart');
stop_pair_safely($pair) if $pair;

# Negative fixture 1: a member identity drift must not be admitted as the
# same recovered pair.  No skip is permitted if the fixture itself fails.
my $identity_pair = new_fixture('e1_crash_reconcile_identity_462');
ok(defined($identity_pair), 'identity-drift fixture provisions');
my $identity_started = 0;
if ($identity_pair)
{
	$identity_started = raw_strict_start($identity_pair);
}
ok($identity_started, 'identity-drift fixture reaches its pre-crash live state');
kill_all($identity_pair) if $identity_pair;
if ($identity_pair)
{
	PostgreSQL::Test::Utils::append_to_file(
		$identity_pair->node1->data_dir . '/postgresql.conf',
		"\n# E1 negative fixture: drift the declared member identity\n"
		. "cluster.node_id = 99\n");
}
my $identity_restarted = $identity_pair ? raw_strict_start($identity_pair) : 0;
ok(!$identity_restarted, 'identity drift is rejected fail-closed before OPEN');
stop_pair_safely($identity_pair) if $identity_pair;

# Negative fixture 2: removing the durable shared control record must not
# turn a missing recovery fact into a fresh authority or an OPEN member.
my $missing_pair = new_fixture('e1_crash_reconcile_missing_462');
ok(defined($missing_pair), 'missing-record fixture provisions');
my $missing_started = 0;
if ($missing_pair)
{
	$missing_started = raw_strict_start($missing_pair);
}
ok($missing_started, 'missing-record fixture reaches its pre-crash live state');
kill_all($missing_pair) if $missing_pair;
my $control_path = $missing_pair
	? $missing_pair->shared_control_root . '/global/pg_control' : undef;
my $removed_control = defined($control_path) && unlink($control_path);
ok($removed_control, 'negative fixture removes the durable shared control record');
my $missing_restarted = $missing_pair ? raw_strict_start($missing_pair) : 0;
ok(!$missing_restarted, 'missing durable recovery record is rejected fail-closed');
stop_pair_safely($missing_pair) if $missing_pair;

done_testing();
