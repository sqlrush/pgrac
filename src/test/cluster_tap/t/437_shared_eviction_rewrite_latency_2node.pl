#-------------------------------------------------------------------------
#
# 437_shared_eviction_rewrite_latency_2node.pl
#    Exclusive-grant latency for a page that is rewritten right after the
#    writer holding it evicted it, with every checkpointer idle and while
#    the other writer runs a spread checkpoint.  The cluster is a fresh
#    four-member cohort with two writers (node0 and node1); node2 and node3
#    only read.  This is a functional regression, not a performance load.
#    The readings go to t437_readings.jsonl in the test log directory; the
#    test fails when a rewrite ends in an error or a committed update is
#    lost.  A target page may be mastered by any member; every member's
#    checkpointer state is recorded with each reading.
#
#      L1   idle: X handover between the writers (baseline), then eviction
#           by buffer pressure and an immediate rewrite on the evictor
#      L2   spread checkpoint on the other writer: eviction and immediate
#           rewrite while that checkpointer is in its write-delay phase
#      L3   no rewrite ends in an error, the eviction path was exercised in
#           both legs, and every member reads every committed update
#
# IDENTIFICATION
#    src/test/cluster_tap/t/437_shared_eviction_rewrite_latency_2node.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings FATAL => 'all';

use FindBin;
use lib "$FindBin::RealBin/../../perl";

use IPC::Run qw(start finish timeout);
use JSON::PP;
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC sleep);

use constant BUFFER_PAGES => 512;	# shared_buffers = 4MB
use constant FLOOD_PAGES => 6 * BUFFER_PAGES;
use constant BALLAST_PAGES => 400;
use constant TARGETS => 6;
use constant CKPT_ROUNDS => 4;

my $cluster;
my @nodes;
my %updates;	# table -> committed updates
my @readings;
my $ledger = "$PostgreSQL::Test::Utils::log_path/t437_readings.jsonl";

END
{
	if (defined($cluster) && $cluster->{running})
	{
		my $rc = $?;
		eval { $cluster->stop_cluster; 1 } or diag("cleanup stop failed: $@");
		$? = $rc;
	}
}

sub sql { return $_[0]->safe_psql('postgres', $_[1], timeout => 300); }
sub now_ms { return clock_gettime(CLOCK_MONOTONIC) * 1000; }

sub pi_holders
{
	my $total = 0;
	$total += sql($_, q{SELECT value FROM pg_cluster_state
		WHERE category = 'pcm' AND key = 'pi_holders_total_count'}) for @nodes;
	return $total;
}

sub checkpointer_wait
{
	return sql($_[0], q{SELECT coalesce(max(wait_event), '') FROM pg_stat_activity
		WHERE backend_type = 'checkpointer'});
}

# Wait for every past image to retire; returns the wait in ms.
sub wait_pi_retired
{
	my $began = now_ms();
	for (1 .. 1200)
	{
		return now_ms() - $began if pi_holders() == 0;
		sleep(0.1);
	}
	die 'past images did not retire within 120 seconds';
}

# One measured update; an error is a reading, not a test abort.
sub timed_update
{
	my ($member, $table) = @_;
	my $began = now_ms();
	my ($rc, $out, $err) = $nodes[$member]->psql('postgres',
		"SELECT t437_timed_update('$table')", timeout => 300);
	my %r = (member => $member, table => $table, wall_ms => now_ms() - $began);
	if ($rc == 0)
	{
		$r{grant_ms} = $out + 0;
		$updates{$table}++;
	}
	else
	{
		($r{error} = $err) =~ s/\s+$//;
	}
	return \%r;
}

# Load enough new pages on one member to evict everything it cached.
sub flood
{
	my ($member) = @_;
	my $began = now_ms();
	sql($nodes[$member], q{
		SET enable_seqscan = off; SET enable_bitmapscan = off;
		SET enable_indexonlyscan = off;
		SELECT count(length(pad)) FROM t437_flood WHERE id > 0});
	return now_ms() - $began;
}

sub record
{
	my (%r) = @_;
	push @readings, \%r;
	open(my $fh, '>>', $ledger) or die "cannot append $ledger: $!";
	print {$fh} JSON::PP->new->canonical->encode(\%r), "\n";
	close($fh) or die $!;
}

sub summary
{
	my ($label, @values) = @_;
	@values = sort { $a <=> $b } @values;
	return "$label: no samples" unless @values;
	my $at = sub { $values[int($_[0] * $#values + 0.5)] };
	return sprintf('%s: n=%d p50=%.1f p99=%.1f max=%.1f ms', $label, scalar(@values),
		$at->(0.5), $at->(0.99), $values[-1]);
}

$cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('evict_rewrite', nodes => 4,
	blackbox => 1, extra_conf => ['autovacuum = off', 'shared_buffers = 4MB',
		'log_checkpoints = on', 'checkpoint_timeout = 300s', 'max_wal_size = 8GB',
		'checkpoint_completion_target = 0.9']);
$cluster->start_cluster;
@nodes = $cluster->nodes;
unlink($ledger);

# One page per flood or ballast row; one page per target table.
sql($nodes[0], 'CREATE TABLE t437_flood (id int PRIMARY KEY, pad text NOT NULL);'
	  . 'ALTER TABLE t437_flood ALTER pad SET STORAGE PLAIN;'
	  . "INSERT INTO t437_flood SELECT g, repeat('f', 7000) FROM generate_series(1, "
	  . FLOOD_PAGES . ') g');
for my $n (0 .. 1)
{
	sql($nodes[0], "CREATE TABLE t437_ballast$n (id int PRIMARY KEY, v int NOT NULL, "
		  . "pad text NOT NULL); ALTER TABLE t437_ballast$n ALTER pad SET STORAGE PLAIN;"
		  . "INSERT INTO t437_ballast$n SELECT g, 0, repeat('b', 7000) "
		  . 'FROM generate_series(1, ' . BALLAST_PAGES . ') g');
}
for my $k (1 .. TARGETS)
{
	sql($nodes[0], "CREATE TABLE t437_target$k (id int PRIMARY KEY, v int NOT NULL);"
		  . "INSERT INTO t437_target$k VALUES (1, 0)");
	$updates{"t437_target$k"} = 0;
}
sql($nodes[0], q{
	CREATE FUNCTION t437_timed_update(t regclass) RETURNS float8 LANGUAGE plpgsql AS $$
	DECLARE t0 timestamptz := clock_timestamp();
	BEGIN
		EXECUTE format('UPDATE %s SET v = v + 1 WHERE id = 1', t);
		RETURN extract(epoch FROM clock_timestamp() - t0) * 1000;
	END $$});
sql($_, 'CHECKPOINT') for @nodes;

# L1: every checkpointer idle.
for my $evictor (0 .. 1)
{
	my $other = 1 - $evictor;
	for my $k (1 .. TARGETS)
	{
		my $table = "t437_target$k";
		for (1 .. 600)
		{
			last unless grep { checkpointer_wait($_) eq 'CheckpointWriteDelay' } @nodes;
			sleep(0.1);
		}
		my $settled = wait_pi_retired();
		my $first = timed_update($other, $table);
		my $handover = timed_update($evictor, $table);
		my $handover_retire_ms = wait_pi_retired();
		my $flood_ms = flood($evictor);
		my $pending = pi_holders();
		my @ckpt = map { checkpointer_wait($_) } @nodes;
		my $rewrite = timed_update($evictor, $table);
		my $evict_retire_ms = eval { wait_pi_retired() };
		record(leg => 'idle', evictor => $evictor, table => $table, settled_ms => $settled,
			first => $first, handover => $handover, handover_retire_ms => $handover_retire_ms,
			flood_ms => $flood_ms, pi_pending_after_flood => $pending + 0,
			checkpointer_wait => \@ckpt, rewrite => $rewrite,
			evict_retire_ms => $evict_retire_ms);
	}
}

# L2: the other writer runs a spread checkpoint while the evictor floods
# and rewrites.  The flood also cleans the evictor's own checkpoint, so
# each round uses one evictor.
for my $round (1 .. CKPT_ROUNDS)
{
	my $evictor = $round % 2;
	my $other = 1 - $evictor;
	wait_pi_retired();
	sql($nodes[$_], "UPDATE t437_ballast$_ SET v = v + 1") for 0 .. 1;
	my @backups;
	for my $n (0 .. 1)
	{
		my $script = "SELECT pg_backup_start('t437-$round-$n', false);\n"
		  . "SELECT lsn FROM pg_backup_stop(false);\n";
		my ($out, $err) = ('', '');
		push @backups, [start(['psql', '-XAtq', '-v', 'ON_ERROR_STOP=1',
					'-d', $nodes[$n]->connstr('postgres')],
				'<', \$script, '>', \$out, '2>', \$err, timeout(600)), \$err, $n];
	}
	my $spreading = 0;
	for (1 .. 600)
	{
		$spreading = checkpointer_wait($nodes[$other]) eq 'CheckpointWriteDelay';
		last if $spreading;
		sleep(0.1);
	}
	ok($spreading, "L2 round $round: node$other checkpoint is spreading its writes");
	my @tables = map { "t437_target" . (1 + ($round * 2 + $_) % TARGETS) } 0 .. 2;
	timed_update($evictor, $_) for @tables;
	my $flood_ms = flood($evictor);
	for my $table (@tables)
	{
		my $pending = pi_holders();
		my @ckpt = map { checkpointer_wait($_) } @nodes;
		my $rewrite = timed_update($evictor, $table);
		record(leg => 'checkpoint', round => $round, evictor => $evictor, table => $table,
			flood_ms => $flood_ms, pi_pending_after_flood => $pending + 0,
			checkpointer_wait => \@ckpt, rewrite => $rewrite);
	}
	for my $b (@backups)
	{
		my ($h, $err, $n) = @$b;
		ok(finish($h), "L2 round $round: node$n spread checkpoint completed")
		  or diag(${$err});
	}
}

# L3: readings, errors, exercised paths, and no lost update.
for my $leg ('idle', 'checkpoint')
{
	my @mine = grep { $_->{leg} eq $leg } @readings;
	my @errors = grep { exists $_->{rewrite}{error} } @mine;
	my @spread = grep { $_->{checkpointer_wait}[1 - $_->{evictor}] eq 'CheckpointWriteDelay' }
	  @mine;
	diag(summary("$leg rewrite after eviction",
			map { $_->{rewrite}{grant_ms} // () } $leg eq 'idle' ? @mine : @spread));
	diag(summary("$leg handover baseline", map { $_->{handover}{grant_ms} // () } @mine))
	  if $leg eq 'idle';
	diag(summary("$leg eviction past-image retirement",
			map { $_->{evict_retire_ms} // () } @mine)) if $leg eq 'idle';
	ok((grep { $_->{pi_pending_after_flood} > 0 } @mine) > 0,
		"L3 $leg: an eviction left a past image pending before the rewrite");
	ok(@spread > 0, "L3 $leg: a rewrite ran during the other writer's spread checkpoint")
	  if $leg eq 'checkpoint';
	is(scalar(@errors), 0, "L3 $leg: no rewrite ended in an error")
	  or diag(join("\n", map { "$_->{table}: $_->{rewrite}{error}" } @errors));
}
for my $k (1 .. TARGETS)
{
	my $table = "t437_target$k";
	for my $i (0 .. $#nodes)
	{
		is(sql($nodes[$i], "SELECT v FROM $table WHERE id = 1"), $updates{$table},
			"L3 node$i reads every committed update of $table");
	}
}
$cluster->stop_cluster;

done_testing();
