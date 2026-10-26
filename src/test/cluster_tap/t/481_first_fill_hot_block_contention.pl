#!/usr/bin/env perl
# Eight real readers share three first fills, then release a waiting writer.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils qw(slurp_file);
use Test::More;
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC sleep);

my $cluster;
my $stop_attempted = 0;
END
{
	if (defined($cluster) && !$stop_attempted)
	{
		$stop_attempted = 1;
		eval { $cluster->stop_cluster; 1 }
		  or diag("original failure-path normal stop failed: $@");
	}
}

sub require_true
{
	BAIL_OUT($_[1]) unless $_[0];
}

sub log_since
{
	my ($node, $offset) = @_;
	return substr(slurp_file($node->logfile), $offset);
}

sub wait_log
{
	my ($node, $offset, $pattern, $description) = @_;
	my $until = clock_gettime(CLOCK_MONOTONIC) + 30;
	while (clock_gettime(CLOCK_MONOTONIC) < $until)
	{
		my $text = log_since($node, $offset);
		return $text if $text =~ $pattern;
		sleep(0.02);
	}
	BAIL_OUT("native evidence missing: $description");
}

sub wait_sql
{
	my ($session, $sql, $expected, $description) = @_;
	my $until = clock_gettime(CLOCK_MONOTONIC) + 30;
	my $actual = '';
	while (clock_gettime(CLOCK_MONOTONIC) < $until)
	{
		$actual = $session->query_safe($sql);
		return if $actual eq $expected;
		sleep(0.02);
	}
	BAIL_OUT("native evidence missing: $description (last: $actual)");
}

sub page_digest
{
	my ($output, $description) = @_;
	my @digests = ($output =~ /^8192:([0-9a-f]{32})$/mg);
	require_true(@digests == 1, "$description must return one complete 8192-byte page");
	return $digests[0];
}

sub writer_follows_shared_releases
{
	my ($output, $release_times) = @_;
	my @done = ($output =~ /^HOT_WRITE_DONE:([1-9][0-9]*)$/mg);
	return 0 unless $output =~ /^20\|60$/m && @done == 1 && @$release_times == 8;
	for my $time (@$release_times)
	{
		return 0 unless $time =~ /^[1-9][0-9]*$/ && $done[0] >= $time;
	}
	return 1;
}

$cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('first_fill_hot',
	nodes => 4, blackbox => 1, required_operations => [],
	extra_conf => ['autovacuum=off', 'cluster.trace_block_io=on',
		'cluster.trace_tx_identity=on', 'log_error_verbosity=verbose',
		'cluster.read_scache=off']);
$cluster->start_cluster;
my @nodes = $cluster->nodes;
my $a = $nodes[0];
for my $name (qw(cached page flush))
{
	my $type = $name eq 'cached' ? 'boolean' : 'bytea';
	$a->safe_psql('postgres', qq{CREATE FUNCTION hot_source_$name(regclass) RETURNS $type
	 AS '\$libdir/test_pcm_read_source', 'test_pcm_read_source_$name'
	 LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE});
}
$a->safe_psql('postgres', q{CREATE TABLE first_fill_hot_data(id int, v int, pad text);
	INSERT INTO first_fill_hot_data SELECT i,1,repeat(md5(i::text),2)
	FROM generate_series(1,20)i});
$a->safe_psql('postgres', 'VACUUM(FREEZE) first_fill_hot_data');
$a->safe_psql('postgres', 'CHECKPOINT');
$a->safe_psql('postgres', 'UPDATE first_fill_hot_data SET v=2');
my $tag = $a->safe_psql('postgres', q{SELECT
	COALESCE(NULLIF(c.reltablespace,0),d.dattablespace)::text || '/' ||
	d.oid::text || '/' || pg_relation_filenode(c.oid)::text || '/0/0'
	FROM pg_class c, pg_database d
	WHERE c.oid='first_fill_hot_data'::regclass AND d.datname=current_database()});
require_true($tag =~ /^([1-9][0-9]*)\/([1-9][0-9]*)\/([1-9][0-9]*)\/0\/0$/,
	'exact native target BufferTag is required');
my ($spc, $db, $rel) = ($1, $2, $3);
require_true($spc == 1663, 'the fixture uses the default tablespace layout');
# Measure the declared native shared heap, rather than a node-local path.
my $physical_heap = $cluster->shared_root . "/base/$db/$rel";
is(-s $physical_heap, 8192,
	'all original and updated tuples occupy the one target heap block');
my $target = qr/tag=\Q$tag\E /;

# All clients, module loads, relation metadata and statement shapes are ready
# before the physical write interval; none of these calls reads the target page.
my $writer = $a->background_psql('postgres', timeout => 120);
my $wpid = $writer->query_safe('SELECT pg_backend_pid()');
require_true($wpid =~ /^[1-9][0-9]*$/, 'exact native writer PID is required');
$writer->query_safe(q{PREPARE hot_flush AS WITH image AS MATERIALIZED
	(SELECT hot_source_flush('first_fill_hot_data') AS p)
	SELECT octet_length(p)::text || ':' || md5(p) FROM image});
$writer->query_safe(q{PREPARE hot_write AS WITH changed AS
	(UPDATE first_fill_hot_data SET v=3 RETURNING v) SELECT count(*),sum(v) FROM changed});
$writer->query_safe(q{PREPARE hot_write_done AS SELECT 'HOT_WRITE_DONE:' ||
	(extract(epoch FROM (clock_timestamp() - TIMESTAMPTZ '2000-01-01 00:00:00+00'))
	* 1000000)::bigint});
my @readers;
my @by_node = ([], [], [], []);
for my $ordinal (0 .. 7)
{
	my $node = 1 + $ordinal % 3;
	my $session = $nodes[$node]->background_psql('postgres', timeout => 120);
	my $pid = $session->query_safe('SELECT pg_backend_pid()');
	require_true($pid =~ /^[1-9][0-9]*$/, 'exact native reader PID is required');
	is($session->query_safe(q{SELECT hot_source_cached('first_fill_hot_data')}),
		'f', "reader $ordinal starts without a target descriptor on node $node");
	$session->query_safe(q{SET test_pcm_read_source.hold_shared_ms=15000;
		PREPARE hot_read AS WITH image AS MATERIALIZED
		(SELECT hot_source_page('first_fill_hot_data') AS p)
		SELECT octet_length(p)::text || ':' || md5(p) FROM image});
	push @readers, {session => $session, pid => $pid, node => $node};
	push @{$by_node[$node]}, $pid;
	note("hot-block actor=reader ordinal=$ordinal node=$node pid=$pid tag=$tag");
}
note("hot-block actor=writer node=0 pid=$wpid tag=$tag");
my @observers = map { $_->background_psql('postgres', timeout => 120) } @nodes;
for my $node (1 .. 3)
{
	my $pids = join(',', @{$by_node[$node]});
	$observers[$node]->query_safe(qq{PREPARE hot_wait AS SELECT count(*)
		FROM pg_stat_activity WHERE pid IN ($pids) AND state='active' AND wait_event='BufferIO'});
	is($observers[$node]->query_safe('EXECUTE hot_wait'), '0',
		"node $node activity observer is ready before injection");
}
$observers[0]->query_safe(qq{PREPARE hot_writer_wait AS SELECT count(*)
	FROM pg_stat_activity WHERE pid=$wpid AND state='active' AND wait_event IS NOT NULL});
is($observers[0]->query_safe('EXECUTE hot_writer_wait'), '0',
	'writer activity observer is ready before injection');
my @offsets = map { -s $_->logfile } @nodes;
$writer->query_safe(q{SELECT cluster_inject_fault('cluster-pcm-write-before-data','sleep',8000000)});
$writer->query_until(qr/^FLUSH_SENT$/m, "\\echo FLUSH_SENT\nEXECUTE hot_flush;\n");
wait_log($a, $offsets[0], qr/BLOCK_IO event=WRITE_PAUSE .*pid=$wpid .*$target/,
	'the original writer owns native target output IO');
pass('writer holds the physical target write before data reaches smgrwrite');
for my $reader (@readers)
{
	$reader->{session}->query_until(qr/^READ_SENT$/m,
		"\\echo READ_SENT\nEXECUTE hot_read;\n");
}
for my $node (1 .. 3)
{
	wait_log($nodes[$node], $offsets[$node],
		qr/BLOCK_IO event=READ_SOURCE phase=REQUEST .*$target/,
		"node $node starts its target first fill");
	my $followers = @{$by_node[$node]} - 1;
	wait_sql($observers[$node], 'EXECUTE hot_wait', "$followers",
		"node $node has $followers original native BufferIO followers");
	pass("node $node coalesces its readers on the original native IO bit");
}
unlike(log_since($a, $offsets[0]),
	qr/BLOCK_IO event=WRITE_COMPLETE .*pid=$wpid .*$target/,
	'all eight readers entered the overlapping physical write interval');
like(log_since($a, $offsets[0]), qr/BLOCK_IO event=FIRST_FILL_HOLDER_PARK .*$target/,
	'holder retained a first-fill continuation against the native write');
my $writer_hash = page_digest($writer->query_safe("SELECT 'FLUSH_DONE'"), 'writer flush');
$writer->query_safe(q{SELECT cluster_inject_fault('cluster-pcm-write-before-data','none',0)});

for my $reader (@readers)
{
	my ($node, $pid) = @{$reader}{qw(node pid)};
	wait_log($nodes[$node], $offsets[$node],
		qr/TEST_PCM_READ_SOURCE phase=SHARED_HELD .*pid=$pid .*$target/,
		"reader $pid holds SHARE over the actual target");
}
for my $reader (@readers)
{
	unlike(log_since($nodes[$reader->{node}], $offsets[$reader->{node}]),
		qr/TEST_PCM_READ_SOURCE phase=SHARED_RELEASE .*pid=$reader->{pid} .*$target/,
		"reader $reader->{pid} still holds native SHARE before writer X");
}
my $x_offset = -s $a->logfile;
$writer->query_until(qr/^WRITE_SENT$/m,
	"\\echo WRITE_SENT\nEXECUTE hot_write;\nEXECUTE hot_write_done;\n");
# SQL activity is only a live-wait observation. The native PCM row separately
# binds that wait to the exact block and X mode; it is not an inferred edge.
wait_sql($observers[0], 'EXECUTE hot_writer_wait', '1', 'the writer is actively waiting');
wait_log($a, $x_offset,
	qr/WAIT_RESOURCE event=PCM_WAIT .*pid=$wpid .*tag_known=1 spc=$spc db=$db rel=$rel fork=0 block=0 .*mode=2 /,
	'the actual writer waits for X over this target block');
for my $reader (@readers)
{
	unlike(log_since($nodes[$reader->{node}], $offsets[$reader->{node}]),
		qr/TEST_PCM_READ_SOURCE phase=SHARED_RELEASE .*pid=$reader->{pid} .*$target/,
		"reader $reader->{pid} overlaps the writer's native target X wait");
}
for my $reader (@readers)
{
	my $hash = page_digest($reader->{session}->query_safe("SELECT 'READ_DONE'"),
		"reader $reader->{pid}");
	is($hash, $writer_hash, "reader $reader->{pid} sees the complete original current image");
}
my $write_output = $writer->query_safe("SELECT 'WRITE_DONE'");
like($write_output, qr/^20\|60$/m, 'writer completes the original native UPDATE');

my @logs = map { log_since($nodes[$_], $offsets[$_]) } 0 .. 3;
my @release_times;
for my $reader (@readers)
{
	my ($node, $pid) = @{$reader}{qw(node pid)};
	my @times = ($logs[$node] =~
		/TEST_PCM_READ_SOURCE phase=SHARED_RELEASE time=(\d+) node=$node pid=$pid $target/g);
	require_true(@times == 1, "reader $pid must have one native target release record");
	push @release_times, $times[0];
}
# All four instances run in this test's single VM and share its kernel clock.
# This backend clock is sampled after hot_write commits, independently of when
# the controller happens to consume psql's buffered output.
ok(writer_follows_shared_releases($write_output, \@release_times),
	'backend completion follows all eight held-reader release records');
note($write_output);
my $all_logs = join("\n", @logs);
for my $node (1 .. 3)
{
	my @requests = ($logs[$node] =~ /^(.*BLOCK_IO event=READ_SOURCE phase=REQUEST .*$target.*)$/mg);
	is(scalar(@requests), 1, "node $node has one first-fill episode for all local readers");
	require_true(@requests == 1, "node $node needs its unique original episode");
	my ($pid, $generation, $token) = $requests[0] =~
	  / pid=(\d+) .* generation=(\d+) token=(\d+) /;
	require_true(defined($token) && grep($_ == $pid, @{$by_node[$node]}),
		"node $node episode belongs to one of its original readers");
	my @decisions = ($logs[$node] =~ /^(.*BLOCK_IO event=READ_SOURCE phase=DECISION .*pid=$pid .*$target.*)$/mg);
	is(scalar(@decisions), 1, "node $node receives one terminal source decision");
	require_true(@decisions == 1, "node $node needs its terminal decision");
	like($decisions[0],
		qr/source=2 master=\d+ generation=$generation token=$token request=[1-9][0-9]* epoch=[1-9][0-9]* .*durable=1 /,
		"node $node installs a durable S image for its original generation and token");
	my ($request, $epoch) = $decisions[0] =~ / request=(\d+) epoch=(\d+) /;
	require_true(defined($epoch), 'the original native decision must bind request and epoch');
	my $key = qr/request=$request epoch=$epoch requester=$node /;
	my @ingress = ($all_logs =~ /^(.*BLOCK_IO event=FIRST_FILL_MASTER action=INGRESS .*$target$key.*)$/mg);
	is(scalar(@ingress), 1, "node $node submitted its original request once to the master");
	unlike($logs[$node], qr/BLOCK_IO event=READ_SOURCE phase=DISK_BEGIN .*$target/,
		"node $node never reads the target from disk under foreign current authority");
}
for my $node (0 .. 3)
{
	is($nodes[$node]->safe_psql('postgres', 'SELECT count(*),sum(v) FROM first_fill_hot_data'),
		'20|60', "ordinary node $node query sees the writer's complete final result");
}
$_->{session}->quit for @readers;
$writer->quit;
$_->quit for @observers;
$stop_attempted = 1;
$cluster->stop_cluster;
undef $cluster;
for my $node (@nodes)
{
	unlike(slurp_file($node->logfile), qr/PANIC|invalid page in block|checksum verification failed/,
		'original normal stop closes without page-verification error or PANIC');
}
done_testing();
