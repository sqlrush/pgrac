#!/usr/bin/env perl
# Cancel an actual pending input, then use the original normal-stop observer.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils qw(slurp_file);
use Test::More;
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC sleep);

my ($cluster, $stop_attempted);
END
{
	# A failed original stop is evidence, not permission to try it again.
	if (defined($cluster) && !$stop_attempted)
	{
		$stop_attempted = 1;
		eval { $cluster->stop_cluster };
		warn "cleanup normal stop failed: $@" if $@;
	}
}

sub require_true
{
	my ($condition, $description) = @_;
	ok($condition, $description) or BAIL_OUT($description);
}

sub log_since
{
	my ($node, $offset) = @_;
	return substr(slurp_file($node->logfile), $offset);
}

sub wait_log
{
	my ($node, $pattern, $offset, $description) = @_;
	my $until = clock_gettime(CLOCK_MONOTONIC) + 30;
	while (clock_gettime(CLOCK_MONOTONIC) < $until)
	{
		return if log_since($node, $offset) =~ $pattern;
		sleep(0.02);
	}
	BAIL_OUT($description);
}

$cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('cancel_first_fill',
	nodes => 4, blackbox => 1, required_operations => [],
	extra_conf => [
		'autovacuum=off', 'cluster.trace_block_io=on',
		'log_error_verbosity=verbose', 'cluster.read_scache=off'
	]);
$cluster->start_cluster;
my @nodes = $cluster->nodes;
my ($holder, $requester) = @nodes[0, 1];

for my $name (qw(cached page flush))
{
	my $type = $name eq 'cached' ? 'boolean' : 'bytea';
	$holder->safe_psql('postgres', qq{
		CREATE FUNCTION cancel_fill_$name(regclass) RETURNS $type
		AS '\$libdir/test_pcm_read_source', 'test_pcm_read_source_$name'
		LANGUAGE C STRICT VOLATILE PARALLEL UNSAFE;
		REVOKE ALL ON FUNCTION cancel_fill_$name(regclass) FROM PUBLIC});
}
$holder->safe_psql('postgres', q{
	CREATE TABLE cancel_fill_data(id int, v int, pad text);
	INSERT INTO cancel_fill_data
	SELECT i, 1, repeat(md5(i::text), 2) FROM generate_series(1, 20) i});
$holder->safe_psql('postgres', 'VACUUM(FREEZE) cancel_fill_data');
$holder->safe_psql('postgres', 'CHECKPOINT');
$holder->safe_psql('postgres', 'UPDATE cancel_fill_data SET v=2');
my $rel = $holder->safe_psql('postgres',
	q{SELECT pg_relation_filenode('cancel_fill_data')});
require_true($rel =~ /^[1-9][0-9]*$/, 'exact target relfilenumber');
require_true($requester->safe_psql('postgres',
	q{SELECT cancel_fill_cached('cancel_fill_data')}) eq 'f',
	'no requester descriptor before the actual first fill');

# Establish all clients before the existing eight-second write pause.
my $writer = $holder->background_psql('postgres', timeout => 120);
my $reader = $requester->background_psql('postgres', timeout => 120);
my $control = $requester->background_psql('postgres', timeout => 120);
my $wpid = $writer->query_safe('SELECT pg_backend_pid()');
my $rpid = $reader->query_safe('SELECT pg_backend_pid()');
my $cpid = $control->query_safe('SELECT pg_backend_pid()');
require_true($wpid =~ /^[1-9][0-9]*$/ && $rpid =~ /^[1-9][0-9]*$/
	&& $cpid =~ /^[1-9][0-9]*$/ && $rpid ne $cpid,
	'exact writer, reader and independent requester control PIDs');
diag("target=$rel/0/0 holder_pid=$wpid reader_pid=$rpid control_pid=$cpid");
$reader->query_safe("\\set VERBOSITY verbose\n\\set ON_ERROR_STOP off");
my ($ho, $ro) = (-s $holder->logfile, -s $requester->logfile);
my $tag = qr/\d+\/\d+\/$rel\/0\/0 /;
my $write_complete = qr/BLOCK_IO event=WRITE_COMPLETE .*pid=$wpid .*tag=$tag/;

$writer->query_safe(q{
	SELECT cluster_inject_fault('cluster-pcm-write-before-data','sleep',8000000)});
$writer->query_until(qr/FLUSH_SENT/,
	"\\echo FLUSH_SENT\nSELECT md5(cancel_fill_flush('cancel_fill_data'));\n");
wait_log($holder, qr/BLOCK_IO event=WRITE_PAUSE .*pid=$wpid .*tag=$tag/,
	$ho, 'holder did not reach its original physical-write pause');
$reader->query_until(qr/READ_SENT/,
	"\\echo READ_SENT\nSELECT md5(cancel_fill_page('cancel_fill_data'));\n");
wait_log($requester,
	qr/BLOCK_IO event=READ_SOURCE phase=REQUEST .*pid=$rpid .*tag=$tag/,
	$ro, 'reader did not request the exact target first fill');

my ($waiting, $activity) = (0, '');
my $until = clock_gettime(CLOCK_MONOTONIC) + 30;
while (clock_gettime(CLOCK_MONOTONIC) < $until)
{
	$activity = $control->query_safe(qq{
		SELECT state || '|' || coalesce(wait_event, '')
		FROM pg_stat_activity WHERE pid=$rpid});
	if ($activity eq 'active|ClusterGCSBlockShipWait')
	{
		$waiting = 1;
		last;
	}
	sleep(0.02);
}
diag("reader activity before cancellation: $activity");
require_true($waiting, 'original reader is in the real GCS block-ship wait');
require_true($control->query_safe(
	q{SELECT cancel_fill_cached('cancel_fill_data')}) eq 't',
	'exact pending input has a tagged requester descriptor');
require_true(log_since($holder, $ho) !~ $write_complete,
	'holder has not completed its physical write before cancellation');
# RETRY is an unfinished source decision; DISK/IMAGE would finish selection.
require_true(log_since($requester, $ro) !~
	qr/BLOCK_IO event=READ_SOURCE phase=DECISION .*pid=$rpid .*tag=${tag}source=[12] /,
	'original first fill has not selected disk or installed image');
require_true($control->query_safe("SELECT pg_cancel_backend($rpid)") eq 't',
	'control delivers cancellation to the exact waiting reader');
require_true(log_since($holder, $ho) !~ $write_complete,
	'cancellation was delivered inside the original holder-write interval');

# The next query drains the original error and proves the backend survived.
my ($output, $error) = $reader->query('SELECT pg_backend_pid()');
diag("cancelled first fill stderr: $reader->{stderr}");
is($error, 1, 'the original first-fill query reported an error');
like($reader->{stderr}, qr/57014.*canceling statement due to user request/,
	'original first-fill error is query cancellation');
is($output, $rpid, 'same reader backend survives without a completed page digest');
like(log_since($requester, $ro),
	qr/\[$rpid\] ERROR:.*57014:.*canceling statement due to user request/,
	'original requester server log identifies the cancelled backend');
$reader->{stderr} = ''; # Preserve the error above; clear only psql's accumulator.

# cached() scans descriptor headers; it never reads or evicts the target page.
# Do not re-read the page: that could repair an old poisoned mapping before stop.
is($control->query_safe(q{SELECT cancel_fill_cached('cancel_fill_data')}), 'f',
	'original error cleanup retired the failed input mapping');
my $writer_output = $writer->query_safe("SELECT 'WRITER_DONE'");
like($writer_output, qr/^[0-9a-f]{32}\r?\nWRITER_DONE$/,
	'holder completes the original flush without cancellation');
like(log_since($holder, $ho), $write_complete,
	'original physical writer completed the exact target IO');
$writer->quit;
$reader->quit;
$control->quit;

$stop_attempted = 1;
my $stopped = eval { $cluster->stop_cluster; 1 };
my $stop_error = $@;
ok($stopped, 'original four-node shared_stop and shutdown evidence pass');
diag("original normal stop failed: $stop_error") unless $stopped;
ok($cluster->{normal_stop_proven}, 'original observer proves all members closed');
for my $node (@nodes)
{
	unlike(slurp_file($node->logfile),
		qr/\b(?:FATAL|PANIC)\b|invalid page in block|checksum verification failed/,
		'original member log has no FATAL, PANIC or damaged page');
}
done_testing();
