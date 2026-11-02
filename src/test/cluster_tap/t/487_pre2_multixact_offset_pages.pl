#!/usr/bin/env perl
#-------------------------------------------------------------------------
# 487_pre2_multixact_offset_pages.pl
#    Cross native offset pages while retaining exact members on occupied pages.
#
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
#-------------------------------------------------------------------------
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils qw(slurp_file);
use Test::More;
use JSON::PP;
use Math::BigInt;

my ($cluster, @nodes, @holders);
my $evidence = $ENV{TESTLOGDIR} // die 'native evidence directory missing';
my ($stop_attempted, $normal_stops, $query_number) = (0, 0, 0);
my (%issued, $rows_per_origin, @summaries);
my $identity_sql = q{SELECT pg_backend_pid()::text || '|' || backend_start::text
 FROM pg_stat_activity WHERE pid=pg_backend_pid()};

sub save_json
{
	my ($name, $value) = @_;
	open my $file, '>', "$evidence/mx-offset-pages-$name.json" or die $!;
	print {$file} JSON::PP->new->canonical->pretty->encode($value);
	close $file or die $!;
}

sub sql
{
	my ($node, $statement, $timeout) = @_;
	my ($stdout, $stderr) = ('', '');
	my $rc = eval { $nodes[$node]->psql('postgres', $statement,
		timeout => $timeout // 30, on_error_stop => 1,
		stdout => \$stdout, stderr => \$stderr) };
	my $error = $@;
	save_json('sql-' . ++$query_number, {node => $node, sql => $statement,
		rc => $rc, stdout => $stdout, stderr => $stderr, error => $error});
	die $error if $error;
	die "node$node native SQL failed: $stderr" unless $rc == 0;
	return $stdout;
}

sub query_holder
{
	my ($holder, $statement) = @_;
	my $value = eval { $holder->{client}->query_safe($statement) };
	my $error = $@;
	save_json('holder-query-' . ++$query_number, {name => $holder->{name},
		node => $holder->{node}, sql => $statement, stdout => $value,
		error => $error, stderr => $holder->{client}{stderr}});
	die $error if $error;
	return $value;
}

sub close_holder
{
	my ($holder, $command) = @_;
	return if $holder->{closed};
	query_holder($holder, $command) if $holder->{transaction};
	$holder->{transaction} = 0;
	die "native holder $holder->{name} did not exit cleanly" unless $holder->{client}->quit;
	$holder->{closed} = 1;
}

sub make_holder
{
	my ($node, $name) = @_;
	my $holder = {name => "node$node-$name", node => $node,
		client => $nodes[$node]->background_psql('postgres', timeout => 30)};
	push @holders, $holder;
	$holder->{client}->set_query_timer_restart();
	$holder->{identity} = query_holder($holder, $identity_sql);
	die 'native holder PID/birth missing' unless $holder->{identity} =~ /\A[1-9][0-9]*\|.+\z/;
	return $holder;
}

sub begin_holder
{
	my ($holder) = @_;
	$holder->{transaction} = 1;
	query_holder($holder, 'BEGIN');
	my $full = query_holder($holder, 'SELECT pg_current_xact_id()::text');
	die 'native FullXid missing' unless $full =~ /\A[1-9][0-9]*\z/;
	my $number = Math::BigInt->new($full);
	die 'native holder origin differs' unless $number->copy->bmod(16)->numify == $holder->{node};
	$holder->{full_xid} = $full;
	$holder->{raw_xid} = $number->copy->bmod('4294967296')->bstr;
	return $full;
}

sub page_lock_plan
{
	my ($rows) = @_;
	die 'invalid native page-lock row count' unless defined($rows)
	  && $rows =~ /\A[0-9]+\z/ && $rows >= 2 && $rows <= 513;
	return [map { {row => $_, partner => $_ == 0 ? 'anchor' : 'rolling',
		commit => ($_ > 0 && $_ < $rows - 1) ? 1 : 0,
		check_members => ($_ == 0 || $_ == $rows - 1) ? 1 : 0} } 0 .. $rows - 1];
}

sub check_live_holders
{
	my ($phase, @live) = @_;
	for my $holder (@live)
	{
		die 'native endpoint holder already ended' if $holder->{closed} || !$holder->{transaction};
		is(query_holder($holder, $identity_sql), $holder->{identity},
			"$phase: $holder->{name} keeps its backend identity") or die 'native holder identity changed';
		is(query_holder($holder, 'SELECT pg_current_xact_id_if_assigned()::text'), $holder->{full_xid},
			"$phase: $holder->{name} keeps its original active transaction") or die 'native holder transaction changed';
	}
}

sub page_round
{
	my ($node, $per_page) = @_;
	my $a = make_holder($node, 'retained');
	my $anchor = make_holder($node, 'anchor');
	my $rolling = make_holder($node, 'rolling');
	my $plan = page_lock_plan($rows_per_origin);
	save_json("node$node-lock-plan", $plan);
	begin_holder($a);
	my $locked = query_holder($a, "SELECT id FROM g_m1_mx_pages WHERE origin=$node ORDER BY id FOR KEY SHARE");
	is($locked, join("\n", 0 .. $rows_per_origin - 1), "node$node retains every literal key-share lock")
	  or die 'retained holder rows differ';
	my (@multis, @anchors, %second_xids);
	for my $step (@$plan)
	{
		my $row = $step->{row};
		my $b = $step->{partner} eq 'anchor' ? $anchor : $rolling;
		die 'native lock plan would replace a live transaction' if $b->{transaction};
		my $full = begin_holder($b);
		die 'rolling holder reused an original transaction' if $second_xids{$full}++;
		die 'native member XIDs duplicated' if $a->{raw_xid} eq $b->{raw_xid};
		my $value = $node * 100000 + $row;
		is(query_holder($b, "SELECT v FROM g_m1_mx_pages WHERE origin=$node AND id=$row FOR KEY SHARE"),
			"$value", "$b->{name} locks row$row") or die 'partner lock failed';
		my $mx = sql($node, "SELECT xmax::text FROM g_m1_mx_pages WHERE origin=$node AND id=$row");
		die 'native local MultiXact missing' unless $mx =~ /\A[1-9][0-9]*\z/
		  && Math::BigInt->new($mx)->bmod(16)->numify == $node;
		die 'native MultiXact reissued' if $issued{$mx}++;
		my $entry = {node => $node, row => $row, multi => $mx,
			offset_page => Math::BigInt->new($mx)->bdiv($per_page)->numify,
			retained_identity => $a->{identity}, retained_full_xid => $a->{full_xid},
			partner => $step->{partner}, partner_identity => $b->{identity}, partner_full_xid => $full,
			check_members => $step->{check_members},
			expected_members => join("\n", map { "$_:keysh" } sort ($a->{raw_xid}, $b->{raw_xid}))};
		push @multis, $entry;
		push @anchors, $entry if $step->{check_members};
		save_json("node$node-row$row-issued", $entry);
		if ($step->{commit})
		{
			query_holder($b, 'COMMIT');
			$b->{transaction} = 0;
		}
	}
	isnt($multis[0]{offset_page}, $multis[-1]{offset_page},
		"node$node actually crosses a native offset-page boundary") or die 'offset page not crossed';
	is(scalar(@anchors), 2, "node$node checks both live endpoint vectors") or die 'live anchors missing';
	check_live_holders('before member reads', $a, $anchor, $rolling);
	# Intermediate partners have committed and may be legitimately removed.
	# Only the two endpoints retain both original members for these exact reads.
	for my $entry (@anchors)
	{
		my ($row, $mx) = @{$entry}{qw(row multi)};
		is(sql($node, "SELECT xmax::text FROM g_m1_mx_pages WHERE origin=$node AND id=$row"),
			$mx, "node$node row$row retains its issued MX after crossing") or die 'issued MX replaced';
		# One normal psql invocation per cut: its first result binds the actual
		# observer backend, and member lookup uses that connection's native path.
		my $cut = sql($node, $identity_sql . "; SELECT xid::text || ':' || mode "
			. "FROM pg_get_multixact_members('$mx') ORDER BY xid::text COLLATE \"C\", mode");
		my @lines = split /\n/, $cut;
		my $observer = shift @lines;
		die 'native member observer PID/birth missing' unless defined($observer)
		  && $observer =~ /\A[1-9][0-9]*\|.+\z/;
		my $members = join("\n", @lines);
		save_json("node$node-row$row-members", {node => $node, row => $row, multi => $mx,
			observer_identity => $observer, members => $members, expected_members => $entry->{expected_members}});
		is($members, $entry->{expected_members}, "node$node row$row keeps both exact native members")
		  or die 'occupied offset-page members differ';
	}
	check_live_holders('after member reads', $a, $anchor, $rolling);
	close_holder($rolling, 'COMMIT');
	close_holder($anchor, 'COMMIT');
	close_holder($a, 'COMMIT');
	push @summaries, {node => $node, count => scalar(@multis),
		first_multi => $multis[0]{multi}, first_page => $multis[0]{offset_page},
		last_multi => $multis[-1]{multi}, last_page => $multis[-1]{offset_page},
		live_anchor_rows => [map { $_->{row} } @anchors], checked_member_vectors => scalar(@anchors)};
	save_json('origins', \@summaries);
}

sub normal_stop
{
	my ($phase) = @_;
	$stop_attempted = 1;
	my $receipt = $cluster->stop_cluster;
	save_json("$phase-normal-stop", $receipt);
	die 'full original normal-stop proof missing' unless $cluster->{normal_stop_proven};
	$normal_stops++;
	pass("$phase: four members complete the original normal stop");
}

my $execution = eval {
	$cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('m1_mx_pages', nodes => 4,
		blackbox => 1, required_operations => [], extra_conf => ['autovacuum=off',
			'cluster.undo_cleaner_interval_ms=1000', 'log_error_verbosity=verbose',
			q{log_line_prefix='%m [%p] '}]);
	$cluster->start_cluster;
	@nodes = $cluster->nodes;
	is(scalar(@nodes), 4, 'original PRE2 formation admits four normal members') or die 'formation differs';
	save_json('formation', [map { $cluster->observe('open_observation', {node => $_}) } 0 .. 3]);
	my $block = sql(0, 'SHOW block_size');
	die 'unsupported native block size' unless $block =~ /\A[0-9]+\z/ && $block >= 1024
	  && $block <= 32768 && ($block & ($block - 1)) == 0;
	for my $node (1 .. 3)
	{
		is(sql($node, 'SHOW block_size'), $block, "node$node has the same offset-page layout")
		  or die 'native block sizes differ';
	}
	my $per_page = $block / 4;
	$rows_per_origin = int($per_page / 16) + 1;
	my $last = $rows_per_origin - 1;
	save_json('layout', {block_size => 0 + $block, offset_entry_bytes => 4,
		offsets_per_page => $per_page, stride => 16, rows_per_origin => $rows_per_origin});
	sql(0, 'CREATE TABLE g_m1_mx_pages (origin int, id int, v int NOT NULL, PRIMARY KEY(origin,id)); '
		. 'INSERT INTO g_m1_mx_pages SELECT origin, id, origin * 100000 + id '
		. "FROM generate_series(0,3) AS origin CROSS JOIN generate_series(0,$last) AS id", 60);
	page_round($_, $per_page) for 0 .. 3;
	my $expected = join("\n", map { my $origin = $_;
		map { "$origin:$_:" . ($origin * 100000 + $_) } 0 .. $last } 0 .. 3);
	for my $node (0 .. 3)
	{
		is(sql($node, q{SELECT origin::text || ':' || id::text || ':' || v::text
		 FROM g_m1_mx_pages ORDER BY origin,id}), $expected,
			"node$node reads every independent literal row") or die 'native row ledger differs';
	}
	normal_stop('final');
	1;
};
my $first_error = $execution ? '' : $@;
my $cleanup_error = '';
for my $holder (@holders)
{
	next if $holder->{closed};
	eval { close_holder($holder, 'ROLLBACK'); 1 } or do {
		$cleanup_error .= $@;
		eval { $holder->{client}->quit; 1 } or $cleanup_error .= $@;
	};
}
if (defined($cluster) && $cluster->{running} && !$stop_attempted)
{
	eval { normal_stop('error-cleanup'); 1 } or $cleanup_error .= $@;
}
my $logs_clean = 1;
for my $node (@nodes)
{
	$logs_clean = 0 unless unlike(slurp_file($node->logfile),
		qr/(?:ERROR|FATAL|PANIC):|checksum verification failed|invalid page in block/,
		'closed native member has no error, refusal or corruption');
}
save_json('execution', {offset_page_live_members_passed =>
	($execution && $normal_stops == 1 && @summaries == 4 && $logs_clean && $cleanup_error eq '')
	? JSON::PP::true : JSON::PP::false,
	error => $first_error, cleanup_error => $cleanup_error, normal_stops => $normal_stops,
	rows_per_origin => $rows_per_origin, origins => \@summaries,
	x04 => 'NOT_READY', stage_acceptance => 'NOT_READY',
	missing_gates => ['natural G>N qualification', 'raw offset-page image and redo proof',
		'exact concurrent allocator/VACUUM overlap', 'complete M1 acceptance']});
if (!$execution || $normal_stops != 1 || @summaries != 4 || !$logs_clean || $cleanup_error ne '')
{
	fail('native MultiXact offset-page execution and cleanup complete');
	diag($first_error . $cleanup_error);
}
done_testing();
