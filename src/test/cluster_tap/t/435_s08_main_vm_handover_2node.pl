#-------------------------------------------------------------------------
#
# 435_s08_main_vm_handover_2node.pl
#    Two members of a fresh shared cluster hand MAIN and VM pages back and
#    forth; no committed write may be lost, before or after a normal
#    stop and same-DATA restart.  The legs hold whether a page is written
#    before it is handed over or carried as a past image afterwards, so
#    they apply unchanged with and without that write.
#
#      L1   real handover: alternating members update rows of the same
#           heap pages and their primary-key leaf; both members read the
#           expected rows after every batch
#      L2   re-dirtied after handover: the same page is updated by one
#           member, then the other, then the first again, with no
#           checkpoint in between
#      L3   VM handover: VACUUM on one member sets all-visible bits, an
#           update on the other clears one; index-only and sequential
#           scans agree on both members after every exchange
#      L4   handover during checkpoint: both members checkpoint
#           repeatedly while the pages keep changing hands
#      L5   delayed and lost receipts: the requester's completion notice
#           to the master is held back (sleep) or dropped (skip) while
#           the pages keep changing hands
#      L6   normal stop and same-DATA restart: both members read exactly
#           the committed rows and agreeing scans, and a cross-member
#           update after restart is visible to the other member
#
# IDENTIFICATION
#    src/test/cluster_tap/t/435_s08_main_vm_handover_2node.pl
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
use PostgreSQL::Test::ClusterPRE2;
use PostgreSQL::Test::Utils;
use Test::More;

use constant ROWS => 96;
use constant ROUNDS => 40;

my $cluster;
my @nodes;
my %expected;	# id -> v, the committed state

sub sql { return $_[0]->safe_psql('postgres', $_[1], timeout => 60); }

# A failed leg still stops the members it started.
END
{
	if (defined($cluster) && $cluster->{running})
	{
		my $rc = $?;
		eval { $cluster->stop_cluster; 1 } or diag("cleanup stop failed: $@");
		$? = $rc;
	}
}

sub rows_text
{
	return join(',', map { "$_:$expected{$_}" } sort { $a <=> $b } keys %expected);
}

sub check_rows
{
	my ($label) = @_;
	my $want = rows_text();
	for my $i (0 .. $#nodes)
	{
		is(sql($nodes[$i],
				q{SELECT string_agg(id || ':' || v, ',' ORDER BY id) FROM s08_main}),
			$want, "$label: node$i reads every committed row");
	}
}

# An index-only scan answers from the VM; a wrong all-visible bit would
# make it disagree with the heap.
sub check_scans
{
	my ($label) = @_;
	my $sum = 0;
	$sum += $_ for keys %expected;
	my $want = scalar(keys %expected) . '|' . $sum;
	for my $i (0 .. $#nodes)
	{
		my $seq = sql($nodes[$i], q{
			SET enable_indexscan = off; SET enable_indexonlyscan = off;
			SET enable_bitmapscan = off;
			SELECT count(*) || '|' || sum(id) FROM s08_main WHERE id >= 0});
		my $ios = sql($nodes[$i], q{
			SET enable_seqscan = off; SET enable_bitmapscan = off;
			SET enable_indexscan = off;
			SELECT count(*) || '|' || sum(id) FROM s08_main WHERE id >= 0});
		is($seq, $want, "$label: node$i sequential scan");
		is($ios, $want, "$label: node$i index-only scan agrees");
	}
}

sub update_on
{
	my ($member, $id, $delta) = @_;
	sql($nodes[$member], "UPDATE s08_main SET v = v + $delta WHERE id = $id");
	$expected{$id} += $delta;
}

# Rows on the same page as id (fillfactor keeps a few rows per page).
sub same_page_ids
{
	my ($id) = @_;
	return grep { exists $expected{$_} } ($id, $id + 1, $id + 2);
}

$cluster = PostgreSQL::Test::ClusterPRE2->new_cluster('s08_handover', nodes => 2,
	blackbox => 1, extra_conf => ['autovacuum = off']);
$cluster->start_cluster;
@nodes = $cluster->nodes;

# Setup: small rows, low fillfactor so that each heap page holds a few
# rows and every round moves a page that other rows also live on.
sql($nodes[0], q{
	CREATE TABLE s08_main (id int PRIMARY KEY, v int NOT NULL, pad text NOT NULL)
		WITH (fillfactor = 20)});
sql($nodes[0], 'INSERT INTO s08_main SELECT g, 0, repeat(\'x\', 200) FROM generate_series(1, '
	  . ROWS . ') g');
$expected{$_} = 0 for 1 .. ROWS;
sql($nodes[0], 'VACUUM s08_main');
check_rows('setup');
check_scans('setup');

# L1: alternating members update rows of shared pages.
for my $round (1 .. ROUNDS)
{
	my $id = 1 + (($round * 7) % (ROWS - 2));
	update_on($round % 2, $_, $round) for same_page_ids($id);
	check_rows("L1 round $round") if $round % 10 == 0;
}

# L2: the same page goes 0 -> 1 -> 0 with no checkpoint in between.
for my $id (5, 41, 77)
{
	update_on(0, $id, 1);
	update_on(1, $id, 10);
	update_on(0, $id, 100);
	update_on(1, $id + 1, 1000) if exists $expected{$id + 1};
	check_rows("L2 page of $id");
}

# L3: VM pages change hands: VACUUM sets bits on one member, an update
# on the other clears one of them, and back.
for my $cycle (1 .. 6)
{
	my $vacuumer = $cycle % 2;
	my $writer = 1 - $vacuumer;
	sql($nodes[$vacuumer], 'VACUUM s08_main');
	update_on($writer, 1 + (($cycle * 13) % ROWS), 3);
	check_scans("L3 cycle $cycle");
}
check_rows('L3');

# L4: both members checkpoint while the pages keep changing hands.
{
	my @checkpointers;
	for my $i (0 .. $#nodes)
	{
		my $script = join('', map { "CHECKPOINT;\nSELECT pg_sleep(0.05);\n" } 1 .. 20);
		my ($out, $err) = ('', '');
		push @checkpointers, [start(['psql', '-XAtq', '-v', 'ON_ERROR_STOP=1',
					'-d', $nodes[$i]->connstr('postgres')],
				'<', \$script, '>', \$out, '2>', \$err, timeout(120)), \$err, $i];
	}
	for my $round (1 .. ROUNDS)
	{
		my $id = 1 + (($round * 11) % (ROWS - 2));
		update_on($round % 2, $_, 2) for same_page_ids($id);
	}
	for my $c (@checkpointers)
	{
		my ($h, $err, $i) = @$c;
		ok(finish($h), "L4 node$i checkpoints completed during handovers")
		  or diag(${$err});
	}
	check_rows('L4');
	check_scans('L4');
}

# L5: the requesting backend holds back (sleep) or drops (skip) its
# completion notice to the master; later handovers must not lose a write.
for my $mode (['sleep', 300000], ['skip', 0])
{
	my ($type, $arg) = @$mode;
	for my $round (1 .. 10)
	{
		my $member = $round % 2;
		my $id = 1 + (($round * 17) % ROWS);
		sql($nodes[$member],
			"SELECT cluster_inject_fault('cluster-gcs-block-done-drop', '$type', $arg);\n"
			  . "UPDATE s08_main SET v = v + 5 WHERE id = $id;\n"
			  . "SELECT cluster_inject_fault('cluster-gcs-block-done-drop', 'none', 0);");
		$expected{$id} += 5;
		update_on(1 - $member, $id, 7);
	}
	check_rows("L5 $type");
}
check_scans('L5');

# L6: normal stop, same-DATA restart, exact rows and agreeing scans.
my $before = rows_text();
$cluster->stop_cluster;
$cluster->restart_cluster;
@nodes = $cluster->nodes;
is(rows_text(), $before, 'L6 committed model unchanged across the restart');
check_rows('L6 after restart');
check_scans('L6 after restart');
update_on(1, 2, 9);
is(sql($nodes[0], 'SELECT v FROM s08_main WHERE id = 2'), $expected{2},
	'L6 a cross-member update after restart is visible');
$cluster->stop_cluster;

done_testing();
