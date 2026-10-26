#!/usr/bin/env perl
# Execute the real native TAP completion predicate without starting a cluster.
use strict;
use warnings;
use FindBin;
use Test::More tests => 6;

my $source = "$FindBin::RealBin/../cluster_tap/t/481_first_fill_hot_block_contention.pl";
open(my $input, '<', $source) or die "open $source: $!";
my @lines = <$input>;
close($input) or die "close $source: $!";
my @starts = grep { $lines[$_] =~ /^sub writer_follows_shared_releases$/ } 0 .. $#lines;
die 'expected exactly one real completion predicate' unless @starts == 1;
my ($body, $complete) = ('', 0);
for my $line (@lines[$starts[0] .. $#lines])
{
	$body .= $line;
	if ($line =~ /^}$/)
	{
		$complete = 1;
		last;
	}
}
die 'incomplete real completion predicate' unless $complete;
eval $body;
die "real predicate failed to compile: $@" if $@;

my @releases = 200 .. 207;
ok(!writer_follows_shared_releases("20|60\nHOT_WRITE_DONE:150\nWRITE_DONE", \@releases),
	'buffered early writer completion cannot pass after a controller reads late');
ok(writer_follows_shared_releases("20|60\nHOT_WRITE_DONE:208\nWRITE_DONE", \@releases),
	'actual completion after all eight native releases passes');
ok(!writer_follows_shared_releases("20|60\nWRITE_DONE", \@releases),
	'ordinary row result alone cannot prove completion order');
ok(!writer_follows_shared_releases("20|60\nHOT_WRITE_DONE:208\nHOT_WRITE_DONE:209", \@releases),
	'duplicate completion clocks cannot replace the one original result');
ok(!writer_follows_shared_releases("20|60\nHOT_WRITE_DONE:208", [200 .. 206]),
	'a missing reader release cannot be certified');
ok(!writer_follows_shared_releases("20|60\nHOT_WRITE_DONE:208", [200 .. 206, 209]),
	'one still-held reader rejects completion even after seven have released');
