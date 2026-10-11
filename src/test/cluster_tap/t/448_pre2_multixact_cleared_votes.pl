#!/usr/bin/env perl
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# PGRAC: a cold copy with cleared activation must fail before admission.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use Test::More;
use IPC::Run qw(run timeout);
use JSON::PP;

my ($out, $err) = ('', '');
my $passed = eval {
	my $layout = $ENV{PGRAC_PRE2_COLD_LAYOUT};
	die 'missing independent cold-copy fixture' unless defined($layout) && -f $layout;
	run([$ENV{PYTHON} // 'python3', "$FindBin::RealBin/../pre2/mx_cold_restart.py",
		$layout, $ENV{PGRAC_PRE2_ENTRY_FILE}, $ENV{PGRAC_PRE2_BINDIR},
		"$ENV{TESTLOGDIR}/cleared-votes-result.json"], '>', \$out, '2>', \$err, timeout(100))
		or die "cold-copy refusal test failed: $err";
	my $result = decode_json($out);
	die 'cold-copy refusal is incomplete' unless $result->{refused} && $result->{all_stopped};
	1;
};
ok($passed, 'cleared activation cold copy is refused without native-empty proof');
diag($@ || $err) unless $passed;
done_testing();
