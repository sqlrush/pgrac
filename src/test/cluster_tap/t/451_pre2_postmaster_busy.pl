#!/usr/bin/env perl
# Author: SqlRush <sqlrush@gmail.com>
# PGRAC: a real postmaster observes an LMON READY lock held across polls.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::Utils;
use Test::More;

my $bindir = $ENV{PGRAC_PRE2_BINDIR};
die 'an exact installed bindir is required' unless defined($bindir) && -d $bindir;
my $root = "$ENV{TESTLOGDIR}/phase-busy";
my $rc = system('python3', "$FindBin::RealBin/../pre2/phase_busy.py",
	'--bindir', $bindir, '--output', $root);
is($rc, 0, 'real busy READY lock preserves postmaster startup and normal stop');
diag("complete DATA, process observations and logs: $root");
done_testing();
