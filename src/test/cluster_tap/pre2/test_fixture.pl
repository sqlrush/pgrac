# Author: SqlRush <sqlrush@gmail.com>
# Harness-only test. A missing fresh initializer must not invoke a legacy one.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::ClusterPRE2;
use Test::More;

local $ENV{PGRAC_PRE2_TEST_ADAPTER};
delete $ENV{PGRAC_PRE2_TEST_ADAPTER};
local $ENV{PGRAC_PRE2_ENTRY_FILE};
delete $ENV{PGRAC_PRE2_ENTRY_FILE};
my $fixture = eval { PostgreSQL::Test::ClusterPRE2->new_cluster('pre2_no_adapter', nodes => 3) };
ok(!defined($fixture), 'no fixture without the current product initializer');
like($@, qr/^BLOCKED: A S11\/S12: supported fresh PRE2 member initialization/,
    'missing producer explicitly BLOCKED, not skipped or returned as a fixture');
for my $number (337, 339, 346, 361, 362, 366, 371)
{
    eval { PostgreSQL::Test::ClusterPRE2->new_for_tap($number, "shared_$number") };
    like($@, qr/^BLOCKED:/, "$number routes to the fresh entrance, never the backup/seed path");
}
done_testing();
