# Author: SqlRush <sqlrush@gmail.com>
# Harness unit only: in-memory observations, no DATA, server, or acceptance.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use PostgreSQL::Test::ClusterPRE2;
use Test::More;
use File::Temp qw(tempdir);

my $clock = 0;
{
	package UnitNode;
	sub _update_pid { }
	sub safe_psql
	{
		my ($self, $db, $sql) = @_;
		return $self->{identity} // '123|202610220' if $sql =~ /pg_control_system/;
		return 'on|on|on|on|on' if $sql =~ /current_setting/;
		return $self->{id} if $sql =~ /node_id/;
		die 'unexpected unit SQL';
	}
}
{
	package UnitFixture;
	our @ISA = ('PostgreSQL::Test::ClusterPRE2');
	sub _data_identity { return $_[0]->{identity}; }
	sub writer
	{
		my ($node, $generation) = @_;
		return {node => $node, thread => $node + 1, incarnation => 1,
			wal_generation => $generation + 1, boot => "$node-$generation"};
	}
	sub _driver
	{
		my ($self, $op, $args, $budget) = @_;
		push @{$self->{calls}}, $op;
		if ($op eq 'shared_start') { $self->{generation}++; return {}; }
		if ($op eq 'shared_stop') { return {all_processes_exited => JSON::PP::true}; }
		if ($op eq 'shutdown_observation')
		{
			return {system_identifier => '123', final_checkpoint => JSON::PP::true,
				writers => [map { +{%$_, state => 'CLOSED'} } @{$args->{writers}}]};
		}
		my $root = {system_identifier => '123', root_digest => 'a' x 64,
			generation => $self->{generation}};
		if ($op eq 'root_observation')
		{
			$clock = 61 if $self->{late};
			return $root;
		}
		if ($op eq 'open_observation')
		{
			my $writer = writer($args->{node}, $self->{generation});
			$writer->{predecessor} = writer($args->{node}, $self->{generation} - 1);
			$writer->{predecessor}{boot} = 'wrong-boot' if $self->{wrong_predecessor};
			return {%$root, phase => 'OPEN', members => [0 .. 3], writer => $writer};
		}
		die "unexpected unit operation: $op";
	}
}

sub fixture
{
	return bless {nodes => [map { bless {id => $_}, 'UnitNode' } 0 .. 3],
		identity => 'same-data', data_identity => 'same-data', generation => 0,
		calls => []}, 'UnitFixture';
}
{
	no warnings 'redefine';
	local *PostgreSQL::Test::ClusterPRE2::_now = sub { return $clock; };
	my $cluster = fixture();
	ok(eval { $cluster->start_cluster; 1 }, 'unit OPEN has four distinct native writers') or diag($@);
	ok(eval { $cluster->stop_cluster; 1 }, 'unit stop checks CLOSED against the current boot') or diag($@);
	ok(eval { $cluster->restart_cluster; 1 }, 'unit restart consumes the exact CLOSED predecessors') or diag($@);
	is(scalar(grep { $_ eq 'shared_start' } @{$cluster->{calls}}), 2, 'restart starts existing DATA');
	is(scalar(grep { $_ eq 'fresh_init' } @{$cluster->{calls}}), 0, 'restart never initializes DATA');
	$cluster->stop_cluster;
	$cluster->{identity} = 'replacement-data';
	eval { $cluster->restart_cluster };
	like($@, qr/directories were replaced/, 'directory replacement cannot pass same-DATA restart');
	$cluster->{identity} = 'same-data';
	$cluster->{wrong_predecessor} = 1;
	eval { $cluster->restart_cluster };
	like($@, qr/exact CLOSED predecessor/, 'unrelated predecessor cannot pass restart');
	my $late = fixture();
	$late->{system_identifier} = '123';
	$late->{generation} = 1;
	$late->{late} = 1;
	eval { $late->wait_open };
	like($@, qr/OPEN deadline expired/, 'all OPEN after deadline remains a failure');
	$clock = 0;
	for my $catalog ('202609290', '202610221', '', 'unknown', '0202610220')
	{
		my $mixed = fixture();
		$mixed->{nodes}[-1]{identity} = "123|$catalog";
		eval { $mixed->start_cluster };
		like($@, qr/PRE2 format is not activated/,
			"non-current catalog '$catalog' on the last member is rejected");
		ok($mixed->{running}, 'partial startup still requires cleanup');
		is(scalar(grep { $_ eq 'root_observation' } @{$mixed->{calls}}), 0,
			'catalog rejection cannot reach root/OPEN validation');
	}
	my $old = fixture();
	$_->{identity} = '123|202609290' for @{$old->{nodes}};
	eval { $old->start_cluster };
	like($@, qr/PRE2 format is not activated/, 'a homogeneous legacy catalog is rejected');
	my $foreign = fixture();
	$foreign->{nodes}[-1]{identity} = '456|202610220';
	eval { $foreign->start_cluster };
	like($@, qr/different system identifiers/, 'current catalogs do not bypass system identity');
}

# Read the exact current macro, never the PRIOR compatibility macro or a range.
my $dir = tempdir(CLEANUP => 1);
my $header = "$dir/catversion.h";
for my $case (
	["#define CATALOG_VERSION_NO 209901010\n#define CATALOG_VERSION_NO_PRIOR 1\n", '209901010'],
	["#define CATALOG_VERSION_NO_PRIOR 209901010\n", undef],
	["#define CATALOG_VERSION_NO 0\n", undef],
	["#define CATALOG_VERSION_NO 0209901010\n", undef],
	["#define CATALOG_VERSION_NO unknown\n", undef],
	["#define CATALOG_VERSION_NO 209901010\n#define CATALOG_VERSION_NO 209901010\n", undef],
	["#define CATALOG_VERSION_NO unknown\n#define CATALOG_VERSION_NO 209901010\n", undef])
{
	open my $file, '>', $header or die "write unit header: $!";
	print {$file} $case->[0];
	close $file or die "close unit header: $!";
	my $version = eval { PostgreSQL::Test::ClusterPRE2::_catalog_version($header) };
	if (defined $case->[1])
	{
		is($version, $case->[1], 'catalog expectation follows the exact source macro');
	}
	else
	{
		like($@, qr/invalid source catalog version/, 'ambiguous or invalid source catalog is rejected');
	}
}
eval { PostgreSQL::Test::ClusterPRE2::_catalog_version("$dir/missing.h") };
like($@, qr/cannot read source catalog version/, 'missing source catalog has no fallback');
done_testing();
