# Author: SqlRush <sqlrush@gmail.com>
# Harness unit: real configuration/files, with server/backup commands replaced.
# This does not establish cluster formation or control-file write authority.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl";
use Cwd qw(abs_path);
use File::Copy qw(copy);
use File::Path qw(make_path);
use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;

{
	package PairProvisioningNode;
	our @ISA = ('PostgreSQL::Test::Cluster');

	sub new
	{
		my ($class, $name, %opts) = @_;
		return bless {
			_basedir => PostgreSQL::Test::Utils::tempdir(), _name => $name,
			_port => $opts{port},
		}, $class;
	}

	sub init
	{
		my ($self, %opts) = @_;
		$self->{init_extra} = $opts{extra} // [];
		my ($root_arg) = grep { /^--pgrac-hw-snapshot-root=/ } @{$self->{init_extra}};
		if (defined $root_arg)
		{
			(my $root = $root_arg) =~ s/^--pgrac-hw-snapshot-root=//;
			$self->{hw_root_was_empty} = !scalar(glob("$root/*"));
			mkdir "$root/global" or die "initdb shared global: $!";
		}
		my $data = $self->data_dir;
		File::Path::make_path("$data/global", "$data/pg_wal");
		PostgreSQL::Test::Utils::append_to_file("$data/global/pg_control", "12345\n");
		PostgreSQL::Test::Utils::append_to_file("$data/postgresql.conf", "# initdb\n");
	}

	sub setting
	{
		my ($self, $key) = @_;
		my $conf = PostgreSQL::Test::Utils::slurp_file($self->data_dir . '/postgresql.conf');
		my @values = $conf =~ /^\s*\Q$key\E\s*=\s*([^\n]+)$/mg;
		return undef unless @values;
		my $value = $values[-1];
		$value =~ s/^'|'\s*$//g;
		return $value;
	}

	# Replace only the seed's server process, preserving its filesystem effects
	# relevant to the harness: one authority and a local symlink to it.
	sub start
	{
		my ($self) = @_;
		my $root = $self->setting('cluster.shared_data_dir');
		my $local = $self->data_dir . '/global/pg_control';
		$self->{seed_root} = $root;
		File::Copy::copy($local, "$root/global/pg_control") or die "copy seed: $!";
		unlink($local) or die "unlink seed: $!";
		symlink("$root/global/pg_control", $local) or die "symlink seed: $!";
		return 1;
	}

	sub stop { return 1; }
	sub installed_command { return $_[1]; }

	# pg_basebackup dereferences the control-file symlink. Retain that shape,
	# the seed configuration, and an ordinary copied pg_wal directory.
	sub backup { }
	sub init_from_backup
	{
		my ($self, $seed) = @_;
		my $data = $self->data_dir;
		File::Path::make_path("$data/global", "$data/pg_wal");
		for my $file ('global/pg_control', 'postgresql.conf', 'pgrac.conf')
		{
			File::Copy::copy($seed->data_dir . "/$file", "$data/$file")
			  or die "copy backup $file: $!";
		}
	}
}

{
	no warnings 'redefine';
	local *PostgreSQL::Test::Cluster::new = sub {
		shift;
		return PairProvisioningNode->new(@_);
	};
	local *PostgreSQL::Test::Utils::run_command = sub {
		my ($argv) = @_;
		die 'unexpected command' unless $argv->[0] eq 'pg_controldata' && @$argv == 2;
		my $sysid = PostgreSQL::Test::Utils::slurp_file("$argv->[1]/global/pg_control");
		return ("Database system identifier: $sysid", '');
	};

	for my $with_data (0, 1)
	{
		my $pair = PostgreSQL::Test::ClusterPair->new_pair("pair_root_$with_data",
			wal_threads_root => 1, quorum_voting_disks => 1,
			true_shared_sysid_cf => 1, shared_data => $with_data);
		my $root = $pair->{shared_control_root};
		is_deeply([grep { /^--pgrac-hw-snapshot-/ } @{$pair->node0->{init_extra}}],
			["--pgrac-hw-snapshot-root=$root", '--pgrac-hw-snapshot-owner=0'],
			'seed delegates its initial HW metadata to the original initdb owner');
		ok($pair->node0->{hw_root_was_empty},
			'initdb receives an empty shared root and owns global directory creation');
		is($pair->node0->{seed_root}, $root, 'seed publishes to the retained authority root');
		is(readlink($pair->node0->data_dir . '/global/pg_control'), "$root/global/pg_control",
			'seed link still selects the original authority');
		is($pair->node0->setting('cluster.shared_data_dir'), $root,
			'node0 final configuration preserves the seed authority');
		is($pair->node1->setting('cluster.shared_data_dir'), $root,
			'node1 final configuration selects the same authority');
		is($pair->shared_data_root, $with_data ? $root : undef,
			'shared user data uses that authority root only when requested');
		ok(!-l($pair->node1->data_dir . '/global/pg_control'),
			'backup peer keeps a regular control image until its own startup');
		is(PostgreSQL::Test::Utils::slurp_file($pair->node1->data_dir . '/global/pg_control'),
			"12345\n", 'backup preserves the seed control image');
		is(readlink($pair->node1->data_dir . '/pg_wal'), $pair->wal_threads_root . '/thread_2',
			'backup peer retains its distinct WAL thread');
	}

	my $data_pair = PostgreSQL::Test::ClusterPair->new_pair('pair_data_only', shared_data => 1);
	ok(!defined($data_pair->{shared_control_root}), 'data-only pair does not create shared control');
	for my $node ($data_pair->node0, $data_pair->node1)
	{
		is($node->setting('cluster.shared_data_dir'), $data_pair->shared_data_root,
			'data-only configuration keeps the requested data root');
		ok(!defined($node->setting('cluster.controlfile_shared_authority')),
			'data-only pair does not enable shared control implicitly');
		ok(!-l($node->data_dir . '/global/pg_control'),
			'data-only pair keeps its per-node control file');
	}

	my $parent = PostgreSQL::Test::Utils::tempdir();
	mkdir "$parent/real" or die "mkdir alias target: $!";
	symlink("$parent/real", "$parent/alias") or die "symlink test root: $!";
	{
		no warnings 'once';
		local $PostgreSQL::Test::Utils::tmp_check = "$parent/alias";
		my $pair = PostgreSQL::Test::ClusterPair->new_pair('pair_parent_alias',
			wal_threads_root => 1, quorum_voting_disks => 1,
			true_shared_sysid_cf => 1, shared_data => 1);
		my $canonical = abs_path($pair->{shared_control_root});
		is($pair->{shared_control_root}, $canonical,
			'shared control root resolves a parent directory alias');
		is($pair->shared_data_root, $canonical,
			'shared data accessor keeps the same canonical root');
		my ($hw_root) = grep { /^--pgrac-hw-snapshot-root=/ } @{$pair->node0->{init_extra}};
		is($hw_root, "--pgrac-hw-snapshot-root=$canonical",
			'initdb receives the canonical root required by its creator contract');
		my $wal_root = abs_path($pair->wal_threads_root);
		is($pair->wal_threads_root, $wal_root,
			'WAL accessor resolves the same parent directory alias');
		my ($wal_arg) = grep { /^--pgrac-wal-state-root=/ } @{$pair->node0->{init_extra}};
		is($wal_arg, "--pgrac-wal-state-root=$wal_root",
			'initdb receives the canonical WAL state root');
		is($pair->node0->{init_extra}->[1], "$wal_root/thread_1",
			'initdb WAL directory uses the canonical WAL state root');
	}
}

done_testing();
