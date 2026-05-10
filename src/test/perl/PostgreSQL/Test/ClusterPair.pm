
# spec-2.2 D15 -- ClusterPair: 2-instance harness for tier1 TAP tests.
#
# Encapsulates the boilerplate that 076 (and future Stage 2.X tests) need
# to spin up two cooperating pgrac instances with mutual interconnect:
#   - allocate 4 random free ports (2 PG + 2 IC)
#   - init both nodes
#   - append cluster.* GUCs (cluster.enabled = on, tier = tier1, node_id)
#     using append_conf (per spec-2.1 v1.0.1 L59 -- adjust_conf is
#     replace-only and would no-op for first-time GUC writes)
#   - write mutually-trusting pgrac.conf to both data dirs
#   - start_pair / stop_pair / heartbeat_seen helpers
#
# Spec authority: pgrac:specs/spec-2.2-* §4.3.
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

package PostgreSQL::Test::ClusterPair;

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;


#-----------------------------------------------------------------------
# new_pair($class, $cluster_name, %opts)
#
#	Allocate two PG instances that share a pgrac cluster name.
#	Optional %opts:
#	  cluster_name_override  : write a different name into the second
#	                           node's pgrac.conf (used by 076 L6 to
#	                           force HELLO mismatch; default = same)
#	  extra_conf             : arrayref of extra GUC lines appended
#	                           to BOTH nodes' postgresql.conf
#	  quorum_voting_disks    : positive integer N — opt-in spec-2.6
#	                           strict-mode harness.  Pre-allocates N
#	                           shared voting-disk files (zero-filled,
#	                           64KB each) in a tempdir and configures
#	                           BOTH nodes with cluster.allow_single_
#	                           node=off + cluster.voting_disks=CSV.
#	                           Default unset = legacy compatibility
#	                           mode (allow_single_node=on, no voting
#	                           disks); preserves behaviour for the 8
#	                           pre-existing TAPs (075/076/079/080/
#	                           081/085/090/097-style negative).  Use
#	                           the ->voting_disk_paths accessor to
#	                           recover the file list (e.g., for fault
#	                           injection in future Hardening v0.5+).
#-----------------------------------------------------------------------
sub new_pair
{
	my ($class, $cluster_name, %opts) = @_;

	# Allocate 4 distinct free ports.  get_free_port() reserves the port
	# until the test exits, so successive calls return distinct values.
	my $pg_port_0 = PostgreSQL::Test::Cluster::get_free_port();
	my $pg_port_1 = PostgreSQL::Test::Cluster::get_free_port();
	my $ic_port_0 = PostgreSQL::Test::Cluster::get_free_port();
	my $ic_port_1 = PostgreSQL::Test::Cluster::get_free_port();

	my $node0 =
	  PostgreSQL::Test::Cluster->new("${cluster_name}_node0", port => $pg_port_0);
	my $node1 =
	  PostgreSQL::Test::Cluster->new("${cluster_name}_node1", port => $pg_port_1);

	# spec-2.6 strict-mode opt-in: pre-allocate N shared voting-disk
	# files (zero-filled, 128 slots × 512B = 64KB each) in a tempdir
	# both postmasters can read/write.  Disks list is built once and
	# written into both nodes' postgresql.conf so the same file paths
	# back the same disk_index slots from both sides.
	my $voting_disks_csv;
	my @voting_disk_paths;
	if (defined $opts{quorum_voting_disks} && $opts{quorum_voting_disks} > 0)
	{
		my $disk_dir = PostgreSQL::Test::Utils::tempdir();
		for my $i (0 .. $opts{quorum_voting_disks} - 1)
		{
			my $path = "$disk_dir/disk$i";
			open(my $fh, '>', $path) or die "open $path: $!";
			binmode $fh;
			print $fh ("\0" x (128 * 512));
			close $fh;
			push @voting_disk_paths, $path;
		}
		$voting_disks_csv = join(',', @voting_disk_paths);
	}

	for my $node ($node0, $node1)
	{
		$node->init;

		# spec-2.2 §3.3 -- enable cluster + tier1.  per L59 first-time
		# GUC writes use append_conf (adjust_conf is replace-only).
		$node->append_conf('postgresql.conf', "cluster.enabled = on\n");
		$node->append_conf('postgresql.conf',
			"cluster.interconnect_tier = tier1\n");

		# spec-2.6 — default to single-node compatibility mode for
		# the harness.  ClusterPair is used by 8 TAPs (075/076/079/
		# 080/081/085/090/096) that test interconnect / cssd / smgr
		# / quorum mechanics, NOT spec-2.1 strict-mode semantics.
		# Strict mode (allow_single_node=off + cluster.voting_disks
		# CSV) triggers Q7 validator FATAL + spawns qvotec polling
		# every 2s; the polling adds background catalog activity
		# that destabilises sensitive observability assertions
		# (e.g. 090 L9c HTAB shrink check).  Tests that genuinely
		# require strict mode opt in via quorum_voting_disks =>
		# N (096 happy path) or set the GUCs explicitly via
		# extra_conf (097 does this); ClusterPair stays neutral.
		if (defined $voting_disks_csv)
		{
			$node->append_conf('postgresql.conf',
				"cluster.allow_single_node = off\n");
			$node->append_conf('postgresql.conf',
				"cluster.voting_disks = '$voting_disks_csv'\n");
		}
		else
		{
			$node->append_conf('postgresql.conf',
				"cluster.allow_single_node = on\n");
		}

		# Keep shared_buffers small so 2 postmasters fit in CI runners
		# (R8 mitigation).
		$node->append_conf('postgresql.conf', "shared_buffers = 16MB\n");

		if ($opts{extra_conf})
		{
			for my $line (@{ $opts{extra_conf} })
			{
				$node->append_conf('postgresql.conf', "$line\n");
			}
		}
	}

	$node0->append_conf('postgresql.conf', "cluster.node_id = 0\n");
	$node1->append_conf('postgresql.conf', "cluster.node_id = 1\n");

	# Mutual pgrac.conf for node0 -- declares both peers in the same cluster.
	my $pgrac_conf_0 = <<EOC;
[cluster]
name = $cluster_name

[node.0]
interconnect_addr = 127.0.0.1:$ic_port_0

[node.1]
interconnect_addr = 127.0.0.1:$ic_port_1
EOC

	# node1's pgrac.conf -- same except optionally a different cluster_name
	# (L6 HELLO mismatch test).
	my $alt_name = $opts{cluster_name_override} // $cluster_name;
	my $pgrac_conf_1 = <<EOC;
[cluster]
name = $alt_name

[node.0]
interconnect_addr = 127.0.0.1:$ic_port_0

[node.1]
interconnect_addr = 127.0.0.1:$ic_port_1
EOC

	PostgreSQL::Test::Utils::append_to_file(
		$node0->data_dir . '/pgrac.conf', $pgrac_conf_0);
	PostgreSQL::Test::Utils::append_to_file(
		$node1->data_dir . '/pgrac.conf', $pgrac_conf_1);

	return bless {
		node0       => $node0,
		node1       => $node1,
		cluster_name => $cluster_name,
		pg_ports    => [ $pg_port_0, $pg_port_1 ],
		ic_ports    => [ $ic_port_0, $ic_port_1 ],
		voting_disk_paths => \@voting_disk_paths,
	}, $class;
}


sub start_pair
{
	my ($self, %opts) = @_;
	$self->{node0}->start(%opts);
	$self->{node1}->start(%opts);
	return;
}

sub stop_pair
{
	my ($self) = @_;
	$self->{node0}->stop if $self->{node0};
	$self->{node1}->stop if $self->{node1};
	return;
}

sub node0   { return $_[0]->{node0}; }
sub node1   { return $_[0]->{node1}; }
sub ic_port { return $_[0]->{ic_ports}[ $_[1] ]; }

#-----------------------------------------------------------------------
# voting_disk_paths($self)
#
#	Returns the list of pre-allocated voting-disk file paths (one
#	per element).  Empty list when ClusterPair was created without
#	the quorum_voting_disks opt.  Lets future fault-injection TAPs
#	(spec-2.6 Hardening v0.5+ — 096 L8 all-disk-fail) reach into the
#	files for write/chmod/unlink scenarios.
#-----------------------------------------------------------------------
sub voting_disk_paths
{
	my ($self) = @_;
	return @{ $self->{voting_disk_paths} // [] };
}


#-----------------------------------------------------------------------
# heartbeat_seen($self, $from, $to)
#
#	Returns 't' / 'f' as a string -- whether $from instance has sent
#	at least one heartbeat to $to peer (heartbeat_send_count > 0).
#	Caller wraps in a poll loop with timeout.
#-----------------------------------------------------------------------
sub heartbeat_seen
{
	my ($self, $from, $to) = @_;
	my $node = $from == 0 ? $self->{node0} : $self->{node1};
	return $node->safe_psql(
		'postgres',
		"SELECT heartbeat_send_count > 0 FROM pg_cluster_ic_peers WHERE node_id = $to"
	);
}


#-----------------------------------------------------------------------
# wait_for_peer_state($self, $from, $to, $expected_state, $timeout_s)
#
#	Polls $from's pg_cluster_ic_peers.state for $to until it matches
#	$expected_state or $timeout_s elapses.  Returns 1 on success, 0 on
#	timeout.  Default timeout = 10 s.
#-----------------------------------------------------------------------
sub wait_for_peer_state
{
	my ($self, $from, $to, $expected_state, $timeout_s) = @_;
	$timeout_s //= 10;
	my $node = $from == 0 ? $self->{node0} : $self->{node1};
	my $deadline = time + $timeout_s;
	while (time < $deadline)
	{
		my $state = $node->safe_psql('postgres',
			"SELECT state FROM pg_cluster_ic_peers WHERE node_id = $to");
		return 1 if defined $state && $state eq $expected_state;
		select(undef, undef, undef, 0.25);
	}
	return 0;
}


1;
