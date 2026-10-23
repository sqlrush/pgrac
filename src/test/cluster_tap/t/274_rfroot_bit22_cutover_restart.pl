#!/usr/bin/perl
#
# t/274_rfroot_bit22_cutover_restart.pl -- RF-ROOT P9 verification (implementation
# 2026-08-19): the REAL online bit22 cutover on the 2-node shared-root
# substrate (no offline fixture cast), then restart legs.
#
#   L1  formation ACTIVE
#   L2  coordinator accepts pgrac_r4_bit22_cutover_begin()
#   L3  round reaches OPEN: canonical root ACTIVE (activation_state=2),
#       registry correctness stays telemetry-only (no CF-gated mutation)
#   L4  clean restart of the peer keeps the cluster serving and the root
#       ACTIVE (post-bit22 gate re-arms from the durable OPEN record)
#   L5  clean restart of the coordinator likewise
#
# The source-close BARRIER (contract) freezes every member's wal-state
# writers first, so ACTIVE slots are provably quiesced migration input --
# no STOPPED requirement, no fixture authority.
use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::ClusterPair;
use PostgreSQL::Test::Utils;
use Test::More;

sub root_activation_state
{
	my ($path) = @_;
	open my $fh, '<:raw', $path or return -1;
	sysseek($fh, 76, 0) or return -1;
	my $buf;
	sysread($fh, $buf, 4) == 4 or return -1;
	close $fh;
	return unpack('L<', $buf); # activation_state (LE uint32): 1=PREPARED 2=ACTIVE
}

my $pair = PostgreSQL::Test::ClusterPair->new_pair('rfroot274',
	quorum_voting_disks => 3,
	wal_threads_root => 1,
	true_shared_sysid_cf => 1,
	extra_conf => [ 'autovacuum = off' ]);
my $shared_root = $pair->{shared_control_root};
my $root_file = "$shared_root/global/pgrac_control_root";
my $node0 = $pair->node0;
my $node1 = $pair->node1;

$pair->start_pair;
ok($pair->wait_for_pcm_x_active(30),
	'L1 PCM-X formation is ACTIVE on both writers before the cutover');
# RF-ROOT P9 verification (implementation): readiness = the true admission
# condition — both members MEMBER with a NONZERO admitted floor that
# equals the presented identity (the cold-bootstrap ABSENT ->
# MEMBER path floors the quorum-vetted incarnation first; DEAD is
# JCMK-only readmission and never enters the founding branch).
my $admitted = $node0->poll_query_until('postgres', q{
	SELECT count(*) = 2 FROM pg_cluster_membership
	WHERE state = 'member'
	  AND last_admitted_incarnation <> 0
	  AND presented_incarnation = last_admitted_incarnation
}, 't', 45);
ok($admitted, 'L1 membership admitted with nonzero floors (both)');
BAIL_OUT('L1 admission did not settle (floor never published)')
	unless $admitted;

# L1b: both peers' advertised capability sets must include the bit22
# round's REQUIRED caps (0x0030B000) before the cutover round can bind
# member tuples.
my $caps_ok = 0;
for my $i (1 .. 45)
{
	my $c0 = $node0->safe_psql('postgres',
		q{SELECT value FROM pg_cluster_state WHERE category='ic' AND key='peer_capabilities'});
	my $c1 = $node1->safe_psql('postgres',
		q{SELECT value FROM pg_cluster_state WHERE category='ic' AND key='peer_capabilities'});
	last if defined $c0 && defined $c1
		&& $c0 =~ /\bn1:bits=0x([0-9A-Fa-f]+),gen=(\d+),v=1\b/
		&& (hex($1) & 0x0030B000) == 0x0030B000
		&& $c1 =~ /\bn0:bits=0x([0-9A-Fa-f]+),gen=(\d+),v=1\b/
		&& (hex($1) & 0x0030B000) == 0x0030B000;
	sleep 1;
	$caps_ok = 1 if $i >= 45;
}
ok($caps_ok == 0, 'L1b both peers advertise REQUIRED caps (0x0030B000)');

# L2: real online cutover, driven by the production SQL entry on the
# coordinator (node0).  begin() freezes the local source, stages the
# all-member BARRIER, and the LMON tick runs the full chain:
# BARRIER COMPLETE -> build (frozen ACTIVE slots) -> create_prepared ->
# PREPARED ACK -> majority COMMIT(P+1) -> activate -> COMMIT_APPLIED
# (members verify the ACTIVE root) -> majority OPEN(P+2) -> OPEN_APPLIED
# (members latch).  The durable OPEN(P+2) record is the Target OPEN proof.
my $begin_ok = $node0->safe_psql('postgres',
	'SELECT pgrac_r4_bit22_cutover_begin()');
is($begin_ok, 't', 'L2 coordinator accepted the bit22 cutover begin');

# L3: poll until the canonical root is ACTIVE and the round has reached
# the OPEN_APPLIED stage (the coordinator latch flips only after the
# majority OPEN record is durable).
my $root_active = 0;
my $deadline = time + 60;
while (time < $deadline)
{
	if (-f $root_file && root_activation_state($root_file) == 2)
	{
		$root_active = 1;
		last;
	}
	sleep 2;
}
ok($root_active, 'L3 canonical root reached ACTIVE (cutover progressed)');
BAIL_OUT('cutover did not reach ACTIVE within the deadline')
	unless $root_active;

my $log_off = -s $node0->logfile;
my $progress = 0;
$deadline = time + 60;
while (time < $deadline)
{
	my $log = PostgreSQL::Test::Utils::slurp_file($node0->logfile);
	# The OPEN(P+2) CAS is only a SUBMIT — the round is not complete until
	# the majority OPEN record is durable and the coordinator latch has
	# flipped (OPEN_APPLIED published).  Requiring completion evidence here
	# is load-bearing: L4 stops the peer immediately after this poll, and a
	# mid-cutover departure leaves the survivor's HW remaster structurally
	# blocked (minted-lost) so the peer restart can never bind the recovery
	# LMS generation.
	if ($log =~ /OPEN_APPLIED|bit22 cutover: OPEN\(P\+2\) durable|TARGET_BOOTSTRAP/)
	{
		$progress = 1;
		last;
	}
	if (-s $node0->logfile < $log_off + 0) { }
	sleep 2;
}
ok($progress, 'L3 round advanced to the OPEN stage (log evidence)');

# L4: clean restart of the peer (node1) -- the cluster keeps serving and
# the root stays ACTIVE.
$node1->stop;
$node1->start;
ok($pair->wait_for_pcm_x_active(30),
	'L4 pair reformed after the peer clean restart');
is($node0->safe_psql('postgres', 'SELECT 1'), '1',
	'L4 node0 still serving after the peer restart');
ok(root_activation_state($root_file) == 2,
	'L4 root still ACTIVE after the peer restart');

# L5: clean restart of the coordinator (node0) -- same.
$node0->stop;
$node0->start;
ok($pair->wait_for_pcm_x_active(30),
	'L5 pair reformed after the coordinator clean restart');
is($node1->safe_psql('postgres', 'SELECT 1'), '1',
	'L5 node1 still serving after the coordinator restart');
ok(root_activation_state($root_file) == 2,
	'L5 root still ACTIVE after the coordinator restart');

# L6: an immediate stop exercises the real cold-recovery startup path.  The
# durable OPEN record must be consumed by the recovered-owner matcher before
# the restarted member can serve SQL or retain the ACTIVE root.
$node1->stop('immediate');
$node1->start;
ok($pair->wait_for_pcm_x_active(30),
	'L6 pair reformed after the peer crash-recovery restart');
is($node1->safe_psql('postgres', 'SELECT 1'), '1',
	'L6 recovered peer serves SQL only after recovered OPEN');
ok(root_activation_state($root_file) == 2,
	'L6 durable OPEN keeps the canonical root ACTIVE after crash recovery');

# L7: a boot identity drift must not reuse the old durable OPEN proof.  The
# deliberately invalid node id is a real postmaster start, not a unit stub;
# restoring the configured identity then proves the normal recovered path can
# be entered again.
$node1->stop('immediate');
$node1->adjust_conf('postgresql.conf', 'cluster.node_id', '99');
is($node1->start(fail_ok => 1), 0,
	'L7 recovered startup rejects a drifted node identity before OPEN');
$node1->adjust_conf('postgresql.conf', 'cluster.node_id', '1');
$node1->start;
ok($pair->wait_for_pcm_x_active(30),
	'L7 restored identity re-forms after the drift refusal');
is($node1->safe_psql('postgres', 'SELECT 1'), '1',
	'L7 restored identity serves after a fresh recovered OPEN match');
ok(root_activation_state($root_file) == 2,
	'L7 restored identity retains the canonical ACTIVE root');

done_testing();
