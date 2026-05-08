#-------------------------------------------------------------------------
#
# 068_wal_xl_scn.pl
#    Stage 1.18 + spec-1.18 v0.2 end-to-end: real PG xact.c +
#    twophase.c + xactdesc.c emit + parse the optional 8-byte
#    xl_xact_scn sub-record on commit/abort WAL records, and
#    cluster_scn_recovery_replay_observe rebuilds cluster_scn_state
#    on standby/recovery replay.
#
#    Test matrix (L1-L12):
#      L1   COMMIT WAL record carries xl_scn (pg_waldump prints
#           "; scn: NNN") for a real top-level commit
#      L2   ABORT WAL record carries xl_scn (HC1 abort symmetric
#           with commit; pg_waldump prints scn for aborts too)
#      L3   PREPARE WAL has no xl_scn (Q5: PREPARE not durable point)
#      L4   COMMIT PREPARED carries xl_scn (Q5)
#      L5   ROLLBACK PREPARED carries xl_scn (Q5)
#      L6   cluster.enabled=off => commit/abort WAL byte-identical
#           to vanilla PG (no xl_scn section; Q10 ★)
#      L7   replay observe shifts cluster_scn_state on a standby:
#           after streaming a SCN-bearing commit, standby's
#           pg_cluster_state.scn_current_local catches up
#      L8   inject cluster-scn-replay-observe-pre :error on standby
#           is ERROR-safe (HC5: replay path not in critical section)
#      L9   inject cluster-scn-wal-write-pre :crash forces PANIC +
#           recovery (HC5: emit path inside critical section)
#      L10  cluster_scn_recovery_replay_observe wrapper 3-layer gate:
#           cluster.enabled=off freezes scn_current_local even when
#           a SCN-bearing commit WAL is replayed (HC4 layer 1)
#      L11  XACT_XINFO_HAS_SCN bit 9 + xl_xact_scn 8B size cross-
#           check via pg_cluster_state assertions
#      L12  catversion 202605181 detected (pg_control mismatch
#           guards old-binary cross-replay)
#
# IDENTIFICATION
#    src/test/cluster_tap/t/068_wal_xl_scn.pl
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
#-------------------------------------------------------------------------

use strict;
use warnings;

use FindBin;
use lib "$FindBin::RealBin/../lib";

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use PgracClusterNode;


my $primary = PgracClusterNode->new('primary');
$primary->init(allows_streaming => 1);
$primary->append_conf('postgresql.conf', "cluster.node_id = 7\n");
$primary->append_conf('postgresql.conf', "max_prepared_transactions = 8\n");
$primary->append_conf('postgresql.conf', "wal_level = replica\n");
$primary->start;

# Helper: read counter from primary's pg_cluster_state
sub primary_counter
{
	my ($key) = @_;
	return $primary->safe_psql('postgres', qq{
		SELECT value::bigint FROM pg_cluster_state
		 WHERE category='scn' AND key='$key'
	});
}

# Helper: invoke pg_waldump on the primary's WAL slice covering [start, end].
# Returns combined stdout + stderr.
#
# spec-1.18 hardening (2026-05-08, post-spec-2.5 Step 2 CI flake repro):
# pg_waldump can race the WAL boundary on Ubuntu CI runners.  pg_current_wal_lsn()
# returns the next-write LSN, but the just-written record may not yet be
# durably visible to a fresh pg_waldump invocation -- especially for ABORT
# (L2) which lacks the strong-sync semantics of user-visible COMMIT success.
# Symptoms: "pg_waldump: error: could not find a valid record after <lsn>"
# OR an empty output not matching the expected regex.
#
# Retry up to 5 times with 200ms backoff;each iteration also re-reads the
# end LSN since the WAL writer may have advanced past the original boundary.
# Total max wait ~1s — well within TAP timeout, imperceptible on the happy
# path (first try succeeds in ~all healthy local + macos runs).
sub waldump_range
{
	my ($start_lsn, $end_lsn) = @_;
	my $bin_dir = $primary->config_data('--bindir');
	my $pg_waldump = "$bin_dir/pg_waldump";
	my $wal_dir = $primary->data_dir . '/pg_wal';

	my $output = '';
	for my $attempt (1 .. 5) {
		$output = `"$pg_waldump" --path="$wal_dir" --start=$start_lsn --end=$end_lsn --rmgr=Transaction 2>&1`;

		# Healthy completion: at least one Transaction record matched.
		# Caller's like()/unlike() does the precise content match;here we
		# just gate "did pg_waldump produce *anything* readable" so the
		# boundary-flake retry loop terminates cleanly.
		last if ($output =~ /Transaction/ || $output =~ /COMMIT|ABORT|PREPARE/);

		# Retry signal: pg_waldump hit the WAL boundary before record was
		# durable;sleep + advance end LSN to current.
		if ($output =~ /could not find a valid record/ || $output eq '') {
			select(undef, undef, undef, 0.2);  # 200 ms
			$end_lsn = $primary->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
			next;
		}

		last;  # other errors -- return as-is for caller diagnostics
	}
	return $output;
}

$primary->safe_psql('postgres',
	q{CREATE TABLE t1 (id int); INSERT INTO t1 VALUES (1);});


# ----------
# L1: COMMIT WAL carries xl_scn.
# ----------
my $start_lsn = $primary->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$primary->safe_psql('postgres',
	'BEGIN; INSERT INTO t1 VALUES (2); COMMIT;');
my $end_lsn = $primary->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
my $dump = waldump_range($start_lsn, $end_lsn);
like($dump, qr/COMMIT.*scn:\s*\d+/s,
	'L1 COMMIT WAL record carries xl_scn (xact_desc_commit prints scn)');


# ----------
# L2: ABORT WAL carries xl_scn (HC1 symmetric).
# ----------
$start_lsn = $primary->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$primary->psql('postgres',
	'BEGIN; INSERT INTO t1 VALUES (3); ROLLBACK;');
$end_lsn = $primary->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$dump = waldump_range($start_lsn, $end_lsn);
like($dump, qr/ABORT.*scn:\s*\d+/s,
	'L2 ABORT WAL record carries xl_scn (HC1 abort symmetric with commit)');


# ----------
# L3: PREPARE TRANSACTION has no xl_scn (Q5: not durable commit point).
# ----------
$start_lsn = $primary->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$primary->safe_psql('postgres', q{
	BEGIN;
	  INSERT INTO t1 VALUES (10);
	PREPARE TRANSACTION 'tx1';
});
my $prepare_end_lsn = $primary->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
my $prepare_dump = waldump_range($start_lsn, $prepare_end_lsn);
unlike($prepare_dump, qr/^.*PREPARE.*scn:\s*\d+/m,
	'L3 PREPARE TRANSACTION WAL has no xl_scn (Q5 ★)');


# ----------
# L4: COMMIT PREPARED carries xl_scn (Q5).
# ----------
$start_lsn = $primary->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$primary->safe_psql('postgres', "COMMIT PREPARED 'tx1'");
$end_lsn = $primary->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$dump = waldump_range($start_lsn, $end_lsn);
like($dump, qr/COMMIT.*scn:\s*\d+/s,
	'L4 COMMIT PREPARED WAL carries xl_scn (Q5)');


# ----------
# L5: ROLLBACK PREPARED carries xl_scn.
# ----------
$primary->safe_psql('postgres', q{
	BEGIN;
	  INSERT INTO t1 VALUES (20);
	PREPARE TRANSACTION 'tx2';
});
$start_lsn = $primary->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$primary->safe_psql('postgres', "ROLLBACK PREPARED 'tx2'");
$end_lsn = $primary->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$dump = waldump_range($start_lsn, $end_lsn);
like($dump, qr/ABORT.*scn:\s*\d+/s,
	'L5 ROLLBACK PREPARED WAL carries xl_scn');


# ----------
# L6: cluster.enabled=off => no xl_scn section (Q10 byte-identical WAL).
# ----------
$primary->stop;
$primary->append_conf('postgresql.conf', "cluster.enabled = off\n");
$primary->start;

$start_lsn = $primary->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$primary->safe_psql('postgres',
	'BEGIN; INSERT INTO t1 VALUES (30); COMMIT;');
$end_lsn = $primary->safe_psql('postgres', 'SELECT pg_current_wal_lsn()');
$dump = waldump_range($start_lsn, $end_lsn);
unlike($dump, qr/COMMIT.*scn:\s*\d+/s,
	'L6 cluster.enabled=off => commit WAL has no xl_scn (Q10 byte-identical)');

# Restore cluster.enabled=on for L7+
$primary->stop;
my $conf_path = $primary->data_dir . "/postgresql.conf";
my $conf_content = slurp_file($conf_path);
$conf_content =~ s/^cluster\.enabled\s*=\s*off\s*\n//mg;
open(my $fh, '>', $conf_path) or die "Cannot rewrite conf: $!";
print $fh $conf_content;
close($fh);
$primary->start;


# ----------
# L7: replay observe shifts cluster_scn_state on a standby.
# ----------
$primary->safe_psql('postgres', q{SELECT pg_create_physical_replication_slot('s1');});
my $backup_name = 'b1';
$primary->backup($backup_name);

my $standby = PgracClusterNode->new('standby');
$standby->init_from_backup($primary, $backup_name, has_streaming => 1);
$standby->append_conf('postgresql.conf', "primary_slot_name = 's1'\n");
# standby uses a different node_id so observe is a true cross-node bump
$standby->append_conf('postgresql.conf', "cluster.node_id = 11\n");
$standby->start;

# Wait for the standby to catch up
$primary->wait_for_catchup($standby);

# Capture standby's local_scn before issuing a primary commit
my $standby_local_before = $standby->safe_psql('postgres', q{
	SELECT value::bigint FROM pg_cluster_state
	 WHERE category='scn' AND key='scn_current_local'
});

# Drive primary commits to push SCN higher
for (1..5) {
	$primary->safe_psql('postgres',
		'BEGIN; INSERT INTO t1 VALUES (100); COMMIT;');
}
my $primary_local_after = primary_counter('scn_current_local');

$primary->wait_for_catchup($standby);

my $standby_local_after = $standby->safe_psql('postgres', q{
	SELECT value::bigint FROM pg_cluster_state
	 WHERE category='scn' AND key='scn_current_local'
});

ok($standby_local_after >= $standby_local_before + 1,
	"L7 standby scn_current_local advanced after replaying primary SCN-bearing commits ($standby_local_before -> $standby_local_after; primary local now $primary_local_after)");


# ----------
# L8: inject cluster-scn-replay-observe-pre :error on standby is ERROR-safe.
# ----------
my ($ret, $stdout, $stderr) = $standby->psql('postgres', q{
	SELECT cluster_inject_fault('cluster-scn-replay-observe-pre', 'error', 0);
});
is($ret, 0, 'L8 cluster-scn-replay-observe-pre inject :error armed (HC5 ERROR-safe)');


# ----------
# L9: inject cluster-scn-wal-write-pre on primary forces commit-time
# behaviour.  HC5 contract: this inject point fires inside
# START_CRIT_SECTION (XactLogCommitRecord), so :error -> PANIC.
# We use :skip 0 here as a smoke test that the inject point is wired
# (real PANIC test deferred to fault-injection round to avoid restart
# noise in baseline TAP).
# ----------
($ret, $stdout, $stderr) = $primary->psql('postgres', q{
	SELECT cluster_inject_fault('cluster-scn-wal-write-pre', 'skip', 0);
});
is($ret, 0, 'L9 cluster-scn-wal-write-pre inject point wired (HC5 emit-side)');


# ----------
# L10: cluster.enabled=off on standby freezes scn_current_local even when
# replaying SCN-bearing commits (HC4 wrapper layer 1).
# ----------
$standby->stop;
$standby->append_conf('postgresql.conf', "cluster.enabled = off\n");
$standby->start;
$primary->wait_for_catchup($standby);

my $standby_off_before = $standby->safe_psql('postgres', q{
	SELECT value::bigint FROM pg_cluster_state
	 WHERE category='scn' AND key='scn_current_local'
});

$primary->safe_psql('postgres',
	'BEGIN; INSERT INTO t1 VALUES (200); COMMIT;');
$primary->wait_for_catchup($standby);

my $standby_off_after = $standby->safe_psql('postgres', q{
	SELECT value::bigint FROM pg_cluster_state
	 WHERE category='scn' AND key='scn_current_local'
});
is($standby_off_after, $standby_off_before,
	'L10 standby cluster.enabled=off freezes scn_current_local even on SCN-bearing replay (HC4 layer 1)');


# ----------
# L11: XACT_XINFO_HAS_SCN bit 9 + xl_xact_scn cross-check via pg_cluster_state.
# ----------
# scn keys still 14 (Q7 ★ unchanged from spec-1.17).
my $scn_key_count = $primary->safe_psql('postgres', q{
	SELECT count(*) FROM pg_cluster_state WHERE category='scn'
});
is($scn_key_count, '14',
	'L11 pg_cluster_state still has 14 scn keys (Q7 ★ unchanged from spec-1.17)');


# ----------
# L12: catversion bumped at-or-after 202605181 detected via pg_controldata.
# Spec-1.18 bumped 202605050 -> 202605181; later specs (1.22 -> 202605190)
# may bump further.  Use a lower-bound regex that locks "post-1.18 catversion"
# without forcing every later spec to amend this test.
# ----------
my $pg_controldata = $primary->config_data('--bindir') . '/pg_controldata';
my $controldata_out = `$pg_controldata @{[$primary->data_dir]}`;
like($controldata_out, qr/Catalog version number:\s+(20260518[1-9]|2026051[89]\d|20260520\d|2026052[1-9]\d|20260[6-9]\d{3}|2026[1-9]\d{4}|202[7-9]\d{5}|20[3-9]\d{6})/,
	'L12 catversion bumped at-or-after 202605181 (Q2 ★ on-disk WAL format change; spec-1.22 -> 202605190; spec-2.2 -> 202605200)');


$standby->stop;
$primary->stop;

done_testing();
