# PRE1 shared-filesystem probe

Author: SqlRush <sqlrush@gmail.com>

Before each kernel change or reboot, verify that the intended boot kernel has
both GFS2 and DLM modules installed. Checking only the currently loaded modules is
insufficient. On the Ubuntu ARM64 laboratory profile, GFS2 needs the matching
`linux-modules-extra-<kernel-release>` package; an image-only kernel update may
not install it. Record package/kernel versions, normally unmount through the
cluster resource manager, reboot one guest at a time, and verify the original
filesystem UUID and scratch payload after remounting. Never format a volume to
resolve a missing-module mount failure.

This is a deployment test tool, not a database process. It exercises real file
operations on a dedicated scratch directory. It neither formats devices nor
mounts filesystems, and does not certify a deployment by itself.

The automated storage provisioning adapter and complete four-VM acceptance suite
are not yet delivered. A successful local test, two-VM test, or individual probe
must not be presented as PRE1 certification. The existing stable MVP is unchanged.

## Prerequisites

For actual GFS2 testing, each participant must be an independent Linux VM. Before
creating the scratch directory, an operator must verify:

1. Every VM sees the same authorized DATA LUN WWID, PV/VG/LV identity and GFS2 UUID.
2. GFS2 uses `lock_dlm` and the correct cluster locktable, with enough journals
   for all participants. Do not use `lock_nolock` or `localflocks`.
3. Corosync, Pacemaker, DLM and lvmlockd are healthy; shared LV activation precedes
   the filesystem resource. Fencing is enabled and exact VM targets have been
   verified. The storage cluster does not manage PostgreSQL as a restart resource.
4. `findmnt -T /srv/pgrac-shared -o TARGET,SOURCE,FSTYPE,OPTIONS,UUID` identifies
   the intended GFS2 mount, **not the local root filesystem beneath a missing mount**.
5. All probe users have the same numeric UID/GID. A dedicated, owned scratch parent
   exists on that mount. Do not use a relation, PGDATA, voting device or undo path.
6. No database load is running on the tested filesystem. Keep one trusted test
   controller and preserve its command/result log.

On Ubuntu, also verify that `open-iscsi.service` actually owns the configured
automatic sessions. A manual `iscsiadm --login` after an initially skipped service
does not establish its shutdown/logout lifecycle. Set the exact target record's
startup policy, start `open-iscsi.service`, and verify it is active before the
storage test. During teardown, stop the cluster filesystem/LV resources first;
confirm session logout and LVM-monitor completion before declaring shutdown clean.

The probe itself also supports local filesystems for developer tests. Consequently
it cannot infer that the supplied directory is an approved shared mount. The
deployment controller/operator must check the mount and identity independently.

## Build and developer tests

From the source checkout:

```sh
cc -std=c11 -Wall -Wextra -Werror -O2 \
  scripts/deploy/pre1/storage_probe.c -o /tmp/pgrac-storage-probe
python3 -B -m unittest discover -s scripts/deploy/pre1/tests -v
```

Build for the guest architecture, copy the same executable to all participants
and record its SHA-256 on each. The tests compile a temporary executable and use
temporary local files; they do not qualify GFS2 or a cloud storage product.

## Dedicated scratch directory

Choose a new 32-character lowercase hexadecimal token for every run. The final
directory name must be exactly `pre1-probe-<token>`. All path components must be
real directories, not symlinks. Only `init` creates this directory; it refuses to
reuse an existing one.

For example, on node 0, after verifying the mount and creating its dedicated
scratch parent with the correct owner:

```sh
PROBE=/tmp/pgrac-storage-probe
TOKEN=0123456789abcdef0123456789abcdef
SCRATCH=/srv/pgrac-shared/probe-scratch/pre1-probe-$TOKEN

"$PROBE" --case init --root "$SCRATCH" --token "$TOKEN" --node 0
"$PROBE" --case write --root "$SCRATCH" --token "$TOKEN" --node 0 --sequence 1
```

The example token is illustrative; generate a fresh token for a real run. The
directory is mode 0700, its files mode 0600. `.pre1-manifest` binds the token and
lists the fixed scratch filenames `data`, `next` and `lock`. Existing symlinks,
hard links, wrong owners, permissive modes and missing markers are rejected.
There is no recursive cleanup command. Retain the directory and evidence until
the run is classified; any later cleanup must be limited to that exact manifest.

On node 1, using the same token, path and binary:

```sh
"$PROBE" --case read --root "$SCRATCH" --token "$TOKEN" \
  --node 1 --writer 0 --sequence 1
```

`--node` is the observing VM (0–3). `--writer` is the expected payload writer and
defaults to `--node`. `--sequence` is 0–1,000,000,000. The normal payload is 8 KiB;
its header explicitly carries sequence, writer and token, followed by deterministic
bytes. Verification compares every byte; CRC is also reported, not used alone.

## Cases and external barriers

| Case | Operation |
|---|---|
| `init` | Exclusively create and sync the marked scratch directory. |
| `write` / `read` | Write+fsync or verify a complete version. |
| `resize` | `ftruncate`+fsync to `--length` (0–32768 bytes). |
| `flock-hold` / `flock-try` | Exclusive nonblocking whole-file locks. |
| `fcntl-hold` / `fcntl-try` | Exclusive nonblocking record locks; `--offset` and `--length` select the range. |
| `flock-exit` | Hold until the barrier, then exit normally without explicit unlock/close. |
| `cache-reader` | Prime an fd; after the barrier verify the newer version through both that fd and a fresh open. |
| `rename` / `rename-reader` | Replace `data` with a synced new object; verify the old fd retains old bytes and the new fd names the new inode/version. |
| `unlink` / `unlink-reader` | Sync removal; verify an existing fd retains its bytes while the name returns ENOENT. |
| `fence-writer` | Exclusively create `data`, hold `lock`, and write+fsync an increasing version every 100 ms. Never reports a successful isolation result. |
| `observe` | Read a recorded version and writer, then verify the entire 8 KiB token-bound payload. Use after an external barrier. |
| `capacity-fill` | Fill a separately authorized small scratch filesystem until actual ENOSPC; never an ordinary PASS. |

Nonzero `--offset` is accepted only for `fcntl` cases. `read --length` verifies
the exact file size and content: bytes beyond the initial 8 KiB are zero after
extension; truncation preserves the corresponding prefix. Recreate the full
payload with `write` before beginning another cache or rename test.

Reader and lock-holder cases emit a JSON event with `status=READY` and then wait
on stdin. The controller must observe READY, complete the other VM's operation,
and only then send the matching barrier:

```text
GO <token>
```

For `cache-reader` and `rename-reader`, send:

```text
GO <token> <new-sequence> <new-writer>
```

The new sequence must increase. These witnesses require at least the complete
40-byte identity header. Sending the old version without doing work cannot yield
a valid witness. EOF, a missing newline or a wrong barrier is not success.

For lock tests, hold on VM A, require a real conflict on VM B, release A, then
require B to succeed. Test `flock` and `fcntl` independently; they are not assumed
to exclude each other. For record locks, also require a non-overlapping range to
succeed while the original holder remains active. Repeat each direction.

## Storage-fencing witness

The off-only profile requires both the cluster property `stonith-action=off`
and the fence device parameter `pcmk_reboot_action=off`. DLM may request a
reboot independently of the cluster's default action. Verify the installed
Pacemaker/agent actually keeps the victim OFF, including after such a request;
configuration parsing alone is not proof. Do not disable DLM fencing to avoid
the race, and do not allow an automatic database restart.

Use a new token-owned scratch directory with no existing `data` file, before
any database is initialized. `fence-writer` retains the whole-file lock while
writing and syncing. Its first completed write emits `fence-progress/READY`;
later completed writes emit `fence-progress/SYNCED`, with the version in
`result`. Unlike a barrier case, it does not wait for stdin. The controller
must keep draining stdout and observe increasing versions before fencing.

`--iterations` applies only to this case (1–1200, default 1200). If the loop
finishes naturally, it returns RC 3 / `witness-budget-exhausted`, not PASS.
Killing this process or losing its SSH connection is not proof of VM isolation.
Do not use it on a database file or the voting devices.

The external controller must separately record the exact VM UUID, a successful
Pacemaker fence-off operation, independent libvirt OFF status, cessation of the
old writer and a survivor's successful lock acquisition. After that barrier,
`observe` verifies the complete last payload; repeated observations must be
stable before the survivor writes a newer version. A lost final stdout record
does not imply the corresponding write did not complete: use observed bytes,
not only the last acknowledged version. A torn payload is failure, not a reason
to skip validation. Repeat for every victim. Never automatically reboot or
restart an old database as part of this witness.

## Isolated capacity-error witness

This destructive-to-free-space case requires a **separate expendable filesystem**,
not the DATA filesystem, PGDATA, voting media or a directory on the host root.
Its complete filesystem size must be at most 1 GiB. The operator must verify the
mount's UUID/LV identity and separation before creating a fresh marked directory.
Supply `--capacity-bytes` equal to `statvfs.f_blocks * statvfs.f_frsize` and
`--capacity-device` equal to that directory's numeric `stat.st_dev`, as observed
on the executing guest. These are not a LUN's raw size or its `major:minor` string.

The helper checks both values before exclusively creating `data`, writes only
that file, flushes regularly, and writes no more than the supplied byte budget.
Actual ENOSPC records its syscall, errno and offset, then returns RC 4 with
`EXPECTED_INJECTION`. Other errors fail. Reaching the budget without ENOSPC is
INCOMPLETE, not a successful capacity witness. Reentry never overwrites the file;
there is no automatic deletion or truncation. Preserve the result and verify an
existing DATA payload separately before any explicitly scoped scratch cleanup.

Developer testing includes a separate opt-in Linux-root test using a new 2 MiB
tmpfs, with a normal unmount afterwards. Ordinary test discovery skips that
privileged case; it must not be reported as an executed storage test.

## Evidence and limits

Every stdout line is JSON with case, syscall, result, errno, actual offset, length,
CRC, token, observer node and monotonic timestamp. Do not compare monotonic times
from different kernels as a global clock; order operations with the barriers.
Use a bounded external controller and preserve stdout, stderr and process status.

| Exit code | Meaning |
|---|---|
| 0 | This one operation completed successfully. |
| 1 | Syscall, identity, size or content failure. |
| 2 | Invalid arguments. |
| 3 | Incomplete barrier or evidence output. |
| 4 | Real lock conflict, or `EXPECTED_INJECTION` for capacity ENOSPC; acceptable only in the corresponding explicit negative leg. |

Full deployment acceptance additionally needs all four-VM directed combinations,
normal shutdown/restart readback, independently authorized capacity-error tests,
fencing and voting admission, database correctness, and preserved evidence. A
file `fsync` and normal restart do not certify power-loss durability, storage HA
or database crash recovery. Never fill the business or voting volume to test
ENOSPC; that test requires a separate authorized scratch LV.
