# PRE1 voting-device preparation tools

Author: SqlRush <sqlrush@gmail.com>

Status: development tooling. **The public Python interface does not yet provide
authorized device apply or four-node deployment qualification.** The C helper now
contains a bounded administrative write primitive for controlled qualification;
it is not an end-user shortcut around the controller's safety checks. Do not start a database
merely because an image or read-only observation succeeds.

## Prerequisites

- Three distinct, explicitly authorized shared voting LUNs, separate from DATA.
- Four independent Linux guests must eventually see the same WWID for each index.
- Logical sectors of 512 bytes; each LUN at least 525,824 bytes and 512-byte aligned.
- The initial inspector supports whole SCSI NAA devices (16 or 32 hexadecimal NAA
  digits), not NVMe, multipath maps, partitions, loop files or arbitrary aliases.
  Unsupported device identities require a separately verified adapter.
- Exact WWIDs, capacities and purposes in the deployment profile. A `fresh: true`
  declaration is not proof that media is blank or that all databases are stopped.

Three LUNs on one target remain one storage failure domain. These tools do not
certify resistance to target, host or power failure.

## 1. Produce a read-only plan

From the source checkout, with a previously validated deployment profile:

```sh
python3 scripts/deploy/pre1/voting.py plan \
  --profile /secure/pre1/profile.json \
  --out /secure/pre1/voting-plan.json
```

The output is `PLANNED_NOT_EXECUTED`, with `device_writes_enabled: false`.
It includes each index, exact WWID/capacity and canonical image SHA-256. A missing
dependency remains pending; no command is generated to overwrite an existing LUN.
Output paths must be new. Existing files and symlinks are not replaced.

## 2. Generate and validate local initial images

Use an existing, private working directory on the controller:

```sh
python3 scripts/deploy/pre1/voting.py image --index 0 --out /secure/pre1/vote0.img
python3 scripts/deploy/pre1/voting.py image --index 1 --out /secure/pre1/vote1.img
python3 scripts/deploy/pre1/voting.py image --index 2 --out /secure/pre1/vote2.img
python3 scripts/deploy/pre1/voting.py check-image --index 0 --image /secure/pre1/vote0.img
python3 scripts/deploy/pre1/voting.py check-image --index 1 --image /secure/pre1/vote1.img
python3 scripts/deploy/pre1/voting.py check-image --index 2 --image /secure/pre1/vote2.img
```

Each image is exactly 525,824 bytes: 128 member slots of 512 bytes, followed by
the frozen all-zero initial marker region. Member identity and Castagnoli CRC32C
are validated independently. Live generations, flags or marker records are not
accepted as fresh. Files are mode 0600 and published without replacement.

The Python and standalone C image generators are tested against the existing
`PostgreSQL::Test::ClusterVotingDisk` formatter, byte for byte for all three indexes.
This does **not** authorize copying those images to devices with `dd` or truncation.
Never reinitialize voting media after formation, including before a normal restart.

## 3. Inspect an actual Linux device without writing

Build on the guest or a matching Linux build host:

```sh
cc -std=c11 -O2 -Wall -Wextra -Werror \
  scripts/deploy/pre1/voting_io.c -o /secure/pre1/voting_io
```

First resolve the approved `/dev/disk/by-id/scsi-<WWID>` and inspect its actual
identity with `readlink -e`, `lsblk -b` and `udevadm info`. Do not assume `/dev/sdX`
letters remain stable across guests or boots. Supply the resulting exact path,
SCSI WWID, index, capacity and current major/minor numbers:

```sh
# Syntax; replace every placeholder with verified values for one approved LUN.
sudo /secure/pre1/voting_io inspect \
  /dev/RESOLVED_DEVICE SCSI_WWID INDEX CAPACITY_BYTES MAJOR MINOR
```

The helper rejects symlinks intentionally: resolution belongs to inventory and
the opened fd must still match the expected device number and WWID. It checks
`S_ISBLK`, actual `F_GETFL`/`O_DIRECT`, `BLKGETSIZE64`, `BLKSSZGET`, whole-device
identity and absence of holders or partition children. The `inspect` operation
reads the complete 525,824-byte range through an aligned buffer and never writes.
There is no truncate, unmount or fencing operation in this helper.

| Output | Meaning |
|---|---|
| `BLANK` | The entire frozen range read as zero during this observation. |
| `FRESH_INITIAL_IMAGE` | Every byte matches the exact expected index's initial image. |
| `NONFRESH_OR_INVALID` | Not blank and not that initial image; preserve it. It may contain valid runtime state. |
| `strict_authority: true` | This actual read-only block fd satisfied the inspected direct-I/O requirements. |
| `deployment_qualified: false` | No formatting, quorum, fencing or database admission has been certified. |

An inspect RC of zero means **observation completed**, not "safe to erase" or
"database ready". Concurrent runtime writes can change the observation; it is not
an atomic cluster snapshot and must not replace the database's voting judgement.
The mount/swap/whole-cluster stopped checks required for formatting are not implied
by this read-only helper. Wrong identities and geometry fail rather than falling
back to ordinary files or buffered I/O.

### Administrative write primitive: not a deployment command

The internal `format-fresh` operation accepts the same exact device identity
arguments. Its controller must first bind a new-media authorization, plan and
fresh four-node inventory; confirm no database is running, no formation has begun,
no system/data/swap/mounted device is selected; and preserve a durable attempt
record. Do not invoke it directly to repair or reinitialize an existing deployment.
Those whole-cluster checks cannot be proved by the local C helper alone.

On Linux it exclusively opens the actual block device with direct I/O, checks its
identity and geometry, and rejects any nonzero byte in the full frozen range.
It makes one bounded write of the independently checked canonical image, calls
`fdatasync`, closes, reopens with direct I/O, rechecks identity and compares every
byte. It neither truncates the LUN nor writes outside the declared range.

`FORMATTED_LOCAL` means only this local write/flush/reopen/readback completed.
All four guests must still independently verify the correct index and WWID.
Any failure after a write attempt is `PARTIAL_FORMAT`, even when the write reports
an error. Preserve the device and attempt record; never automatically retry.
Any nonblank media is rejected, including an already correct fresh image.
Normal restarts use read-only verification, never formatting.

## 4. Fencing boundary

Storage-level VM fencing must use an installed agent, exact UUID mapping, pinned
management host keys and an explicit OFF action. Unknown, unreachable or ambiguous
domains are not OFF. Qualification additionally needs an independent hypervisor
state check, proof that the old scratch writer stopped, and survivor lock progress.

A GFS2 fence result is not a PGRAC external-fence certificate. Do not set database
capabilities or erase voting records to bypass an unavailable producer. Database
crash recovery and automatic rejoin are not enabled by these preparation tools.
Credential material stays outside source control; this document does not provide
a destructive fence command or authorize a target.

The `fencing.py` evidence library checks a single `fence_virsh` resource, the
four exact node-to-UUID mappings, explicit OFF policy, static target checking and
disabled missing-as-off handling. It rejects action overrides, fixed port/plug
targets and host-argument overrides that bypass the mapping. This parser does
not execute fencing and its success does not prove that a VM is off.

Before a scratch isolation test, confirm all four database directories are empty
and there are no database processes. Check that package maintenance is inactive
and all storage resources have completed startup. A background OS update can
restart Pacemaker and unmount GFS2 even though no database command was issued.
Keep package versions fixed during the test campaign; schedule security updates
in a separate maintenance window and recapture the inventory afterwards. Do not
kill an active package manager or disable fencing to make the test proceed.

The writer must show synchronized progress and a competing guest must observe a
real lock conflict before OFF. Afterwards, retain independent hypervisor OFF
evidence, successful survivor lock acquisition and repeated full-payload reads.
An expired writer budget, missing output or failed status query is incomplete
evidence, not a passing isolation result. Any later VM startup is a separate
explicit operation; it cannot automatically restart a crashed database.

## Developer verification

```sh
python3 -B -m unittest discover -s scripts/deploy/pre1/tests -v
```

Local image and syscall tests do not qualify a storage deployment. Real inspection
must bind guest identity, exact binary/source hashes, device facts and all four
guests' results into the deployment evidence before any broader verdict.
