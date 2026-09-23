# PRE1: operator-assisted four-VM installation

Author: SqlRush <sqlrush@gmail.com>

This guide orders the existing guarded tools for **four independent Linux VMs,
one shared GFS2 DATA filesystem and three separate raw voting LUNs**. It is not
the single-host/container Quick Start and is not a one-command storage installer.
The current bootstrap adapter supports `pre1-gfs2-arm64-lab-v1` only. RHEL 9
x86_64, other filesystems, cloud disks and production HA are not certified by an
ARM64 laboratory result. Consult the release notes for an actually qualified
commit; development-branch or tool-test success is not a release qualification.

## 1. Prepare and identify the environment

Use one trusted controller. Prepare four independent KVM/libvirt guests with
Python 3, OpenSSH, matching non-root database UID/GID, and non-colliding SQL/control/
data address-and-port endpoints. Record each VM UUID, machine ID, current boot ID and verified
SSH host key. Keep administrative keys and evidence private.

The laboratory uses Ubuntu 24.04 ARM64, four 16 GiB guests and a separate Linux
management/storage host. These are recorded laboratory resources, not a tested
minimum or a claim about four physical failure domains. Keep the host powered
and prevent host sleep throughout a live cluster run.

Prepare, through the storage administrator:

- One dedicated shared DATA LUN, seen under the same WWID on every guest;
  shared LVM, DLM/lvmlockd and GFS2 with `lock_dlm`, a common locktable and enough
  journals for four simultaneous mounts. Do not independently format four copies.
- Three different whole SCSI NAA voting LUNs, separate from DATA, with 512-byte
  logical sectors. The laboratory uses 16 MiB each; required format extent is
  525,824 bytes. Voting LUNs have **no filesystem** and are not mounted.
- Healthy Corosync/Pacemaker storage resources and verified exact-VM OFF fencing.
  Both `stonith-action=off` and the fence resource's `pcmk_reboot_action=off`
  are required. Do not enable automatic database restart or treat storage fencing
  as a database recovery certificate.
- Local, dedicated PGDATA/install/log/backup paths for each instance, and the
  same canonical GFS2 shared-data path on all guests. Private PGDATA still contains
  native catalog/control/WAL. Do not symlink all four `pg_control` or `pg_wal`
  paths together. Shared native catalog/control/WAL and crash takeover are not
  enabled by this procedure.

Check the intended boot kernel has both GFS2 and DLM modules. Ensure iSCSI
sessions are owned by the normal service so storage can unmount and logout
cleanly. Pacemaker owns the DLM/LV/filesystem sequence; competing service
autostarts must not manage the same resources. Follow the exact checks in
[storage preparation](pre1-storage.md) and [voting/fencing](pre1-voting-fencing.md).
No `mkfs`, `dd`, generic disk-group membership or wildcard device permission is
an acceptable substitute for an authorized device inventory.

## 2. Get and build one immutable candidate

On the Linux build host, set `PGRAC_SOURCE_SHA` to the **full commit from the
chosen release**, not a moving branch. Use the same CPU architecture and compatible
runtime libraries as all four guests. Install the compiler, make, Git, pkg-config,
Bison, Flex, Perl TAP dependencies, and development packages for readline, zlib,
OpenSSL, ICU, LZ4 and Zstandard using the distribution package manager.

```sh
test -n "$PGRAC_SOURCE_SHA"
git clone https://github.com/sqlrush/pgrac.git pgrac-source
git -C pgrac-source checkout --detach "$PGRAC_SOURCE_SHA"
test "$(git -C pgrac-source rev-parse HEAD)" = "$PGRAC_SOURCE_SHA"
mkdir pgrac-build
cd pgrac-build
../pgrac-source/configure --prefix=/opt/pgrac \
  --enable-cluster --enable-cassert --enable-tap-tests \
  --with-openssl --with-icu --with-lz4 --with-zstd
make -j4
make install DESTDIR="$PWD/stage"
sha256sum stage/opt/pgrac/bin/postgres
stage/opt/pgrac/bin/pg_config --configure
```

Distribute this same staged installation and its required shared libraries to
`/opt/pgrac` on each stopped guest. Copy the complete `scripts/deploy/pre1`
directory to `/opt/pgrac-pre1-tools`. Record source/build/configuration and actual
installed binary hashes; verify all four `postgres` hashes match. Do not replace
binaries underneath a running cluster or perform a mixed-version rolling upgrade.

## 3. Freeze inputs and verify storage before database initialization

Create the operator-reviewed profile from
[profile.schema.json](../../scripts/deploy/pre1/profile.schema.json), using actual
observations, not example identities. Complete the required storage/fence scratch
tests **before** database initialization. Each victim needs actual isolation and
survivor lock/progress evidence; a parsed fence configuration is insufficient.

From the source checkout on the controller:

```sh
python3 scripts/deploy/pre1/preflight.py check-profile \
  --profile /secure/pre1/profile.json
python3 scripts/deploy/pre1/preflight.py inventory \
  --profile /secure/pre1/profile.json \
  --out /secure/pre1/evidence/identity-001.json
python3 scripts/deploy/pre1/voting.py plan \
  --profile /secure/pre1/profile.json \
  --out /secure/pre1/evidence/voting-plan-001.json
```

These commands do not format disks or grant database admission. Some intentionally
report `QUALIFICATION_PENDING` until the separate physical evidence is supplied.
The storage administrator must execute the controlled **fresh-media-only** voting
preparation and collect independent direct-I/O readback on all four guests, as
described in [voting preparation](pre1-voting-fencing.md). Never reformat voting
media before a normal restart. Use separate media/paths for destructive negative
tests; they must never share a live MAIN database identity.

## 4. Create schema once, then clone the same database identity

Prepare new, empty database/shared directories and the exact seed request
described in [seed and bootstrap commands](pre1-remote-status.md). Put the desired
fixed schema in the reviewed seed SQL. Do not run four independent `initdb`s.

On node 0, as the designated administrator:

```sh
sudo -n python3 /opt/pgrac-pre1-tools/seed.py create-seed \
  --request /secure/pre1/seed-request.json \
  --out /secure/pre1/evidence/seed-create-001.json
```

The guarded tool creates the seed, takes and verifies its native plain backup,
and normally stops the seed. Transfer that unchanged backup and its bound request/
result to nodes 1–3. On each joiner, use its own exact empty-target request:

```sh
sudo -n python3 /opt/pgrac-pre1-tools/seed_clone.py \
  --request /secure/pre1/clone-node1.json \
  --out /secure/pre1/evidence/clone-node1-001.json
```

Repeat for nodes 2 and 3 with the corresponding node-specific filenames. A partial
copy or failed seed is preserved, not overwritten or treated as a clean database.

## 5. Configure all four, then start once and verify OPEN

Use the exact closed configuration and request shapes in
[bootstrap commands](pre1-remote-status.md#configure-an-unused-laboratory-seed-and-start-it-once).
The current HBA is an isolated-lab policy: peer locally and superuser trust from
the designated controller only. Do not expose this deployment on an untrusted
network. Do not invent GUC overrides to bypass a rejected input.

First configure each guest with its own request and new output:

```sh
sudo -n python3 /opt/pgrac-pre1-tools/bootstrap_runtime.py configure-initial \
  --request /secure/pre1/initial-node0.json \
  --out /secure/pre1/evidence/configured-node0.json
```

Complete **all four configurations before the first start**. Then start in the
frozen node order, substituting each node's own request, output and the actual
SHA-256 of its retained configuration artifact:

```sh
sudo -n python3 /opt/pgrac-pre1-tools/bootstrap_runtime.py start-initial \
  --request /secure/pre1/evidence/configured-node0.json \
  --sha256 CONFIGURED_RESULT_SHA256 \
  --out /secure/pre1/evidence/started-node0.json
```

`PROCESS_STARTED_NOT_ADMITTED` is not OPEN. The deployment operator must complete
the candidate's formation/semantic activation and verify four connected members,
valid quorum, `resource_x_gate_phase=open` and `resource_x_writer_path=target`
on every node before any application workload. A refusal or unknown SQL outcome
is not permission to skip this step. Preserve startup attempt markers.

## 6. Populate once and verify all four endpoints

After OPEN, populate only the already-seeded shared tables. Runtime cross-node
DDL is not qualified here. Connect using the controller's reviewed SQL endpoint:

```sh
/opt/pgrac/bin/psql -X -v ON_ERROR_STOP=1 \
  -h NODE0_SQL_IP -p 5432 -U pgrac -d postgres -f reviewed-populate.sql
```

Use application-specific reviewed SQL and an independently checked expected CSV.
Run [four-node complete data/health verification](pre1-verification.md) before
and after the workload. This checks every row, not a COUNT or sample. A user
verification PASS is not the formal release PRE verdict. For ordinary row-lock
waiting, see [wait/cancel semantics](pre1-row-lock-wait.md).

## 7. Stop together and reuse clean data

Stop writers. The controller dispatches `stop-exact` concurrently to all four
exact started owners, then waits for all results. Never stop one and wait for it
before signaling the others. See [shutdown commands](pre1-remote-status.md).

Require the complete clean-stop conjunction: all clean native controls, exact
process absence, this shutdown's protocol closure, cleared matching voting ALIVE
slots and zero required debt. `pg_controldata` alone is insufficient. Keep native
artifact bytes unchanged when copying them; their SHA binds the actual bytes.

Only a successful guarded `clean_restart.py prepare` followed by its native
`start-guest` path may reuse that clean dataset. Do not repeat seed, populate,
formation initialization or voting formatting. Verify complete data before new
business, and again afterwards. [Complete cold snapshots](pre1-cold-snapshots.md)
preserve all four PGDATAs, shared data and all voting images together.

For an unclean exit, missing closure, changed identity, failed snapshot or
incomplete verification: preserve data/logs and stop. This procedure does not
authorize crash recovery, guessed ALIVE clearing, forced process cleanup or
restoring only one member's files.
