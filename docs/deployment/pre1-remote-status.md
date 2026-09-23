# PRE1: read-only node status

Author: SqlRush <sqlrush@gmail.com>

Run this tool on the single designated deployment controller. It collects
observations; it does **not** start, stop, recover or authorize a database.

## Requirements

- Python 3.9 or later and OpenSSH on the controller.
- The complete `scripts/deploy/pre1` directory from the same source checkout.
- Four independent Linux KVM guests, with Python 3 and `systemd-detect-virt`.
- An explicitly pinned Ed25519 SSH host key for each guest.
- A dedicated controller SSH identity, owned by the controller user and mode 0600.
- Guest administrative access to run the fixed read-only collector with
  `sudo -n /usr/bin/python3`. This administrative access is powerful: use only
  the isolated, trusted deployment-management network and designated operator.
- The installed candidate's actual `postgres` SHA-256 and dedicated database UID/GID.

## Node input

Use the corresponding `nodes[]` record from the deployment inventory, including
the actual VM UUID, machine ID, current boot ID, SSH host key and endpoints,
PGDATA, installation/log directories, UID and GID. Its format is the `node`
definition in `scripts/deploy/pre1/profile.schema.json`.

For example, extract node 0 from an existing validated inventory:

```sh
jq '.nodes[] | select(.node_id == 0)' deployment-profile.json > node0.json
chmod 600 node0.json
```

Before initialization, a node-only record may be prepared from actual inventory.
Do not fill missing seed, backup or runtime hashes with invented values to make
a complete deployment profile pass validation. The node-only command grants no
deployment or storage qualification.

## Collect

```sh
python3 scripts/deploy/pre1/remote.py status \
  --node node0.json \
  --binary-sha256 ACTUAL_POSTGRES_SHA256 \
  --out status-node0-001.json
```

Repeat for nodes 1–3, using separate output names. Run commands sequentially when
they publish into the same output directory: simultaneous publication is refused
by the controller lock. Existing evidence is never overwritten.

The program sends a fixed guest program and structured JSON input. It accepts no
shell action or start/stop option. It reads guest identities and PostgreSQL
process identities; `pg_controldata`, if applicable, runs as the database user.
It does not execute SQL, modify PGDATA or signal processes.

## Interpret the result

| Result | Meaning |
|---|---|
| RC 0 / `NODE_OBSERVED` | Required node observations were collected and bound to the request. |
| RC 2 / `BLOCKED` | An input or identity prerequisite was not met. |
| RC 3 / `ERROR` | Transport, execution, parsing or evidence publication failed. |

The private mode-0600 output preserves SSH rc/stdout/stderr separately from the
observation. A timeout has no fabricated exit code. SSH disconnection does not
prove that the database process exited.

PGDATA states are `ABSENT`, `EMPTY`, `PARTIAL` and `INITIALIZED`. Native control
state, PID file and PostgreSQL PID/starttime/executable observations are separate
facts. A stale PID file, an empty process list or `shut down` alone is not the
complete four-node clean-stop proof. Every result remains
`deployment_qualified=false`; no output from this command permits recovery,
formatting or startup.

Native `pg_controldata` can return zero even when a CRC or WAL-segment warning
says the results are untrustworthy. Such output, or unexpected stderr, is retained
but cannot supply a clean-state fact.

## Reconcile all four nodes

```sh
python3 scripts/deploy/pre1/lifecycle.py reconcile \
  --nodes node0.json node1.json node2.json node3.json \
  --binary-sha256 ACTUAL_POSTGRES_SHA256 \
  --out lifecycle-001.json
```

`status` accepts the same arguments and performs the same read-only collection.
For an existing database, add `--system-identifier EXPECTED_DECIMAL_IDENTIFIER`
from its retained seed record. All four observed database identities must agree.
The artifact retains each node's command output and observation interval; these
are separate observations, not an atomic four-node snapshot.

| State | Meaning |
|---|---|
| `EMPTY_NOT_INITIALIZED` | No initialized dataset or observed database processes. This does not authorize initialization. |
| `PARTIAL_DATASET` | Missing or inconsistent initialization; preserve the directories. |
| `PROCESSES_PRESENT` | At least one process or PID file remains. |
| `IDENTITY_MISMATCH` | Observed database identities disagree with each other or the supplied identity. |
| `UNCLEAN_OR_UNKNOWN` | Native control state does not prove a clean shutdown. No crash recovery is attempted. |
| `DATA_CLEAN_CLOSURE_UNPROVEN` | Clean native controls and absent processes, but protocol and persistent closure are still unproved. |
| `OBSERVATION_INCOMPLETE` | Transport or control-file evidence is missing or untrusted. |

An observation `PASS` means collection completed, not that lifecycle operations
are authorized. `restart_allowed` and `deployment_qualified` are always false in
this read-only implementation. Start, stop and recovery commands are not exposed
by lifecycle.py. The separate guarded seed-only utility is described below.

After a VM reboot, collect and record the new boot identity through the trusted
management channel before creating a new node record. Preserve old observations;
do not edit them to look current. Keep raw artifacts private: diagnostic output
may reveal operational paths and identities.

## Verify a native seed backup

This command checks an unused **plain-format PostgreSQL 16 backup**, not a copy
of a running PGDATA directory. Use the same candidate's native backup tools. The
backup must include its label, manifest, per-file checksums and required WAL;
external tablespaces, symlinks, hard links and special files are refused.

```sh
python3 scripts/deploy/pre1/seed.py verify-backup \
  --backup /secure/seed-backup \
  --install-root /opt/pgrac \
  --binary-sha256 ACTUAL_POSTGRES_SHA256 \
  --manifest-sha256 ACTUAL_BACKUP_MANIFEST_SHA256 \
  --system-identifier ACTUAL_SOURCE_SYSTEM_IDENTIFIER \
  --out /secure/pre1-evidence/seed-verify-001.json
```

Both directories must be absolute canonical paths. The output must be outside
the backup directory and must not exist. Keep the backup immutable during and
after verification. Checksum-free manifests are refused even if native
`pg_verifybackup` would accept them. The command does not skip checksum or WAL
verification, retains native command output and rejects changes during checking.

`BACKUP_CONTENT_VERIFIED` is only a content prerequisite. It does **not** prove
the source's provenance, the seed's clean stop, safe target directories or cloned
node identities. Those remain pending; no backup result authorizes initialization,
startup or recovery. A native verifier failure or timeout is incomplete evidence,
not a successful backup.

## Create the initial node0 seed

This is an administrative **new-dataset-only** primitive, not the full four-node
bootstrap command. First qualify storage and fencing, record all four nodes as
empty and stopped, disable database autostart, and freeze the intended schema.
Do not run it on any existing, partly initialized or failed database directory.

On node0, prepare an operator-reviewed JSON request containing exactly:

- `schema_version: 1`, `action: "create-seed"`, and a unique `dataset_id`.
- `node`: only `node_id` (0), `vm_uuid`, `machine_id`, current `boot_id`, `pgdata`,
  `install_root`, and the database user's numeric `uid`/`gid`.
- `binary_sha256`: the installed `postgres` hash.
- `shared_mount`, its actual GFS2 `fs_uuid`, and an empty `shared_root` beneath it.
- `schema_path` and its reviewed `schema_sha256`.
- New, nonexisting `backup` and `log` paths outside PGDATA and the shared mount.

PGDATA and shared_root must already be empty, canonical, dedicated directories
owned by the database UID/GID, with no other-user access. Backup/log parents must
also belong to that user. Paths may use letters, digits, underscore, dot, hyphen
and slash only. The output must be a new path outside all inputs. Keep the request,
tool directory and schema under trusted administrative control.

```sh
sudo -n python3 /opt/pgrac-pre1-tools/seed.py create-seed \
  --request /secure/pre1/seed-request.json \
  --out /secure/pre1-evidence/seed-create-001.json
```

Native database commands run as the dedicated database user, not root. The seed
uses a local peer-authenticated socket, no TCP listener, cluster/LMS disabled and
private catalog/control/WAL. It creates schema once, takes a plain SHA-256 backup
with streamed WAL and fsync, then requests normal fast shutdown using SIGINT.
Linux pidfd support is required to bind that signal to the verified postmaster;
PID reuse or an unproved startup never authorizes a guessed stop target.

`SEED_BACKUP_READY` means the seed stopped cleanly and native backup verification
passed. It is **not** permission to start four nodes: clone identity, configuration,
voting admission and current storage checks remain required. Failed commands,
partial directories and artifacts are retained. Never erase them or rerun initdb
to turn a failed attempt into success. No crash recovery is attempted.

## Copy a verified seed to an empty joiner

Transfer that same plain backup and its immutable seed request/result to each
joiner over the trusted administrative channel. Preserve the source backup.
Do not copy shared user files and do not rerun initdb. On each of nodes 1–3,
prepare a JSON request with exactly `schema_version: 1`, `action: "clone-seed"`,
the target's same eight-field `node` identity, `binary_sha256`, local `backup`
path, `seed_request_path`, `seed_request_sha256`, `seed_artifact_path` and
`seed_artifact_sha256`. Both source hashes are from the retained node0 artifacts,
not recalculated from an untrusted replacement.

```sh
sudo -n python3 /opt/pgrac-pre1-tools/seed_clone.py \
  --request /secure/pre1/clone-node1.json \
  --out /secure/pre1-evidence/clone-node1-001.json
```

The target PGDATA must already be empty, stopped and owned by the database user.
Each joiner verifies the local backup with the native checksum/WAL checker, then
copies through held directory descriptors with exclusive child creation and
without following links. It fsyncs the copied files/directories and verifies the
entire resulting backup again. A failed copy remains partial; it is never merged
with a subsequent attempt or automatically removed.

`CLONED_NOT_CONFIGURED` proves only this copy. The database remains stopped and
startup permission remains false. Keep the seed backup immutable; apply the
reviewed per-node runtime configuration only as part of the subsequent guarded
four-node bootstrap workflow. This command never rewrites pg_control or WAL.

## Configure an unused laboratory seed and start it once

The `bootstrap_config.py` / `bootstrap_runtime.py` adapters currently support
only the isolated `pre1-gfs2-arm64-lab-v1` profile. They are not a customer
production authentication policy or a general restart utility. The rendered
HBA permits peer authentication locally and SQL access from the one trusted
controller address only; that controller has laboratory superuser trust.

Keep the configuration request, seed request/result, clone request/result and
their SHA-256 references under trusted administrative control. The configuration
request contains `schema_version: 1`, `profile_id`, four complete profile `nodes`,
`cluster_name`, `shared_root`, `controller_addr` and exactly three `voting_wwids`.
Arbitrary GUC overrides and additional fields are rejected. It retains private
control/catalog/WAL, two LMS workers, fsync and full-page writes, and disables
automatic restart after a server crash.

On each guest, the initial request contains `schema_version: 1`,
`action: "configure-initial"`, `node_id`, `binary_sha256`, and references named
`config`, `seed_request`, `seed_artifact`, `clone_request`, `clone_artifact`.
Each reference is exactly `{"path": "/absolute/path", "sha256": "..."}`;
the two clone references are null on node0. Actual GFS2 identity, guest identity,
stopped processes, native control and pristine clone contents are checked.
Voting permissions must already be bound to the exact three whole-device WWIDs;
do not give the database account general membership of the `disk` group.

```sh
sudo -n python3 /opt/pgrac-pre1-tools/bootstrap_runtime.py configure-initial \
  --request /secure/pre1/initial-node0.json \
  --out /secure/pre1-evidence/configured-node0.json
```

The adapter exclusively creates three runtime configuration files and a fresh
protected socket directory. It proves a configuration-only delta; it does not
rewrite native control/WAL or overwrite the native seed configuration. First
startup explicitly selects the generated runtime configuration.

Only after **all four** configuration results and current storage/voting gates
pass, start the nodes in the frozen deployment order. Bind each invocation to
the corresponding retained configuration result:

```sh
sudo -n python3 /opt/pgrac-pre1-tools/bootstrap_runtime.py start-initial \
  --request /secure/pre1-evidence/configured-node0.json \
  --sha256 CONFIGURED_RESULT_SHA256 \
  --out /secure/pre1-evidence/started-node0.json
```

A durable exclusive attempt marker remains even on failure. Never remove it to
retry. `PROCESS_STARTED_NOT_ADMITTED` means only native startup succeeded;
membership, quorum, semantic activation and workload acceptance are separate
checks. No result from these commands authorizes crash recovery or later restart.

For normal shutdown, the controller must dispatch the following action to all
four nodes concurrently, **before waiting for any one node to finish**:

```sh
sudo -n python3 /opt/pgrac-pre1-tools/bootstrap_runtime.py stop-exact \
  --request /secure/pre1-evidence/started-node0.json \
  --sha256 STARTED_RESULT_SHA256 \
  --out /secure/pre1-evidence/stopped-node0.json
```

It sends native fast-stop SIGINT through a revalidated Linux pidfd and waits up
to 600 seconds without escalating to kill. Process exit alone is not a clean
cluster verdict: independently verify all native controls, new shutdown log
markers, persisted ALIVE/closure state and outstanding debt before any restart.
