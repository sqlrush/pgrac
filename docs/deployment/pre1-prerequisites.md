# Four-VM deployment: preliminary checks

Author: SqlRush <sqlrush@gmail.com>

Status: development tooling, **not a certified four-VM installation procedure**.
The current commands validate planned inputs and collect guest identity. Storage,
fencing, installed binary, effective configuration and seed verification are still
separate, mandatory checks. Do not format or mount shared disks based on a profile
validation result.

## Requirements

- Python 3.9 or newer and OpenSSH on the designated Linux/macOS controller.
- Four independent KVM/libvirt guests, not four containers sharing one kernel.
- Dedicated shared block storage for GFS2 and three distinct voting devices.
- Separate negative-test voting devices and directories; never reuse live data.
- Exact WWIDs and capacities, not `/dev/sdX` names or wildcard device permission.
- Known SSH server public keys obtained through a trusted management channel.
- A protected SSH key file per administrative endpoint; no passwords or private
  key contents in JSON. The controller disables inherited SSH configuration and
  agent forwarding. It never automatically accepts a new host key.
- Non-root database UID/GID, explicit local data/install/log paths and a shared
  mountpoint. Lexical path checks do not replace subsequent guest realpath checks.
- A frozen source commit/tree, binary hash, build information, configuration
  hashes, seed identity and workload/judge identity.

The original deployment profile is `pre1-gfs2-v1` (RHEL 9 x86_64). The separate
`pre1-gfs2-arm64-lab-v1` is an ARM64 laboratory candidate, not RHEL/x86 certification.
Accepting a profile name in JSON does not certify that platform. Shared cloud disks,
other filesystems and failure-domain HA also require their own qualification.

## Prepare the profile

Use [profile.schema.json](../../scripts/deploy/pre1/profile.schema.json) as the
closed input contract. All fields are required; unknown fields are rejected.
Fill values from the actual build, guests and storage inventory. Do not copy
synthetic unit-test identities into a deployment manifest.

Each `nodes` entry declares node ID 0–3, VM UUID, machine ID, boot ID, administrative
SSH endpoint, pinned `ssh-ed25519` host public key, SQL/control/data addresses,
number of data workers, local paths and database UID/GID. `admin_endpoint` contains
`host`, `port`, `user` and `identity_file`. The last is a local private-key **path**,
not the key itself. IPv4 addresses must be concrete and non-loopback.

`votes` contains three records (`index`, `wwid`, `size` in bytes,
`logical_sector` = 512). `authorization.device_allowlist` separately identifies
each authorized device's WWID, byte size, purpose and fresh-media declaration.
`fixture_inventory.MAIN` and `.NEGATIVE` have separate roots and voting WWIDs.
Declaring `fresh: true` does not cause a write and is not proof a disk is empty.

Protect manifests and raw inventory as operational information. Public reports
must not include credentials, private keys or customer infrastructure identities.

## Check inputs without contacting guests

```sh
python3 scripts/deploy/pre1/preflight.py check-profile --profile /secure/pre1.json
```

`PASS` here means **PROFILE_ONLY**: syntax, required fields and internal consistency.
The result always includes `deployment_qualified: false`. It is not a database,
storage, fencing or four-node test result.

## Collect read-only guest identity

Create a private evidence directory first, then choose a new output filename:

```sh
install -d -m 700 /secure/pre1-evidence
python3 scripts/deploy/pre1/preflight.py inventory \
  --profile /secure/pre1.json \
  --out /secure/pre1-evidence/identity-001.json
python3 scripts/deploy/pre1/preflight.py verify \
  --profile /secure/pre1.json \
  --inventory /secure/pre1-evidence/identity-001.json
```

The guest probe reads machine/boot/domain UUIDs and asks `systemd-detect-virt` for
the virtualization type. Reading the DMI UUID may require passwordless permission
for the exact read-only `sudo -n cat /sys/class/dmi/id/product_uuid` command.
It does not install packages, mount disks, alter PostgreSQL or execute SQL writes.

At this implementation stage, a successful identity collection still returns
`BLOCKED / QUALIFICATION_PENDING` and lists checks not yet performed. In particular,
four different boot IDs are not sufficient proof of four independent libvirt
domains. `verify` cannot remove those pending obligations.

## Results and preservation

| Exit | Meaning |
|---:|---|
| 0 | Requested scoped check passed; inspect `scope`, not just exit status |
| 2 | Invalid/missing inputs, identity mismatch, or qualification pending |
| 3 | SSH, evidence, parsing or filesystem operation failed |

Standard output is one JSON result. Existing evidence is never intentionally
replaced. Each output uses a private temporary file, local controller lock, file
sync and directory sync. A publication failure is not PASS; an artifact left by a
directory-sync failure must be reconciled, not overwritten. Only one controller
may operate a campaign. A local lock does not coordinate two separate hosts.

The identity artifact is bound to the canonical profile hash. Changing the binary,
configuration or other profile fields invalidates that binding. Preserve old
artifacts and collect a new observation under a new filename.

## Scope limitations

The deployment work does not enable shared native catalogs/control files/WAL,
crash takeover, online membership changes or database raw-device storage. Normal
shutdown and same-data restart must be validated separately. A normal-restart
result never authorizes reusing an unclean crash image as if it were clean.

## Tool tests

```sh
python3 -B -m unittest discover -s scripts/deploy/pre1/tests -v
```

These tests use synthetic profiles and temporary local files. Their PASS is not
GFS2, fencing or live database certification.
