# PRE1: complete cold snapshots

Author: SqlRush <sqlrush@gmail.com>

This tool preserves a **normally stopped** four-node dataset. It does not
implement crash recovery, hot backup, PITR, or automatic restoration into an
installed cluster. The restore command creates an independent offline copy;
it never writes a voting device or overwrites an existing PGDATA.

## Prerequisites

- All four instances have passed the native clean-stop checks: clean controls,
  exact process absence, this shutdown's protocol closure, and cleared ALIVE
  slots with no remaining required debt.
- `clean_restart.py prepare` has produced an immutable
  `CLEAN_RESTART_PREPARED` JSON artifact from that closure. Keep the request,
  native logs, observations and their hashes. Do not invent a PASS document.
- The same deployment-tool tree is present on the controller and each guest.
- No operator, service manager or automation may start an instance during
  copying. Leave voting, GFS2, DLM and the underlying storage available.
- Destination parents already exist, are trusted and are outside the protected
  data/install/shared roots. Destination directories themselves must not exist.
- Budget space for all four private PGDATAs, one shared-data tree, and three
  full voting images. Files containing credentials remain private.

## Create four pieces

Transfer the prepared artifact to every guest without modifying it. On each
guest, create an input JSON using its actual node ID and that file's SHA-256:

```json
{
  "prepared": {
    "path": "/srv/pgrac/evidence/restart-prepared.json",
    "sha256": "REPLACE_WITH_ACTUAL_SHA256"
  },
  "node_id": 0
}
```

Run locally as the designated administrative operator. Use a new destination
for each attempt; never delete a partial result to reuse its name.

```sh
sudo python3 scripts/deploy/pre1/snapshot.py create-piece \
  --request snapshot-node0.json --out /srv/pgrac/snapshots/run001-node0
```

Repeat on nodes 1, 2 and 3 with their own input and destination. Each piece
contains that member's complete PGDATA, including its own WAL and control.
Node 0's piece additionally contains the shared-data tree and all three raw
voting images. Member pieces alone are **not** a complete snapshot.

The command checks the native stopped state and identity before and after
copying, uses exclusive no-follow destinations, and records file hashes.
Copy failure leaves an incomplete directory, not a usable snapshot.

## Seal the complete set

Transfer all four piece directories intact to protected controller storage.
Do not include newly started or independently initialized members. Prepare:

```json
{
  "prepared": {
    "path": "/srv/pgrac/evidence/restart-prepared.json",
    "sha256": "REPLACE_WITH_ACTUAL_SHA256"
  },
  "pieces": [
    "/srv/pgrac/cold/run001/node0",
    "/srv/pgrac/cold/run001/node1",
    "/srv/pgrac/cold/run001/node2",
    "/srv/pgrac/cold/run001/node3"
  ]
}
```

```sh
python3 scripts/deploy/pre1/snapshot.py seal \
  --request snapshot-set.json --out /srv/pgrac/cold/run001/set.json
sha256sum /srv/pgrac/cold/run001/set.json
```

Sealing verifies every copied file against its source capture and re-observes
all four stopped instances through pinned SSH. It embeds the configuration and
closure provenance. Missing members, changed WAL, wrong voting images, links,
source changes or inconsistent identities are failures. Only the sealed
`COLD_SET_VERIFIED` artifact represents a complete set.

## Verify restoration to an independent directory

Create a request with the sealed manifest's actual path and SHA-256:

```json
{
  "collection": {
    "path": "/srv/pgrac/cold/run001/set.json",
    "sha256": "REPLACE_WITH_ACTUAL_SHA256"
  }
}
```

```sh
python3 scripts/deploy/pre1/snapshot.py restore \
  --request restore-set.json --out /srv/pgrac/restore-check/run001
```

The result is `COLD_SET_RESTORED_NOT_STARTED`: all four members, shared bytes,
voting images and provenance have been copied and verified as one generation.
The output is deliberately **not** connected to a running database. Startup
permission remains false. Never copy one member's WAL/control into another
member, restore just the shared table files, or write these images to live
voting devices. Keep the original dataset until the independent check succeeds.

Any unclean control or missing shutdown closure remains outside this workflow;
preserve it for the separately qualified recovery procedure.
