# Membership client preparation

`membership.py` validates an exact administrative request and sends one
parameterized SQL command through a libpq service. It provides `precheck`,
`execute` and `status` commands. The service must name an administrative
connection; use a protected service/password file rather than a password on
the command line.

**Membership execution is not available yet.** The database command validates
the complete request and returns `blocked` while its production operation owner
is unavailable. It reports `cluster_disabled` when clustering is disabled, or
`membership_authority_unavailable` otherwise. No membership change is made.
The client does not fall back to legacy target-only commands.

An operation request is a JSON object with these exact fields:

| Field | Value |
|---|---|
| `version` | `1` |
| `operation_kind` | `leave`, `remove` or `rejoin` |
| `target_node` | Integer from 0 to 15 |
| `guest_uuid` | Canonical lowercase, nonzero UUID |
| `expected_formation` | Positive unsigned 64-bit integer |
| `operation_generation` | Positive unsigned 64-bit integer assigned by the server owner |
| `expected_old_incarnation` | Positive unsigned 64-bit integer |
| `reserved_new_incarnation` | Greater than the old incarnation for rejoin; otherwise zero |

Do not invent a generation or derive it from a clock. Fresh online joins are
unsupported. Duplicate JSON keys, unknown fields and inconsistent identities
are rejected before starting `psql`.

Use a superuser connection for these administrative commands:

```sh
python3 membership.py precheck --service cluster-admin --request request.json
python3 membership.py execute --service cluster-admin --request request.json
python3 membership.py status --service cluster-admin --request request.json
```

`status` can omit `--request` to inspect the current operation. Exit status 0
means the command returned a valid non-blocking response; it does not imply
that an accepted operation has finished. Exit status 2 means the server reports
blocked/conflict/stale/rejected; 1 means input or transport/response failure.
On connection loss, inspect status using the same exact request. The client
does not retry execution or cancel the server operation automatically.
