# PRE1: four-node data and health checks

Author: SqlRush <sqlrush@gmail.com>

`scripts/deploy/pre1/verify.py` performs read-only user checks. It does not
replace release testing, certify storage or fencing, or authorize restart.
Use it only after the four-node deployment has reached its normal OPEN state.

## Inputs

- The unchanged four-node configuration JSON used by `bootstrap_config.py`.
- An installed `psql` executable with its matching client libraries.
- The expected database system identifier from the same-origin seed record.
- A trusted, complete reference CSV for the table: two columns, no header,
  positive unique integer keys in ascending order, then the expected value.
- A SQL account permitted to read the table and cluster health views.

Stop test writers before capturing or comparing the reference. A reference
exported from one node proves agreement with that node, not that the business
values are correct: generate or independently check the expected values first.
Use the same CSV representation as PostgreSQL `COPY ... WITH (FORMAT csv)`.
Keep credentials in the usual protected libpq password file, not command-line
arguments or committed configuration. The command never prompts for a password.

## Run

Create the trusted output parent first. The output directory must not exist and
must be outside PGDATA, install, log and shared-data roots.

```sh
python3 scripts/deploy/pre1/verify.py \
  --config /srv/pgrac/evidence/bootstrap-config.json \
  --psql /opt/pgrac/bin/psql \
  --system-identifier REPLACE_WITH_SEED_SYSTEM_IDENTIFIER \
  --relation public.demo_account --key-column id --value-column value \
  --rows 10000 --expected-csv /srv/pgrac/reference/demo_account.csv \
  --user pgrac --database postgres \
  --out /srv/pgrac/evidence/verify-001
```

Replace identifiers, row count, paths and system identifier with the actual
deployment values. If a staged installation needs it, set `LD_LIBRARY_PATH`
to that installation's `lib` directory before invoking the command.

For each of the four configured endpoints the tool checks health, exports all
rows ordered by key, compares every byte with the reference, and checks health
again. Health requires the expected node/database identities, no recovery,
quorum membership, Resource-X OPEN and the target writer path. Verification
connections are read-only and retain the 600-second statement limit; this does
not alter application-session settings.

## Interpret the result

Exit status zero and `USER_CHECKS_PASSED_NOT_DEPLOYMENT_CERTIFIED` mean all twelve
checks completed successfully. `formal_pre_pass`, `deployment_qualified` and
`restart_allowed` deliberately remain false. Normal shutdown and same-data
restart require their separate native closure proofs; see
[cold snapshots](pre1-cold-snapshots.md) and
[bootstrap and lifecycle commands](pre1-remote-status.md).

Failure, timeout, incomplete rows, unexpected output or an identity mismatch
is retained under the new output directory with raw stdout/stderr, return code,
timing and input hashes. Do not delete a failed attempt or reuse its directory.
A timeout means verification is incomplete, not that the data was proved wrong
or correct. Investigate the failed step and retry into another output directory;
this tool never repairs data, resets voting or starts recovery.
