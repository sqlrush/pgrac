# PRE2 fresh four-node database tools

Author: SqlRush <sqlrush@gmail.com>

These tools initialize a new four-node ARM64 database on an existing GFS2
cluster. Keep this directory together with `scripts/deploy/pre1`; the PRE1
helpers are runtime dependencies. Follow the [installation manual](../../../../docs/pre2/install.md)
to prepare the operating system, storage, voting media and node identities.

The tools use the supplied product source and installed binaries. They do not
create virtual machines, format shared storage, start servers, or overwrite
existing databases. Use empty, dedicated destination paths. Keep request files,
SSH credentials and generated output outside the source tree.

## Create and install a plan

Set `LAB`, `SRC`, `WORK`, `GUEST_TOOLS` and `POSTGRES_SHA256` as described in the
installation manual. `SRC` must be the source of the binary installed on all
four nodes. `GUEST_TOOLS` must be a new versioned path such as
`/opt/pgrac-pre2-lab-tools-eval01`.

```sh
python3 "$LAB/fresh_controller.py" new-identity > "$WORK/identity.json"
# Fill request.json using the new identity and the actual four-node inventory.
python3 "$LAB/install_guest_tools.py" --request "$WORK/request.json" \
  --dest "$GUEST_TOOLS" --out "$WORK/tools-installed.json"
python3 "$LAB/fresh_controller.py" plan --request "$WORK/request.json" \
  --source "$SRC" --binary-sha256 "$POSTGRES_SHA256" --out "$WORK/plan.json"
python3 "$LAB/fresh_controller.py" render --plan "$WORK/plan.json" \
  --request "$WORK/request.json" --source "$SRC" --out-dir "$WORK/rendered"
```

The installer sends only its named source dependencies over SSH with a pinned
host key. It verifies the archive before installing a new directory, and keeps
partial output on failure. It does not package adjacent request files or keys.

For this PRE2 version, the plan's `creation` must be `cohort`. Check the rendered
configuration before creating the database. The `lab-8g` memory profile sets
`shared_buffers=512MB` and `max_connections=100`; other configured values are
listed in the generated plan. Do not treat this profile as all product defaults.

## Initialize once and distribute

```sh
python3 "$LAB/fresh_controller.py" cohort --plan "$WORK/plan.json" \
  --request "$WORK/request.json" --source "$SRC" --transport ssh \
  --tools-root "$GUEST_TOOLS" --out "$WORK/cohort.json"
python3 "$LAB/fresh_controller.py" distribute --plan "$WORK/plan.json" \
  --request "$WORK/request.json" --source "$SRC" --transport ssh \
  --tools-root "$GUEST_TOOLS" --out "$WORK/distribute.json"
```

The original node runs native `initdb --pgrac-initdb-cohort` once. Distribution
installs each resulting `node_N` directory on its matching member, preserving
permissions and links. It is not four independent `initdb` runs or a clone of a
running node's PGDATA. Install each node's rendered configuration as described
in the manual, then check the installed bootstrap configuration:

```sh
python3 "$LAB/fresh_controller.py" bootstrap-check --plan "$WORK/plan.json" \
  --request "$WORK/request.json" --source "$SRC" --transport ssh \
  --tools-root "$GUEST_TOOLS" --out "$WORK/bootstrap-check.json"
```

`bootstrap-check` uses the candidate's own `postgres -C shared_memory_size`
entry. A successful check does not start the database. Continue with the manual's
four-node startup commands. Do not use `--local-validation` for this deployment;
that mode omits the real guest identity and GFS2 checks.

Any failed command requires investigation before continuing. Keep partial DATA,
WAL, configuration and output files and contact support; do not overwrite them
or edit control metadata to bypass a refusal.

## Offline tool tests

```sh
python3 "$LAB/tests/test_lab.py" SourceContract Request Render Plan \
  MemoryProfile Identity ConfigRequest GuestValidation Installer
```

This command selects only local tool tests; it does not launch a database or VM.
