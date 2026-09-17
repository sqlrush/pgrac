# Releases and version identification

Author: SqlRush <sqlrush@gmail.com>

## Current release

[v0.130.0 — first stable MVP](v0.130.0.md) is the current release. It combines
the MVP release CI gate with four valid four-node correctness samples on the
CI-fixed kernel. Stable means the documented MVP scope, not production HA,
independent-host shared-storage certification or a performance guarantee.

[v0.130.0-mvp.1](v0.130.0-mvp.1.md) remains the immutable first evaluation
snapshot. Its original CI limitation is retained in its historical notes;
it is not retroactively qualified by this release.

## Identifying a release

- `PGRAC_VERSION` records the source release label, without the leading `v`.
- The annotated Git tag `v<version>` identifies the immutable source commit.
- Each release has notes describing its tested scope, limitations and usage.
- A source tag does not identify an arbitrary installed binary. Keep the source
  commit, build options, configuration and binary SHA-256 with each installation.

For this MVP, the legacy compiled `pgrac_version()` string predates the release
label. Use the tag, commit and recorded binary hash to identify the installation;
do not use that legacy string as proof of an MVP build. The PostgreSQL base
version remains 16.13.

## Version policy

Versions use `MAJOR.MINOR.PATCH`, optionally followed by a prerelease label:

| Label | Meaning |
|---|---|
| `v0.130.0-mvp.1` | First frozen MVP baseline |
| `v0.130.0` | First stable release within the documented MVP scope |
| `-mvp.N`, `-alpha.N`, `-beta.N` | Numbered evaluation prereleases |
| `-rc.N` | Release candidates with their own published qualification scope |
| No suffix | Stable release; only after its acceptance criteria pass |

Existing historical tags remain unchanged. New feature milestones increment
the minor version; maintenance releases increment the patch version. Revisions
within one prerelease line increment its numbered suffix. The `1.0.0` name is
reserved for the first formally qualified full-product stable release;
qualification of this narrower MVP scope does not grant that status.

`main` is the integration branch. Feature and fix branches start from an exact
commit; releases are selected commits, not moving branch names. Published tags
must never be moved or reused. A correction receives a new version and retains
the superseded release's evidence and limitations.

## Selecting a version

```sh
git fetch origin tag v0.130.0
git switch --detach v0.130.0
git rev-parse HEAD
cat PGRAC_VERSION
```

Build in a separate prefix using the [installation guide](../user-guide/install.md).
Returning to an older source version is not a data downgrade procedure. This MVP
does not certify cross-version data migration, mixed-version clusters or rolling
upgrades. Preserve existing data and use a separate evaluation environment when
changing versions.
