<!-- Thanks for contributing to pgrac. Keep PRs focused; small is easier to review. -->

## What this changes

<!-- A short description of the change and why. Link the issue, e.g. Closes #123 -->

## How it was tested

<!-- Commands you ran, the platform (OS + version), and the result. -->

## Checklist

- [ ] The `--disable-cluster` build stays binary-equivalent to upstream
      PostgreSQL 16.13 and passes the 219-test regression suite — or this PR
      doesn't touch the non-cluster path.
- [ ] Any required runtime check has a real production branch, not a check that
      lives only inside `Assert()` (see `AGENTS.md`).
- [ ] Code matches upstream PostgreSQL C style (`.clang-format` / `.editorconfig`).
- [ ] Docs and/or tests updated where relevant.

<!--
Cache Fusion / GES / cross-node recovery are still in flux. For changes in
those areas, please open an issue first so we can coordinate the protocol.
-->
