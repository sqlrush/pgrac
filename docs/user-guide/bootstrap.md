# Bootstrap a node

Once the [install](install.md) is complete, the `pgrac-init` and
`pgrac-start` shell tools provide a one-command path to creating a
PostgreSQL data directory with cluster-aware configuration and
starting the server.

## Quick start (single node)

```bash
# 1. Create PGDATA + write cluster.node_id + write pgrac.conf
pgrac-init -D /tmp/linkdb-demo --node-id=0 --cluster-name=demo

# 2. Adjust port + socket dir if needed
echo "port = 65433"                                  >> /tmp/linkdb-demo/postgresql.conf
echo "unix_socket_directories = '/tmp'"              >> /tmp/linkdb-demo/postgresql.conf
echo "listen_addresses = ''"                         >> /tmp/linkdb-demo/postgresql.conf

# 3. Start the server
pgrac-start -D /tmp/linkdb-demo -l /tmp/linkdb-demo.log -w

# 4. Connect and inspect cluster state
psql -h /tmp -p 65433 -d postgres -c 'SELECT * FROM pg_cluster_nodes;'
#  node_id |        hostname         | interconnect_addr | public_addr |  role   | region | is_self
# ---------+-------------------------+-------------------+-------------+---------+--------+---------
#        0 | <your-hostname>         | 127.0.0.1:6432    |             | primary |        | t

# 5. Stop when done
pg_ctl -D /tmp/linkdb-demo -m fast stop
```

## What `pgrac-init` does

Under the hood `pgrac-init` is a shell wrapper that performs three
steps in sequence:

1. Runs `initdb -D <PGDATA>` to create a fresh PostgreSQL data
   directory (skipped if the directory is already initialised).
2. Appends `cluster.node_id = <N>` to `postgresql.conf` (idempotent
   if the same value is already there; refuses to overwrite a
   different value unless `--force` is passed).
3. Writes `<PGDATA>/pgrac.conf` with a `[cluster]` section and a
   `[node.<N>]` section.  Existing `pgrac.conf` is preserved unless
   `--force` is passed.

See `pgrac-init --help` for the full option list.

## What `pgrac-start` does

`pgrac-start` is a thin wrapper around `pg_ctl start` with three
preflight checks:

1. PGDATA exists and contains `PG_VERSION`.
2. `postgresql.conf` declares `cluster.node_id`.
3. If `pgrac.conf` is present, the first `[node.<N>]` section
   matches `cluster.node_id` (mismatches produce a warning, not a
   hard fail; the postmaster itself FATALs on a true mismatch with
   a precise hint).

After preflight `pgrac-start` exec's `pg_ctl -D <PGDATA> start`
with all `-l / -w / -W / -t / -o` flags passed through unchanged.

## Stop / status / reload

`pgrac-init` and `pgrac-start` only cover the bootstrap and start
paths.  Use the standard `pg_ctl` for everything else:

```bash
pg_ctl -D /tmp/linkdb-demo status        # check status
pg_ctl -D /tmp/linkdb-demo -m fast stop  # graceful stop
pg_ctl -D /tmp/linkdb-demo reload        # reread postgresql.conf (PGC_SIGHUP GUCs only)
```

## Single-node fallback (no `pgrac.conf`)

If `pgrac.conf` is missing entirely (e.g. you ran plain `initdb`
without `pgrac-init`), the server falls back to a single-node
topology containing one row -- the local node, with `node_id`
taken from the `cluster.node_id` GUC.  A `LOG`-level message is
emitted at startup:

```
LOG: cluster_conf: "<path>/pgrac.conf" not found, falling back to
     single-node mode (node_id=<N>)
```

`pg_cluster_nodes` then returns one row with the local node
information.  This makes single-node development convenient: you
do not need to write a `pgrac.conf` to run linkdb as a stand-alone
PostgreSQL server with the cluster GUCs available.

## Multi-node cluster (`tier1` TCP interconnect)

The default `cluster.interconnect_tier = stub` runs a single node.  To run
a real multi-node cluster with the LMON heartbeat interconnect, set
`tier1` and declare every peer (including self) in a shared `pgrac.conf`.

This example brings up two nodes on one host over loopback — interconnect
ports `6432`/`6433`, client SQL ports `65433`/`65434`:

```bash
# One identical pgrac.conf for BOTH nodes — every peer, self included.
read -r -d '' PGRAC_CONF <<'CONF'
[cluster]
name = demo

[node.0]
interconnect_addr = 127.0.0.1:6432
hostname          = demo-0
role              = primary

[node.1]
interconnect_addr = 127.0.0.1:6433
hostname          = demo-1
role              = standby
CONF

for n in 0 1; do
  D=/tmp/pgrac-demo/n$n
  pgrac-init -D "$D" --node-id=$n --cluster-name=demo
  printf '%s\n' "$PGRAC_CONF" > "$D/pgrac.conf"
  {
    echo "port = $((65433 + n))"
    echo "unix_socket_directories = '/tmp'"
    echo "listen_addresses = ''"
    echo "cluster.interconnect_tier = tier1"
  } >> "$D/postgresql.conf"
  pgrac-start -D "$D" -l "/tmp/pgrac-demo/n$n.log" -w
done

# Membership + live interconnect state, observed from node 0:
psql -h /tmp -p 65433 -d postgres \
  -c 'SELECT node_id, hostname, role, is_self FROM pg_cluster_nodes ORDER BY node_id;'
psql -h /tmp -p 65433 -d postgres \
  -c 'SELECT node_id, state, heartbeat_send_count, heartbeat_recv_count
        FROM pg_cluster_ic_peers WHERE heartbeat_recv_count > 0;'
```

Each node connects to its peers' `interconnect_addr`, exchanges a HELLO
handshake, and trades a 1 Hz heartbeat; `pg_cluster_ic_peers` exposes the
per-peer `state`, heartbeat counters, and last-seen timestamps.  Re-run the
second query a few seconds apart and the counters climb.

Because the shared `pgrac.conf` lists `[node.0]` first, `pgrac-start` prints
a heuristic warning on every node whose id isn't `0` — startup still succeeds
as long as this node's `cluster.node_id` appears as some `[node.N]`.  Across
hosts, replace the loopback addresses with routable `host:port` pairs and make
sure the interconnect ports are reachable.

## Current limitations

- **`tier1` carries the heartbeat path only**: the cross-node interconnect
  carries LMON heartbeat and membership today.  Higher tiers (`tier2`+) are
  not yet supported and cause the postmaster to refuse to start with
  `ERRCODE_FEATURE_NOT_SUPPORTED`; cross-node GES / Cache Fusion / recovery
  are scaffolded and likewise return `FEATURE_NOT_SUPPORTED`.  See
  [Configuration](configuration.md) for the GUC reference.
- **Bash only**: `pgrac-init` / `pgrac-start` are bash scripts;
  they require a POSIX shell.  Windows is not currently supported.
- **No one-shot multi-node bootstrap**: there is no built-in tool for
  initialising N nodes in a single command.  Each node is bootstrapped
  individually with its matching `--node-id`, and topology changes require a
  restart (`pgrac.conf` has no online reload).

## Reporting issues

File issues at <https://github.com/sqlrush/pgrac/issues>.
