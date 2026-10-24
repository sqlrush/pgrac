# Catalog diagnostics

`cluster.trace_visibility_relids` is a superuser session setting containing a comma-separated list of relation OIDs. The default empty string disables tracing. It accepts up to 64 distinct OIDs from 1 through 4294967295; duplicate OIDs are ignored and invalid lists are rejected. The list is parsed and cached on assignment. Disabled diagnostic hooks only test the cached enable flag.

Find the table OID and include the catalogs involved in the operation:

```sql
SELECT 'my_table'::regclass::oid;
SET cluster.trace_visibility_relids = '1259,1249,1247,16385';
-- Replace 16385 with the table OID returned above, then reproduce the operation.
RESET cluster.trace_visibility_relids;
```

1259, 1249 and 1247 select pg_class, pg_attribute and pg_type. Selecting a table also selects its pg_class and pg_attribute rows.

Server LOG records use `PGRAC_CATALOG_TRACE`. Combine the process ID with the per-process `call` number. `input_kind` distinguishes a live tuple from an already reconstructed FULL scratch input. Records include actual return locations, raw/combo command IDs, snapshot curcid, selected ITL origin/generation, catalog versions, descriptor bounds and default column/type inputs. An effective xmax is marked unknown until the original visibility code has determined it; tracing performs no additional MultiXact lookup.

The setting preserves visibility verdicts. Enable it for a focused diagnostic session because selected catalog scans can produce many log records.
