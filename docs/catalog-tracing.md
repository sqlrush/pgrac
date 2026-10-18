# Catalog diagnostics

`cluster.catalog_trace_relation` logs visibility and descriptor inputs for one relation OID. It is a superuser session setting. The default `0` disables logging; accepted values are 0 through 2147483647.

Find the table OID before reproducing an operation, then set its numeric OID:

```sql
SELECT 'my_table'::regclass::oid;
SET cluster.catalog_trace_relation = 16385;
-- Run the operation in this session.
RESET cluster.catalog_trace_relation;
```

Server LOG records use `PGRAC_CATALOG_TRACE`. Visibility records carry a per-process `call` number; combine it with the server-log process ID. Records include matching pg_class and pg_attribute rows, raw command IDs, snapshot command IDs, selected ITL and CR fields, return source locations, descriptor bounds and default type inputs. Decoded command IDs are recorded when the visibility code evaluates them.

Tracing does not change visibility decisions. Enable it only for a focused diagnostic session because it can produce many log records.
