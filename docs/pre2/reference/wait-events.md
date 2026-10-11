# Startup phase publication wait

`ClusterStartupPhasePublishWait` belongs to the `Cluster: StartupPhase` wait type.
A process reports it while waiting to obtain a complete local startup-phase
snapshot. It does not represent a remote block request or checkpoint I/O.

To inspect active waits:

```sql
SELECT pid, backend_type, wait_event_type, wait_event
FROM pg_stat_activity
WHERE wait_event = 'ClusterStartupPhasePublishWait';
```

To find the registered event:

```sql
SELECT type, name
FROM pg_stat_cluster_wait_events
WHERE name = 'ClusterStartupPhasePublishWait';
```

The existing `ClusterStartupPhase0Wait` through `ClusterStartupPhase4Wait`
events keep their names and meanings. An individual sample shows the wait at
that instant; use repeated activity samples to observe its frequency.
