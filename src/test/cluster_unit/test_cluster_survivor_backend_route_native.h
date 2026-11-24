/* Real metadata, signal and target readers for the backend route fixture.
 * Shared allocation, PID roster, OS delivery and role scheduling stay host
 * boundaries. No metadata syntax is promoted to a receipt or authority.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "port/pg_crc32c.h"
#include "test_cluster_survivor_pgsa.inc"
#pragma push_macro("proc")
#undef proc
#include "../../backend/cluster/cluster_semantic_survivor.c"
#include "../../backend/cluster/cluster_semantic_survivor_candidate.c"
#pragma pop_macro("proc")
#include "test_cluster_survivor_signal_fixture.h"
Latch *MyLatch;

#include "utils/backend_status.h"
static PgBackendStatus route_status[100+NUM_AUXPROCTYPES];
static PgBackendStatus *BackendStatusArray=route_status;
#define NumBackendStatSlots (MaxBackends + NUM_AUXPROCTYPES)
#include "test_cluster_survivor_status_identity.inc"
#undef NumBackendStatSlots
#pragma push_macro("proc")
#undef proc
#include "../../backend/cluster/cluster_semantic_survivor_targets.c"
#pragma pop_macro("proc")
