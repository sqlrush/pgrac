/* Original creator's durable relmap authorities; no startup admission.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef PGRAC_INITDB_RELMAP_H
#define PGRAC_INITDB_RELMAP_H

/* Only the original creator, after its successful post-bootstrap child exit
 * and synced shutdown checkpoint. Both directory FDs remain caller-owned.
 * Preflight all four native maps, then create new authorities without adopting
 * or overwriting any prior file. Refusal never authorizes ROOT publication. */
extern bool pgrac_initdb_relmap_create(int source_fd, int shared_fd);

#endif
