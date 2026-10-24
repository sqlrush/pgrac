/* Original creator's durable relmap authorities; no startup admission.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef PGRAC_INITDB_RELMAP_H
#define PGRAC_INITDB_RELMAP_H

/* Only the original creator, after its successful post-bootstrap child exit
 * and synced shutdown checkpoint. Both directory FDs remain caller-owned.
 * Preflight all four native maps, then create new authorities without adopting
 * or overwriting any prior file. Refusal never authorizes ROOT publication. */
extern bool pgrac_initdb_relmap_create(int source_fd, int shared_fd);

/* Dormant v2 entry for that SAME original creator and fresh directory cohort.
 * Caller binds system_identifier to its synced native control and the database
 * incarnation to its original creation context. No input here authenticates
 * those facts or adopts an existing installation. Creates both copies in all
 * four namespaces; any failure, including partial creation, forbids ROOT
 * publication. No retry/repair/adoption. Existing live initdb still selects v1.
 */
extern bool pgrac_initdb_relmap_v2_create(int source_fd, int shared_fd,
									   uint64 system_identifier, uint64 database_incarnation);

#endif
