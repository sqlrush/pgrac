/* Original initdb owner's native SIDE copy, not startup authority.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef PGRAC_INITDB_SIDE_H
#define PGRAC_INITDB_SIDE_H

/* Caller owns the original creation, the successful child exit and exact
 * synced shutdown checkpoint. FDs remain caller-owned. Never overwrites or
 * adopts a prior native_side tree, changes source files or grants admission. */
extern bool pgrac_initdb_side_create(int source_fd, int shared_fd);

/* The same still-live original cohort creator already owns native_side.
 * Populate only a new peer origin from that peer's completed native child;
 * refuse founder/unknown nodes and any prior complete or partial origin. */
extern bool pgrac_initdb_side_origin_create(int source_fd, int native_fd, uint32 node);

#endif
