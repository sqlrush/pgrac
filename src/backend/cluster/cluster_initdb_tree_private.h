/* Original creation observations, without publication or serving authority.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_INITDB_TREE_PRIVATE_H
#define CLUSTER_INITDB_TREE_PRIVATE_H

#include "c.h"

typedef struct ClusterInitdbTree
{
	uint8 content[32];
	uint8 identity[32];
} ClusterInitdbTree;

/* Read complete relative-path-sorted regular files and directories from the
 * borrowed fd. Refuse aliases, unsafe ownership, concurrent changes or bounds.
 * recheck omits only original creator's derived global/wal_startup subtree and
 * two ROOT files; these must be independently persisted/read back by creator.
 * No writes, symlink following or descriptor transfer. Failure clears out. */
extern bool cluster_initdb_tree_read(int fd, bool recheck, ClusterInitdbTree *out);
#endif
