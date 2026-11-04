/* Original creation observations, without publication or serving authority.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_INITDB_TREE_PRIVATE_H
#define CLUSTER_INITDB_TREE_PRIVATE_H

#include "c.h"

typedef struct ClusterInitdbTree {
	uint8 content[32];
	uint8 identity[32];
} ClusterInitdbTree;

/* Read complete relative-path-sorted regular files and directories from the
 * borrowed fd. Refuse aliases, unsafe ownership, concurrent changes or bounds.
 * recheck omits only original creator's derived global/wal_startup subtree and
 * two ROOT files; these must be independently persisted/read back by creator.
 * No writes, symlink following or descriptor transfer. Failure clears out. */
extern bool cluster_initdb_tree_read(int fd, bool recheck, ClusterInitdbTree *out);
/* Only after the original creator has written and independently read back its
 * complete initial registry. Additionally omit its exact generation-1 file
 * and parent directory; still traverse the directory and include every other
 * entry. Require the initial file to exist. The caller must separately recheck
 * that full registry before ROOT publication; this observation grants nothing. */
extern bool cluster_initdb_tree_read_registered(int fd, ClusterInitdbTree *out);
#endif
