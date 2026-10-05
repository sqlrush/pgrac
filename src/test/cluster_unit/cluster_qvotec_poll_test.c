/* Real QVOTEC poll with test-owned descriptors; no installed test seam.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "cluster/cluster_voting_disk_io.h"

extern ClusterVotingDiskIoState cluster_qvotec_test_poll_read_slot(int fd, uint32 disk,
																uint32 node, ClusterVotingSlot *out);
extern void cluster_qvotec_test_poll_read_slots(int fd, int disk, uint32 first,
	uint32 count, ClusterVotingSlot *out, ClusterVotingDiskIoState *states);
#define cluster_voting_disk_read_slot cluster_qvotec_test_poll_read_slot
#define cluster_voting_disk_read_slots cluster_qvotec_test_poll_read_slots
#include QVOTEC_SOURCE_PATH
#undef cluster_voting_disk_read_slot
#undef cluster_voting_disk_read_slots

extern void cluster_qvotec_test_poll_once(const int *fds, int n_disks, uint64 incarnation);
extern void cluster_qvotec_test_register_wakeup(void);

void
cluster_qvotec_test_register_wakeup(void)
{
	qvotec_register_wakeup_latch();
}

void
cluster_qvotec_test_poll_once(const int *fds, int n_disks, uint64 incarnation)
{
	Assert(n_disks >= 0 && n_disks <= CLUSTER_MAX_VOTING_DISKS);
	qvotec_n_disks = n_disks;
	for (int i = 0; i < n_disks; i++)
		qvotec_fds[i] = fds[i];
	qvotec_self_incarnation = incarnation;
	if (qvotec_slot_matrix == NULL)
		qvotec_slot_matrix = calloc(CLUSTER_MAX_VOTING_DISKS * CLUSTER_MAX_NODES,
								   sizeof(ClusterVotingSlot));
	Assert(qvotec_slot_matrix != NULL);
	qvotec_poll_once();
}
