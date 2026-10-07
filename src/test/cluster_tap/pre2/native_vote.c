/* Fresh disposable voting-image fixture. Link the real voting disk I/O code.
 * The only mutation is O_EXCL creation; an existing output is never replaced.
 * No runtime membership, formation or recovery record is synthesized here.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#include "postgres.h"
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include "cluster/cluster_voting_disk_io.h"
#include "cluster/cluster_storage_quorum.h"

bool cluster_shared_config = false;
int cluster_node_id = 0;
bool cluster_storage_quorum_allows_node(int node pg_attribute_unused()) { abort(); }

int
main(int argc, char **argv)
{
	int fd;
	unsigned index;
	ClusterVotingSlot slot;
	bool attest = argc == 4 && strcmp(argv[1], "--attest") == 0;
	const char *path;

	if ((!attest && argc != 3) || strlen(argv[argc - 1]) != 1
		|| argv[argc - 1][0] < '0' || argv[argc - 1][0] > '2')
		return 2;
	index = argv[argc - 1][0] - '0';
	path = argv[attest ? 2 : 1];
	if (attest)
		goto readback;
	fd = open(path, O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW, 0600);
	if (fd < 0)
		return 3;
	/* The fixed-region minimum ends exactly where stripe history begins.
	 * A loop device cannot grow at EOF; leave real zero-filled capacity for
	 * the first reservation and same-cohort restart history. */
	if (cluster_voting_disk_format(fd, CLUSTER_MAX_NODES, index) != CLUSTER_VOTING_DISK_IO_OK
		|| ftruncate(fd, (off_t)16 * 1024 * 1024) != 0 || fdatasync(fd) != 0
		|| close(fd) != 0)
		return 4;
readback:
	fd = cluster_voting_disk_open(path, false);
	if (fd < 0)
		return 5;
	if (attest && (!cluster_voting_disk_epoch_ballot_authority_attest(fd)
				   || !cluster_voting_disk_pgrd_authority_attest(fd)))
		return 8;
	for (unsigned node = 0; node < CLUSTER_MAX_NODES; node++)
	{
		if (cluster_voting_disk_read_slot(fd, index, node, &slot) != CLUSTER_VOTING_DISK_IO_OK
			|| slot.generation != 0 || slot.flags != 0 || slot.incarnation != 0)
			return 6;
	}
	return close(fd) == 0 ? 0 : 7;
}
