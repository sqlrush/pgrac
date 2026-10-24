/* Complete physical ROOT union through the original relation/history readers.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
static void
whole_relation_add(int fd, uint32 relation, bool empty)
{
	ClusterSpaceReservation reservation = { 0 };
	PGAlignedBlock pages[2], data;
	char path[80];

	reservation.identity.key.system_identifier = 817;
	reservation.identity.key.database_incarnation = 91;
	memcpy(reservation.identity.key.storage_uuid, store_uuid, 16);
	reservation.identity.key.locator = (RelFileLocator){ 1663, 5, relation };
	reservation.identity.incarnation[0] = 3;
	reservation.identity.sequence = 2;
	reservation.identity.operation = 9;
	reservation.identity.state = CLUSTER_SPACE_IDENTITY_LIVE;
	reservation.next_block = empty ? 0 : 1;
	UT_ASSERT(cluster_space_identity_page_encode(&reservation.identity, 7, pages[0].data, BLCKSZ));
	UT_ASSERT(cluster_space_reservation_page_encode(&reservation, 11, pages[1].data, BLCKSZ));
	snprintf(path, sizeof(path), "base/5/%u_space", relation);
	census_file(fd, path, pages, sizeof(pages));
	/* A non-ITL MAIN page is a raw owner obligation, not an empty heap. */
	memset(&data, 0, sizeof(data));
	data.data[BLCKSZ - 1] = 73;
	snprintf(path, sizeof(path), "base/5/%u", relation);
	census_file(fd, path, data.data, empty ? 0 : BLCKSZ);
}

static int
whole_relation_files(void)
{
	int fd = census_files(0);
	PGAlignedBlock page;

	memset(&page, 0, sizeof(page));
	page.data[BLCKSZ - 1] = 51;
	census_file(fd, "base/5/16385_vm", page.data, BLCKSZ);
	census_file(fd, "base/5/16385_fsm", page.data, BLCKSZ);
	census_file(fd, "unowned", "kept", 4);
	whole_relation_add(fd, 16386, false);
	whole_relation_add(fd, 16387, true);
	return fd;
}

static ClusterRecoveryRelationCensus *
whole_relation_open(ClusterRecoveryRootSet *roots, ClusterRecoveryReferenceGraph *graph)
{
	ClusterRecoveryRelationCensus *census = NULL;
	UT_ASSERT_EQ(cluster_recovery_relation_census_begin(roots, graph, 512 * 1024, &census),
				 CLUSTER_RECOVERY_RELATIONS_MORE);
	return census;
}

static ClusterRecoveryRelationCensusResult
whole_relation_drain(ClusterRecoveryRelationCensus *census)
{
	ClusterRecoveryRelationCensusEntry entry;
	ClusterRecoveryRelationCensusResult result = CLUSTER_RECOVERY_RELATIONS_INVALID;
	for (unsigned i = 0; census != NULL && i < 10000; i++) {
		result = cluster_recovery_relation_census_next(census, &entry);
		if (result >= CLUSTER_RECOVERY_RELATIONS_DRAINED)
			break;
	}
	return result;
}

static void
whole_relation_failed(ClusterRecoveryRelationCensus *census, ClusterRecoveryRootSet *roots,
					  ClusterRecoveryReferenceGraph *graph,
					  ClusterRecoveryRelationCensusResult result)
{
	ClusterRecoveryRelationCensusEntry entry;
	uint8 digest[32];
	memset(&entry, 0x77, sizeof(entry));
	UT_ASSERT_EQ(cluster_recovery_relation_census_next(census, &entry), result);
	UT_ASSERT(entry.root == NULL && entry.page.page == NULL);
	UT_ASSERT(entry.reference.entry.tuple_bytes == NULL
			  && entry.reference.transaction.record == NULL);
	UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
	UT_ASSERT(!cluster_recovery_reference_graph_observed(graph));
}

UT_TEST(test_whole_relation_union_keeps_all_roots_pages_and_history)
{
	int fd = whole_relation_files();
	ClusterRecoveryRootSet *roots = store_roots(fd);
	ClusterRecoveryUndoStore *store = NULL;
	ClusterRecoveryReferenceGraph *graph = census_graph(roots, &store);
	ClusterRecoveryRelationCensus *census = whole_relation_open(roots, graph);
	ClusterRecoveryRelationCensusEntry entry;
	ClusterRecoveryRelationCensusResult result = CLUSTER_RECOVERY_RELATIONS_INVALID;
	unsigned seen = 0, forks = 0, pages = 0, heaps = 0, itls = 0, tuples = 0, unknown = 0, raw = 0;
	uint32 expected_roots = cluster_recovery_root_set_count(roots);
	uint8 before[32], after[32];
	UT_ASSERT(cluster_recovery_root_set_digest(roots, before));
	for (unsigned i = 0; census != NULL && i < 10000; i++) {
		result = cluster_recovery_relation_census_next(census, &entry);
		if (result >= CLUSTER_RECOVERY_RELATIONS_DRAINED)
			break;
		if (result == CLUSTER_RECOVERY_RELATIONS_MORE) {
			UT_ASSERT(entry.root == NULL && entry.page.page == NULL);
			continue;
		}
		UT_ASSERT(entry.root != NULL);
		if (entry.kind == CLUSTER_RECOVERY_RELATIONS_ROOT) {
			UT_ASSERT_EQ(entry.root_index, seen++);
			if (strcmp(entry.root->file.path, "unowned") == 0)
				unknown++;
		} else if (entry.kind == CLUSTER_RECOVERY_RELATIONS_FORK_END)
			forks++;
		else if (entry.kind == CLUSTER_RECOVERY_RELATIONS_PAGE) {
			pages++;
			if (entry.heap)
				heaps++;
			else {
				raw++;
				UT_ASSERT(entry.reference.transaction.record == NULL);
			}
		} else if (entry.kind == CLUSTER_RECOVERY_RELATIONS_REFERENCE) {
			UT_ASSERT(entry.heap);
			if (entry.reference.entry.kind == CLUSTER_RECOVERY_HEAP_ITL)
				itls++;
			if (entry.reference.entry.tuple_bytes != NULL) {
				HeapTupleHeader tuple = (HeapTupleHeader)entry.reference.entry.tuple_bytes;
				tuples++;
				UT_ASSERT_EQ(HeapTupleHeaderGetRawXmax(tuple), 4195140);
				UT_ASSERT(tuple->t_infomask & HEAP_XMAX_IS_MULTI);
			}
		} else
			UT_ASSERT(false);
	}
	UT_ASSERT_EQ(result, CLUSTER_RECOVERY_RELATIONS_DRAINED);
	UT_ASSERT_EQ(seen, expected_roots);
	UT_ASSERT_EQ(forks, 8);
	UT_ASSERT_EQ(pages, 10);
	UT_ASSERT_EQ(heaps, 1);
	UT_ASSERT_EQ(raw, 9);
	UT_ASSERT_EQ(itls, 16);
	UT_ASSERT_EQ(tuples, 1);
	UT_ASSERT_EQ(unknown, 1);
	UT_ASSERT(cluster_recovery_root_set_digest(roots, after));
	UT_ASSERT_EQ(memcmp(before, after, sizeof(before)), 0);
	UT_ASSERT_EQ(whole_relation_drain(census), CLUSTER_RECOVERY_RELATIONS_DRAINED);
	cluster_recovery_relation_census_destroy(census);
	cluster_recovery_undo_store_destroy(store);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_whole_relation_missing_wal_target_is_not_an_empty_fork)
{
	int fd = census_files(0);
	ClusterRecoveryRootSet *roots = NULL;
	ClusterRecoveryRootsEvent event;
	ClusterSpaceIdentityKey key = { 0 };
	ClusterRecoveryUndoStore *store = NULL;
	ClusterRecoveryReferenceGraph *graph;
	ClusterRecoveryRelationCensus *census;
	key.system_identifier = 817;
	key.database_incarnation = 91;
	memcpy(key.storage_uuid, store_uuid, 16);
	key.locator = (RelFileLocator){ 1663, 5, 19999 };
	UT_ASSERT_EQ(cluster_recovery_root_set_begin(fd, 817, 91, store_uuid, 1024 * 1024, &roots), 0);
	UT_ASSERT_EQ(cluster_recovery_root_set_add_fork(roots, &key, MAIN_FORKNUM, 0), 0);
	for (unsigned i = 0; i < 1000; i++)
		if (cluster_recovery_root_set_step(roots, &event) >= CLUSTER_RECOVERY_ROOTS_OBSERVED)
			break;
	graph = census_graph(roots, &store);
	census = whole_relation_open(roots, graph);
	UT_ASSERT_EQ(whole_relation_drain(census), CLUSTER_RECOVERY_RELATIONS_STRUCTURE_REQUIRED);
	whole_relation_failed(census, roots, graph, CLUSTER_RECOVERY_RELATIONS_STRUCTURE_REQUIRED);
	cluster_recovery_relation_census_destroy(census);
	cluster_recovery_undo_store_destroy(store);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_whole_relation_missing_history_revokes_both_owners)
{
	int fd = census_files(4);
	ClusterRecoveryRootSet *roots = store_roots(fd);
	ClusterRecoveryUndoStore *store = NULL;
	ClusterRecoveryReferenceGraph *graph = census_graph(roots, &store);
	ClusterRecoveryRelationCensus *census = whole_relation_open(roots, graph);
	UT_ASSERT_EQ(whole_relation_drain(census), CLUSTER_RECOVERY_RELATIONS_BASE_REQUIRED);
	whole_relation_failed(census, roots, graph, CLUSTER_RECOVERY_RELATIONS_BASE_REQUIRED);
	cluster_recovery_relation_census_destroy(census);
	cluster_recovery_undo_store_destroy(store);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}

UT_TEST(test_whole_relation_io_and_short_reads_have_no_prefix)
{
	for (unsigned fault = 1; fault <= 2; fault++) {
		int fd = whole_relation_files();
		ClusterRecoveryRootSet *roots = store_roots(fd);
		ClusterRecoveryUndoStore *store = NULL;
		ClusterRecoveryReferenceGraph *graph = census_graph(roots, &store);
		ClusterRecoveryRelationCensus *census = whole_relation_open(roots, graph);
		inject_pread_fault = fault;
		UT_ASSERT_EQ(whole_relation_drain(census), CLUSTER_RECOVERY_RELATIONS_IO);
		inject_pread_fault = 0;
		whole_relation_failed(census, roots, graph, CLUSTER_RECOVERY_RELATIONS_IO);
		cluster_recovery_relation_census_destroy(census);
		cluster_recovery_undo_store_destroy(store);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_whole_relation_final_reobservation_includes_untouched_files)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		int fd = whole_relation_files();
		ClusterRecoveryRootSet *roots = store_roots(fd);
		ClusterRecoveryUndoStore *store = NULL;
		ClusterRecoveryReferenceGraph *graph = census_graph(roots, &store);
		ClusterRecoveryRelationCensus *census = whole_relation_open(roots, graph);
		ClusterRecoveryRelationCensusEntry entry;
		unsigned forks = 0;
		for (unsigned i = 0; census != NULL && i < 10000 && forks < 8; i++) {
			ClusterRecoveryRelationCensusResult result
				= cluster_recovery_relation_census_next(census, &entry);
			if (result >= CLUSTER_RECOVERY_RELATIONS_DRAINED)
				break;
			if (result == CLUSTER_RECOVERY_RELATIONS_ENTRY
				&& entry.kind == CLUSTER_RECOVERY_RELATIONS_FORK_END)
				forks++;
		}
		UT_ASSERT_EQ(forks, 8);
		if (fault == 0)
			census_file(fd, "unowned", "drft", 4); /* unchanged inode and size */
		else if (fault == 1)
			census_file(fd, "later", "new", 3);
		else if (fault == 2) {
			UT_ASSERT_EQ(unlinkat(fd, "unowned", 0), 0);
			census_file(fd, "unowned", "kept", 4); /* same content, new object */
		} else {
			UndoSegmentHeaderData header;
			load_store_header(fd, 257, &header);
			header.tt_slots[47].xid++;
			replace_store_header(fd, 257, &header);
		}
		UT_ASSERT_EQ(whole_relation_drain(census), CLUSTER_RECOVERY_RELATIONS_CHANGED);
		whole_relation_failed(census, roots, graph, CLUSTER_RECOVERY_RELATIONS_CHANGED);
		cluster_recovery_relation_census_destroy(census);
		cluster_recovery_undo_store_destroy(store);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_whole_relation_lifecycle_failure_stays_revoked)
{
	for (unsigned mode = 0; mode < 5; mode++) {
		int fd = whole_relation_files();
		ClusterRecoveryRootSet *roots = store_roots(fd);
		ClusterRecoveryUndoStore *store = NULL;
		ClusterRecoveryReferenceGraph *graph = census_graph(roots, &store);
		ClusterRecoveryRelationCensus *census = whole_relation_open(roots, graph);
		if (mode < 2)
			UT_ASSERT_EQ(whole_relation_drain(census), CLUSTER_RECOVERY_RELATIONS_DRAINED);
		if (mode == 0 || mode == 2)
			cluster_recovery_root_set_invalidate(roots);
		else if (mode == 1 || mode == 3)
			cluster_recovery_reference_graph_invalidate(graph);
		else
			UT_ASSERT_EQ(cluster_recovery_relation_census_next(census, NULL),
						 CLUSTER_RECOVERY_RELATIONS_INVALID);
		whole_relation_failed(census, roots, graph,
							  mode == 4 ? CLUSTER_RECOVERY_RELATIONS_INVALID
										: CLUSTER_RECOVERY_RELATIONS_CHANGED);
		cluster_recovery_relation_census_destroy(census);
		cluster_recovery_undo_store_destroy(store);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_whole_relation_arguments_and_owned_budget)
{
	for (unsigned mode = 0; mode < 6; mode++) {
		int fd = whole_relation_files();
		ClusterRecoveryRootSet *roots = store_roots(fd);
		ClusterRecoveryUndoStore *store = NULL;
		ClusterRecoveryReferenceGraph *graph = census_graph(roots, &store);
		ClusterRecoveryRelationCensus *census = NULL;
		ClusterRecoveryRelationCensusEntry entry;
		ClusterRecoveryRootsEvent event;
		uint8 digest[32];
		if (mode == 4)
			UT_ASSERT_EQ(cluster_recovery_root_set_recheck_begin(roots),
						 CLUSTER_RECOVERY_ROOTS_MORE);
		if (mode == 5)
			cluster_recovery_reference_graph_invalidate(graph);
		UT_ASSERT_EQ(cluster_recovery_relation_census_begin(
						 mode == 1 ? NULL : roots, mode == 2 ? NULL : graph,
						 mode == 0 ? 1 : 512 * 1024, mode == 3 ? NULL : &census),
					 mode == 0 ? CLUSTER_RECOVERY_RELATIONS_CAPACITY
							   : CLUSTER_RECOVERY_RELATIONS_INVALID);
		UT_ASSERT(census == NULL);
		if (mode != 1)
			UT_ASSERT(!cluster_recovery_root_set_digest(roots, digest));
		if (mode != 2)
			UT_ASSERT(!cluster_recovery_reference_graph_observed(graph));
		if (mode == 4)
			UT_ASSERT_EQ(cluster_recovery_root_set_step(roots, &event),
						 CLUSTER_RECOVERY_ROOTS_INVALID);
		memset(&entry, 0x66, sizeof(entry));
		UT_ASSERT_EQ(cluster_recovery_relation_census_next(NULL, &entry),
					 CLUSTER_RECOVERY_RELATIONS_INVALID);
		UT_ASSERT(entry.root == NULL && entry.page.page == NULL);
		cluster_recovery_relation_census_destroy(NULL);
		cluster_recovery_undo_store_destroy(store);
		cluster_recovery_root_set_destroy(roots);
		remove_root(fd);
	}
}

UT_TEST(test_whole_relation_many_nonrelation_roots_remain_stepwise)
{
	int fd = census_files(0);
	ClusterRecoveryRootSet *roots;
	ClusterRecoveryUndoStore *store = NULL;
	ClusterRecoveryReferenceGraph *graph;
	ClusterRecoveryRelationCensus *census;
	ClusterRecoveryRelationCensusEntry entry;
	unsigned emitted = 0;
	uint32 expected;
	for (unsigned i = 0; i < 160; i++) {
		char name[40];
		snprintf(name, sizeof(name), "raw_%u", i);
		census_file(fd, name, "", 0);
	}
	roots = store_roots(fd);
	graph = census_graph(roots, &store);
	expected = cluster_recovery_root_set_count(roots);
	census = whole_relation_open(roots, graph);
	for (unsigned i = 0; census != NULL && i < 10000; i++) {
		ClusterRecoveryRelationCensusResult result
			= cluster_recovery_relation_census_next(census, &entry);
		if (result >= CLUSTER_RECOVERY_RELATIONS_DRAINED) {
			UT_ASSERT_EQ(result, CLUSTER_RECOVERY_RELATIONS_DRAINED);
			break;
		}
		if (result == CLUSTER_RECOVERY_RELATIONS_ENTRY
			&& entry.kind == CLUSTER_RECOVERY_RELATIONS_ROOT)
			emitted++;
	}
	UT_ASSERT(expected > 160);
	UT_ASSERT_EQ(emitted, expected);
	cluster_recovery_relation_census_destroy(census);
	cluster_recovery_undo_store_destroy(store);
	cluster_recovery_root_set_destroy(roots);
	remove_root(fd);
}
