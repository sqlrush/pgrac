# Additional entry point; use with -f Makefile -f /path/to/this/file.
QVOTEC_OWNER_OBJECTS = $(CLUSTER_VERSION_O) $(CLUSTER_QVOTEC_PGSA_TEST_O) \
	$(CLUSTER_NODE_REMOVE_POLICY_O) $(CLUSTER_EPOCH_BALLOT_O) \
	$(CLUSTER_REPLACEMENT_REQUEST_O) $(CLUSTER_MEMBERSHIP_O) \
	$(CLUSTER_CLEAN_LEAVE_POLICY_O) cluster_qvotec_io_test.o \
	$(CLUSTER_UNDO_BLOCK0_O) $(CLUSTER_UNDO_ROOT_DESCRIPTOR_O) \
	test_cluster_qvotec_activation_product.o test_cluster_storage_quorum_product.o

test_cluster_qvotec_owner_recovery: $(srcdir)/test_cluster_qvotec_owner_recovery.c \
		$(srcdir)/test_cluster_qvotec.c $(srcdir)/unit_test.h $(QVOTEC_OWNER_OBJECTS)
	$(CC) $(CFLAGS) $(CPPFLAGS) -I$(srcdir) -DCLUSTER_QVOTEC_PGSA_UNIT_TEST \
		-DQVOTEC_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_qvotec.c"' \
		-DLMON_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_lmon.c"' \
		-DSHMEM_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_shmem.c"' \
		-DSEMANTIC_SOURCE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/cluster_semantic_activation.c"' \
		-DSEMANTIC_HEADER_PATH='"$(abspath $(top_srcdir))/src/include/cluster/cluster_semantic_activation.h"' \
		-DCLUSTER_MAKEFILE_PATH='"$(abspath $(top_srcdir))/src/backend/cluster/Makefile"' $< \
		$(QVOTEC_OWNER_OBJECTS) $(R4_RUNTIME_VIS_TEST_DEAD_STRIP) \
		$(top_builddir)/src/common/libpgcommon.a $(CLUSTER_UNIT_PORT_LIBS) \
		$(LDFLAGS) -lssl -lcrypto -o $@
