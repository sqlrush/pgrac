/* Actual sequence DDL entry prefixes must qualify before reading the page.
 * Author: SqlRush <sqlrush@gmail.com> */
int sequence_version_fixture_main(void);
#define main sequence_version_fixture_main
#include "test_cluster_sequence_version.c"
#undef main
#include "access/table.h"
#include "catalog/storage.h"

static unsigned qualifications;
static bool reject_ddl;
static jmp_buf ddl_jump;

/* Qualification itself is linked/tested in storage_subtransaction. Force its
 * refusal here to detect earlier page access or transaction side effects. */
void
RelationCheckStorageTransaction(RelFileLocator locator, char persistence,
								const char *operation)
{
	UT_ASSERT(RelFileLocatorEquals(locator, relation_data.rd_locator));
	UT_ASSERT_EQ(persistence, relform.relpersistence);
	qualifications++;
	if (reject_ddl)
		longjmp(ddl_jump, 1);
}

Relation
table_open(Oid relid, LOCKMODE lockmode)
{
	UT_ASSERT_EQ(relid, SequenceRelationId);
	return &relation_data;
}

HeapTuple
SearchSysCacheCopy(int cacheId, Datum key, Datum key2, Datum key3, Datum key4)
{
	UT_ASSERT_EQ(key2 | key3 | key4, 0);
	return SearchSysCache1(cacheId, key);
}

static void
reset_entry(void)
{
	Oid seq_relid = 20000;
	SeqTable elm;
	Relation seq_rel;
	Buffer buf;
	HeapTupleData seqdatatuple;

#include "test_cluster_sequence_ddl_reset.inc"
}

static void
alter_entry(void)
{
	Oid relid = 20000;
	SeqTable elm;
	Relation seqrel;
	Relation rel pg_attribute_unused();
	HeapTuple seqtuple;
	Form_pg_sequence seqform pg_attribute_unused();
	Buffer buf;
	HeapTupleData datatuple;

#include "test_cluster_sequence_ddl_alter.inc"
}

static void
persistence_entry(void)
{
	Oid relid = 20000;
	SeqTable elm;
	Relation seqrel;
	Buffer buf;
	HeapTupleData seqdatatuple;

#include "test_cluster_sequence_ddl_persistence.inc"
}

static void
check_entry(void (*entry)(void))
{
	reset(false);
	identity_reads = 1; /* Existing page-read fixture's precondition. */
	qualifications = 0;
	reject_ddl = true;
	if (setjmp(ddl_jump) == 0)
	{
		entry();
		UT_ASSERT(false);
	}
	UT_ASSERT_EQ(qualifications, 1);
	UT_ASSERT(!locked);
	UT_ASSERT_EQ(xid_count + data_count + wal_count + edge_count + dirties, 0);
	/* The same original prefix still reaches the page when eligible. */
	reset(false);
	identity_reads = 1;
	qualifications = 0;
	reject_ddl = false;
	entry();
	UT_ASSERT_EQ(qualifications, 1);
	UT_ASSERT(locked);
}

UT_TEST(reset_refuses_before_sequence_page) { check_entry(reset_entry); }
UT_TEST(alter_refuses_before_sequence_page) { check_entry(alter_entry); }
UT_TEST(persistence_refuses_before_sequence_page) { check_entry(persistence_entry); }

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(reset_refuses_before_sequence_page);
	UT_RUN(alter_refuses_before_sequence_page);
	UT_RUN(persistence_refuses_before_sequence_page);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
