/* Execute the actual heap publication body with PostgreSQL assertions on/off. */
#include "postgres.h"
#include <setjmp.h>
#include "access/htup_details.h"
#include "cluster/cluster_itl.h"
#include "cluster/cluster_pcm_x_bufmgr.h"
#include "cluster/cluster_multixact_current.h"
#include "cluster/cluster_terminal_ref_census.h"
/* Standalone TAP uses libc; the product Assert macro remains unchanged. */
#undef printf
#undef fprintf
#include "unit_test.h"

#if defined(PUBLICATION_EXPECT_CASSERT) != defined(USE_ASSERT_CHECKING)
#error publication test compiled with the wrong PostgreSQL Assert configuration
#endif

UT_DEFINE_GLOBALS();

static jmp_buf publication_error;
static bool catch_publication_error;
static int error_level, error_code, error_messages;

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	if (catch_publication_error)
		longjmp(publication_error, 2);
	fprintf(stderr, "%s:%d: assertion failed: %s\n", fileName, lineNumber, conditionName);
	abort();
}
#include "test_cluster_current_mx_publication_types.inc"

/* Only the backend ERROR boundary is replaced; FSM and heap body are real. */
bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	error_level = elevel;
	return elevel >= ERROR;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}

int
errcode(int sqlerrcode)
{
	error_code = sqlerrcode;
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	error_messages++;
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{
	if (catch_publication_error)
		longjmp(publication_error, 1);
	abort();
}

static unsigned transitions, publications;
static ClusterCurrentMxStampPlan *publishing_plan;

#define cluster_multixact_current_heap_publish_transition publication_actual_transition
#include "test_cluster_current_mx_publication_transition.inc"
#undef cluster_multixact_current_heap_publish_transition

bool
cluster_multixact_current_heap_publish_transition(ClusterCurrentMxHeapPublishStage stage,
	ClusterCurrentMxHeapPublishEvent event, ClusterCurrentMxHeapPublishStage *out)
{
	transitions++;
	return publication_actual_transition(stage, event, out);
}

void
cluster_ctrc_note_publication_after_apply(const ClusterCtrcReceiptHandle *handle, bool published)
{
	UT_ASSERT(publishing_plan != NULL && published);
	UT_ASSERT(handle == &publishing_plan->handles[publications]);
	UT_ASSERT_EQ(publishing_plan->stage, CMX_HEAP_STAGE_REFERENCE_PUBLISHED);
	publications++;
}

#include "test_cluster_current_mx_publication.inc"

UT_TEST(publication_executes_transition_with_or_without_assertions)
{
	const uint16 counts[] = {0, 1, 2, CLUSTER_CURRENT_MX_MAX_MEMBERS};
	unsigned i;

	for (i = 0; i < lengthof(counts); i++)
	{
		ClusterCurrentMxStampPlan plan = {0};

		plan.active = true;
		plan.stage = CMX_HEAP_STAGE_RECEIPT_APPLIED;
		plan.receipt_count = counts[i];
		publishing_plan = &plan;
		transitions = publications = 0;
		error_level = error_code = error_messages = 0;
		cluster_current_mx_stamp_published(&plan);
		UT_ASSERT_EQ(transitions, 1);
		UT_ASSERT_EQ(plan.stage, CMX_HEAP_STAGE_REFERENCE_PUBLISHED);
		UT_ASSERT_EQ(publications, counts[i]);
		UT_ASSERT(!plan.active);
		UT_ASSERT_EQ(error_level, 0);
	}
	publishing_plan = NULL;
}

UT_TEST(invalid_publication_is_a_production_error_without_plan_mutation)
{
	const ClusterCurrentMxHeapPublishStage stages[] = {
		CMX_HEAP_STAGE_LOCAL_DESCRIPTOR, CMX_HEAP_STAGE_RECEIPT_PREPARED,
		CMX_HEAP_STAGE_REFERENCE_PUBLISHED, CMX_HEAP_STAGE_CANCELLED,
		(ClusterCurrentMxHeapPublishStage) -1, (ClusterCurrentMxHeapPublishStage) INT_MAX
	};
	unsigned i;

	for (i = 0; i < lengthof(stages); i++)
	{
		/* Static storage remains defined after the controlled ERROR longjmp. */
		static ClusterCurrentMxStampPlan plan;
		ClusterCurrentMxStampPlan before;
		int caught;

		memset(&plan, 0x5a, sizeof(plan));
		plan.active = true;
		plan.stage = stages[i];
		plan.receipt_count = 2;
		memcpy(&before, &plan, sizeof(plan));
		publishing_plan = &plan;
		transitions = publications = 0;
		error_level = error_code = error_messages = 0;
		catch_publication_error = true;
		caught = setjmp(publication_error);
		if (caught == 0)
			cluster_current_mx_stamp_published(&plan);
		catch_publication_error = false;
		UT_ASSERT_EQ(caught, 1); /* Assert failure uses 2, never a production ERROR. */
		UT_ASSERT_EQ(error_level, ERROR);
		UT_ASSERT_EQ(error_code, ERRCODE_INTERNAL_ERROR);
		UT_ASSERT_EQ(error_messages, 1);
		UT_ASSERT_EQ(transitions, 1);
		UT_ASSERT_EQ(publications, 0);
		UT_ASSERT(memcmp(&plan, &before, sizeof(plan)) == 0);
	}
	publishing_plan = NULL;
}

UT_TEST(null_and_inactive_publication_are_noops)
{
	ClusterCurrentMxStampPlan plan = {0};
	ClusterCurrentMxStampPlan before;

	plan.stage = CMX_HEAP_STAGE_RECEIPT_APPLIED;
	memcpy(&before, &plan, sizeof(plan));
	transitions = publications = 0;
	error_level = error_code = error_messages = 0;
	cluster_current_mx_stamp_published(NULL);
	cluster_current_mx_stamp_published(&plan);
	UT_ASSERT_EQ(transitions, 0);
	UT_ASSERT_EQ(publications, 0);
	UT_ASSERT_EQ(error_level, 0);
	UT_ASSERT(memcmp(&plan, &before, sizeof(plan)) == 0);
}

int
main(void)
{
	UT_PLAN(3);
	UT_RUN(publication_executes_transition_with_or_without_assertions);
	UT_RUN(invalid_publication_is_a_production_error_without_plan_mutation);
	UT_RUN(null_and_inactive_publication_are_noops);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
