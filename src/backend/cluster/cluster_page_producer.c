/*-------------------------------------------------------------------------
 *
 * cluster_page_producer.c
 *    STOP-06 mutation-token and PageVersion producer-batch contract.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xloginsert.h"
#include "cluster/cluster_page_producer.h"
#include "cluster/cluster_scn.h"

static bool
incarnation_nonzero(const uint8 incarnation[16])
{
	uint8 any = 0;
	int i;

	for (i = 0; i < 16; i++)
		any |= incarnation[i];
	return any != 0;
}

uint64
rf_page_mutation_token_next(void)
{
	SCN token = cluster_scn_advance();

	if (!SCN_VALID(token)) {
#ifdef USE_CLUSTER_UNIT
		return 0;
#else
		elog(ERROR, "STOP-06 mutation-token allocator returned zero");
#endif
	}
	return (uint64)token;
}

void
rf_page_mutation_token_observe(uint64 token)
{
	if (token == 0) {
#ifndef USE_CLUSTER_UNIT
		elog(ERROR, "cannot observe a zero STOP-06 mutation token");
#endif
		return;
	}
	cluster_scn_observe((SCN)token);
}

static bool
component_shape_valid(const RfPageProducerComponentV1 *component)
{
	if (component->reserved_zero != 0 || component->reserved_zero2 != 0)
		return false;

	if (component->page_class == RF_PAGE_CLASS_ORDINARY) {
		uint64 before_token;

		if (component->page == NULL || !incarnation_nonzero(component->segment_incarnation))
			return false;
		before_token = (uint64)((PageHeader)component->page)->pd_block_scn;
		switch (component->before_kind) {
		case RF_PAGE_STATE_PRESENT:
			return !PageIsNew(component->page) && before_token != 0;
		case RF_PAGE_STATE_UNFORMATTED:
		case RF_PAGE_STATE_ABSENT:
			return PageIsNew(component->page) && before_token == 0;
		default:
			return false;
		}
	}

	if (incarnation_nonzero(component->segment_incarnation) || component->page != NULL)
		return false;
	if (component->page_class == RF_PAGE_CLASS_REBUILDABLE_FSM)
		return component->before_kind == RF_PAGE_STATE_REBUILDABLE;
	if (component->page_class == RF_PAGE_CLASS_ROUTED_SPACE
		|| component->page_class == RF_PAGE_CLASS_ROUTED_HEADER
		|| component->page_class == RF_PAGE_CLASS_ROUTED_SIDE)
		return component->before_kind == RF_PAGE_STATE_ROUTED;
	return false;
}

bool
rf_page_producer_prepare_v1(const RfPageProducerComponentV1 *components, uint8 component_count,
							RfPageProducerBatchV1 *batch)
{
	RfPageProducerBatchV1 prepared;
	uint64 token;
	int i;
	int j;

	if (components == NULL || batch == NULL || component_count == 0
		|| component_count > RF_PAGE_PRODUCER_MAX_COMPONENTS)
		return false;

	memset(&prepared, 0, sizeof(prepared));
	for (i = 0; i < component_count; i++) {
		const RfPageProducerComponentV1 *component = &components[i];
		RfPageVersionEdgeEntryV1 *entry = &prepared.entries[i];

		if (!component_shape_valid(component) || component->component_ordinal != i
			|| (i > 0 && components[i - 1].block_id >= component->block_id))
			return false;
		for (j = 0; j < i; j++)
			if (component->page != NULL && component->page == components[j].page)
				return false;

		entry->block_id = component->block_id;
		entry->page_class = component->page_class;
		entry->before_kind = component->before_kind;
		entry->component_ordinal = component->component_ordinal;
		if (component->page_class == RF_PAGE_CLASS_ORDINARY) {
			entry->result_kind = RF_PAGE_STATE_PRESENT;
			if (component->before_kind != RF_PAGE_STATE_ABSENT)
				memcpy(entry->before.segment_incarnation, component->segment_incarnation, 16);
			entry->before.mutation_token = (uint64)((PageHeader)component->page)->pd_block_scn;
			memcpy(entry->result_incarnation, component->segment_incarnation, 16);
			prepared.ordinary_pages[i] = component->page;
		} else if (component->page_class == RF_PAGE_CLASS_REBUILDABLE_FSM)
			entry->result_kind = RF_PAGE_STATE_REBUILDABLE;
		else
			entry->result_kind = RF_PAGE_STATE_ROUTED;
	}

	/* This is deliberately after complete validation and before any stamp. */
	token = rf_page_mutation_token_next();
	if (token == 0)
		return false;
	prepared.result_token = token;
	prepared.entry_count = component_count;
	prepared.prepared = true;
	*batch = prepared;
	return true;
}

bool
rf_page_producer_stamp_v1(RfPageProducerBatchV1 *batch)
{
	int i;

	if (batch == NULL || !batch->prepared || batch->result_token == 0 || batch->entry_count == 0
		|| batch->entry_count > RF_PAGE_PRODUCER_MAX_COMPONENTS)
		return false;
	if (batch->stamped)
		return true;

	/* Global revalidation first: a failure must leave every page untouched. */
	for (i = 0; i < batch->entry_count; i++) {
		const RfPageVersionEdgeEntryV1 *entry = &batch->entries[i];
		Page page = batch->ordinary_pages[i];

		if (entry->page_class != RF_PAGE_CLASS_ORDINARY)
			continue;
		if (page == NULL
			|| (uint64)((PageHeader)page)->pd_block_scn != entry->before.mutation_token)
			return false;
	}

	for (i = 0; i < batch->entry_count; i++) {
		if (batch->entries[i].page_class == RF_PAGE_CLASS_ORDINARY)
			((PageHeader)batch->ordinary_pages[i])->pd_block_scn = (SCN)batch->result_token;
	}
	batch->stamped = true;
	return true;
}

bool
rf_page_producer_register_wal_v1(const RfPageProducerBatchV1 *batch)
{
	int i;

	if (batch == NULL || !batch->prepared || !batch->stamped || batch->result_token == 0
		|| batch->entry_count == 0 || batch->entry_count > RF_PAGE_PRODUCER_MAX_COMPONENTS)
		return false;
	for (i = 0; i < batch->entry_count; i++) {
		if (batch->entries[i].page_class == RF_PAGE_CLASS_ORDINARY
			&& (batch->ordinary_pages[i] == NULL
				|| (uint64)((PageHeader)batch->ordinary_pages[i])->pd_block_scn
					   != batch->result_token))
			return false;
	}

	XLogRegisterPageVersionEdge(batch->result_token, batch->entries, batch->entry_count);
	return true;
}
