/*-------------------------------------------------------------------------
 *
 * cluster_page_producer.h
 *    STOP-06 mutation-token and PageVersion producer-batch contract.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_PAGE_PRODUCER_H
#define CLUSTER_PAGE_PRODUCER_H

#include "access/xlogrecord.h"
#include "storage/bufpage.h"

#define CLUSTER_PAGE_PRODUCER_INTERFACE_V1 1
#define RF_PAGE_PRODUCER_MAX_COMPONENTS XLR_PAGE_VERSION_EDGE_MAX_ENTRIES

typedef struct RfPageProducerComponentV1 {
	uint8 block_id;
	uint8 page_class;
	uint8 before_kind;
	uint8 reserved_zero;
	uint16 component_ordinal;
	uint16 reserved_zero2;
	uint8 segment_incarnation[16];
	Page page;
} RfPageProducerComponentV1;

/*
 * Caller-owned, fixed-capacity state.  prepare captures all exact before
 * versions and obtains the one batch token without changing a page.  stamp
 * first revalidates every ordinary page, then installs that token on all of
 * them before any WAL image/copy may be formed.
 */
typedef struct RfPageProducerBatchV1 {
	uint64 result_token;
	uint8 entry_count;
	bool prepared;
	bool stamped;
	Page ordinary_pages[RF_PAGE_PRODUCER_MAX_COMPONENTS];
	RfPageVersionEdgeEntryV1 entries[RF_PAGE_PRODUCER_MAX_COMPONENTS];
} RfPageProducerBatchV1;

extern uint64 rf_page_mutation_token_next(void);
extern void rf_page_mutation_token_observe(uint64 token);

extern bool rf_page_producer_prepare_v1(const RfPageProducerComponentV1 *components,
										uint8 component_count, RfPageProducerBatchV1 *batch);
extern bool rf_page_producer_stamp_v1(RfPageProducerBatchV1 *batch);
extern bool rf_page_producer_register_wal_v1(const RfPageProducerBatchV1 *batch);

#endif /* CLUSTER_PAGE_PRODUCER_H */
