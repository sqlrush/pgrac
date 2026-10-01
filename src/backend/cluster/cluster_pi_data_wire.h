/* PGRAC background PI value codecs. No receipt or authority is constructed.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_PI_DATA_WIRE_H
#define CLUSTER_PI_DATA_WIRE_H

static bool
pi_data_overlap(const void *a, Size an, const void *b, Size bn)
{
	uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
	return a != NULL && b != NULL && (x <= y ? y - x < an : x - y < bn);
}

static uint64
pi_data_get(const uint8 *p, unsigned n)
{
	uint64 v = 0;
	for (unsigned i = 0; i < n; i++)
		v |= (uint64)p[i] << (8 * i);
	return v;
}

static void
pi_data_put(uint8 *p, uint64 v, unsigned n)
{
	for (unsigned i = 0; i < n; i++)
		p[i] = (uint8)(v >> (8 * i));
}


static void
pi_data_cut_encode(uint8 *p, const ClusterPcmPiWriteCutV1 *c)
{
	const ResourceXMasterSnapshot *h = &c->holder;
	const BufferTag *t = &h->assertion.resource;
	pi_data_put(p, t->spcOid, 4);
	pi_data_put(p + 4, t->dbOid, 4);
	pi_data_put(p + 8, t->relNumber, 4);
	pi_data_put(p + 12, t->forkNum, 4);
	pi_data_put(p + 16, t->blockNum, 4);
	pi_data_put(p + 20, h->assertion.requester_node, 4);
	pi_data_put(p + 24, h->base_authority_generation, 8);
	pi_data_put(p + 32, h->resource_formation, 8);
	pi_data_put(p + 40, h->master_session_incarnation, 8);
	pi_data_put(p + 48, h->assertion_sequence, 8);
	pi_data_put(p + 56, h->final_authority_generation, 8);
	pi_data_put(p + 64, h->source_carrier_generation, 8);
	pi_data_put(p + 72, h->requester_target_generation, 8);
	pi_data_put(p + 80, h->incompatible_holders_bitmap, 4);
	pi_data_put(p + 84, h->blocked_holders_bitmap, 4);
	pi_data_put(p + 88, h->source_node, 4);
	p[92] = h->phase;
	p[93] = h->proof_kind;
	p[94] = h->source_disposition;
	p[95] = h->is_head;
	pi_data_put(p + 96, c->binding_generation, 8);
	pi_data_put(p + 104, c->transition_count, 8);
	pi_data_put(p + 112, c->master_generation, 8);
	pi_data_put(p + 120, c->master_node, 4);
	pi_data_put(p + 124, c->pi_holders_bitmap, 4);
}

static void
pi_data_cut_decode(const uint8 *p, ClusterPcmPiWriteCutV1 *c)
{
	ResourceXMasterSnapshot *h = &c->holder;
	BufferTag *t = &h->assertion.resource;
	t->spcOid = pi_data_get(p, 4);
	t->dbOid = pi_data_get(p + 4, 4);
	t->relNumber = pi_data_get(p + 8, 4);
	t->forkNum = pi_data_get(p + 12, 4);
	t->blockNum = pi_data_get(p + 16, 4);
	h->assertion.requester_node = (int32)pi_data_get(p + 20, 4);
	h->base_authority_generation = pi_data_get(p + 24, 8);
	h->resource_formation = pi_data_get(p + 32, 8);
	h->master_session_incarnation = pi_data_get(p + 40, 8);
	h->assertion_sequence = pi_data_get(p + 48, 8);
	h->final_authority_generation = pi_data_get(p + 56, 8);
	h->source_carrier_generation = pi_data_get(p + 64, 8);
	h->requester_target_generation = pi_data_get(p + 72, 8);
	h->incompatible_holders_bitmap = pi_data_get(p + 80, 4);
	h->blocked_holders_bitmap = pi_data_get(p + 84, 4);
	h->source_node = (int32)pi_data_get(p + 88, 4);
	h->phase = p[92];
	h->proof_kind = p[93];
	h->source_disposition = p[94];
	h->is_head = p[95];
	c->binding_generation = pi_data_get(p + 96, 8);
	c->transition_count = pi_data_get(p + 104, 8);
	c->master_generation = pi_data_get(p + 112, 8);
	c->master_node = (int32)pi_data_get(p + 120, 4);
	c->pi_holders_bitmap = pi_data_get(p + 124, 4);
}

static void
pi_data_binding_encode(uint8 *p, const ClusterPageWalBindingV1 *b)
{
	const ClusterControlRootIdentity *id = &b->source.claim.identity;
	pi_data_put(p, id->system_identifier, 8);
	memcpy(p + 8, id->storage_uuid, 16);
	memcpy(p + 24, id->authority_uuid, 16);
	pi_data_put(p + 40, id->origin_thread_id, 2);
	pi_data_put(p + 44, id->origin_node_id, 4);
	pi_data_put(p + 48, id->thread_claim_created_at, 8);
	pi_data_put(p + 56, id->thread_claim_crc32c, 4);
	pi_data_put(p + 64, id->origin_owner_incarnation, 8);
	pi_data_put(p + 72, id->root_lineage_seq, 8);
	pi_data_put(p + 80, b->source.claim.database_incarnation, 8);
	pi_data_put(p + 88, b->source.claim.max_config_generation, 8);
	memcpy(p + 96, b->source.claim.claim_sha256, 32);
	pi_data_put(p + 128, b->source.timeline, 4);
	pi_data_put(p + 136, b->identity.system_identifier, 8);
	memcpy(p + 144, b->identity.storage_uuid, 16);
	pi_data_put(p + 160, b->identity.locator.spcOid, 4);
	pi_data_put(p + 164, b->identity.locator.dbOid, 4);
	pi_data_put(p + 168, b->identity.locator.relNumber, 4);
	pi_data_put(p + 172, b->identity.forknum, 4);
	pi_data_put(p + 176, b->identity.blockno, 4);
	memcpy(p + 184, b->version.segment_incarnation, 16);
	pi_data_put(p + 200, b->version.mutation_token, 8);
	pi_data_put(p + 208, b->record_start, 8);
	pi_data_put(p + 216, b->record_end, 8);
	pi_data_put(p + 224, b->record_crc, 4);
	p[228] = b->rmid;
	p[229] = b->info;
	pi_data_put(p + 230, b->flags, 2);
}

static bool
pi_data_binding_decode(const uint8 *p, ClusterPageWalBindingV1 *b)
{
	ClusterControlRootIdentity *id = &b->source.claim.identity;
	if (pi_data_get(p + 42, 2) || pi_data_get(p + 60, 4) || pi_data_get(p + 132, 4)
		|| pi_data_get(p + 180, 4))
		return false;
	id->system_identifier = pi_data_get(p, 8);
	memcpy(id->storage_uuid, p + 8, 16);
	memcpy(id->authority_uuid, p + 24, 16);
	id->origin_thread_id = pi_data_get(p + 40, 2);
	id->origin_node_id = (int32)pi_data_get(p + 44, 4);
	id->thread_claim_created_at = (int64)pi_data_get(p + 48, 8);
	id->thread_claim_crc32c = pi_data_get(p + 56, 4);
	id->origin_owner_incarnation = pi_data_get(p + 64, 8);
	id->root_lineage_seq = pi_data_get(p + 72, 8);
	b->source.claim.database_incarnation = pi_data_get(p + 80, 8);
	b->source.claim.max_config_generation = pi_data_get(p + 88, 8);
	memcpy(b->source.claim.claim_sha256, p + 96, 32);
	b->source.timeline = pi_data_get(p + 128, 4);
	b->identity.system_identifier = pi_data_get(p + 136, 8);
	memcpy(b->identity.storage_uuid, p + 144, 16);
	b->identity.locator.spcOid = pi_data_get(p + 160, 4);
	b->identity.locator.dbOid = pi_data_get(p + 164, 4);
	b->identity.locator.relNumber = pi_data_get(p + 168, 4);
	b->identity.forknum = pi_data_get(p + 172, 4);
	b->identity.blockno = pi_data_get(p + 176, 4);
	memcpy(b->version.segment_incarnation, p + 184, 16);
	b->version.mutation_token = pi_data_get(p + 200, 8);
	b->record_start = pi_data_get(p + 208, 8);
	b->record_end = pi_data_get(p + 216, 8);
	b->record_crc = pi_data_get(p + 224, 4);
	b->rmid = p[228];
	b->info = p[229];
	b->flags = pi_data_get(p + 230, 2);
	return true;
}

#endif
