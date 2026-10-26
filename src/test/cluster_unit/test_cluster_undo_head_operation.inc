/* Independent WAL fixture: 32B original prefix followed by 48B identity.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#include "cluster/cluster_undo_record.h"

typedef struct TestUndoHeadOperation {
	xl_undo_tt_slot_set_head head;
	UndoOperationTrailer operation;
} TestUndoHeadOperation;

static TestUndoHeadOperation
test_head_operation(void)
{
	TestUndoHeadOperation value = { 0 };

	value.head.segment_id = 1;
	value.head.slot_offset = 7;
	value.head.wrap = 5;
	value.head.xid = 778;
	value.head.instance = 1;
	value.head._pad[0] = 1;
	value.head.first_undo_block = uba_encode(1, 9, 7, 0);
	value.operation.version = 1;
	value.operation.length = 48;
	value.operation.effect_state = CLUSTER_SAVEPOINT_EFFECT_LIVE;
	value.operation.canonical_top_fxid = (UINT64_C(5) << 32) | 778;
	value.operation.operation_fxid = (UINT64_C(5) << 32) | 779;
	value.operation.operation_sequence = 31;
	value.operation.canonical_segment_generation = 4;
	value.operation.command_id = 7;
	value.operation.subid = 2;
	return value;
}
