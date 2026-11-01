/* Author: SqlRush <sqlrush@gmail.com> */
/* Compile the complete product translation unit in both assertion modes. */
#include "postgres.h"
#ifdef FIXTURE_NO_ASSERTS
#undef USE_ASSERT_CHECKING
#undef Assert
#define Assert(condition) ((void)true)
#undef AssertMacro
#define AssertMacro(condition) ((void)true)
#undef PG_USED_FOR_ASSERTS_ONLY
#define PG_USED_FOR_ASSERTS_ONLY pg_attribute_unused()
#endif
#include "../../backend/access/heap/visibilitymap.c"
