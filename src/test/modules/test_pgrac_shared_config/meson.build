# PGRAC native configuration policy tests.
# Author: SqlRush <sqlrush@gmail.com>
test_pgrac_shared_config = shared_module('test_pgrac_shared_config',
  files('test_pgrac_shared_config.c'),
  kwargs: pg_test_mod_args,
)
test_install_libs += test_pgrac_shared_config
test_install_data += files(
  'test_pgrac_shared_config.control', 'test_pgrac_shared_config--1.0.sql',
)
tests += {
  'name': 'test_pgrac_shared_config',
  'sd': meson.current_source_dir(), 'bd': meson.current_build_dir(),
  'tap': {'tests': ['t/001_policy.pl', 't/002_early_startup.pl', 't/003_startup_apply.pl']},
}
