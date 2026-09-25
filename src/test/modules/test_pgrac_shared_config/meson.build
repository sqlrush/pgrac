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
  'tap': {'tests': ['t/001_policy.pl', 't/002_early_startup.pl', 't/003_startup_apply.pl',
                    't/004_bootstrap_bindings.pl', 't/005_native_tool_guards.pl',
                    't/006_native_entry_guard.pl', 't/007_control_image.pl',
                    't/008_recovery_capacity.pl', 't/009_bootstrap_native.pl']},
}
