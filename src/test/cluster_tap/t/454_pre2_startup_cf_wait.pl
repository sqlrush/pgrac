#!/usr/bin/env perl
# Author: SqlRush <sqlrush@gmail.com>
# PGRAC: native writer selection/binding must continue the original CF owner.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl", "$FindBin::RealBin/../pre2";
use StartupCfScenario;
startup_cf_scenario('writer');
