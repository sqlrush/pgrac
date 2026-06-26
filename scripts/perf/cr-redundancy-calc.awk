# scripts/perf/cr-redundancy-calc.awk
#	  spec-5.50 D3 — CR construction redundancy engine (driven by
#	  cr-redundancy-calc.sh; invoke with `awk -F, -f`).
#
#	  Reads a CSV with a header row, locates the columns by NAME (robust to
#	  extra columns the profile harness adds), and appends three columns:
#	      redundancy         = construct_delta / distinct_cr_keys
#	      dedup_savings_pct  = (1 - 1/redundancy) * 100
#	      verdict            = measured | inconclusive
#
#	  P0 denominator contract (spec-5.50 r2 P0 + errata 1): the divisor is
#	  distinct_cr_keys (the exact 5-field CR key), never distinct_blocks.  A row
#	  is inconclusive — and the redundancy / savings fields read INCONCLUSIVE,
#	  never a number — when:
#	      key_stable != 1        (harness did not prove same read_scn +
#	                              stable base_page_lsn => denominator suspect), or
#	      distinct_cr_keys <= 0  (no keys touched, or a constructs>0/keys=0
#	                              contradiction => cannot divide).
#	  Inconclusive rows MUST NOT feed the value gate (spec-5.50 §3.4).
#
#	  Required input columns: distinct_cr_keys, key_stable, construct_delta.
#	  A missing required column is a hard error (exit 4), never a silent guess.
#
# Author: SqlRush <sqlrush@gmail.com>
# Portions Copyright (c) 2026, pgrac contributors
#
# Spec: spec-5.50-cr-read-path-profile.md (FROZEN v1.0 + errata 1)

BEGIN {
	col_keys = 0
	col_stable = 0
	col_construct = 0
}

# Header row: map required column names to 1-based indices, then re-emit the
# header with the three appended column names.
NR == 1 {
	for (i = 1; i <= NF; i++) {
		name = $i
		gsub(/^[ \t\r]+|[ \t\r]+$/, "", name)
		if (name == "distinct_cr_keys") col_keys = i
		else if (name == "key_stable")  col_stable = i
		else if (name == "construct_delta") col_construct = i
	}
	if (col_keys == 0 || col_stable == 0 || col_construct == 0) {
		printf("cr-redundancy-calc: missing required column(s); need distinct_cr_keys, key_stable, construct_delta\n") > "/dev/stderr"
		exit 4
	}
	print $0 ",redundancy,dedup_savings_pct,verdict"
	next
}

# Skip blank lines so trailing newlines in the CSV do not emit junk rows.
/^[ \t\r]*$/ { next }

# Data row.
{
	keys = $col_keys + 0
	stable = $col_stable + 0
	construct = $col_construct + 0

	if (stable != 1 || keys <= 0) {
		# Proof not established (or no keys / contradiction): never invent a
		# number the value gate could trust.
		print $0 ",INCONCLUSIVE,INCONCLUSIVE,inconclusive"
		next
	}

	redundancy = construct / keys
	if (redundancy > 0)
		savings = (1.0 - 1.0 / redundancy) * 100.0
	else
		savings = 0.0

	printf("%s,%.2f,%.2f,measured\n", $0, redundancy, savings)
}
