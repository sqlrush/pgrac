#!/bin/bash
#-------------------------------------------------------------------------
#
# check-format.sh
#    CI helper: enforce clang-format compliance on pgrac cluster sources.
#
#    Run from repository root.  Iterates pgrac-original cluster paths
#    (NOT PG-original files) and verifies each .c/.h matches the
#    .clang-format configuration at the repo root.
#
# IDENTIFICATION
#    scripts/ci/check-format.sh
#
# Author: SqlRush <sqlrush@gmail.com>
#
# Portions Copyright (c) 2026, pgrac contributors
#
# NOTES
#    Exit codes:
#      0  - all files formatted correctly
#      1  - one or more files need formatting
#      2  - clang-format binary not found
#
#    To fix locally:  clang-format -i <file>
#
#-------------------------------------------------------------------------

set -e
set -o pipefail

# Locate clang-format.  Prefer clang-format 18 because GitHub Actions
# ubuntu-24.04 installs 18.1.x, and later releases differ on comment
# alignment in several long-lived cluster files.
if [ -z "${CLANG_FORMAT:-}" ]; then
    for candidate in \
        clang-format-18 \
        /opt/homebrew/opt/llvm@18/bin/clang-format \
        clang-format \
        clang-format-17 \
        clang-format-16 \
        clang-format-15 \
        clang-format-14; do
        if command -v "$candidate" >/dev/null 2>&1; then
            CLANG_FORMAT="$candidate"
            break
        fi
    done
fi

if ! command -v "$CLANG_FORMAT" >/dev/null 2>&1; then
    echo "::error::clang-format not found in PATH; install via apt/brew"
    exit 2
fi

echo "# check-format.sh: using $($CLANG_FORMAT --version | head -1)"

# Cluster paths to check (pgrac-original files only; PG upstream is
# formatted with pgindent, not clang-format).
PATHS=(
    "src/backend/cluster"
    "src/include/cluster"
    "src/test/cluster_unit"
)

errors=0
checked=0

for dir in "${PATHS[@]}"; do
    if [ ! -d "$dir" ]; then
        continue
    fi
    while IFS= read -r f; do
        checked=$((checked + 1))
        # Diff actual file vs clang-format output
        if ! diff -u "$f" <("$CLANG_FORMAT" "$f") > /tmp/check-format-diff.txt 2>&1; then
            echo "::error file=$f::clang-format violation (run: $CLANG_FORMAT -i $f)"
            head -20 /tmp/check-format-diff.txt | sed 's/^/    /'
            errors=$((errors + 1))
        fi
    done < <(find "$dir" -type f \( -name '*.c' -o -name '*.h' \))
done

rm -f /tmp/check-format-diff.txt

echo ""
echo "# check-format.sh: summary"
echo "#   files checked: $checked"
echo "#   violations:    $errors"

if [ "$errors" -gt 0 ]; then
    echo "# FAILED: $errors file(s) need clang-format -i"
    exit 1
fi

echo "# OK"
exit 0
