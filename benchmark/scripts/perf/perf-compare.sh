#!/usr/bin/env bash
# Compare two perf-iter.sh runs.
#
# Usage:
#   perf-compare.sh <tag-A> <tag-B>
#
# Reads $PERF_DIR/<tag>.folded for each, emits a differential flame graph
# (red = hotter in B than in A, blue = hotter in A) plus a textual ranking
# of the biggest sample-count deltas at the leaf-function level.

set -euo pipefail

A="${1:?usage: $0 <tag-A> <tag-B>}"
B="${2:?usage: $0 <tag-A> <tag-B>}"
PERF_DIR="${PERF_DIR:-/ephemeral/perf}"

A_FOLDED="$PERF_DIR/${A}.folded"
B_FOLDED="$PERF_DIR/${B}.folded"
[ -f "$A_FOLDED" ] || { echo "missing $A_FOLDED" >&2; exit 1; }
[ -f "$B_FOLDED" ] || { echo "missing $B_FOLDED" >&2; exit 1; }

DIFF_SVG="$PERF_DIR/diff__${A}__vs__${B}.svg"
DELTA_TXT="$PERF_DIR/diff__${A}__vs__${B}.delta.txt"

# Differential flame: stacks coloured by sample delta B-A.
inferno-diff-folded "$A_FOLDED" "$B_FOLDED" \
    | inferno-flamegraph --title "diff: $A (blue=less) vs $B (red=more)" \
    > "$DIFF_SVG"

# Leaf-function delta ranking. Each folded line is "stack;...;leaf <count>".
# Sum counts per leaf, then diff.
python3 - "$A_FOLDED" "$B_FOLDED" "$DELTA_TXT" <<'PY'
import sys, collections
a_path, b_path, out_path = sys.argv[1:]
def leaves(path):
    c = collections.Counter()
    for line in open(path):
        line = line.rstrip("\n")
        if not line: continue
        stack, _, count = line.rpartition(" ")
        try: n = int(count)
        except ValueError: continue
        leaf = stack.rsplit(";", 1)[-1]
        c[leaf] += n
    return c
a = leaves(a_path)
b = leaves(b_path)
keys = set(a) | set(b)
rows = sorted(((b[k]-a[k], a[k], b[k], k) for k in keys), key=lambda r: abs(r[0]), reverse=True)
with open(out_path, "w") as f:
    f.write(f"{'delta':>10}  {'A':>10}  {'B':>10}  symbol\n")
    f.write("-" * 100 + "\n")
    for d, av, bv, k in rows[:60]:
        sign = "+" if d >= 0 else ""
        f.write(f"{sign}{d:>9}  {av:>10}  {bv:>10}  {k}\n")
print(out_path)
PY

echo "diff_svg=$DIFF_SVG"
echo "delta_txt=$DELTA_TXT"
echo
echo "=== top 30 leaf-function deltas (B-A; +ve = hotter in B) ==="
head -32 "$DELTA_TXT"
