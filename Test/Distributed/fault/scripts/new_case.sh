#!/usr/bin/env bash
# new_case.sh — scaffold a new fault-case (test + harness + perf gate)
# from the templates in this directory.
#
# Usage:
#   Test/Distributed/fault/scripts/new_case.sh <slug> [SuiteName]
#
#   <slug>       short-kebab-case slug; matches the design-doc filename
#                in tasks/distributed-index-scale-2k/design-docs/
#                ft-fault-cases/<slug>.md and the branch fault/<slug>.
#   SuiteName    Boost.Test suite name. Default: Pascal-case of slug
#                with non-alnum stripped (e.g. tikv-grpc-stub-channel-
#                broken -> TikvGrpcStubChannelBrokenTest).
#
# Creates (only if absent):
#   Test/Distributed/fault/<slug>_test.cpp
#   Test/Distributed/fault/<slug>_test.sh    (chmod +x)
#
# Stamps a Closure checklist into
#   <workspace-repo>/tasks/distributed-index-scale-2k/design-docs/
#       ft-fault-cases/<slug>.md
# (set WORKSPACE_REPO env to override the default $HOME/workspace/openclaw).
#
# The perf gate is a single repo-level runner
#   evaluation/distributed/run_perf_validation.sh <slug> --scale 1m
# per the locked perf-validation protocol; we do NOT scaffold a per-case
# *_perf.sh wrapper.

set -euo pipefail

SLUG="${1:-}"
SUITE="${2:-}"

if [[ -z "$SLUG" ]]; then
    echo "Usage: $0 <slug> [SuiteName]" >&2
    exit 2
fi
if [[ ! "$SLUG" =~ ^[a-z][a-z0-9-]*$ ]]; then
    echo "ERROR: slug must be lowercase kebab-case, got '$SLUG'" >&2
    exit 2
fi

if [[ -z "$SUITE" ]]; then
    SUITE="$(echo "$SLUG" | awk -F- '{ for(i=1;i<=NF;i++) printf toupper(substr($i,1,1)) substr($i,2); }')Test"
fi

DIR="$(cd "$(dirname "$0")/.." && pwd)"
TPL_TEST="${DIR}/_template_test.cpp.tmpl"
TPL_HARN="${DIR}/_template_test.sh.tmpl"

for t in "$TPL_TEST" "$TPL_HARN"; do
    [[ -f "$t" ]] || { echo "ERROR: template missing: $t" >&2; exit 3; }
done

OUT_TEST="${DIR}/${SLUG}_test.cpp"
OUT_HARN="${DIR}/${SLUG}_test.sh"

SLUG_UPPER="$(echo "$SLUG" | tr 'a-z-' 'A-Z_')"

render() {
    local in="$1" out="$2"
    if [[ -e "$out" ]]; then
        echo "SKIP (exists): $out" >&2
        return 0
    fi
    sed -e "s|__SLUG__|${SLUG}|g" -e "s|__SLUG_UPPER__|${SLUG_UPPER}|g" -e "s|__SUITE__|${SUITE}|g" "$in" > "$out"
}

render "$TPL_TEST" "$OUT_TEST"
render "$TPL_HARN" "$OUT_HARN"
chmod +x "$OUT_HARN" 2>/dev/null || true

# Stamp the Closure checklist into the case design doc, if it exists.
# Path is derived relative to the SPTAG worktree: ../../openclaw/...
# but we accept an override via WORKSPACE_REPO env.
WS_REPO="${WORKSPACE_REPO:-$HOME/workspace/openclaw}"
CASE_MD="${WS_REPO}/tasks/distributed-index-scale-2k/design-docs/ft-fault-cases/${SLUG}.md"
stamp_checklist() {
    local md="$1"
    [[ -f "$md" ]] || { echo "(skip) case md not found: $md"; return 0; }
    if grep -q '^## Closure checklist' "$md"; then
        echo "(skip) Closure checklist already present in $md"
        return 0
    fi
    cat >> "$md" <<EOF

## Closure checklist

> Per \`notes/perf-validation-protocol.md\` (locked 2026-04-29 11:50 UTC).
> The case **stays \`implementing\`** until **all** of these are GREEN.

- [ ] Impl committed on \`fault/${SLUG}\`; commit hash recorded.
- [ ] Repro test (\`Test/Distributed/fault/${SLUG}_test.sh\`) exits 0
      on sdr; the case's *Expected behaviour* is observed under fault.
- [ ] Reference cached at \`results/_reference/1M/metrics.json\`
      (refresh via \`evaluation/distributed/refresh-reference.sh --scale 1m\`).
- [ ] **1M perf-validation run** via
      \`evaluation/distributed/run_perf_validation.sh ${SLUG} --scale 1m\`
      produces \`results/${SLUG}/1M/{reference,baseline,fault}/metrics.json\`
      and \`closure.json\` with **verdict=PASS**:
      - p50/p95/p99 (baseline vs reference) ≤ +5 %
      - query_qps, insert_qps (baseline vs reference) ≥ −5 %
      - |Δrecall@K| (baseline vs reference) ≤ 0.005
      - |Δrecall@K| (fault vs baseline) ≤ 0.01
- [ ] (preferred when time allows) Same at **10M** under \`results/${SLUG}/10M/\`.
- [ ] Closure note appended here with: branch + commit hashes,
      paths to the three JSON outputs, the four numeric comparisons
      inline, and an explicit \`status: repro-green\` line.

**Hard rules:** never run on \`spfresh-dev-amd-005\`; never 1B on sdr.
EOF
    echo "stamped Closure checklist into $md"
}
stamp_checklist "$CASE_MD"

cat <<EOF

Scaffolded fault case: ${SLUG}
  test:    ${OUT_TEST}
  harness: ${OUT_HARN}
  suite:   ${SUITE}

Next steps:
  1. Implement recovery contract on branch fault/${SLUG}.
     Gate the impl behind a TestHook env var: SPTAG_FAULT_${SLUG^^}=1.
     Off by default; baseline runs see the unmodified hot path.
  2. Fill in the RecoveryContract test case + any sub-cases.
  3. Build:  cd build/${SLUG} && make -j SPTAGTest  (~25 s incremental).
  4. Run unit:  ./Test/Distributed/fault/${SLUG}_test.sh
  5. Run perf-validation (REQUIRED before closure):
        evaluation/distributed/run_perf_validation.sh ${SLUG} --scale 1m
     -> writes results/${SLUG}/1M/{reference,baseline,fault,closure.json}
        in the workspace repo. Non-zero exit blocks closure.
  6. Closure note (with the four numeric comparisons inline) only
     after BOTH unit-green AND perf verdict=PASS.
EOF
