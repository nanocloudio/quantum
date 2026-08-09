#!/usr/bin/env bash
# Shadow-checkout guard (standards/test-tracking.md §7): tests/ is
# shadow-tracked (.git-shadow/), so a runner holding only the primary
# repo has zero files there — the integration smokes and the mqtt
# suite would be silently absent while CI still reads green.
# Hard-fail instead. Wired as `[ci.test] scripts` (CI phase 3.5).
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -z "$(ls -A tests 2>/dev/null)" ]; then
  echo "ci-shadow-guard: tests/ is empty or absent — the shadow-tracked tree" >&2
  echo "is not materialised on this machine (standards/test-tracking.md §7)." >&2
  exit 1
fi
