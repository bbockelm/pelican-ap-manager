#!/usr/bin/env bash
#
# Fail if any GitHub Actions step is referenced by anything other than a full
# 40-character commit SHA. A tag like @v4 is whatever its owner last pointed it
# at, and one hand-edited workflow would reintroduce that for the whole
# repository.
#
# A pinned reference must also carry a trailing `# vX.Y.Z` comment: that is how
# Dependabot knows which version a SHA represents, so without it the pin stops
# being updatable.

set -euo pipefail

cd "$(dirname "$0")/.."

shopt -s nullglob
workflows=(.github/workflows/*.yml .github/workflows/*.yaml)
if [ ${#workflows[@]} -eq 0 ]; then
  echo "check-action-pins: no workflows found under .github/workflows" >&2
  exit 1
fi

status=0
pinned=0

for file in "${workflows[@]}"; do
  lineno=0
  while IFS= read -r line; do
    lineno=$((lineno + 1))

    # Only `uses:` steps reference an action.
    [[ "$line" =~ ^[[:space:]]*uses:[[:space:]]*(.+)$ ]] || continue
    ref="${BASH_REMATCH[1]}"

    # A local action (./path) is this repository's own code and has no SHA to
    # pin -- it is already whatever this commit contains.
    if [[ "$ref" == ./* ]]; then
      continue
    fi

    # A full commit SHA, then a comment naming the version it came from.
    if [[ "$ref" =~ ^[^@[:space:]]+@[0-9a-f]{40}[[:space:]]+#[[:space:]]*v?[0-9] ]]; then
      pinned=$((pinned + 1))
      continue
    fi

    if [[ "$ref" =~ ^[^@[:space:]]+@[0-9a-f]{40}([[:space:]]|$) ]]; then
      echo "$file:$lineno: pinned but unlabelled: $ref" >&2
      echo "    Add a trailing '# vX.Y.Z' comment; Dependabot needs it to know what to bump." >&2
      status=1
      continue
    fi

    echo "$file:$lineno: not pinned to a commit SHA: $ref" >&2
    echo "    Resolve the tag and pin it, e.g.:" >&2
    echo "      gh api repos/OWNER/REPO/commits/vX.Y.Z --jq .sha" >&2
    echo "      uses: OWNER/REPO@<sha> # vX.Y.Z" >&2
    status=1
  done < "$file"
done

if [ "$status" -eq 0 ]; then
  echo "check-action-pins: $pinned action reference(s), all pinned to commit SHAs"
fi

exit "$status"
