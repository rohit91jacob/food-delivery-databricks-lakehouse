#!/usr/bin/env bash
# Open an issue with this title, or comment on the open one that already has it (one issue per problem).
# Usage: scripts/gh_issue.sh "<title>" "<markdown body>"   (needs GH_TOKEN with issues: write)
set -euo pipefail
title="$1"
body="$2"
number=$(gh issue list --repo "$GITHUB_REPOSITORY" --state open --search "in:title \"$title\"" \
  --json number,title --jq "map(select(.title == \"$title\")) | .[0].number // empty")
if [ -n "$number" ]; then
  gh issue comment "$number" --repo "$GITHUB_REPOSITORY" --body "$body"
  echo "commented on #$number"
else
  gh issue create --repo "$GITHUB_REPOSITORY" --title "$title" --body "$body"
fi
