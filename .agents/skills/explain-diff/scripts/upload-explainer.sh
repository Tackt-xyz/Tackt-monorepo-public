#!/usr/bin/env bash
# upload-explainer.sh <file.html> ["description"]
# Uploads as a secret (unlisted, link-only) gist, prints a live-rendered preview URL.
set -euo pipefail

FILE="${1:?usage: upload-explainer.sh <file.html> [description]}"
DESC="${2:-PR explainer}"

GIST_URL=$(gh gist create "$FILE" -d "$DESC")
GIST_ID=$(basename "$GIST_URL")
RAW_URL=$(gh api "gists/$GIST_ID" --jq '.files | to_entries[0].value.raw_url')

echo "Gist:    $GIST_URL"
echo "Raw:     $RAW_URL"
echo "Preview: https://htmlpreview.github.io/?$RAW_URL"
