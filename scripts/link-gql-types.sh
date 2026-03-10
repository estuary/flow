#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../packages/gql-types"

SDL=../../crates/flow-client/control-plane-api.graphql
if [[ ! -f "$SDL" ]]; then
  echo "SDL not found at $SDL"
  echo "Run: cargo build -p flow-client --features generate"
  exit 1
fi

cp "$SDL" schema/
rm -rf dist/
npm install --silent
npm run build
npm link

echo ""
echo "Linked @estuarydev/gql-types locally."
echo "Run 'npm link @estuarydev/gql-types' in your frontend repo to use it."
