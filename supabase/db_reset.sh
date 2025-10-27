#!/bin/sh
set -e

# Change to supabase directory for supabase CLI to find config
cd "$(dirname "$0")"
supabase db reset
