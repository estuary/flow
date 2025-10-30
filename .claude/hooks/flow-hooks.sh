#!/usr/bin/env bash

set -euo pipefail

# Read the hook input from stdin
input=$(cat)

# Extract tool_name and command from the JSON input
tool_name=$(echo "$input" | jq -r '.tool_name')
command=$(echo "$input" | jq -r '.tool_input.command // empty')

# Prevent expensive "bazelisk clean" operations.
if [[ "$tool_name" == "Bash" ]] && [[ "$command" =~ bazelisk[[:space:]]+clean ]]; then
    # Reject the tool use with a helpful message
    cat <<EOF
{
  "hookSpecificOutput": {
    "hookEventName": "PreToolUse",
    "permissionDecision": "deny",
    "permissionDecisionReason": "Full bazel rebuilds are very expensive and rarely needed. Ask the user to clean, but only when it's otherwise not possible to continue."
  }
}
EOF
    exit 0
fi

# Allow all other tool uses
cat <<EOF
{
  "hookSpecificOutput": {
    "hookEventName": "PreToolUse",
    "permissionDecision": "allow"
  }
}
EOF
