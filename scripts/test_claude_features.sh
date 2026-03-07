#!/bin/bash
# Test claude -p features: output-format, json-schema, system-prompt, agents
# Uses Haiku to minimize token spend.

echo "=== Test 1: --output-format json ==="
echo "(Check if we get token counts and metadata)"
claude -p --model haiku --output-format json "Say OK" 2>/dev/null | head -c 500
echo ""
echo ""

echo "=== Test 2: --system-prompt flag ==="
echo "(Check if system prompt can be passed inline)"
claude -p --model haiku --system-prompt "You are a translator. Always respond in French." "Say hello" 2>/dev/null | head -c 200
echo ""
echo ""

echo "=== Test 3: stdin piping ==="
echo "(Check if user message can be piped via stdin)"
echo "Say OK" | claude -p --model haiku 2>/dev/null | head -c 200
echo ""
echo ""

echo "=== Test 4: --tools empty ==="
echo "(Check if tools can be disabled)"
claude -p --model haiku --tools "" "Say OK" 2>/dev/null | head -c 200
echo ""
echo ""

echo "=== Test 5: --json-schema ==="
echo "(Check if structured output works)"
claude -p --model haiku --json-schema '{"type":"object","properties":{"answer":{"type":"string"}},"required":["answer"]}' "What is 2+2? Respond as JSON." 2>/dev/null | head -c 500
echo ""
echo ""

echo "=== Test 6: --agents flag ==="
echo "(Check what agents are available and how they work)"
claude agents 2>/dev/null | head -c 1000
echo ""
echo ""

echo "=== Test 7: --no-session-persistence ==="
echo "(For pipeline use - avoid cluttering session history)"
claude -p --model haiku --no-session-persistence "Say OK" 2>/dev/null | head -c 200
echo ""
