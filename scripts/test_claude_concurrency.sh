#!/bin/bash
# Test concurrent claude -p calls to determine if parallelism works.
# Uses Haiku model to minimize token spend.
# Run from Git Bash or WSL.

echo "=== Claude -p Concurrency Test ==="
echo "Testing 5 concurrent calls with --model haiku"
echo "Start: $(date +%T)"

for i in 1 2 3 4 5; do
  (
    start=$(date +%s%N)
    result=$(claude -p --model haiku "Respond with exactly: OK_$i" 2>/dev/null)
    end=$(date +%s%N)
    elapsed_ms=$(( (end - start) / 1000000 ))
    echo "  Call $i: ${elapsed_ms}ms - response: $(echo $result | head -c 50)"
  ) &
done

wait
echo "End: $(date +%T)"
echo ""
echo "=== Sequential baseline (2 calls) ==="
echo "Start: $(date +%T)"

for i in 1 2; do
  start=$(date +%s%N)
  result=$(claude -p --model haiku "Respond with exactly: SEQ_$i" 2>/dev/null)
  end=$(date +%s%N)
  elapsed_ms=$(( (end - start) / 1000000 ))
  echo "  Call $i: ${elapsed_ms}ms - response: $(echo $result | head -c 50)"
done

echo "End: $(date +%T)"
echo ""
echo "If parallel calls complete in roughly the same wall time as a single call,"
echo "concurrency works. If they serialize (each takes as long as sequential), it doesn't."
