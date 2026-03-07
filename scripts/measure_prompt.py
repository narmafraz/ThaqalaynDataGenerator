"""Measure system prompt size and command-line budget."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))
os.environ.setdefault("SOURCE_DATA_DIR", "../ThaqalaynDataSources/")

from app.ai_pipeline import build_system_prompt
from app.pipeline_cli.verse_processor import COMPACT_WORD_INSTRUCTIONS

sp = build_system_prompt(few_shot_examples={"examples": []})
sp += "\n" + COMPACT_WORD_INSTRUCTIONS

print(f"System prompt chars: {len(sp):,}")
print(f"System prompt bytes (utf-8): {len(sp.encode('utf-8')):,}")

# Base command without system prompt or json-schema
base_cmd_len = 150  # conservative estimate for flags
remaining = 32767 - base_cmd_len - len(sp)
print(f"Base cmd overhead (est): ~{base_cmd_len} chars")
print(f"Remaining for --json-schema: {remaining:,} chars")
print()

# Check if asyncio subprocess has a different limit
# Windows CreateProcess limit is 32,767 chars
# But Python subprocess passes via CreateProcessW which has same limit
print("Windows CreateProcess limit: 32,767 chars")
print(f"System prompt uses: {len(sp)/32767*100:.1f}% of budget")
