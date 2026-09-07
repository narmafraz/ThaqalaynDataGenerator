"""One-shot runner: N8 chunk-alignment merge + integrity sweep (detached-safe)."""
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
os.environ.setdefault("SOURCE_DATA_DIR", "../ThaqalaynDataSources/")
os.environ.setdefault("DESTINATION_DIR", "../ThaqalaynData/")
sys.path.insert(0, ROOT)

from app.ai_content_merger import merge_chunk_alignment  # noqa: E402

merge_chunk_alignment()
print("MERGE DONE", flush=True)
r = subprocess.run([sys.executable, "scripts/verify_alignment_merge.py"])
print("SWEEP EXIT", r.returncode, flush=True)
