#!/usr/bin/env python3
"""Fix unescaped double quotes inside CJK text in JSON response files."""
import json
import re
import sys

def is_cjk(ch):
    cp = ord(ch)
    return (0x4E00 <= cp <= 0x9FFF or 0x3400 <= cp <= 0x4DBF or
            0xF900 <= cp <= 0xFAFF)

def is_cjk_punct(ch):
    return ch in "\u3002\uFF01\uFF1F\uFF0C\u3001\uFF1B\uFF1A\uFF09\u3011\u300B\u300D\u300F\uFF08\u3010\u300A\u300C\u300E"

def fix_cjk_quotes(text):
    """Replace unescaped ASCII double quotes inside CJK text with Unicode curly quotes."""
    output = list(text)
    i = 0
    n = len(text)
    fixes = 0

    while i < n:
        if text[i] == '"':
            # Start of a JSON string — scan to find the real end
            i += 1
            while i < n:
                ch = text[i]
                if ch == "\\" and i + 1 < n:
                    i += 2  # skip escaped char
                    continue
                if ch == '"':
                    prev = text[i - 1] if i > 0 else ""
                    nxt = text[i + 1] if i + 1 < n else ""

                    prev_cjk = is_cjk(prev) or is_cjk_punct(prev)
                    next_cjk = is_cjk(nxt) or is_cjk_punct(nxt)
                    next_json = nxt in ':,}] \n\r\t'

                    if prev_cjk and next_cjk:
                        output[i] = "\u201C"
                        fixes += 1
                        i += 1
                        continue
                    elif prev_cjk and not next_json:
                        output[i] = "\u201C"
                        fixes += 1
                        i += 1
                        continue
                    elif next_cjk and not (i > 0 and text[i - 1] in ':{[,'):
                        output[i] = "\u201C"
                        fixes += 1
                        i += 1
                        continue
                    else:
                        # Real closing quote
                        i += 1
                        break
                i += 1
        else:
            i += 1

    return "".join(output), fixes


def main():
    if len(sys.argv) < 2:
        print("Usage: python fix_json_quotes.py <file.json>")
        sys.exit(1)

    filepath = sys.argv[1]
    sys.stdout.reconfigure(encoding="utf-8")

    with open(filepath, "r", encoding="utf-8") as f:
        text = f.read()

    # Check if already valid
    try:
        json.loads(text)
        print(f"{filepath}: already valid JSON")
        return
    except json.JSONDecodeError:
        pass

    fixed, num_fixes = fix_cjk_quotes(text)
    # Also fix trailing commas
    fixed = re.sub(r",(\s*[}\]])", r"\1", fixed)

    print(f"Fixed {num_fixes} unescaped quotes")

    try:
        parsed = json.loads(fixed)
        print("JSON VALID!")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(parsed, f, ensure_ascii=False, indent=2)
        print(f"Saved: {filepath}")
    except json.JSONDecodeError as e:
        print(f"Still invalid at pos {e.pos}: {e}")
        print(repr(fixed[max(0, e.pos - 80):e.pos + 80]))
        sys.exit(1)


if __name__ == "__main__":
    main()
