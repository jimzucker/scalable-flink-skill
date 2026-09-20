#!/usr/bin/env python3
"""The prose and the code state the same numbers, or this fails.

Two defects found by clean-room run 31 were both the shipped text disagreeing
with the shipped behaviour: SKILL.md quoted a 10% spread ceiling the harness
had refused at 20% since run 9, and pipeline.example.json carried a fan-out of
8 directly under a comment deriving 5. Neither cost that run anything -- it
read the code. A first reader follows the prose, so the agreement is checked
here rather than written down and hoped for.

Run it directly, or as part of CI.
"""

import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)


def read(*parts):
    with open(os.path.join(*parts), encoding="utf-8") as f:
        return f.read()


def check_spread_ceiling(fail):
    """Every place the prose names the spread ceiling names the code's value."""
    import lib
    pct = f"{lib.T['spreadCeil']:.0%}"
    skill = read(ROOT, "SKILL.md")
    for phrase in (f"spread exceeds {pct}", f"spread >{pct}"):
        if phrase not in skill:
            fail(f"SKILL.md does not say {phrase!r}; the harness refuses at {pct}")
    # and no stale figure survives next to the word "spread"
    for m in re.finditer(r"spread (?:exceeds |>)(\d+)%", skill):
        if m.group(1) + "%" != pct:
            fail(f"SKILL.md states a spread ceiling of {m.group(1)}%, code says {pct}")
    return f"spread ceiling {pct} in SKILL.md and lib.py"


def check_tiny_ratio_band(fail):
    """The tiny proof's band, as stated, is the band the code applies.

    The code scales it by the ideal ratio -- `tinyRatioLo * ideal / 2` -- so a
    tiny proof run at one and four cores refuses outside 3x-5x, not 1.5x-2.5x.
    The prose quoted the doubling's numbers as if they were absolute, which
    reads as "superlinear is refused" when 25% above ideal is not.
    """
    import lib
    band = f"{lib.T['tinyRatioLo'] / 2:.2f}\u00d7\u2013{lib.T['tinyRatioHi'] / 2:.2f}\u00d7"
    if band not in read(ROOT, "SKILL.md"):
        fail(f"SKILL.md does not state the tiny-proof band as {band} of the ideal")
    return f"tiny-proof band {band} of ideal"


def check_example_comments(fail):
    """A `_field` comment that derives a number and the `field` beside it agree.

    The example is the file README.md tells you to copy. A comment there that
    argues against the value under it is worse than no comment: it is a trap
    with an explanation attached.
    """
    path = os.path.join(HERE, "pipeline.example.json")
    cfg = json.loads(read(path))
    checked = 0
    for key, note in list(cfg.items()):
        if not key.startswith("_") or not isinstance(note, str):
            continue
        field = key[1:]
        if field not in cfg or not isinstance(cfg[field], (int, float)):
            continue
        # "... is 1 + 4 = 5, not 8." -- the comment's own conclusion
        m = re.search(r"=\s*(\d+),\s*not\s+(\d+)", note)
        if not m:
            continue
        derived, rejected = int(m.group(1)), int(m.group(2))
        checked += 1
        if cfg[field] != derived:
            fail(f"pipeline.example.json: {field} is {cfg[field]}, but {key} derives {derived}"
                 + (f" and rejects {rejected}" if cfg[field] == rejected else ""))
    return f"{checked} derived value(s) in pipeline.example.json"


def main():
    problems = []
    lines = []
    for check in (check_spread_ceiling, check_tiny_ratio_band, check_example_comments):
        lines.append(check(problems.append))
    for p in problems:
        print(f"doccheck: {p}")
    if problems:
        print(f"doccheck: {len(problems)} disagreement(s) between the prose and the code")
        return 1
    print("doccheck: " + "; ".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
