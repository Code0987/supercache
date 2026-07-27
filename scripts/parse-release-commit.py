#!/usr/bin/env python3
"""Parse SuperCache release metadata from a git commit message and/or PR body.

Only one pattern is allowed (own line, exact):

  release: v1.2.3

- Keyword is `release:` (case-insensitive)
- Version must be `v` + MAJOR.MINOR.PATCH
- Optional notes = body after that line until a `---` ruler, a `##` heading,
  or git trailers
- Markdown fenced code blocks (``` ... ```) are ignored so PR template
  examples do not trigger a release
- No marker → should_release=false (not an error)

Sources (first hit wins):
  1. --message-file / --message / stdin (usually the push commit message)
  2. --fallback-file (usually the merged PR body)

Outputs GitHub Actions-style key=value lines to stdout / GITHUB_OUTPUT.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

# Single allowed form: release: v1.2.3
MARKER = re.compile(r"(?i)^release:\s*(v\d+\.\d+\.\d+)\s*$")
TRAILER = re.compile(
    r"(?i)^(signed-off-by|co-authored-by|reviewed-by|acked-by|suggested-by|reported-by|tested-by|merge-request|change-id)\s*:"
)
FENCE = re.compile(r"^```")


def strip_fenced_blocks(text: str) -> str:
    """Remove markdown fenced code blocks so template examples are ignored."""
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    out: list[str] = []
    in_fence = False
    for line in lines:
        if FENCE.match(line.strip()):
            in_fence = not in_fence
            continue
        if not in_fence:
            out.append(line)
    return "\n".join(out)


def parse(msg: str, *, strip_fences: bool = True) -> dict:
    if strip_fences:
        msg = strip_fenced_blocks(msg)
    lines = msg.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    subject = next((ln.strip() for ln in lines if ln.strip()), "")

    tag = None
    marker_idx = None
    for i, line in enumerate(lines):
        m = MARKER.match(line.strip())
        if m:
            tag = m.group(1)
            marker_idx = i
            break

    if not tag:
        return {
            "should_release": "false",
            "version": "",
            "tag": "",
            "prerelease": "false",
            "notes": "",
            "subject": subject,
            "source": "",
        }

    version = tag[1:]

    body = lines[marker_idx + 1 :]
    while body and body[0].strip() == "":
        body.pop(0)

    trimmed: list[str] = []
    for line in body:
        s = line.strip()
        if TRAILER.match(s):
            break
        if s == "---" or s.startswith("## "):
            break
        trimmed.append(line)
    while trimmed and trimmed[-1].strip() == "":
        trimmed.pop()

    notes = "\n".join(trimmed).strip()
    if not notes:
        notes = subject if subject and not MARKER.match(subject) else f"Release {tag}"

    return {
        "should_release": "true",
        "version": version,
        "tag": tag,
        "prerelease": "false",
        "notes": notes,
        "subject": subject,
        "source": "",  # filled by main when known
    }


def write_output(result: dict, out_file: str | None) -> None:
    def emit(fh, k, v):
        if "\n" in v:
            fh.write(f"{k}<<RELEASE_EOF\n{v}\nRELEASE_EOF\n")
        else:
            fh.write(f"{k}={v}\n")

    targets = [sys.stdout]
    opened = []
    if out_file:
        opened.append(open(out_file, "a", encoding="utf-8"))
        targets.append(opened[-1])
    try:
        for fh in targets:
            for k, v in result.items():
                emit(fh, k, v)
    finally:
        for fh in opened:
            fh.close()


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--message-file", help="Primary text (commit message)")
    ap.add_argument("--message", help="Primary text string")
    ap.add_argument(
        "--fallback-file",
        help="Secondary text if primary has no release marker (e.g. PR body)",
    )
    ap.add_argument(
        "--github-output",
        action="store_true",
        help="Also append to $GITHUB_OUTPUT",
    )
    args = ap.parse_args()

    if args.message_file:
        primary = Path(args.message_file).read_text(encoding="utf-8")
        primary_src = "commit"
    elif args.message is not None:
        primary = args.message
        primary_src = "commit"
    else:
        primary = sys.stdin.read()
        primary_src = "commit"

    result = parse(primary)
    if result["should_release"] == "true":
        result["source"] = primary_src
    elif args.fallback_file and Path(args.fallback_file).is_file():
        fallback = Path(args.fallback_file).read_text(encoding="utf-8")
        result = parse(fallback)
        if result["should_release"] == "true":
            result["source"] = "pr_body"
        else:
            result["source"] = ""
    else:
        result["source"] = ""

    out = os.environ.get("GITHUB_OUTPUT") if args.github_output else None
    write_output(result, out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
