#!/usr/bin/env python3
"""Parse SuperCache release metadata from a commit message or PR title.

Formats (exact):

  Commit message line:   release: v1.2.3
  PR title (or title
  embedded in a merge
  commit):               [release: v1.2.3]   (anywhere in the title)

Sources (first hit wins):
  1. --message-file / --message / stdin  (git commit message)
  2. --fallback-file                     (PR title)

Commit messages are scanned for:
  - a full line  release: vX.Y.Z
  - a line containing  [release: vX.Y.Z]  (merge commits often embed the PR title)

PR titles (--fallback-file) only accept the bracket form.

- Version must be v + MAJOR.MINOR.PATCH (no prerelease)
- Optional notes = lines after an unbracketed marker in the commit message
- No marker → should_release=false

Outputs GitHub Actions-style key=value lines to stdout / GITHUB_OUTPUT.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

# Commit message: whole line
LINE_MARKER = re.compile(r"(?i)^release:\s*(v\d+\.\d+\.\d+)\s*$")
# PR title / embedded title: square brackets
BRACKET_MARKER = re.compile(r"(?i)\[release:\s*(v\d+\.\d+\.\d+)\s*\]")
TRAILER = re.compile(
    r"(?i)^(signed-off-by|co-authored-by|reviewed-by|acked-by|suggested-by|"
    r"reported-by|tested-by|merge-request|change-id)\s*:"
)
FENCE = re.compile(r"^```")


def strip_fenced_blocks(text: str) -> str:
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


def _notes_from(lines: list[str], marker_idx: int, subject: str, tag: str) -> str:
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
        notes = subject if subject and not LINE_MARKER.match(subject) else f"Release {tag}"
    return notes


def parse_commit(msg: str) -> dict:
    """Parse a git commit message (line form + bracket form for merge titles)."""
    msg = strip_fenced_blocks(msg)
    lines = msg.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    subject = next((ln.strip() for ln in lines if ln.strip()), "")

    empty = {
        "should_release": "false",
        "version": "",
        "tag": "",
        "prerelease": "false",
        "notes": "",
        "subject": subject,
        "source": "",
    }

    # Prefer unbracketed full-line marker (explicit release commits).
    for i, line in enumerate(lines):
        m = LINE_MARKER.match(line.strip())
        if m:
            tag = m.group(1)
            return {
                "should_release": "true",
                "version": tag[1:],
                "tag": tag,
                "prerelease": "false",
                "notes": _notes_from(lines, i, subject, tag),
                "subject": subject,
                "source": "",
            }

    # Bracket form: PR title often appears as a line in a merge commit.
    for i, line in enumerate(lines):
        m = BRACKET_MARKER.search(line)
        if m:
            tag = m.group(1)
            # Notes from bracket-only title lines are usually empty; use default.
            notes = _notes_from(lines, i, subject, tag)
            # If the only content on the line is the bracket marker (+ whitespace),
            # subject may be the merge first line — fine.
            return {
                "should_release": "true",
                "version": tag[1:],
                "tag": tag,
                "prerelease": "false",
                "notes": notes,
                "subject": subject,
                "source": "",
            }

    return empty


def parse_pr_title(title: str) -> dict:
    """PR title must include [release: vX.Y.Z] (unbracketed form is rejected)."""
    title = title.replace("\r\n", "\n").replace("\r", "\n").strip()
    subject = title.split("\n")[0].strip() if title else ""
    empty = {
        "should_release": "false",
        "version": "",
        "tag": "",
        "prerelease": "false",
        "notes": "",
        "subject": subject,
        "source": "",
    }
    # Reject unbracketed title-only form
    if LINE_MARKER.match(subject) and not BRACKET_MARKER.search(subject):
        return empty

    m = BRACKET_MARKER.search(title)
    if not m:
        return empty
    tag = m.group(1)
    return {
        "should_release": "true",
        "version": tag[1:],
        "tag": tag,
        "prerelease": "false",
        "notes": f"Release {tag}",
        "subject": subject,
        "source": "",
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
        help="Secondary text if primary has no marker (PR title)",
    )
    ap.add_argument(
        "--github-output",
        action="store_true",
        help="Also append to $GITHUB_OUTPUT",
    )
    args = ap.parse_args()

    if args.message_file:
        primary = Path(args.message_file).read_text(encoding="utf-8")
    elif args.message is not None:
        primary = args.message
    else:
        primary = sys.stdin.read()

    result = parse_commit(primary)
    if result["should_release"] == "true":
        # Bracket form in commit is typically the embedded PR title (merge).
        if BRACKET_MARKER.search(primary) and not any(
            LINE_MARKER.match(ln.strip()) for ln in primary.splitlines()
        ):
            result["source"] = "commit_title_embed"
        else:
            result["source"] = "commit"
    elif args.fallback_file and Path(args.fallback_file).is_file():
        title = Path(args.fallback_file).read_text(encoding="utf-8")
        result = parse_pr_title(title)
        if result["should_release"] == "true":
            result["source"] = "pr_title"
        else:
            result["source"] = ""
    else:
        result["source"] = ""

    out = os.environ.get("GITHUB_OUTPUT") if args.github_output else None
    write_output(result, out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
