#!/usr/bin/env python3
"""Parse SuperCache release metadata from a commit message or PR title.

Formats (exact):

  Commit message — Markdown YAML front matter:

      ---
      release: v1.2.3
      ---

  PR title (or title embedded in a merge commit):

      [release: v1.2.3]

Sources (first hit wins):
  1. --message-file / --message / stdin  (git commit message)
  2. --fallback-file                     (PR title)

A bare line `release: vX.Y.Z` outside front matter does **not** release.

- Version must be v + MAJOR.MINOR.PATCH (no prerelease)
- Optional notes = text after the closing `---` of front matter
  (until another `---` / `##` / git trailers)
- No marker → should_release=false

Outputs GitHub Actions-style key=value lines to stdout / GITHUB_OUTPUT.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

FM_RELEASE = re.compile(r"(?i)^release:\s*(v\d+\.\d+\.\d+)\s*$")
BRACKET_MARKER = re.compile(r"(?i)\[release:\s*(v\d+\.\d+\.\d+)\s*\]")
TRAILER = re.compile(
    r"(?i)^(signed-off-by|co-authored-by|reviewed-by|acked-by|suggested-by|"
    r"reported-by|tested-by|merge-request|change-id)\s*:"
)


def _normalize(msg: str) -> str:
    return msg.replace("\r\n", "\n").replace("\r", "\n")


def _strip_code_fences(text: str) -> str:
    """Remove ``` fenced code blocks (not YAML --- front matter)."""
    lines = text.split("\n")
    out: list[str] = []
    in_fence = False
    for line in lines:
        if line.strip().startswith("```"):
            in_fence = not in_fence
            continue
        if not in_fence:
            out.append(line)
    return "\n".join(out)


def _subject(lines: list[str]) -> str:
    return next((ln.strip() for ln in lines if ln.strip()), "")


def _extract_notes(lines: list[str], start: int, subject: str, tag: str) -> str:
    body = lines[start:]
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
    if not notes or BRACKET_MARKER.fullmatch(notes):
        notes = subject if subject and not BRACKET_MARKER.search(subject) else f"Release {tag}"
    if FM_RELEASE.match(notes):
        notes = f"Release {tag}"
    return notes


def find_front_matter(lines: list[str]) -> tuple[str, int] | None:
    """Find first YAML front-matter block; return (tag, index_after_closing_fence).

    Front matter is a `---` line, then body lines, then a closing `---` line.
    The body must contain `release: vX.Y.Z` on its own line.
    """
    n = len(lines)
    i = 0
    while i < n:
        if lines[i].strip() != "---":
            i += 1
            continue
        # potential opening fence
        j = i + 1
        body: list[str] = []
        while j < n and lines[j].strip() != "---":
            body.append(lines[j])
            j += 1
        if j >= n:
            return None  # unclosed
        # closing fence at j
        tag = None
        for bl in body:
            m = FM_RELEASE.match(bl.strip())
            if m:
                tag = m.group(1)
                break
        if tag:
            return tag, j + 1
        # not a release front matter; continue search after this block
        i = j + 1
    return None


def parse_commit(msg: str) -> dict:
    text = _strip_code_fences(_normalize(msg))
    lines = text.split("\n")
    subject = _subject(lines)

    empty = {
        "should_release": "false",
        "version": "",
        "tag": "",
        "prerelease": "false",
        "notes": "",
        "subject": subject,
        "source": "",
    }

    fm = find_front_matter(lines)
    if fm:
        tag, after = fm
        return {
            "should_release": "true",
            "version": tag[1:],
            "tag": tag,
            "prerelease": "false",
            "notes": _extract_notes(lines, after, subject, tag),
            "subject": subject,
            "source": "",
        }

    # Bracket form: PR title embedded in merge commit
    for i, line in enumerate(lines):
        m = BRACKET_MARKER.search(line)
        if m:
            tag = m.group(1)
            return {
                "should_release": "true",
                "version": tag[1:],
                "tag": tag,
                "prerelease": "false",
                "notes": _extract_notes(lines, i + 1, subject, tag),
                "subject": subject,
                "source": "",
            }

    return empty


def parse_pr_title(title: str) -> dict:
    title = _normalize(title).strip()
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
        has_fm = find_front_matter(_strip_code_fences(_normalize(primary)).split("\n"))
        if has_fm:
            result["source"] = "commit"
        else:
            result["source"] = "commit_title_embed"
    elif args.fallback_file and Path(args.fallback_file).is_file():
        result = parse_pr_title(Path(args.fallback_file).read_text(encoding="utf-8"))
        result["source"] = "pr_title" if result["should_release"] == "true" else ""
    else:
        result["source"] = ""

    out = os.environ.get("GITHUB_OUTPUT") if args.github_output else None
    write_output(result, out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
