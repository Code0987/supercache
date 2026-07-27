#!/usr/bin/env python3
"""Parse SuperCache release metadata from a git commit message.

Supported markers (case-insensitive, line must match):

  release: v1.2.3
  release: 1.2.3
  Release-As: v1.2.3
  Release-Version: v1.2.3
  version: v1.2.3

Optional notes = body after the release marker line (excluding git trailers).
If notes are empty, falls back to the subject line (first line).

Outputs GitHub Actions-style key=value lines to stdout (and optionally GITHUB_OUTPUT).
When no release marker is found, prints should_release=false and exits 0.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

MARKER = re.compile(
    r"(?im)^(?:release(?:-as|-version)?|version)\s*:\s*v?(\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?)\s*$"
)
TRAILER = re.compile(
    r"(?i)^(signed-off-by|co-authored-by|reviewed-by|acked-by|suggested-by|reported-by|tested-by|merge-request|change-id)\s*:"
)


def parse(msg: str) -> dict:
    lines = msg.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    subject = lines[0].strip() if lines else ""

    version = None
    marker_idx = None
    for i, line in enumerate(lines):
        m = MARKER.match(line.strip())
        if m:
            version = m.group(1)
            marker_idx = i
            break

    if not version:
        return {
            "should_release": "false",
            "version": "",
            "tag": "",
            "prerelease": "false",
            "notes": "",
            "subject": subject,
        }

    tag = f"v{version}"
    prerelease = "true" if "-" in version else "false"

    body = lines[marker_idx + 1 :]
    while body and body[0].strip() == "":
        body.pop(0)

    trimmed: list[str] = []
    for line in body:
        if TRAILER.match(line.strip()):
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
        "prerelease": prerelease,
        "notes": notes,
        "subject": subject,
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
    ap.add_argument("--message-file", help="Read commit message from file")
    ap.add_argument("--message", help="Commit message string")
    ap.add_argument(
        "--github-output",
        action="store_true",
        help="Also append to $GITHUB_OUTPUT",
    )
    args = ap.parse_args()

    if args.message_file:
        msg = Path(args.message_file).read_text(encoding="utf-8")
    elif args.message is not None:
        msg = args.message
    else:
        msg = sys.stdin.read()

    result = parse(msg)
    out = os.environ.get("GITHUB_OUTPUT") if args.github_output else None
    write_output(result, out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
