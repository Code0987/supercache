#!/usr/bin/env python3
"""Guard: proposed release tag must be strictly greater than the latest vX.Y.Z tag.

Usage:
  python3 scripts/check-release-version.py --tag v1.2.3
  python3 scripts/check-release-version.py --tag v1.2.3 --tags-file tags.txt

Exit 0 if OK (no prior tags, or proposed > latest).
Exit 1 if proposed <= latest, invalid, or already exists.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterable, Optional, Tuple

TAG_RE = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")
Version = Tuple[int, int, int]


def parse_tag(tag: str) -> Optional[Version]:
    m = TAG_RE.fullmatch(tag.strip())
    if not m:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


def collect_versions(tags: Iterable[str]) -> list[tuple[Version, str]]:
    out: list[tuple[Version, str]] = []
    for t in tags:
        t = t.strip()
        if not t:
            continue
        v = parse_tag(t)
        if v is not None:
            out.append((v, t))
    return out


def latest(tags: Iterable[str]) -> Optional[str]:
    vers = collect_versions(tags)
    if not vers:
        return None
    vers.sort(key=lambda x: x[0])
    return vers[-1][1]


def git_tags() -> list[str]:
    r = subprocess.run(
        ["git", "tag", "-l", "v*"],
        check=False,
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        print(f"git tag failed: {r.stderr}", file=sys.stderr)
        sys.exit(1)
    return [ln.strip() for ln in r.stdout.splitlines() if ln.strip()]


def check(proposed: str, tags: list[str]) -> None:
    prop_v = parse_tag(proposed)
    if prop_v is None:
        print(
            f"Invalid tag {proposed!r}: must match vMAJOR.MINOR.PATCH (e.g. v1.2.3)",
            file=sys.stderr,
        )
        sys.exit(1)

    tag_set = set(t.strip() for t in tags if t.strip())
    if proposed in tag_set:
        print(f"Tag {proposed} already exists — refusing to re-release.", file=sys.stderr)
        sys.exit(1)

    cur = latest(tags)
    if cur is None:
        print(f"No prior v*.*.* tags; {proposed} is allowed as first release.")
        return

    cur_v = parse_tag(cur)
    assert cur_v is not None
    if prop_v <= cur_v:
        print(
            f"Version {proposed} is not greater than current latest {cur}.\n"
            f"Bump the version (release: vX.Y.Z must be > {cur}).",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"OK: {proposed} > latest {cur}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tag", required=True, help="Proposed tag, e.g. v1.2.3")
    ap.add_argument(
        "--tags-file",
        help="Optional file with one tag per line (default: git tag -l 'v*')",
    )
    args = ap.parse_args()

    if args.tags_file:
        tags = Path(args.tags_file).read_text(encoding="utf-8").splitlines()
    else:
        tags = git_tags()

    check(args.tag, tags)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
