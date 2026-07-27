#!/usr/bin/env python3
import importlib.util
import subprocess
import sys
import tempfile
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "parse_release_commit", Path(__file__).with_name("parse-release-commit.py")
)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
parse = mod.parse


def test_basic():
    r = parse("Ship multi-seed CLI\n\nrelease: v0.3.0\n\n- CLI multi-seed\n")
    assert r["should_release"] == "true"
    assert r["tag"] == "v0.3.0"
    assert "- CLI multi-seed" in r["notes"]


def test_requires_v_prefix():
    assert parse("release: 1.0.0\n")["should_release"] == "false"


def test_rejects_other_markers():
    assert parse("version: v0.1.0\n")["should_release"] == "false"


def test_pr_title_exact():
    r = parse("release: v0.3.0", strip_fences=False)
    assert r["should_release"] == "true"
    assert r["tag"] == "v0.3.0"


def test_pr_title_with_extra_text_rejected():
    # Format must be exact full line — no trailing description on same line.
    assert parse("release: v0.3.0 add feature", strip_fences=False)["should_release"] == "false"


def test_ignores_fenced_in_commit():
    msg = "summary\n\n```text\nrelease: v0.3.0\n```\n"
    assert parse(msg)["should_release"] == "false"


def test_fallback_is_title_not_body():
    with tempfile.TemporaryDirectory() as d:
        commit = Path(d) / "c.txt"
        title = Path(d) / "t.txt"
        body = Path(d) / "b.txt"
        commit.write_text("Merge pull request #2\n\nno marker\n")
        title.write_text("release: v0.4.0\n")
        body.write_text("release: v9.9.9\n\nshould not be used\n")
        r = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).with_name("parse-release-commit.py")),
                "--message-file",
                str(commit),
                "--fallback-file",
                str(title),
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        assert "tag=v0.4.0" in r.stdout
        assert "source=pr_title" in r.stdout
        # body file is not passed — ensure we don't invent 9.9.9
        assert "v9.9.9" not in r.stdout


if __name__ == "__main__":
    test_basic()
    test_requires_v_prefix()
    test_rejects_other_markers()
    test_pr_title_exact()
    test_pr_title_with_extra_text_rejected()
    test_ignores_fenced_in_commit()
    test_fallback_is_title_not_body()
    print("ok")
