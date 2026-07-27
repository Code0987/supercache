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


def test_commit_line():
    r = mod.parse_commit("Ship it\n\nrelease: v0.3.0\n\n- note\n")
    assert r["should_release"] == "true"
    assert r["tag"] == "v0.3.0"
    assert "- note" in r["notes"]


def test_commit_requires_v():
    assert mod.parse_commit("release: 1.0.0\n")["should_release"] == "false"


def test_commit_rejects_unbracketed_with_extra():
    assert mod.parse_commit("release: v0.3.0 ship it\n")["should_release"] == "false"


def test_merge_commit_embeds_bracket_title():
    msg = (
        "Merge pull request #4 from Code0987/feature\n"
        "\n"
        "Add probe [release: v0.2.0]\n"
    )
    r = mod.parse_commit(msg)
    assert r["should_release"] == "true"
    assert r["tag"] == "v0.2.0"


def test_pr_title_bracket():
    r = mod.parse_pr_title("Add probe [release: v0.2.0]")
    assert r["should_release"] == "true"
    assert r["tag"] == "v0.2.0"


def test_pr_title_bracket_only():
    r = mod.parse_pr_title("[release: v0.2.0]")
    assert r["should_release"] == "true"
    assert r["tag"] == "v0.2.0"


def test_pr_title_rejects_unbracketed():
    assert mod.parse_pr_title("release: v0.2.0")["should_release"] == "false"


def test_fallback_title_cli():
    with tempfile.TemporaryDirectory() as d:
        commit = Path(d) / "c.txt"
        title = Path(d) / "t.txt"
        commit.write_text("Merge pull request #9\n\nsome title without marker\n")
        title.write_text("Cool feature [release: v0.5.0]\n")
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
        assert "tag=v0.5.0" in r.stdout
        assert "source=pr_title" in r.stdout


if __name__ == "__main__":
    test_commit_line()
    test_commit_requires_v()
    test_commit_rejects_unbracketed_with_extra()
    test_merge_commit_embeds_bracket_title()
    test_pr_title_bracket()
    test_pr_title_bracket_only()
    test_pr_title_rejects_unbracketed()
    test_fallback_title_cli()
    print("ok")
