#!/usr/bin/env python3
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "parse_release_commit", Path(__file__).with_name("parse-release-commit.py")
)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
parse = mod.parse


def test_basic():
    r = parse("Ship multi-seed CLI\n\nrelease: v0.3.0\n\n- CLI multi-seed\n- REPL\n")
    assert r["should_release"] == "true"
    assert r["tag"] == "v0.3.0"
    assert r["version"] == "0.3.0"
    assert r["prerelease"] == "false"
    assert "- CLI multi-seed" in r["notes"]


def test_no_v_prefix():
    r = parse("release: 1.0.0\n")
    assert r["tag"] == "v1.0.0"


def test_prerelease():
    r = parse("Release-As: v1.2.3-rc.1\n\nRC notes\n")
    assert r["prerelease"] == "true"
    assert r["tag"] == "v1.2.3-rc.1"
    assert r["notes"] == "RC notes"


def test_no_marker():
    r = parse("just a normal commit\n\nbody\n")
    assert r["should_release"] == "false"


def test_trailers_stripped():
    r = parse(
        "feat: x\n\nrelease: v2.0.0\n\nNotes here\n\nCo-authored-by: A <a@b.c>\n"
    )
    assert r["notes"] == "Notes here"
    assert "Co-authored-by" not in r["notes"]


def test_version_alias():
    r = parse("version: v0.1.0\n")
    assert r["should_release"] == "true"
    assert r["tag"] == "v0.1.0"


if __name__ == "__main__":
    test_basic()
    test_no_v_prefix()
    test_prerelease()
    test_no_marker()
    test_trailers_stripped()
    test_version_alias()
    print("ok")
