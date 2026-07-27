#!/usr/bin/env python3
import importlib.util
import tempfile
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
    assert "- CLI multi-seed" in r["notes"]


def test_requires_v_prefix():
    assert parse("release: 1.0.0\n")["should_release"] == "false"


def test_rejects_other_markers():
    assert parse("version: v0.1.0\n")["should_release"] == "false"


def test_ignores_fenced_example():
    msg = """## Summary

stuff

## Release

```text
release: v0.3.0
```

more text
"""
    assert parse(msg)["should_release"] == "false"


def test_pr_body_with_marker_outside_fence():
    msg = """Short summary

release: v0.3.0

- Bullet one

---

## Release

```text
release: v9.9.9
```
"""
    r = parse(msg)
    assert r["should_release"] == "true"
    assert r["tag"] == "v0.3.0"
    assert r["notes"] == "- Bullet one"


def test_notes_stop_at_heading():
    r = parse("x\n\nrelease: v1.0.0\n\nnote line\n\n## Release\n\nignore\n")
    assert r["notes"] == "note line"


def test_fallback_file():
    with tempfile.TemporaryDirectory() as d:
        commit = Path(d) / "c.txt"
        pr = Path(d) / "p.txt"
        commit.write_text("Merge pull request #2\n\nno marker\n")
        pr.write_text("Summary\n\nrelease: v0.4.0\n\n- from pr\n")
        import subprocess, sys
        r = subprocess.run(
            [sys.executable, str(Path(__file__).with_name("parse-release-commit.py")),
             "--message-file", str(commit), "--fallback-file", str(pr)],
            capture_output=True, text=True, check=True,
        )
        assert "should_release=true" in r.stdout
        assert "tag=v0.4.0" in r.stdout
        assert "source=pr_body" in r.stdout


if __name__ == "__main__":
    test_basic()
    test_requires_v_prefix()
    test_rejects_other_markers()
    test_ignores_fenced_example()
    test_pr_body_with_marker_outside_fence()
    test_notes_stop_at_heading()
    test_fallback_file()
    print("ok")
