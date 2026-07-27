#!/usr/bin/env python3
import importlib.util
import subprocess
import sys
import tempfile
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "check_release_version", Path(__file__).with_name("check-release-version.py")
)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def test_latest():
    assert mod.latest([]) is None
    assert mod.latest(["v0.1.0", "v0.2.0", "v0.1.9"]) == "v0.2.0"
    assert mod.latest(["v1.0.0", "v0.9.9", "bogus", "v1.0.1"]) == "v1.0.1"
    # numeric compare, not lexicographic
    assert mod.latest(["v1.9.0", "v1.10.0"]) == "v1.10.0"


def test_check_ok_first():
    mod.check("v0.1.0", [])


def test_check_ok_newer():
    mod.check("v0.2.0", ["v0.1.0", "v0.1.5"])


def test_check_rejects_equal():
    try:
        mod.check("v0.2.0", ["v0.2.0"])
        raise AssertionError("expected exit")
    except SystemExit as e:
        assert e.code == 1


def test_check_rejects_older():
    try:
        mod.check("v0.1.0", ["v0.2.0"])
        raise AssertionError("expected exit")
    except SystemExit as e:
        assert e.code == 1


def test_check_rejects_invalid():
    try:
        mod.check("1.2.3", ["v1.0.0"])
        raise AssertionError("expected exit")
    except SystemExit as e:
        assert e.code == 1


def test_cli():
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        f.write("v0.1.0\nv0.3.0\n")
        path = f.name
    r = subprocess.run(
        [sys.executable, str(Path(__file__).with_name("check-release-version.py")),
         "--tag", "v0.2.0", "--tags-file", path],
        capture_output=True,
        text=True,
    )
    assert r.returncode == 1, r.stdout + r.stderr
    r2 = subprocess.run(
        [sys.executable, str(Path(__file__).with_name("check-release-version.py")),
         "--tag", "v0.3.1", "--tags-file", path],
        capture_output=True,
        text=True,
    )
    assert r2.returncode == 0, r2.stdout + r2.stderr


if __name__ == "__main__":
    test_latest()
    test_check_ok_first()
    test_check_ok_newer()
    test_check_rejects_equal()
    test_check_rejects_older()
    test_check_rejects_invalid()
    test_cli()
    print("ok")
