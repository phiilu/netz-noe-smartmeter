"""Reject a beta version unless it is newer than every existing release tag; print the latest final tag.

Usage: python scripts/check_beta_version.py <version> <file with one tag per line>
"""

import pathlib
import re
import sys

BETA = re.compile(r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)-beta\.([1-9]\d*)$")
TAG = re.compile(r"^v(\d+)\.(\d+)\.(\d+)(?:-beta\.(\d+))?$")


def key(major: str, minor: str, patch: str, beta: str | None) -> tuple[int, int, int, int, int]:
    """Order versions so that X.Y.Z-beta.N sorts below X.Y.Z."""
    return (int(major), int(minor), int(patch), 0 if beta else 1, int(beta or 0))


def main(version: str, tags_file: str) -> None:
    """Exit non-zero with a message when the version is malformed or not the newest."""
    match = BETA.match(version)
    if match is None:
        sys.exit(f"Version must look like 0.3.1-beta.2, got {version!r}")
    new = key(*match.groups())
    finals: list[tuple[tuple[int, int, int, int, int], str]] = []
    for tag in pathlib.Path(tags_file).read_text().split():
        existing = TAG.match(tag)
        if existing is None:
            continue
        if key(*existing.groups()) >= new:
            sys.exit(f"{version} is not newer than existing release {tag}")
        if existing.group(4) is None:
            finals.append((key(*existing.groups()), tag))
    if not finals:
        sys.exit("No final release found to base the beta on")
    _ = sys.stdout.write(max(finals)[1] + "\n")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
