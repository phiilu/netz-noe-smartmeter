"""Render the beta release notes from beta_notes.md.

Usage: python scripts/beta_notes.py <version> <latest final tag> <test focus, may be empty> <generated changes file>
"""

import pathlib
import re
import string
import sys

TEMPLATE = pathlib.Path(__file__).with_name("beta_notes.md")
# Entries release-please would also leave out of the changelog
SKIP = re.compile(r"^\* (chore|ci|docs|test|build)(\(.*\))?:")


def main(version: str, latest_final: str, test_focus: str, changes_file: str) -> None:
    """Print the notes to stdout."""
    changes = [line for line in pathlib.Path(changes_file).read_text().splitlines() if not SKIP.match(line)]
    notes = string.Template(TEMPLATE.read_text()).substitute(
        version=version,
        final=version.split("-beta.")[0],
        latest_final=latest_final,
        test_focus=test_focus or "Everything listed under the changes below.",
        changes="\n".join(changes).strip(),
    )
    _ = sys.stdout.write(notes)


if __name__ == "__main__":
    main(*sys.argv[1:5])
