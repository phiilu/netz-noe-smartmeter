"""Build the HACS release zip: the integration files at the zip root, manifest version set to the release version.

Usage: python scripts/build_zip.py <version> <output.zip>
"""

import json
import pathlib
import sys
import zipfile
from typing import cast

ROOT = pathlib.Path(__file__).resolve().parent.parent / "custom_components" / "netz_noe_smartmeter"


def main(version: str, output: str) -> None:
    """Write the zip for the given version."""
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as zf:
        for path in sorted(ROOT.rglob("*")):
            if not path.is_file() or any(part.startswith((".", "__pycache__")) for part in path.relative_to(ROOT).parts):
                continue
            name = path.relative_to(ROOT).as_posix()
            if name == "manifest.json":
                manifest = cast(dict[str, object], json.loads(path.read_text()))
                manifest["version"] = version
                zf.writestr(name, json.dumps(manifest, indent=2, ensure_ascii=False) + "\n")
            else:
                zf.write(path, name)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
