#!/usr/bin/env python3
"""
This script was used when I had several results files for the same day, due to executing the modules separately.
Merge results_YYYY-MM-DD-h-m-s-something.txt files that share the same
timestamp prefix (everything up to and including the seconds field) into a
single file. Each file is expected to contain a single dict written out as
a JSON-like string (NaN / Infinity are supported, since Python's json
module accepts them as an extension).

The merged file:
  - takes the name of the last file (alphabetically) within its group
  - contains a dict with the keys of all files in that group

Usage:
    python merge_results.py /path/to/folder [--delete-originals] [--dry-run]

If no folder is given, the current directory is used.
"""

import argparse
import json
import os
import re
import sys
from collections import defaultdict

# Matches: results_2024-01-02-3-4-5-something.txt
# Group 1 = the shared prefix (results_2024-MM-dd-h-m-s)
# Group 2 = the "something" suffix (not used, just consumed)
FILENAME_RE = re.compile(
    r"^(results_\d{4}-\d{2}-\d{2}-\d{1,2}-\d{1,2}-\d{1,2})-(.+)\.txt$"
)


def find_groups(folder):
    """Group filenames in `folder` by their shared timestamp prefix."""
    groups = defaultdict(list)
    skipped = []
    for name in os.listdir(folder):
        full_path = os.path.join(folder, name)
        if not os.path.isfile(full_path):
            continue
        match = FILENAME_RE.match(name)
        if match:
            prefix = match.group(1)
            groups[prefix].append(name)
        else:
            skipped.append(name)
    return groups, skipped


def load_dict(path):
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        raise ValueError(f"Could not parse {path} as a dict: {e}") from e


def merge_group(folder, filenames):
    """Merge all files in a group (sorted alphabetically) into one dict.
    Returns (output_filename, merged_dict)."""
    filenames_sorted = sorted(filenames)
    merged = {}
    for name in filenames_sorted:
        data = load_dict(os.path.join(folder, name))
        for key, value in data.items():
            if key in merged:
                print(
                    f"  Warning: key '{key}' appears in multiple files in this "
                    f"group; keeping the value from '{name}' (later file wins).",
                    file=sys.stderr,
                )
            merged[key] = value
    output_filename = filenames_sorted[-1]  # last one alphabetically
    return output_filename, merged


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "folder", nargs="?", default=".", help="Folder containing the result files"
    )
    parser.add_argument(
        "--delete-originals",
        action="store_true",
        help="Delete the original files in a group after writing the merged file "
        "(the file that becomes the output is kept/overwritten, not deleted).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print what would be done, without writing/deleting anything.",
    )
    args = parser.parse_args()

    folder = os.path.abspath(args.folder)
    if not os.path.isdir(folder):
        print(f"Error: '{folder}' is not a directory.", file=sys.stderr)
        sys.exit(1)

    groups, skipped = find_groups(folder)

    if skipped:
        print("Skipped files (did not match the expected naming pattern):")
        for name in skipped:
            print(f"  - {name}")
        print()

    if not groups:
        print("No matching files found.")
        return

    for prefix, filenames in sorted(groups.items()):
        if len(filenames) < 2:
            print(f"Group '{prefix}': only one file ({filenames[0]}), skipping merge.")
            continue

        print(f"Group '{prefix}': merging {len(filenames)} files ->")
        for name in sorted(filenames):
            print(f"    {name}")

        output_filename, merged = merge_group(folder, filenames)
        output_path = os.path.join(folder, output_filename)
        print(f"  Output: {output_filename}")

        if args.dry_run:
            continue

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(merged, f, indent=2, allow_nan=True, ensure_ascii=False)

        if args.delete_originals:
            for name in sorted(filenames):
                if name != output_filename:
                    os.remove(os.path.join(folder, name))

    print("\nDone.")


if __name__ == "__main__":
    main()