#!/usr/bin/env python3
"""
collect_huggingface.py — Download mainframe code from Hugging Face datasets.

Sources:
  1. bigcode/the-stack-v2         — 658 languages, COBOL present
  2. bigcode/the-stack-dedup      — 358 languages (v1, easier access)
  3. bigcode/rosetta-code         — Labeled code snippets (PL/I, COBOL, REXX, ooRexx)
  4. bigcode/the-stack-smol-xl    — 10K samples per language (quick test)

Usage:
  # Download COBOL from The Stack v1 (deduplicated)
  python3 collect_huggingface.py --source the-stack --language cobol --output ./output/cobol

  # Download PL/I from Rosetta Code
  python3 collect_huggingface.py --source rosetta --language "PL/I" --output ./output/pli

  # Download COBOL from The Stack v2 (requires SWH agreement for bulk)
  python3 collect_huggingface.py --source the-stack-v2 --language cobol --output ./output/cobol

  # Quick test with small subset
  python3 collect_huggingface.py --source the-stack-smol --language cobol --output ./output/cobol_test
"""

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

try:
    from datasets import load_dataset
    from tqdm import tqdm
except ImportError:
    print("Install dependencies: pip install datasets tqdm huggingface_hub")
    sys.exit(1)


# Language name mappings for each dataset
STACK_LANG_DIRS = {
    # The Stack uses directory names matching github-linguist
    "cobol": "cobol",
    "rexx": "rexx",
    # PL/I is NOT in The Stack (not recognized by linguist)
    # JCL is NOT in The Stack
    # HLASM might be under "assembly" or not present
}

ROSETTA_LANG_NAMES = {
    # Rosetta Code uses its own naming
    "pli": "PL/I",
    "cobol": "COBOL",
    "rexx": "REXX",
    "oorexx": "ooRexx",
    "jcl": None,  # Not in Rosetta Code
    "hlasm": None,
}

# File extensions to use when saving
LANG_EXTENSIONS = {
    "cobol": ".cbl",
    "pli": ".pli",
    "PL/I": ".pli",
    "rexx": ".rexx",
    "REXX": ".rexx",
    "ooRexx": ".rexx",
    "jcl": ".jcl",
    "hlasm": ".asm",
}


def collect_from_the_stack(language: str, output_dir: str, min_chars: int,
                           version: str = "v1"):
    """Download from The Stack (v1 or v2)."""
    lang_dir = STACK_LANG_DIRS.get(language.lower())
    if not lang_dir:
        print(f"WARNING: {language} is not available in The Stack.")
        print("  Available languages: " + ", ".join(STACK_LANG_DIRS.keys()))
        return []

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    if version == "v2":
        dataset_name = "bigcode/the-stack-v2"
        print(f"NOTE: The Stack v2 requires an agreement with Software Heritage.")
        print(f"  Visit https://huggingface.co/datasets/{dataset_name}")
    elif version == "smol":
        dataset_name = "bigcode/the-stack-smol-xl"
    else:
        dataset_name = "bigcode/the-stack-dedup"

    print(f"\nLoading {dataset_name} / {lang_dir}...")
    try:
        ds = load_dataset(dataset_name, data_dir=f"data/{lang_dir}", split="train",
                          streaming=True)  # Stream to avoid downloading everything
    except Exception as e:
        print(f"ERROR loading dataset: {e}")
        print(f"  You may need to accept the dataset license at:")
        print(f"  https://huggingface.co/datasets/{dataset_name}")
        return []

    collected = []
    seen_hashes = set()
    ext = LANG_EXTENSIONS.get(language.lower(), ".txt")

    print(f"Filtering files >= {min_chars} characters...")
    for i, example in enumerate(tqdm(ds, desc="Scanning")):
        content = example.get("content", "")
        if len(content) < min_chars:
            continue

        # Dedup
        h = hashlib.sha256(content.encode("utf-8", errors="replace")).hexdigest()
        if h in seen_hashes:
            continue
        seen_hashes.add(h)

        # Save
        repo_name = example.get("repository_name", f"unknown_{i}")
        file_path = example.get("path", f"file_{i}{ext}")
        safe_name = f"{repo_name.replace('/', '__')}__{Path(file_path).stem}{ext}"
        safe_name = "".join(c if c.isalnum() or c in "._-" else "_" for c in safe_name)

        dest = output / safe_name
        with open(dest, "w", encoding="utf-8") as f:
            f.write(content)

        collected.append({
            "file": safe_name,
            "source": dataset_name,
            "source_repo": repo_name,
            "source_path": file_path,
            "size": len(content),
            "hash": h,
        })

        if len(collected) % 100 == 0:
            print(f"  Collected {len(collected)} files so far...")

        # Safety limit — remove or increase as needed
        if len(collected) >= 50000:
            print("  Reached 50,000 file limit. Stopping.")
            break

    return collected


def collect_from_rosetta(language: str, output_dir: str, min_chars: int):
    """Download from Rosetta Code dataset on Hugging Face."""
    lang_name = ROSETTA_LANG_NAMES.get(language.lower(), language)
    if lang_name is None:
        print(f"WARNING: {language} is not available in Rosetta Code.")
        return []

    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    print(f"\nLoading bigcode/rosetta-code...")
    try:
        ds = load_dataset("bigcode/rosetta-code", split="train")
    except Exception as e:
        print(f"ERROR loading dataset: {e}")
        return []

    collected = []
    seen_hashes = set()
    ext = LANG_EXTENSIONS.get(lang_name, LANG_EXTENSIONS.get(language.lower(), ".txt"))

    print(f"Filtering for language: {lang_name}")
    for example in tqdm(ds, desc="Scanning"):
        task_name = example.get("task_name", "")
        # Rosetta Code stores solutions in a nested structure
        # Check if this task has a solution in our target language
        code = None

        # The dataset structure varies — handle both formats
        if "code" in example and "language" in example:
            if example["language"] == lang_name:
                code = example["code"]
        elif "solutions" in example:
            for sol in example.get("solutions", []):
                if sol.get("language") == lang_name:
                    code = sol.get("code", "")
                    break

        if not code or len(code) < min_chars:
            continue

        h = hashlib.sha256(code.encode("utf-8", errors="replace")).hexdigest()
        if h in seen_hashes:
            continue
        seen_hashes.add(h)

        safe_task = "".join(c if c.isalnum() or c in "._-" else "_" for c in task_name)
        dest_name = f"rosetta__{safe_task}{ext}"
        dest = output / dest_name
        with open(dest, "w", encoding="utf-8") as f:
            f.write(code)

        collected.append({
            "file": dest_name,
            "source": "rosetta-code",
            "task": task_name,
            "size": len(code),
            "hash": h,
        })

    return collected


def main():
    parser = argparse.ArgumentParser(
        description="Collect mainframe code from Hugging Face datasets"
    )
    parser.add_argument("--source", "-s", required=True,
                        choices=["the-stack", "the-stack-v2", "the-stack-smol",
                                 "rosetta"],
                        help="Dataset source")
    parser.add_argument("--language", "-l", required=True,
                        help="Target language (cobol, pli, rexx, etc.)")
    parser.add_argument("--output", "-o", default="./output",
                        help="Output directory")
    parser.add_argument("--min-chars", type=int, default=10000,
                        help="Minimum file size in characters")
    args = parser.parse_args()

    if args.source == "rosetta":
        collected = collect_from_rosetta(args.language, args.output, args.min_chars)
    elif args.source == "the-stack-v2":
        collected = collect_from_the_stack(args.language, args.output, args.min_chars,
                                           version="v2")
    elif args.source == "the-stack-smol":
        collected = collect_from_the_stack(args.language, args.output, args.min_chars,
                                           version="smol")
    else:
        collected = collect_from_the_stack(args.language, args.output, args.min_chars)

    # Save manifest
    output = Path(args.output)
    manifest_path = output / f"manifest_{args.source}.json"
    with open(manifest_path, "w") as f:
        json.dump({
            "source": args.source,
            "language": args.language,
            "min_chars": args.min_chars,
            "total_files": len(collected),
            "total_bytes": sum(c["size"] for c in collected),
            "files": collected,
        }, f, indent=2)

    print(f"\n{'='*60}")
    print(f"RESULTS: {args.language} from {args.source}")
    print(f"{'='*60}")
    print(f"Files collected: {len(collected)}")
    total = sum(c["size"] for c in collected)
    print(f"Total size:      {total:,} bytes ({total/1024/1024:.1f} MB)")
    print(f"Output:          {args.output}")
    print(f"Manifest:        {manifest_path}")


if __name__ == "__main__":
    main()
