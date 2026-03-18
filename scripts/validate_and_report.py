#!/usr/bin/env python3
"""
validate_and_report.py — Validate collected files, dedup across sources, and report.

Run this AFTER the collection scripts to:
  1. Merge files from all sources (github, gitlab, huggingface, swh, known_repos)
  2. Cross-source deduplication
  3. Content validation
  4. Generate final statistics and a combined manifest

Usage:
  python3 validate_and_report.py --input ./output --language pli --final ./final/pli
  python3 validate_and_report.py --input ./output --language cobol --final ./final/cobol
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
from collections import Counter
from pathlib import Path


VALIDATORS = {
    "pli": {
        "positive": [
            r"(?i)\bPROC(EDURE)?\b.*\bOPTIONS\s*\(",
            r"(?i)\bDCL\b|\bDECLARE\b",
            r"(?i)\bFIXED\s+(BIN|BINARY|DEC|DECIMAL)\b",
            r"(?i)\bCHAR(ACTER)?\s*\(",
            r"(?i)\bPUT\s+(SKIP\s+)?LIST\b",
            r"(?i)\bGET\s+(LIST|EDIT|DATA)\b",
            r"(?i)\b%INCLUDE\b",
            r"(?i)\bALLOCATE\b",
            r"(?i)\bON\s+(ENDFILE|ERROR|CONVERSION|OVERFLOW|UNDEFINEDFILE)\b",
            r"(?i)\bBEGIN\s*;",
            r"(?i)\bEND\s+\w+\s*;",
            r"(?i)\bBIT\s*\(\d+\)",
        ],
        "negative": [
            r"#!/usr/bin/perl",
            r"\buse\s+strict\b",
            r"\bmy\s+\$",
            r"\bsub\s+\w+\s*\{",
            r"\bpackage\s+\w+::",
            r"(?i)^\s*#\s*include\s+<",   # C header
            r"(?i)^\s*import\s+\w+",       # Python/Java
        ],
        "min_matches": 2,
    },
    "cobol": {
        "positive": [
            r"(?i)\bIDENTIFICATION\s+DIVISION\b",
            r"(?i)\bPROCEDURE\s+DIVISION\b",
            r"(?i)\bWORKING-STORAGE\s+SECTION\b",
            r"(?i)\bDATA\s+DIVISION\b",
            r"(?i)\bENVIRONMENT\s+DIVISION\b",
            r"(?i)\bPERFORM\b",
            r"(?i)\bMOVE\b.*\bTO\b",
            r"(?i)\b01\s+\w+",
            r"(?i)\bPIC(TURE)?\s+",
            r"(?i)\bEVALUATE\b",
            r"(?i)\bSTOP\s+RUN\b",
            r"(?i)\bCOPY\s+\w+\b",
        ],
        "negative": [],
        "min_matches": 3,
    },
    "rexx": {
        "positive": [
            r"(?i)\bSAY\b",
            r"(?i)\bPARSE\s+(ARG|VAR|PULL|VALUE|SOURCE|VERSION)\b",
            r"(?i)\bADDRESS\s+(TSO|ISPEXEC|MVS|SYSCALL|COMMAND)\b",
            r"(?i)\bEXECIO\b",
            r"(?i)\bSIGNAL\s+ON\b",
            r"(?i)/\*\s*REXX\s*\*/",
            r"(?i)\bINTERPRET\b",
            r"(?i)\bCALL\s+\w+",
            r"(?i)\bARG\b",
        ],
        "negative": [
            r"(?i)#!/bin/(ba)?sh",
            r"(?i)^\s*function\s+\w+\s*\(\)",
            r"(?i)^\s*#\s*include\s+<",
        ],
        "min_matches": 2,
    },
    "jcl": {
        "positive": [
            r"^//\w+\s+JOB\b",
            r"^//\w+\s+EXEC\b",
            r"^//\w+\s+DD\b",
            r"\bDSN=",
            r"\bDISP=",
            r"\bPGM=",
            r"\bSYSIN\b",
            r"\bSYSOUT\b",
            r"\bCLASS=",
        ],
        "negative": [],
        "min_matches": 3,
    },
    "hlasm": {
        "positive": [
            r"(?i)\bCSECT\b",
            r"(?i)\bUSING\b.*,\s*\d+",
            r"(?i)\bBALR\b",
            r"(?i)\bDS\s+\d*[CFHXPAZ]",
            r"(?i)\bDC\s+[CFHXPAZ]",
            r"(?i)\bSTM\b",
            r"(?i)\bLR\b",
            r"(?i)\bMVC\b",
            r"(?i)\bLTORG\b",
            r"(?i)\bEND\b",
        ],
        "negative": [
            r"\.section\b",
            r"\.globl\b",
            r"%rax|%rsp|%rbp",
            r"\bmov[lqbw]?\b.*%",
            r"\.intel_syntax",
            r"ARM|Thumb",
        ],
        "min_matches": 3,
    },
}


def validate_content(content, language):
    v = VALIDATORS.get(language)
    if not v:
        return True, 0

    for pat in v.get("negative", []):
        if re.search(pat, content, re.MULTILINE):
            return False, 0

    pos = sum(1 for pat in v["positive"] if re.search(pat, content, re.MULTILINE))
    return pos >= v["min_matches"], pos


def main():
    parser = argparse.ArgumentParser(
        description="Validate, dedup, and report on collected mainframe code"
    )
    parser.add_argument("--input", "-i", required=True,
                        help="Input directory (language subdir, e.g. ./output/pli)")
    parser.add_argument("--language", "-l", required=True,
                        choices=list(VALIDATORS.keys()))
    parser.add_argument("--final", "-f", default=None,
                        help="Final output directory (deduped, validated)")
    parser.add_argument("--min-chars", type=int, default=10000)
    args = parser.parse_args()

    input_dir = Path(args.input)
    if not input_dir.exists():
        print(f"ERROR: {input_dir} does not exist")
        sys.exit(1)

    final_dir = Path(args.final) if args.final else input_dir / "final"
    final_dir.mkdir(parents=True, exist_ok=True)

    print(f"Language:     {args.language}")
    print(f"Input:        {input_dir}")
    print(f"Final output: {final_dir}")
    print(f"Min chars:    {args.min_chars}")
    print()

    # Scan all files
    all_files = []
    for fpath in input_dir.rglob("*"):
        if fpath.is_file() and not fpath.name.startswith("manifest"):
            all_files.append(fpath)

    print(f"Total files found: {len(all_files)}")

    # Process
    seen_hashes = set()
    accepted = []
    rejected_size = 0
    rejected_content = 0
    rejected_dup = 0

    for fpath in all_files:
        # Size check
        try:
            with open(fpath, "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
        except Exception:
            rejected_content += 1
            continue

        if len(content) < args.min_chars:
            rejected_size += 1
            continue

        # Content validation
        valid, score = validate_content(content, args.language)
        if not valid:
            rejected_content += 1
            continue

        # Dedup
        h = hashlib.sha256(content.encode("utf-8", errors="replace")).hexdigest()
        if h in seen_hashes:
            rejected_dup += 1
            continue
        seen_hashes.add(h)

        # Accept
        dest = final_dir / fpath.name
        if dest.exists():
            # Rename to avoid collision
            dest = final_dir / f"{fpath.stem}_{h[:8]}{fpath.suffix}"
        shutil.copy2(fpath, dest)
        accepted.append({
            "file": dest.name,
            "size_chars": len(content),
            "size_bytes": os.path.getsize(fpath),
            "validation_score": score,
            "hash": h,
            "source_path": str(fpath),
        })

    # Statistics
    print(f"\n{'='*60}")
    print(f"VALIDATION REPORT: {args.language.upper()}")
    print(f"{'='*60}")
    print(f"Total files scanned:     {len(all_files)}")
    print(f"Rejected (too small):    {rejected_size}")
    print(f"Rejected (wrong content):{rejected_content}")
    print(f"Rejected (duplicate):    {rejected_dup}")
    print(f"ACCEPTED:                {len(accepted)}")
    print()

    if accepted:
        sizes = [a["size_chars"] for a in accepted]
        print(f"Size statistics (chars):")
        print(f"  Min:    {min(sizes):,}")
        print(f"  Max:    {max(sizes):,}")
        print(f"  Mean:   {sum(sizes)//len(sizes):,}")
        print(f"  Median: {sorted(sizes)[len(sizes)//2]:,}")
        total_bytes = sum(a["size_bytes"] for a in accepted)
        print(f"  Total:  {sum(sizes):,} chars ({total_bytes/1024/1024:.1f} MB)")

        scores = [a["validation_score"] for a in accepted]
        print(f"\nValidation scores:")
        score_dist = Counter(scores)
        for s in sorted(score_dist):
            print(f"  Score {s}: {score_dist[s]} files")

        # Target assessment
        print(f"\n--- TARGET ASSESSMENT ---")
        target_files = 1000
        target_chars = 10000
        if len(accepted) >= target_files:
            print(f"  ✅ FILE COUNT: {len(accepted)} >= {target_files} target")
        else:
            deficit = target_files - len(accepted)
            print(f"  ❌ FILE COUNT: {len(accepted)} < {target_files} target "
                  f"(need {deficit} more)")

        qualifying = sum(1 for a in accepted if a["size_chars"] >= target_chars)
        if qualifying >= target_files:
            print(f"  ✅ SIZE: {qualifying} files >= {target_chars} chars")
        else:
            print(f"  ❌ SIZE: only {qualifying} files >= {target_chars} chars")

    # Save manifest
    manifest = final_dir / "manifest_final.json"
    with open(manifest, "w") as f:
        json.dump({
            "language": args.language,
            "min_chars": args.min_chars,
            "total_accepted": len(accepted),
            "total_scanned": len(all_files),
            "rejected_size": rejected_size,
            "rejected_content": rejected_content,
            "rejected_duplicate": rejected_dup,
            "files": accepted,
        }, f, indent=2)
    print(f"\nManifest: {manifest}")


if __name__ == "__main__":
    main()
