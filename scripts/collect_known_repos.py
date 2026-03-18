#!/usr/bin/env python3
"""
collect_known_repos.py — Clone specific known repositories that contain mainframe code.

This is the FASTEST path to getting real files. These repos are verified to contain
mainframe source code. No API search needed — just clone and extract.

Usage:
  python3 collect_known_repos.py --output ./output --min-chars 10000
  python3 collect_known_repos.py --output ./output --min-chars 5000 --language pli
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

try:
    from tqdm import tqdm
except ImportError:
    print("Install: pip install tqdm")
    sys.exit(1)


# -----------------------------------------------------------------------
# Verified repositories containing mainframe source code
# -----------------------------------------------------------------------

KNOWN_REPOS = [
    # === COBOL-heavy repos ===
    {
        "url": "https://github.com/openmainframeproject/cobol-code-dataset.git",
        "desc": "Production COBOL for LLM training (Open Mainframe Project)",
        "languages": ["cobol"],
    },
    {
        "url": "https://github.com/openmainframeproject/cobol-programming-course.git",
        "desc": "COBOL training course with sample programs",
        "languages": ["cobol", "jcl"],
    },
    {
        "url": "https://github.com/openmainframeproject/cobol-check.git",
        "desc": "COBOL unit testing framework",
        "languages": ["cobol"],
    },
    {
        "url": "https://github.com/OCamlPro/gnucobol.git",
        "desc": "GnuCOBOL compiler — includes extensive test suite",
        "languages": ["cobol"],
    },
    {
        "url": "https://github.com/opensourcecobol/opensourcecobol4j.git",
        "desc": "Open source COBOL to Java compiler",
        "languages": ["cobol"],
    },

    # === Multi-language mainframe repos ===
    {
        "url": "https://github.com/IBM/zopeneditor-sample.git",
        "desc": "IBM Z Open Editor samples — COBOL, PL/I, HLASM, REXX",
        "languages": ["cobol", "pli", "hlasm", "rexx", "jcl"],
    },
    {
        "url": "https://github.com/IBA-mainframe-dev/Global-Repository-for-Mainframe-Developers.git",
        "desc": "Large mainframe developer resource collection",
        "languages": ["cobol", "pli", "rexx", "jcl", "hlasm"],
    },

    # === PL/I-specific repos ===
    {
        "url": "https://github.com/RobJTingay/PL1GCC.git",
        "desc": "PL/I compiler for GCC — includes test files",
        "languages": ["pli"],
    },
    {
        "url": "https://github.com/nicebyte/iron-spring-pli.git",
        "desc": "Iron Spring PL/I compiler",
        "languages": ["pli"],
    },

    # === REXX repos ===
    {
        "url": "https://github.com/RexxLA/rexx-repository.git",
        "desc": "Official REXX Language Association repository",
        "languages": ["rexx"],
    },

    # === Broadcom / Code4z ecosystem ===
    {
        "url": "https://github.com/BroadcomMFD/cobol-rpc.git",
        "desc": "Broadcom COBOL RPC samples",
        "languages": ["cobol", "jcl"],
    },

    # === Zowe ecosystem ===
    {
        "url": "https://github.com/zowe/zowe-pli-language-support.git",
        "desc": "Zowe PL/I Language Support (contains PL/I test/sample files)",
        "languages": ["pli"],
    },
    {
        "url": "https://github.com/zowe/zowe-cobol-language-support.git",
        "desc": "Zowe COBOL Language Support (contains COBOL test/sample files)",
        "languages": ["cobol"],
    },

    # === Hercules / MVS emulator community ===
    {
        "url": "https://github.com/mvslovers/brexx.git",
        "desc": "BREXX — REXX interpreter for MVS 3.8j",
        "languages": ["rexx"],
    },
    {
        "url": "https://github.com/mvslovers/mvs38j-langtest.git",
        "desc": "MVS 3.8J language tests (COBOL, PL/I, etc.)",
        "languages": ["cobol", "pli", "hlasm"],
    },

    # === Academic / training ===
    {
        "url": "https://github.com/mikerowehl/cobol-programming-examples.git",
        "desc": "COBOL programming examples collection",
        "languages": ["cobol"],
    },

    # === Mainframe modernization tools (contain sample code) ===
    {
        "url": "https://github.com/FSoft-AI4Code/XMainframe.git",
        "desc": "XMainframe — LLM for mainframe (training data methodology)",
        "languages": ["cobol"],
    },
]


# File extensions per language
EXTENSIONS = {
    "pli": {".pli", ".pl1", ".plinc", ".PLI", ".PL1"},
    "cobol": {".cbl", ".cob", ".cobol", ".cpy", ".CBL", ".COB", ".COBOL"},
    "rexx": {".rexx", ".rex", ".exec", ".REXX", ".REX", ".EXEC"},
    "jcl": {".jcl", ".JCL", ".job", ".proc"},
    "hlasm": {".hlasm", ".asm", ".mac", ".HLASM", ".ASM"},
}

# Content validation patterns
VALIDATORS = {
    "pli": {
        "positive": [
            r"(?i)\bPROC(EDURE)?\b.*\bOPTIONS\b",
            r"(?i)\bDCL\b|\bDECLARE\b",
            r"(?i)\bFIXED\s+(BIN|DEC)",
            r"(?i)\bPUT\s+(SKIP\s+)?LIST\b",
            r"(?i)\b%INCLUDE\b",
        ],
        "negative": [r"#!/usr/bin/perl", r"\buse\s+strict\b", r"\bmy\s+\$"],
        "min_matches": 1,
    },
    "cobol": {
        "positive": [
            r"(?i)\bIDENTIFICATION\s+DIVISION\b",
            r"(?i)\bPROCEDURE\s+DIVISION\b",
            r"(?i)\bPERFORM\b",
            r"(?i)\bMOVE\b",
            r"(?i)\bPIC(TURE)?\s+",
        ],
        "negative": [],
        "min_matches": 2,
    },
    "rexx": {
        "positive": [
            r"(?i)\bSAY\b",
            r"(?i)\bPARSE\b",
            r"(?i)/\*\s*REXX",
        ],
        "negative": [r"#!/bin/bash"],
        "min_matches": 1,
    },
    "jcl": {
        "positive": [
            r"^//\w+\s+(JOB|EXEC|DD)\b",
            r"\bDSN=",
            r"\bPGM=",
        ],
        "negative": [],
        "min_matches": 2,
    },
    "hlasm": {
        "positive": [
            r"(?i)\bCSECT\b",
            r"(?i)\bUSING\b",
            r"(?i)\bDS\s+\d*[CFHXPAZ]",
        ],
        "negative": [r"\.section\b", r"%rax"],
        "min_matches": 2,
    },
}


def validate_file(filepath, language):
    """Validate file content matches expected language patterns."""
    v = VALIDATORS.get(language)
    if not v:
        return True
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except Exception:
        return False

    for pat in v.get("negative", []):
        if re.search(pat, content):
            return False

    pos = sum(1 for pat in v["positive"] if re.search(pat, content))
    return pos >= v["min_matches"]


def main():
    parser = argparse.ArgumentParser(
        description="Clone known mainframe repos and extract source files"
    )
    parser.add_argument("--output", "-o", default="./output",
                        help="Base output directory")
    parser.add_argument("--clone-dir", default="./clones",
                        help="Where to clone repos")
    parser.add_argument("--min-chars", type=int, default=10000)
    parser.add_argument("--language", "-l", default=None,
                        choices=["pli", "cobol", "rexx", "jcl", "hlasm"],
                        help="Filter to specific language (default: all)")
    args = parser.parse_args()

    clone_base = Path(args.clone_dir)
    clone_base.mkdir(parents=True, exist_ok=True)

    # Filter repos by language if specified
    repos = KNOWN_REPOS
    if args.language:
        repos = [r for r in repos if args.language in r["languages"]]

    print(f"Repos to clone: {len(repos)}")
    print(f"Min file size:  {args.min_chars} chars")
    if args.language:
        print(f"Language:       {args.language}")
    print()

    results_by_lang = {}

    for repo_info in tqdm(repos, desc="Cloning repos"):
        url = repo_info["url"]
        name = url.split("/")[-1].replace(".git", "")
        org = url.split("/")[-2]
        full_name = f"{org}/{name}"
        clone_dir = clone_base / f"{org}__{name}"

        print(f"\n--- {full_name}: {repo_info['desc']} ---")

        if not clone_dir.exists():
            try:
                result = subprocess.run(
                    ["git", "clone", "--depth", "1", url, str(clone_dir)],
                    capture_output=True, text=True, timeout=300,
                )
                if result.returncode != 0:
                    print(f"  Clone failed: {result.stderr[:200]}")
                    continue
            except subprocess.TimeoutExpired:
                print(f"  Clone timed out.")
                continue
        else:
            print(f"  Already cloned.")

        # Scan for each target language
        target_langs = repo_info["languages"]
        if args.language:
            target_langs = [args.language]

        for lang in target_langs:
            ext_set = EXTENSIONS.get(lang, set())
            if not ext_set:
                continue

            if lang not in results_by_lang:
                results_by_lang[lang] = {"files": [], "hashes": set()}

            lang_output = Path(args.output) / lang
            lang_output.mkdir(parents=True, exist_ok=True)

            file_count = 0
            for root, dirs, files in os.walk(clone_dir):
                dirs[:] = [d for d in dirs if d != ".git"]
                for fname in files:
                    if Path(fname).suffix in ext_set or Path(fname).suffix.lower() in {e.lower() for e in ext_set}:
                        fpath = os.path.join(root, fname)
                        try:
                            size = os.path.getsize(fpath)
                        except OSError:
                            continue

                        if size < args.min_chars:
                            continue

                        if not validate_file(fpath, lang):
                            continue

                        # Dedup
                        with open(fpath, "rb") as f:
                            h = hashlib.sha256(f.read()).hexdigest()
                        if h in results_by_lang[lang]["hashes"]:
                            continue
                        results_by_lang[lang]["hashes"].add(h)

                        # Copy
                        dest_name = f"{org}__{name}__{Path(fpath).stem}{Path(fpath).suffix}"
                        dest = lang_output / dest_name
                        shutil.copy2(fpath, dest)
                        results_by_lang[lang]["files"].append({
                            "file": dest_name,
                            "source_repo": full_name,
                            "source_path": os.path.relpath(fpath, clone_dir),
                            "size": size,
                            "hash": h,
                        })
                        file_count += 1

            if file_count > 0:
                print(f"  {lang.upper()}: {file_count} files extracted")

    # Final report
    print(f"\n{'='*70}")
    print(f"FINAL RESULTS — Known Repos Collection")
    print(f"{'='*70}")
    for lang, data in sorted(results_by_lang.items()):
        files = data["files"]
        total_bytes = sum(f["size"] for f in files)
        print(f"  {lang.upper():8s}: {len(files):6d} files, {total_bytes:12,d} bytes "
              f"({total_bytes/1024/1024:.1f} MB)")

        # Save manifest
        lang_output = Path(args.output) / lang
        manifest = lang_output / "manifest_known_repos.json"
        with open(manifest, "w") as f:
            json.dump({
                "language": lang,
                "min_chars": args.min_chars,
                "total_files": len(files),
                "total_bytes": total_bytes,
                "files": files,
            }, f, indent=2)

    print(f"\nOutput directory: {args.output}/")


if __name__ == "__main__":
    main()
