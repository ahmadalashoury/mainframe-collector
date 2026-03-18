#!/usr/bin/env python3
"""
collect_github.py — Discover and clone GitHub repositories containing mainframe source code.

Strategy:
  1. Search for REPOSITORIES (not individual files) to avoid the brutal
     code-search rate limit (10 req/min). Repo search uses the general
     rate limit (5,000 req/hr authenticated).
  2. Clone each repo with --depth 1 (no history).
  3. Walk the clone and extract files matching target extensions.
  4. Filter by minimum size (default 10,000 chars).
  5. Copy qualifying files into an organized output directory.

Usage:
  export GITHUB_TOKEN=ghp_xxxxx
  python3 collect_github.py --language pli --output ./output/pli
  python3 collect_github.py --language cobol --output ./output/cobol
  python3 collect_github.py --language rexx --output ./output/rexx
  python3 collect_github.py --language jcl --output ./output/jcl
  python3 collect_github.py --language hlasm --output ./output/hlasm
"""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

try:
    import requests
    from tqdm import tqdm
except ImportError:
    print("Install dependencies: pip install requests tqdm")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Language configuration
# ---------------------------------------------------------------------------

LANG_CONFIG = {
    "pli": {
        "extensions": [".pli", ".pl1", ".plinc"],
        # GitHub doesn't recognize PL/I — we must use keyword + path queries
        "repo_queries": [
            "PLI mainframe",
            "PL/I source code",
            "PL1 compiler",
            "PL/I z/OS",
            "pl1 mainframe programming",
            "PLI CICS",
            "PLI DB2",
            "PROCEDURE OPTIONS MAIN pl1",
            "zopeneditor PLI",
            "enterprise PLI",
            "iron spring PL/I",
            "PL1GCC",
            "pli language support",
            "mainframe PL/I samples",
            "pl1 examples mainframe",
        ],
        "code_queries": [
            # These hit the code search endpoint (10 req/min!) — use sparingly
            "PROCEDURE OPTIONS(MAIN) path:*.pli",
            "DECLARE FIXED BINARY path:*.pli",
            "DCL CHAR VARYING path:*.pl1",
            "PUT SKIP LIST path:*.pli",
            "%INCLUDE path:*.pli",
            "PROC OPTIONS(MAIN) path:*.pl1",
            "ALLOCATE BASED path:*.pli",
            "ON ENDFILE path:*.pli",
            "FETCH path:*.pli",
            "BEGIN DECLARE path:*.pl1",
        ],
        # Patterns that indicate the file is genuinely PL/I
        "positive_patterns": [
            r"(?i)\bPROC(EDURE)?\b.*\bOPTIONS\s*\(",
            r"(?i)\bDCL\b|\bDECLARE\b",
            r"(?i)\bFIXED\s+(BIN|BINARY|DEC|DECIMAL)\b",
            r"(?i)\bCHAR(ACTER)?\s*\(",
            r"(?i)\bPUT\s+(SKIP\s+)?LIST\b",
            r"(?i)\bGET\s+(LIST|EDIT|DATA)\b",
            r"(?i)\b%INCLUDE\b",
            r"(?i)\bALLOCATE\b.*\bBASED\b",
            r"(?i)\bON\s+(ENDFILE|ERROR|CONVERSION|OVERFLOW)\b",
            r"(?i)\bDO\s+WHILE\b.*;\s*$",
        ],
        # Patterns that indicate this is NOT PL/I (Perl, etc.)
        "negative_patterns": [
            r"#!/usr/bin/perl",
            r"\buse\s+strict\b",
            r"\bmy\s+\$",
            r"\bsub\s+\w+\s*\{",
            r"\bpackage\s+\w+::",
            r"require\s+['\"]",
        ],
        "min_positive_matches": 2,
    },
    "cobol": {
        "extensions": [".cbl", ".cob", ".cobol", ".cpy"],
        "repo_queries": [
            "language:COBOL",
            "COBOL mainframe",
            "COBOL z/OS",
            "COBOL CICS",
            "COBOL DB2",
            "COBOL VSAM",
            "GnuCOBOL",
            "cobol-check",
            "cobol programming course",
            "cobol examples",
            "enterprise COBOL",
            "COBOL copybook",
            "mainframe COBOL samples",
            "COBOL batch processing",
            "COBOL IMS",
        ],
        "code_queries": [
            'language:COBOL "IDENTIFICATION DIVISION"',
            'language:COBOL "PROCEDURE DIVISION"',
            'language:COBOL "WORKING-STORAGE SECTION"',
            'language:COBOL "PERFORM" "MOVE"',
            'language:COBOL "EVALUATE" "WHEN"',
            '"IDENTIFICATION DIVISION" "DATA DIVISION" path:*.cbl',
        ],
        "positive_patterns": [
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
        ],
        "negative_patterns": [],
        "min_positive_matches": 3,
    },
    "rexx": {
        "extensions": [".rexx", ".rex", ".exec", ".rxj", ".rxo"],
        "repo_queries": [
            "REXX mainframe",
            "REXX z/OS",
            "REXX TSO",
            "REXX ISPF",
            "ooRexx",
            "Regina REXX",
            "REXX exec",
            "REXX scripts mainframe",
            "topic:rexx",
            "REXX utilities",
            "REXX examples",
            "zVM REXX",
        ],
        "code_queries": [
            '"SAY" "PARSE" path:*.rexx',
            '"ADDRESS TSO" path:*.rexx',
            '"EXECIO" path:*.rexx',
            '"SIGNAL ON" path:*.rexx',
            '"DO" "END" "SAY" path:*.rex',
        ],
        "positive_patterns": [
            r"(?i)\bSAY\b",
            r"(?i)\bPARSE\b.*\b(ARG|VAR|PULL|VALUE|SOURCE|VERSION)\b",
            r"(?i)\bADDRESS\s+(TSO|ISPEXEC|MVS|SYSCALL)\b",
            r"(?i)\bEXECIO\b",
            r"(?i)\bSIGNAL\s+ON\b",
            r"(?i)\bDO\b.*\bEND\b",
            r"(?i)/\*\s*REXX\s*\*/",
            r"(?i)^\s*/\*",
        ],
        "negative_patterns": [
            r"(?i)#!/bin/(ba)?sh",
            r"(?i)\bfunction\s+\w+\s*\(\)",
        ],
        "min_positive_matches": 2,
    },
    "jcl": {
        "extensions": [".jcl", ".job", ".proc"],
        "repo_queries": [
            "JCL mainframe",
            "JCL z/OS",
            "JCL job",
            "JCL examples",
            "JCL procedures",
            "JCL PROC",
            "mainframe JCL samples",
            "MVS JCL",
            "JCL batch",
        ],
        "code_queries": [
            '"//JOB" "//EXEC" path:*.jcl',
            '"DD DSN=" "DISP=" path:*.jcl',
            '"//STEP" "PGM=" path:*.jcl',
        ],
        "positive_patterns": [
            r"^//\w+\s+JOB\b",
            r"^//\w+\s+EXEC\b",
            r"^//\w+\s+DD\b",
            r"\bDSN=",
            r"\bDISP=",
            r"\bPGM=",
            r"\bSYSIN\b",
            r"\bSYSOUT\b",
        ],
        "negative_patterns": [],
        "min_positive_matches": 3,
    },
    "hlasm": {
        "extensions": [".asm", ".hlasm", ".mac", ".s"],
        "repo_queries": [
            "HLASM mainframe",
            "HLASM z/OS",
            "assembler mainframe z/OS",
            "MVS assembler",
            "370 assembler",
            "mainframe assembler samples",
            "HLASM macro",
        ],
        "code_queries": [
            '"CSECT" "USING" path:*.asm',
            '"BALR" "USING" path:*.hlasm',
            '"DS" "DC" "CSECT" path:*.asm',
        ],
        "positive_patterns": [
            r"(?i)\bCSECT\b",
            r"(?i)\bUSING\b.*,",
            r"(?i)\bBALR\b",
            r"(?i)\bDS\s+\d*[CFHXPAZ]",
            r"(?i)\bDC\s+[CFHXPAZ]",
            r"(?i)\bSTM\b",
            r"(?i)\bLR\b",
            r"(?i)\bMVCL?\b",
        ],
        "negative_patterns": [
            r"(?i)\.section\b",
            r"(?i)\.globl\b",
            r"(?i)%rax|%rsp|%rbp",
            r"(?i)\bmov[lqbw]?\b.*%",
        ],
        "min_positive_matches": 3,
    },
}


# ---------------------------------------------------------------------------
# GitHub API helpers
# ---------------------------------------------------------------------------

class GitHubClient:
    BASE = "https://api.github.com"

    def __init__(self, token: str):
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github+json",
        })
        self.search_count = 0
        self.code_search_count = 0

    def _get(self, url, params=None) -> dict:
        for attempt in range(5):
            resp = self.session.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 403:
                # Rate limited — wait and retry
                reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
                wait = max(reset - time.time(), 10)
                print(f"  Rate limited. Waiting {wait:.0f}s...")
                time.sleep(wait + 1)
                continue
            if resp.status_code == 422:
                print(f"  Validation error for query. Skipping.")
                return {"items": [], "total_count": 0}
            resp.raise_for_status()
        return {"items": [], "total_count": 0}

    def search_repos(self, query: str, per_page=100, max_pages=10):
        """Search for repositories. Uses general rate limit (5000/hr)."""
        all_repos = []
        for page in range(1, max_pages + 1):
            data = self._get(f"{self.BASE}/search/repositories", params={
                "q": query,
                "sort": "stars",
                "order": "desc",
                "per_page": per_page,
                "page": page,
            })
            items = data.get("items", [])
            if not items:
                break
            all_repos.extend(items)
            self.search_count += 1
            total = data.get("total_count", 0)
            if len(all_repos) >= total or len(all_repos) >= 1000:
                break
            time.sleep(1)  # Be polite
        return all_repos

    def search_code(self, query: str, per_page=100, max_pages=5):
        """Search code. CAUTION: 10 req/min rate limit."""
        all_results = []
        for page in range(1, max_pages + 1):
            data = self._get(f"{self.BASE}/search/code", params={
                "q": query,
                "per_page": per_page,
                "page": page,
            })
            items = data.get("items", [])
            if not items:
                break
            all_results.extend(items)
            self.code_search_count += 1
            # Code search: strict 10/min limit
            time.sleep(7)  # ~8.5 requests/min to stay safe
            if len(all_results) >= 500:
                break
        return all_results


# ---------------------------------------------------------------------------
# File collection
# ---------------------------------------------------------------------------

def clone_repo(clone_url: str, dest: str, timeout=180) -> bool:
    """Shallow-clone a repo. Returns True on success."""
    try:
        result = subprocess.run(
            ["git", "clone", "--depth", "1", "--single-branch", clone_url, dest],
            capture_output=True, text=True, timeout=timeout,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, Exception) as e:
        print(f"  Clone failed: {e}")
        return False


def find_matching_files(repo_dir: str, extensions: list, min_chars: int) -> list:
    """Walk a cloned repo and return paths of files matching extensions + size."""
    matches = []
    ext_set = {e.lower() for e in extensions}
    for root, dirs, files in os.walk(repo_dir):
        # Skip .git
        dirs[:] = [d for d in dirs if d != ".git"]
        for fname in files:
            if Path(fname).suffix.lower() in ext_set:
                fpath = os.path.join(root, fname)
                try:
                    size = os.path.getsize(fpath)
                    if size >= min_chars:
                        matches.append(fpath)
                except OSError:
                    pass
    return matches


def validate_content(filepath: str, positive_patterns: list,
                     negative_patterns: list, min_matches: int) -> bool:
    """Check file content against language-specific patterns."""
    try:
        with open(filepath, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except Exception:
        return False

    # Check negative patterns first
    for pat in negative_patterns:
        if re.search(pat, content):
            return False

    # Count positive pattern matches
    pos_count = sum(1 for pat in positive_patterns if re.search(pat, content))
    return pos_count >= min_matches


def file_hash(filepath: str) -> str:
    """SHA256 of file content for dedup."""
    h = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Main collection pipeline
# ---------------------------------------------------------------------------

def collect(language: str, output_dir: str, min_chars: int, clone_dir: str,
            use_code_search: bool, github_token: str):
    config = LANG_CONFIG[language]
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    clones = Path(clone_dir)
    clones.mkdir(parents=True, exist_ok=True)

    client = GitHubClient(github_token)
    seen_repos = set()
    seen_hashes = set()
    collected = []

    # --- Phase 1: Repository search ---
    print(f"\n{'='*60}")
    print(f"Phase 1: Searching GitHub for {language.upper()} repositories")
    print(f"{'='*60}")

    repo_urls = []
    for query in config["repo_queries"]:
        print(f"\n  Query: {query}")
        repos = client.search_repos(query, per_page=100, max_pages=3)
        for repo in repos:
            url = repo["clone_url"]
            if url not in seen_repos:
                seen_repos.add(url)
                repo_urls.append((repo["full_name"], url, repo.get("stargazers_count", 0)))
        print(f"  Found {len(repos)} repos ({len(repo_urls)} unique total)")
        time.sleep(1)

    # Sort by stars descending — popular repos more likely to have real code
    repo_urls.sort(key=lambda x: x[2], reverse=True)
    print(f"\nTotal unique repositories to scan: {len(repo_urls)}")

    # --- Phase 2: Clone and extract ---
    print(f"\n{'='*60}")
    print(f"Phase 2: Cloning repos and extracting {language.upper()} files")
    print(f"{'='*60}")

    for i, (name, url, stars) in enumerate(tqdm(repo_urls, desc="Repos")):
        repo_clone_dir = clones / name.replace("/", "__")
        if repo_clone_dir.exists():
            # Already cloned in a previous run
            pass
        else:
            ok = clone_repo(url, str(repo_clone_dir))
            if not ok:
                continue

        matches = find_matching_files(
            str(repo_clone_dir), config["extensions"], min_chars
        )
        for fpath in matches:
            # Validate content
            if not validate_content(
                fpath,
                config["positive_patterns"],
                config["negative_patterns"],
                config["min_positive_matches"],
            ):
                continue

            # Dedup
            fh = file_hash(fpath)
            if fh in seen_hashes:
                continue
            seen_hashes.add(fh)

            # Copy to output
            ext = Path(fpath).suffix
            dest_name = f"{name.replace('/', '__')}__{Path(fpath).stem}{ext}"
            dest_path = output / dest_name
            shutil.copy2(fpath, dest_path)
            collected.append({
                "file": dest_name,
                "source_repo": name,
                "source_path": fpath,
                "size": os.path.getsize(fpath),
                "hash": fh,
            })

        # Optionally clean up clones to save disk
        # shutil.rmtree(repo_clone_dir, ignore_errors=True)

    # --- Phase 3: Code search (optional, slow) ---
    if use_code_search and config.get("code_queries"):
        print(f"\n{'='*60}")
        print(f"Phase 3: GitHub code search for additional {language.upper()} files")
        print(f"(SLOW — 10 req/min limit)")
        print(f"{'='*60}")

        code_repos = set()
        for query in config["code_queries"]:
            print(f"\n  Query: {query}")
            results = client.search_code(query, per_page=100, max_pages=2)
            for item in results:
                repo_name = item["repository"]["full_name"]
                repo_url = f"https://github.com/{repo_name}.git"
                if repo_url not in seen_repos:
                    code_repos.add((repo_name, repo_url))
            print(f"  Found {len(results)} code results, {len(code_repos)} new repos")

        # Clone newly found repos
        for name, url in tqdm(code_repos, desc="Code-search repos"):
            seen_repos.add(url)
            repo_clone_dir = clones / name.replace("/", "__")
            if not repo_clone_dir.exists():
                ok = clone_repo(url, str(repo_clone_dir))
                if not ok:
                    continue

            matches = find_matching_files(
                str(repo_clone_dir), config["extensions"], min_chars
            )
            for fpath in matches:
                if not validate_content(
                    fpath, config["positive_patterns"],
                    config["negative_patterns"], config["min_positive_matches"],
                ):
                    continue
                fh = file_hash(fpath)
                if fh in seen_hashes:
                    continue
                seen_hashes.add(fh)
                ext = Path(fpath).suffix
                dest_name = f"{name.replace('/', '__')}__{Path(fpath).stem}{ext}"
                dest_path = output / dest_name
                shutil.copy2(fpath, dest_path)
                collected.append({
                    "file": dest_name,
                    "source_repo": name,
                    "source_path": fpath,
                    "size": os.path.getsize(fpath),
                    "hash": fh,
                })

    # --- Report ---
    print(f"\n{'='*60}")
    print(f"RESULTS: {language.upper()}")
    print(f"{'='*60}")
    print(f"Repositories scanned:   {len(seen_repos)}")
    print(f"Files collected:        {len(collected)}")
    total_bytes = sum(c["size"] for c in collected)
    print(f"Total size:             {total_bytes:,} bytes ({total_bytes/1024/1024:.1f} MB)")
    print(f"Output directory:       {output_dir}")
    print(f"API calls (repo search):{client.search_count}")
    print(f"API calls (code search):{client.code_search_count}")

    # Save manifest
    manifest_path = output / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump({
            "language": language,
            "min_chars": min_chars,
            "total_files": len(collected),
            "total_bytes": total_bytes,
            "repos_scanned": len(seen_repos),
            "files": collected,
        }, f, indent=2)
    print(f"Manifest saved:         {manifest_path}")

    return collected


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Collect mainframe source code from GitHub"
    )
    parser.add_argument("--language", "-l", required=True,
                        choices=list(LANG_CONFIG.keys()),
                        help="Target language")
    parser.add_argument("--output", "-o", default="./output",
                        help="Output directory for collected files")
    parser.add_argument("--clone-dir", default="./clones",
                        help="Directory for temporary repo clones")
    parser.add_argument("--min-chars", type=int, default=10000,
                        help="Minimum file size in characters (default: 10000)")
    parser.add_argument("--code-search", action="store_true",
                        help="Also use code search (slow, 10 req/min)")
    parser.add_argument("--token", default=None,
                        help="GitHub token (or set GITHUB_TOKEN env var)")
    args = parser.parse_args()

    token = args.token or os.environ.get("GITHUB_TOKEN")
    if not token:
        print("ERROR: Set GITHUB_TOKEN env var or use --token")
        sys.exit(1)

    collect(
        language=args.language,
        output_dir=args.output,
        min_chars=args.min_chars,
        clone_dir=args.clone_dir,
        use_code_search=args.code_search,
        github_token=token,
    )


if __name__ == "__main__":
    main()
