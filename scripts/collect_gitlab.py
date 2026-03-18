#!/usr/bin/env python3
"""
collect_gitlab.py — Search GitLab.com for mainframe source code.

GitLab's API has better rate limits than GitHub for code search:
  - 10 req/sec for authenticated users
  - Projects and blobs search endpoints

Usage:
  export GITLAB_TOKEN=glpat-xxxxx   # optional, works unauthenticated too
  python3 collect_gitlab.py --language pli --output ./output/pli
  python3 collect_gitlab.py --language cobol --output ./output/cobol
"""

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

try:
    import requests
    from tqdm import tqdm
except ImportError:
    print("Install: pip install requests tqdm")
    sys.exit(1)


LANG_CONFIG = {
    "pli": {
        "extensions": [".pli", ".pl1"],
        "project_queries": [
            "PL/I mainframe",
            "PLI source",
            "PL1 compiler",
            "PL/I z/OS",
        ],
        "blob_queries": [
            "PROCEDURE OPTIONS MAIN",
            "DECLARE FIXED BINARY",
            "DCL CHAR VARYING",
            "PUT SKIP LIST",
        ],
    },
    "cobol": {
        "extensions": [".cbl", ".cob", ".cobol", ".cpy"],
        "project_queries": [
            "COBOL",
            "COBOL mainframe",
            "GnuCOBOL",
            "COBOL z/OS",
            "enterprise COBOL",
        ],
        "blob_queries": [
            "IDENTIFICATION DIVISION",
            "PROCEDURE DIVISION",
            "WORKING-STORAGE SECTION",
        ],
    },
    "rexx": {
        "extensions": [".rexx", ".rex", ".exec"],
        "project_queries": [
            "REXX",
            "REXX mainframe",
            "ooRexx",
            "Regina REXX",
        ],
        "blob_queries": [
            "PARSE ARG",
            "ADDRESS TSO",
            "SAY PARSE",
        ],
    },
    "jcl": {
        "extensions": [".jcl"],
        "project_queries": [
            "JCL mainframe",
            "JCL z/OS",
        ],
        "blob_queries": [
            "//JOB",
            "DD DSN=",
        ],
    },
    "hlasm": {
        "extensions": [".hlasm", ".asm"],
        "project_queries": [
            "HLASM",
            "mainframe assembler",
            "370 assembler",
        ],
        "blob_queries": [
            "CSECT USING",
            "BALR",
        ],
    },
}


class GitLabClient:
    BASE = "https://gitlab.com/api/v4"

    def __init__(self, token=None):
        self.session = requests.Session()
        if token:
            self.session.headers["PRIVATE-TOKEN"] = token

    def _get(self, url, params=None):
        for attempt in range(3):
            resp = self.session.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 10))
                print(f"  Rate limited. Waiting {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code in (401, 403):
                print(f"  Auth error {resp.status_code}. Check GITLAB_TOKEN.")
                return []
            return []
        return []

    def search_projects(self, query, per_page=100):
        return self._get(f"{self.BASE}/search", params={
            "scope": "projects",
            "search": query,
            "per_page": per_page,
        }) or []

    def search_blobs(self, query, per_page=100):
        return self._get(f"{self.BASE}/search", params={
            "scope": "blobs",
            "search": query,
            "per_page": per_page,
        }) or []

    def project_tree(self, project_id, path="", recursive=True, per_page=100):
        return self._get(
            f"{self.BASE}/projects/{project_id}/repository/tree",
            params={"path": path, "recursive": str(recursive).lower(),
                    "per_page": per_page},
        ) or []

    def file_content(self, project_id, file_path, ref="HEAD"):
        import urllib.parse
        encoded = urllib.parse.quote(file_path, safe="")
        return self._get(
            f"{self.BASE}/projects/{project_id}/repository/files/{encoded}/raw",
            params={"ref": ref},
        )


def collect_gitlab(language: str, output_dir: str, min_chars: int, token=None):
    config = LANG_CONFIG[language]
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    client = GitLabClient(token)

    seen_projects = set()
    seen_hashes = set()
    collected = []
    ext_set = {e.lower() for e in config["extensions"]}

    # Phase 1: Find projects
    print(f"\nSearching GitLab for {language.upper()} projects...")
    project_ids = []

    for query in config["project_queries"]:
        print(f"  Query: {query}")
        projects = client.search_projects(query)
        for p in projects:
            pid = p.get("id")
            if pid and pid not in seen_projects:
                seen_projects.add(pid)
                project_ids.append((pid, p.get("path_with_namespace", str(pid)),
                                   p.get("http_url_to_repo", "")))
        time.sleep(0.2)

    # Phase 2: Also find projects via blob search
    for query in config["blob_queries"]:
        print(f"  Blob query: {query}")
        blobs = client.search_blobs(query)
        for b in blobs:
            pid = b.get("project_id")
            if pid and pid not in seen_projects:
                seen_projects.add(pid)
                project_ids.append((pid, str(pid), ""))
        time.sleep(0.2)

    print(f"\nFound {len(project_ids)} unique projects. Scanning for files...")

    # Phase 3: Clone or API-walk each project
    for pid, pname, clone_url in tqdm(project_ids, desc="Projects"):
        if clone_url:
            # Clone approach (faster for large repos)
            clone_dir = Path(f"./gitlab_clones/{pname.replace('/', '__')}")
            if not clone_dir.exists():
                try:
                    subprocess.run(
                        ["git", "clone", "--depth", "1", clone_url, str(clone_dir)],
                        capture_output=True, timeout=120,
                    )
                except Exception:
                    continue

            if clone_dir.exists():
                for root, dirs, files in os.walk(clone_dir):
                    dirs[:] = [d for d in dirs if d != ".git"]
                    for fname in files:
                        if Path(fname).suffix.lower() in ext_set:
                            fpath = os.path.join(root, fname)
                            try:
                                with open(fpath, "r", encoding="utf-8",
                                          errors="replace") as f:
                                    content = f.read()
                            except Exception:
                                continue
                            if len(content) < min_chars:
                                continue
                            h = hashlib.sha256(content.encode()).hexdigest()
                            if h in seen_hashes:
                                continue
                            seen_hashes.add(h)

                            dest_name = (f"gitlab__{pname.replace('/', '__')}__"
                                        f"{Path(fname).stem}{Path(fname).suffix}")
                            dest = output / dest_name
                            with open(dest, "w", encoding="utf-8") as f:
                                f.write(content)
                            collected.append({
                                "file": dest_name,
                                "source": "gitlab",
                                "project": pname,
                                "size": len(content),
                                "hash": h,
                            })

    # Report
    print(f"\n{'='*60}")
    print(f"RESULTS: {language.upper()} from GitLab")
    print(f"{'='*60}")
    print(f"Projects scanned: {len(seen_projects)}")
    print(f"Files collected:  {len(collected)}")
    total = sum(c["size"] for c in collected)
    print(f"Total size:       {total:,} bytes ({total/1024/1024:.1f} MB)")

    manifest = output / "manifest_gitlab.json"
    with open(manifest, "w") as f:
        json.dump({"language": language, "files": collected,
                   "total_files": len(collected), "total_bytes": total}, f, indent=2)

    return collected


def main():
    parser = argparse.ArgumentParser(description="Collect mainframe code from GitLab")
    parser.add_argument("--language", "-l", required=True,
                        choices=list(LANG_CONFIG.keys()))
    parser.add_argument("--output", "-o", default="./output")
    parser.add_argument("--min-chars", type=int, default=10000)
    parser.add_argument("--token", default=None)
    args = parser.parse_args()

    token = args.token or os.environ.get("GITLAB_TOKEN")
    collect_gitlab(args.language, args.output, args.min_chars, token)


if __name__ == "__main__":
    main()
