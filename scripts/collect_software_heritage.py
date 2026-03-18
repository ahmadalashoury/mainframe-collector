#!/usr/bin/env python3
"""
collect_software_heritage.py — Search the Software Heritage archive for mainframe code.

Software Heritage is the world's largest public source code archive.
It is the upstream source for The Stack v2.

Strategy:
  1. Search for known mainframe repos by their origin URLs (GitHub, GitLab, etc.)
  2. Use the "vault" API to download directory snapshots as tar.gz
  3. Extract and filter files locally

Rate limits:
  - Anonymous: ~60 req/hour
  - Authenticated: higher (request via contact@softwareheritage.org)
  - Get a token at: https://archive.softwareheritage.org/

Usage:
  python3 collect_software_heritage.py --language pli --output ./output/pli
  python3 collect_software_heritage.py --repos-file known_repos.txt --output ./output/mixed
"""

import argparse
import hashlib
import json
import os
import sys
import tarfile
import tempfile
import time
from pathlib import Path

try:
    import requests
    from tqdm import tqdm
except ImportError:
    print("Install: pip install requests tqdm")
    sys.exit(1)


SWH_API = "https://archive.softwareheritage.org/api/1"

# Known repositories containing mainframe code.
# These are the "seed" URLs to look up in Software Heritage.
KNOWN_REPOS = {
    "pli": [
        "https://github.com/RobJTingay/PL1GCC",
        "https://github.com/IBM/zopeneditor-sample",
        "https://github.com/zowe/zowe-pli-language-support",
        "https://github.com/IBA-mainframe-dev/Global-Repository-for-Mainframe-Developers",
    ],
    "cobol": [
        "https://github.com/openmainframeproject/cobol-code-dataset",
        "https://github.com/openmainframeproject/cobol-programming-course",
        "https://github.com/openmainframeproject/cobol-check",
        "https://github.com/IBM/zopeneditor-sample",
        "https://github.com/IBA-mainframe-dev/Global-Repository-for-Mainframe-Developers",
        "https://github.com/OCamlPro/gnucobol",
        "https://github.com/opensourcecobol/opensource-cobol",
        "https://github.com/BroadcomMFD/broadcom-product-scripts",
        "https://github.com/cicsdev/cics-java-liberty-link",
    ],
    "rexx": [
        "https://github.com/IBA-mainframe-dev/Global-Repository-for-Mainframe-Developers",
        "https://github.com/IBM/zopeneditor-sample",
        "https://github.com/RexxLA/rexx-repository",
    ],
    "jcl": [
        "https://github.com/IBA-mainframe-dev/Global-Repository-for-Mainframe-Developers",
        "https://github.com/IBM/zopeneditor-sample",
    ],
    "hlasm": [
        "https://github.com/IBM/zopeneditor-sample",
        "https://github.com/IBA-mainframe-dev/Global-Repository-for-Mainframe-Developers",
    ],
}

EXTENSIONS = {
    "pli": [".pli", ".pl1", ".plinc"],
    "cobol": [".cbl", ".cob", ".cobol", ".cpy"],
    "rexx": [".rexx", ".rex", ".exec"],
    "jcl": [".jcl", ".job", ".proc"],
    "hlasm": [".hlasm", ".asm", ".mac"],
}


class SWHClient:
    def __init__(self, token=None):
        self.session = requests.Session()
        self.session.headers["Accept"] = "application/json"
        if token:
            self.session.headers["Authorization"] = f"Bearer {token}"

    def _get(self, url, params=None, raw=False):
        for attempt in range(5):
            resp = self.session.get(url, params=params, timeout=60, stream=raw)
            if resp.status_code == 200:
                return resp if raw else resp.json()
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 30))
                print(f"  SWH rate limited. Waiting {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code == 404:
                return None
            print(f"  SWH API error {resp.status_code}: {resp.text[:200]}")
            time.sleep(5)
        return None

    def lookup_origin(self, url):
        """Find a repository in the archive by its URL."""
        return self._get(f"{SWH_API}/origin/{url}/get/")

    def origin_visits(self, url, per_page=10):
        """Get visits (snapshots) of a repository."""
        return self._get(f"{SWH_API}/origin/{url}/visits/",
                        params={"per_page": per_page}) or []

    def snapshot(self, snapshot_id):
        """Get snapshot (branch pointers)."""
        return self._get(f"{SWH_API}/snapshot/{snapshot_id}/")

    def revision(self, revision_id):
        """Get revision (commit) metadata."""
        return self._get(f"{SWH_API}/revision/{revision_id}/")

    def directory(self, dir_id):
        """List files in a directory."""
        return self._get(f"{SWH_API}/directory/{dir_id}/") or []

    def content_raw(self, hash_id):
        """Download raw file content."""
        return self._get(f"{SWH_API}/content/sha1_git:{hash_id}/raw/", raw=True)

    def vault_cook_directory(self, dir_id):
        """Request a directory tarball from the vault."""
        resp = self.session.post(f"{SWH_API}/vault/directory/{dir_id}/", timeout=30)
        if resp.status_code in (200, 201):
            return resp.json()
        return None

    def vault_fetch_directory(self, dir_id):
        """Check vault cooking status and download if ready."""
        return self._get(f"{SWH_API}/vault/directory/{dir_id}/")


def walk_swh_directory(client, dir_id, extensions, prefix="", depth=0, max_depth=10):
    """Recursively walk a SWH directory and yield (path, content_hash) for matching files."""
    if depth > max_depth:
        return

    entries = client.directory(dir_id)
    if not entries:
        return

    ext_set = {e.lower() for e in extensions}
    for entry in entries:
        name = entry.get("name", "")
        etype = entry.get("type", "")
        target = entry.get("target", "")
        path = f"{prefix}/{name}" if prefix else name

        if etype == "file":
            if Path(name).suffix.lower() in ext_set:
                yield path, target, entry.get("length", 0)
        elif etype == "dir":
            time.sleep(0.5)  # Be gentle with the API
            yield from walk_swh_directory(client, target, extensions,
                                         prefix=path, depth=depth+1)


def collect_from_swh(language, output_dir, min_chars, token=None, repos_file=None):
    client = SWHClient(token)
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)

    extensions = EXTENSIONS.get(language, [])
    seen_hashes = set()
    collected = []

    # Determine repos to search
    if repos_file:
        with open(repos_file) as f:
            repo_urls = [line.strip() for line in f if line.strip()]
    else:
        repo_urls = KNOWN_REPOS.get(language, [])

    if not repo_urls:
        print(f"No known repos for {language}. Use --repos-file.")
        return []

    print(f"\nSearching Software Heritage for {language.upper()} code...")
    print(f"Repos to check: {len(repo_urls)}")
    print(f"NOTE: SWH API is slow. This will take a while.\n")

    for repo_url in repo_urls:
        print(f"\n--- {repo_url} ---")

        # Look up the origin
        origin = client.lookup_origin(repo_url)
        if not origin:
            print(f"  Not found in SWH archive.")
            continue
        print(f"  Found in archive.")

        # Get latest visit
        visits = client.origin_visits(repo_url, per_page=1)
        if not visits:
            print(f"  No visits recorded.")
            continue

        visit = visits[0]
        snapshot_id = visit.get("snapshot")
        if not snapshot_id:
            print(f"  No snapshot available.")
            continue

        # Get snapshot to find HEAD/main branch
        snap = client.snapshot(snapshot_id)
        if not snap:
            continue

        branches = snap.get("branches", {})
        # Try common branch names
        target_revision = None
        for bname in ["refs/heads/main", "refs/heads/master", "HEAD"]:
            if bname in branches:
                b = branches[bname]
                if b.get("target_type") == "revision":
                    target_revision = b["target"]
                    break
                elif b.get("target_type") == "alias":
                    # Follow alias
                    alias_target = b.get("target")
                    if alias_target in branches:
                        target_revision = branches[alias_target].get("target")
                        break

        if not target_revision:
            # Take the first revision we find
            for bname, b in branches.items():
                if b.get("target_type") == "revision":
                    target_revision = b["target"]
                    break

        if not target_revision:
            print(f"  No revision found.")
            continue

        # Get revision to find root directory
        rev = client.revision(target_revision)
        if not rev:
            continue

        dir_id = rev.get("directory")
        if not dir_id:
            continue

        print(f"  Walking directory tree (this is slow)...")
        file_count = 0
        for fpath, content_hash, size in walk_swh_directory(
            client, dir_id, extensions
        ):
            if size < min_chars:
                continue

            if content_hash in seen_hashes:
                continue
            seen_hashes.add(content_hash)

            # Download file content
            raw_resp = client.content_raw(content_hash)
            if raw_resp is None:
                continue

            try:
                content = raw_resp.content.decode("utf-8", errors="replace")
            except Exception:
                continue

            if len(content) < min_chars:
                continue

            # Save
            repo_name = repo_url.split("github.com/")[-1] if "github.com" in repo_url \
                       else repo_url.split("/")[-1]
            safe_name = (f"swh__{repo_name.replace('/', '__')}__"
                        f"{Path(fpath).stem}{Path(fpath).suffix}")
            safe_name = "".join(c if c.isalnum() or c in "._-" else "_" for c in safe_name)

            dest = output / safe_name
            with open(dest, "w", encoding="utf-8") as f:
                f.write(content)

            collected.append({
                "file": safe_name,
                "source": "software-heritage",
                "source_repo": repo_url,
                "source_path": fpath,
                "swh_hash": content_hash,
                "size": len(content),
            })
            file_count += 1
            time.sleep(0.5)  # API courtesy

        print(f"  Collected {file_count} files from this repo.")

    # Report
    print(f"\n{'='*60}")
    print(f"RESULTS: {language.upper()} from Software Heritage")
    print(f"{'='*60}")
    print(f"Files collected:  {len(collected)}")
    total = sum(c["size"] for c in collected)
    print(f"Total size:       {total:,} bytes ({total/1024/1024:.1f} MB)")

    manifest = output / "manifest_swh.json"
    with open(manifest, "w") as f:
        json.dump({"language": language, "files": collected,
                   "total_files": len(collected), "total_bytes": total}, f, indent=2)

    return collected


def main():
    parser = argparse.ArgumentParser(
        description="Collect mainframe code from Software Heritage"
    )
    parser.add_argument("--language", "-l", required=True,
                        choices=list(KNOWN_REPOS.keys()))
    parser.add_argument("--output", "-o", default="./output")
    parser.add_argument("--min-chars", type=int, default=10000)
    parser.add_argument("--token", default=None,
                        help="SWH bearer token (get from archive.softwareheritage.org)")
    parser.add_argument("--repos-file", default=None,
                        help="File with one repo URL per line to search")
    args = parser.parse_args()

    token = args.token or os.environ.get("SWH_TOKEN")
    collect_from_swh(args.language, args.output, args.min_chars, token,
                     args.repos_file)


if __name__ == "__main__":
    main()
