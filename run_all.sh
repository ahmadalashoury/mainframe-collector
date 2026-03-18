#!/bin/sh
#
# run_all.sh — Master script to collect mainframe source code from all sources.
#
# Prerequisites:
#   pip install requests datasets tqdm huggingface_hub
#   git (for cloning repos)
#   export GITHUB_TOKEN=ghp_xxxxx      # required for GitHub
#   export GITLAB_TOKEN=glpat-xxxxx    # optional for GitLab
#   export SWH_TOKEN=eyJxxxx           # optional for Software Heritage
#
# Usage:
#   ./run_all.sh pli          # Collect PL/I from all sources
#   ./run_all.sh cobol        # Collect COBOL from all sources
#   ./run_all.sh all          # Collect all mainframe languages
#   ./run_all.sh pli 5000     # PL/I with 5000 char minimum (relaxed)
#

LANGUAGE="${1:-pli}"
MIN_CHARS="${2:-10000}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)/scripts"
BASE_OUTPUT="./collected"
CLONE_DIR="./clones"

upper() {
    echo "$1" | tr '[:lower:]' '[:upper:]'
}

log() { echo "[$(date +%H:%M:%S)] $*"; }
warn() { echo "[$(date +%H:%M:%S)] WARNING: $*"; }
err() { echo "[$(date +%H:%M:%S)] ERROR: $*"; }

collect_language() {
    lang="$1"
    min="$2"
    output="${BASE_OUTPUT}/${lang}"
    LANG_UPPER=$(upper "$lang")

    log "============================================"
    log "Collecting: ${LANG_UPPER} (min ${min} chars)"
    log "============================================"

    mkdir -p "${output}"

    # --- Step 1: Known repos (fastest, guaranteed results) ---
    log ""
    log "STEP 1: Cloning known repositories..."
    python3 "${SCRIPT_DIR}/collect_known_repos.py" \
        --language "$lang" \
        --output "${output}" \
        --clone-dir "${CLONE_DIR}" \
        --min-chars "$min" \
        || warn "Known repos collection had errors (continuing)"

    # --- Step 2: Hugging Face — The Stack ---
    log ""
    log "STEP 2: Downloading from Hugging Face (The Stack)..."
    if [ "$lang" = "cobol" ] || [ "$lang" = "rexx" ]; then
        python3 "${SCRIPT_DIR}/collect_huggingface.py" \
            --source the-stack \
            --language "$lang" \
            --output "${output}" \
            --min-chars "$min" \
            || warn "The Stack download had errors (continuing)"
    else
        warn "The Stack may not have ${LANG_UPPER} — skipping (PL/I is not in GitHub linguist)"
    fi

    # --- Step 3: Hugging Face — Rosetta Code ---
    log ""
    log "STEP 3: Downloading from Rosetta Code..."
    rosetta_lang=""
    case "$lang" in
        pli) rosetta_lang="PL/I" ;;
        cobol) rosetta_lang="COBOL" ;;
        rexx) rosetta_lang="REXX" ;;
    esac
    if [ -n "$rosetta_lang" ]; then
        python3 "${SCRIPT_DIR}/collect_huggingface.py" \
            --source rosetta \
            --language "$rosetta_lang" \
            --output "${output}" \
            --min-chars "$min" \
            || warn "Rosetta Code download had errors (continuing)"
    fi

    # --- Step 4: GitHub search ---
    log ""
    log "STEP 4: Searching GitHub (requires GITHUB_TOKEN)..."
    if [ -n "${GITHUB_TOKEN:-}" ]; then
        python3 "${SCRIPT_DIR}/collect_github.py" \
            --language "$lang" \
            --output "${output}" \
            --clone-dir "${CLONE_DIR}" \
            --min-chars "$min" \
            --code-search \
            || warn "GitHub collection had errors (continuing)"
    else
        warn "GITHUB_TOKEN not set — skipping GitHub search"
    fi

    # --- Step 5: GitLab search ---
    log ""
    log "STEP 5: Searching GitLab..."
    python3 "${SCRIPT_DIR}/collect_gitlab.py" \
        --language "$lang" \
        --output "${output}" \
        --min-chars "$min" \
        || warn "GitLab collection had errors (continuing)"

    # --- Step 6: Software Heritage (slow, supplementary) ---
    log ""
    log "STEP 6: Searching Software Heritage..."
    python3 "${SCRIPT_DIR}/collect_software_heritage.py" \
        --language "$lang" \
        --output "${output}" \
        --min-chars "$min" \
        || warn "SWH collection had errors (continuing)"

    # --- Step 7: Validate and dedup ---
    log ""
    log "STEP 7: Validating, deduplicating, and reporting..."
    python3 "${SCRIPT_DIR}/validate_and_report.py" \
        --input "${output}" \
        --language "$lang" \
        --final "${output}/final" \
        --min-chars "$min"

    log ""
    log "Done collecting ${LANG_UPPER}!"
    log "Final files: ${output}/final/"
}


# Main
if [ "$LANGUAGE" = "all" ]; then
    for lang in pli cobol rexx jcl hlasm; do
        collect_language "$lang" "$MIN_CHARS"
    done
else
    collect_language "$LANGUAGE" "$MIN_CHARS"
fi

log ""
log "============================================"
log "ALL DONE"
log "============================================"
log "Results in: ${BASE_OUTPUT}/"
log ""
log "Next steps:"
log "  1. Review ./collected/<lang>/final/manifest_final.json"
log "  2. If file counts are below target:"
log "     - Try relaxing --min-chars to 5000"
log "     - Add more repo URLs to collect_known_repos.py"
log "     - Search GitHub web UI manually for niche repos"
log "  3. Feed final files to your Keras model training pipeline"
