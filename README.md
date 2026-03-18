# Mainframe Source Code Collector

A toolkit for collecting mainframe-specific source code (PL/I, COBOL, REXX, JCL, HLASM) from multiple public sources for training a Keras language detection model.

## Quick Start

```bash
# 1. Install dependencies
pip install requests datasets tqdm huggingface_hub

# 2. Set your GitHub token
export GITHUB_TOKEN=ghp_your_token_here

# 3. Run the full collection pipeline for PL/I
chmod +x run_all.sh
./run_all.sh pli

# Or for COBOL (much more data available)
./run_all.sh cobol

# Or all languages at once
./run_all.sh all

# To relax minimum file size (useful for PL/I)
./run_all.sh pli 5000
```

## What This Collects

| Source | Method | Rate Limit | Expected Yield |
|--------|--------|-----------|----------------|
| Known repos | `git clone` | None | Fastest, guaranteed |
| Hugging Face | `datasets` lib | Bulk download | COBOL: 1000+, PL/I: 0-200 |
| GitHub repos | Search API + clone | 5000 req/hr | COBOL: 5000+, PL/I: 200-500 |
| GitHub code | Code Search API | **10 req/min** | Supplementary |
| GitLab | Search API + clone | 10 req/sec | COBOL: 200+, PL/I: 20-50 |
| Software Heritage | REST API | 60 req/hr | Supplementary |

## Directory Structure

```
mainframe-collector/
├── run_all.sh                  # Master orchestration script
├── requirements.txt
├── scripts/
│   ├── collect_known_repos.py  # Clone verified repos (START HERE)
│   ├── collect_github.py       # GitHub search + clone
│   ├── collect_huggingface.py  # The Stack + Rosetta Code
│   ├── collect_gitlab.py       # GitLab search
│   ├── collect_software_heritage.py  # SWH archive
│   └── validate_and_report.py  # Dedup, validate, report
├── queries/
│   └── ALL_QUERIES.txt         # Complete query reference
└── collected/                  # Output (created by scripts)
    ├── pli/final/
    ├── cobol/final/
    ├── rexx/final/
    └── ...
```

## Running Individual Scripts

```bash
# Step 1: Known repos (fastest — do this first)
python3 scripts/collect_known_repos.py --language pli --output ./collected/pli

# Step 2: Hugging Face (COBOL only — PL/I not in The Stack)
python3 scripts/collect_huggingface.py --source the-stack --language cobol --output ./collected/cobol

# Step 3: GitHub search
python3 scripts/collect_github.py --language pli --output ./collected/pli --code-search

# Step 4: GitLab
python3 scripts/collect_gitlab.py --language pli --output ./collected/pli

# Step 5: Software Heritage (slow but comprehensive)
python3 scripts/collect_software_heritage.py --language pli --output ./collected/pli

# Step 6: Validate and dedup
python3 scripts/validate_and_report.py --input ./collected/pli --language pli --final ./collected/pli/final
```


## Authentication Tokens

| Service | Env Variable | How to Get |
|---------|-------------|-----------|
| GitHub | `GITHUB_TOKEN` | https://github.com/settings/tokens (Scopes: `public_repo`) |
| GitLab | `GITLAB_TOKEN` | https://gitlab.com/-/user_settings/personal_access_tokens |
| Software Heritage | `SWH_TOKEN` | https://archive.softwareheritage.org/ (create account) |
| Hugging Face | `HF_TOKEN` | https://huggingface.co/settings/tokens (for The Stack v2) |
