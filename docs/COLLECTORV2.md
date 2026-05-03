# Collectorv2 — User Guide

> Daily YouTube competitor data collector. Pulls the latest *N* videos per channel via the YouTube Data API v3, writes per-channel Parquet snapshots, and tracks daily API quota usage as an append-only JSONL ledger.

It's idempotent per `(date, channel_id)`, resilient to partial failures (exponential-backoff retries), and aborts before the daily 10 000-unit quota is exhausted.

---

## 1. Setup

### 1.1 Virtual environment

```bash
# From the project root
python -m venv venv

# Activate — Windows (PowerShell)
venv\Scripts\Activate.ps1

# Activate — Windows (Git Bash / CMD)
venv\Scripts\activate

# Activate — Linux / macOS
source venv/bin/activate

# Install dependencies
pip install -r ingestion/requirements.txt
```

### 1.2 API key

```bash
cp .env.example .env
# Open .env and paste your key after YOUTUBE_API_KEY=
# Get one at: https://console.cloud.google.com/apis/credentials
# Restrict it to the YouTube Data API v3.
```

### 1.3 Verify

```bash
python scripts/API_test.py
# Should print 5 video titles for "data analytics for beginners"
```

---

## 2. Attributes

Each attribute toggles one field fetched per video. Listed in `config.yaml` under `attributes:` — comment out any you don't want. Fewer attributes = narrower rows, **same API cost** (one `videos.list` call returns all parts in 1 unit).

| Attribute | Type | What it is | API part |
|---|---|---|---|
| `views` | int-string | Total view count | `statistics` |
| `likes` | int-string | Total like count (0 if hidden) | `statistics` |
| `comments` | int-string | Total comment count (0 if disabled) | `statistics` |
| `duration` | ISO 8601 | e.g. `PT4M13S` — parse to seconds downstream | `contentDetails` |
| `tags` | pipe-delimited | e.g. `fitness\|longevity\|diet` | `snippet` |
| `description` | string | First 500 chars of the video description | `snippet` |
| `thumbnail_url` | URL | High-res thumbnail URL | `snippet` |
| `category_id` | int-string | YouTube category ID ([official list](https://developers.google.com/youtube/v3/docs/videoCategories/list)) | `snippet` |

**Example — narrow run for velocity tracking only:**

```yaml
# config.yaml
attributes:
  - views
  - likes
  - comments
```

---

## 3. Running it

### 3.1 Basic — uses every default from `config.yaml`

```bash
python ingestion/Collectorv2.py
```

### 3.2 CLI arguments

| Flag | Default | Purpose |
|---|---|---|
| `--format {parquet,csv}` | from config (`parquet`) | Output format |
| `--channels HANDLE [HANDLE ...]` | from config (all ~12) | Subset of channels to collect |
| `--max-videos INT` | from config (`10`) | Videos per channel (1–50) |

**Environment variable:**

| Variable | Purpose |
|---|---|
| `COLLECTOR_CONFIG` | Path to an alternate config file (default: `config.yaml`) |
| `YOUTUBE_API_KEY` | Your API key (read via `python-dotenv` from `.env`) |

### 3.3 Common recipes

```bash
# Tiny safe run — one channel, three videos (~4 API units)
python ingestion/Collectorv2.py --channels SiimLand --max-videos 3

# Output as CSV instead of Parquet (eyeballable in Excel)
python ingestion/Collectorv2.py --format csv

# Subset of channels
python ingestion/Collectorv2.py --channels SiimLand Physionic DrBradStanfield

# Maximum allowed videos per channel
python ingestion/Collectorv2.py --max-videos 50

# Use an alternate config file (e.g. for a different niche)
COLLECTOR_CONFIG=config_finance.yaml python ingestion/Collectorv2.py
```

---

## 4. Output

```
data/raw/video_stats/
└── date=2026-04-19/
    ├── channel_id=UCxxxxxxx1/
    │   └── video_stats.parquet        # one row per video fetched
    └── channel_id=UCxxxxxxx2/
        └── video_stats.parquet
```

Each row carries `ingestion_timestamp` (UTC ISO string) so downstream models know *when* within a day the snapshot was captured. Re-runs overwrite per-channel (bronze snapshot semantics — last run wins).

---

## 5. Quota awareness

Before any API call, the collector reads today's `logs/quota/YYYY-MM-DD.jsonl`, estimates this run's cost, and decides:

- **< 80 %** → OK, proceeds
- **80–95 %** → Soft warn, proceeds with a loud log line
- **≥ 95 %** → Hard abort, **no API calls made**, outcome recorded in the ledger

Typical full run (12 channels × 10 videos) costs ~27 units — deep inside the 10 000-unit daily budget.

---

## 6. See also

- [ARCHITECTURE.md](ARCHITECTURE.md) — full design rationale, chapter-by-chapter narrative, target medallion architecture
- [../README.md](../README.md) — project landing page and roadmap
