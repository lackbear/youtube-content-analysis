# Make reference

Shortcuts over the raw `docker compose` CLI. Run `make help` anytime for a terminal-friendly summary.

> **Windows note:** `make` isn't bundled with Windows. Install with `winget install GnuWin32.Make`, then add `C:\Program Files (x86)\GnuWin32\bin` to your user PATH.

---

## Collector commands

| Command | What it does | API cost |
|---|---|---|
| `make build` | Builds the `youtube-collector:0.3.0` image (respects layer cache) | 0 units |
| `make rebuild` | Same, but throws away the cache (`--no-cache`). Use when something feels stuck | 0 |
| `make run` | One-off collection with all config defaults (~12 channels × 10 videos) | ~27 units |
| `make run-csv` | Same as `run` but outputs CSV instead of Parquet | ~27 units |
| `make run-debug` | Tiny safe run: 1 channel, 3 videos | ~4 units |
| `make shell` | Interactive `bash` inside the collector container | 0 |
| `make logs` | Last 200 lines of the most recent collector container's logs | 0 |
| `make quota-today` | Prints today's API unit usage (reads `logs/quota/*.jsonl` on the host) | 0 |

## General / cleanup

| Command | What it does |
|---|---|
| `make help` | Prints all targets with one-line descriptions |
| `make clean` | `docker compose down` + removes dangling images |
| `make prune` | ⚠️ Nuclear: wipes **all** unused Docker resources system-wide (not just this project) |

---

## Common workflows

### Test a collector change quickly

```bash
make run-debug            # 1 channel, 3 videos, ~4 API units
```

### Full daily collection (manual trigger)

```bash
make run                  # uses every default from config.yaml
```

### Poke around inside the container

```bash
make shell                # drops you into bash
# then: python -c "import Collectorv2; print(Collectorv2._cfg)"
```

### CSV output for eyeballing in Excel

```bash
make run-csv              # writes data/raw/.../video_stats.csv
```

### End-of-day cleanup

```bash
make clean                # stop containers + prune dangling images
```

---

## See also

- [COLLECTORV2.md](COLLECTORV2.md) — run the collector without Docker (via venv + direct Python)
- [ARCHITECTURE.md](ARCHITECTURE.md) — why the Makefile exists, the Docker layering rationale

---

*Chapter 4 will add Airflow-related targets (`airflow-up`, `airflow-down`, `airflow-logs`, `airflow-ui`). This doc will be extended at that point.*
