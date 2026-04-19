# ─────────────────────────────────────────────────────────────────────────────
# Makefile — short commands so you don't memorise docker-compose flags.
#
# Run any target with:   make <target>       e.g.  make build, make run
# On Windows you need either WSL2 (recommended, you have this) or
# `choco install make`. Inside WSL2 it Just Works.
# ─────────────────────────────────────────────────────────────────────────────

# .ONESHELL tells make to run ALL lines of a recipe in ONE shell invocation.
# Without this, multi-line heredocs (see `quota-today` below) are broken
# because each line would otherwise start a fresh shell that doesn't know
# about the previous line's heredoc. Modern Makefiles for dev UX use this
# by default.
.ONESHELL:

# .PHONY tells make "these are commands, not files on disk". Without this,
# if a file called `build` ever appeared in the folder, `make build` would
# think there's nothing to do.
.PHONY: help build run run-csv run-debug shell logs quota-today clean prune rebuild airflow-up airflow-down airflow-logs airflow-ui

# Default target when you just type `make`. Prints the list of commands.
help:
	@echo "YouTube Collector — Docker commands"
	@echo ""
	@echo "  make build         Build the collector image (honours requirements.txt changes)"
	@echo "  make rebuild       Build from scratch, ignoring Docker's layer cache"
	@echo "  make run           Run a one-off collection with config defaults"
	@echo "  make run-csv       Run with --format csv (handy for eyeballing output)"
	@echo "  make run-debug     Run with --channels SiimLand --max-videos 3 (tiny, safe)"
	@echo "  make shell         Drop into an interactive bash inside the container"
	@echo "  make logs          Tail the last run's container logs"
	@echo "  make quota-today   Show today's API unit usage from logs/quota/"
	@echo "  make clean         Remove stopped containers + dangling images"
	@echo "  make prune         Aggressive cleanup — removes ALL unused images/volumes"
	@echo ""
	@echo "  Airflow (chapter 4):"
	@echo "  make airflow-up    Start Postgres + Airflow stack (detached)"
	@echo "  make airflow-down  Stop Airflow stack (keeps DAG history in postgres_data)"
	@echo "  make airflow-logs  Tail scheduler + webserver logs"
	@echo "  make airflow-ui    Print the UI URL + admin credentials"

# Build honours the layer cache. Fast on re-runs when requirements.txt hasn't
# changed. `--pull` refreshes the base image so we track upstream CVE fixes.
build:
	docker compose build --pull

# Same as build, but `--no-cache` throws away every cached layer and
# rebuilds from scratch. Use when something feels "stuck" or before
# releasing a new version.
rebuild:
	docker compose build --no-cache --pull

# One-off run of the collector. `--rm` deletes the container after it
# exits, so they don't pile up. Logs still show in your terminal.
run:
	docker compose run --rm collector

# Same run but passing CLI args into Collectorv2.py's argparse.
# The trailing args in `run` come *after* the service name.
run-csv:
	docker compose run --rm collector --format csv

# A safe, minimal dry-run: one channel, three videos. Burns ~4 API units.
# Perfect for testing changes without risking your daily quota.
run-debug:
	docker compose run --rm collector --channels SiimLand --max-videos 3

# Interactive shell inside the container, useful for poking around the
# filesystem or running a one-off `python -c "..."`. `--entrypoint bash`
# overrides the Dockerfile's ENTRYPOINT just for this container.
shell:
	docker compose run --rm --entrypoint bash collector

# Tail the logs of the most recent collector container. `--tail=200`
# prints the last 200 lines; drop it for the full history.
logs:
	docker compose logs --tail=200 collector

# Read today's quota ledger WITHOUT spinning up a container — the file
# lives on your host (bind-mounted), so plain python on the host is fastest.
# One-liner via `python -c` avoids the Makefile-heredoc-tab-indent trap
# (recipe lines in Makefiles are tab-indented, but Python requires zero
# indent at module level — heredocs in recipes break on that).
quota-today:
	@python -c "import json, sys; from pathlib import Path; from datetime import datetime, timezone; d = datetime.now(timezone.utc).date().isoformat(); p = Path(f'logs/quota/{d}.jsonl'); print(f'0 units used today ({d})') if not p.exists() else print(f'{sum(json.loads(l)[\"units_this_run\"] for l in p.read_text().splitlines() if l.strip())} units used today ({d})')"

# Tidy up. `docker compose down` stops and removes the container and
# network. `docker image prune -f` removes "dangling" images (old layers
# from previous builds that nothing tags anymore).
clean:
	docker compose down --remove-orphans
	docker image prune -f

# Nuclear option — wipes ALL unused Docker resources, not just ours.
# Handy when disk fills up; don't run this if you have OTHER Docker
# projects you care about on this machine.
prune:
	docker system prune -af --volumes


# ─────────────────────────────────────────────────────────────────────────────
# Chapter 4 — Airflow stack
# ─────────────────────────────────────────────────────────────────────────────

# Start Postgres + the three Airflow services in the background. The init
# service runs once (db migrate + admin user create) then exits; scheduler
# and webserver stay up via `restart: unless-stopped`. The UI takes ~30s
# to become reachable after this command returns.
airflow-up:
	docker compose up -d postgres airflow-init airflow-scheduler airflow-webserver

# Stop Airflow services cleanly. The `postgres_data` named volume is
# preserved, so DAG run history and the admin account survive restarts.
# To wipe metadata too, add `--volumes`: `docker compose down --volumes`.
airflow-down:
	docker compose down

# Tail scheduler + webserver logs interleaved. Handy when a DAG fails and
# you want to see what's happening in the scheduler without opening the UI.
airflow-logs:
	docker compose logs -f --tail=100 airflow-scheduler airflow-webserver

# Friendly reminder of where the UI lives and what creds to use.
airflow-ui:
	@echo "Airflow UI: http://localhost:8080"
	@echo "Login:      admin / admin   (POC creds — rotate before exposing)"
