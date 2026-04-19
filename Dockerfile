# syntax=docker/dockerfile:1.7
# ─────────────────────────────────────────────────────────────────────────────
# YouTube Collector — Chapter 3 container image
#
# Design choices (each one is a data-engineering lesson):
#
#   1. python:3.10-slim-bookworm  — Debian-based, ~50 MB, glibc-compatible.
#      NOT alpine: alpine uses musl libc, which breaks binary wheels for
#      pandas/pyarrow/numpy and forces source compilation (slow, fragile,
#      huge). "Alpine for Python" is a tarpit the industry learned the hard
#      way. Slim is the modern default.
#
#   2. Multi-stage build — the `builder` stage has compilers and build tools,
#      the final `runtime` stage has only what's needed to run. Smaller
#      image, smaller attack surface, faster pulls.
#
#   3. Non-root user `collector` — never run as root inside a container if
#      you can avoid it. CVEs escape root containers more easily. This is
#      standard in production Kubernetes (PodSecurityStandards "restricted").
#
#   4. requirements.txt copied BEFORE the code — Docker caches each layer.
#      If only your code changes, the (slow) pip install layer is reused.
#      If requirements change, only then does pip re-run. This alone saves
#      minutes on every rebuild.
#
#   5. PYTHONDONTWRITEBYTECODE + PYTHONUNBUFFERED — no .pyc files (nothing
#      to cache in an ephemeral container), and unbuffered stdout so logs
#      appear in `docker logs` in real time instead of in 4KB chunks.
# ─────────────────────────────────────────────────────────────────────────────


# ── Stage 1: builder ─────────────────────────────────────────────────────────
# We install dependencies here with build tools available, then copy only
# the resulting site-packages into the runtime image.
FROM python:3.10-slim-bookworm AS builder

# Fail fast: any shell command in a RUN that pipes or chains should exit
# with the first non-zero status, not silently pass.
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# System packages needed ONLY to build wheels (gcc for anything without a
# prebuilt wheel on our arch). --no-install-recommends skips "nice-to-have"
# packages that bloat the image.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create a virtualenv inside the builder. This is a modern trick: the whole
# /opt/venv directory is self-contained, so we can copy it wholesale into
# the runtime stage without worrying about system-python pollution.
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Upgrade pip once (deterministic) before installing anything.
RUN pip install --no-cache-dir --upgrade pip==24.2 setuptools wheel

# Copy ONLY requirements.txt first. If it doesn't change between builds,
# Docker reuses the cached layer below and skips the pip install entirely.
# This is the single highest-impact caching trick in Python Dockerfiles.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# ── Stage 2: runtime ─────────────────────────────────────────────────────────
# Lean image. No compilers, no pip cache, no build leftovers. Just Python,
# our virtualenv, and our code.
FROM python:3.10-slim-bookworm AS runtime

# OCI labels — metadata read by registries and tools like `docker inspect`.
# Convention these days is to use the opencontainers.org schema.
LABEL org.opencontainers.image.title="youtube-collector" \
      org.opencontainers.image.description="Daily YouTube competitor stats collector (Collectorv2)" \
      org.opencontainers.image.version="0.3.0" \
      org.opencontainers.image.source="https://github.com/your-user/youtube-data-pipeline"

# Python runtime hygiene:
#   PYTHONDONTWRITEBYTECODE — don't write .pyc files; container is ephemeral.
#   PYTHONUNBUFFERED       — print() / logging output flushed immediately.
#   TZ=UTC                 — every pipeline should run in UTC. Period.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=UTC \
    PATH="/opt/venv/bin:$PATH"

# Copy the built virtualenv from the builder stage. This single line replaces
# re-running pip install in the runtime stage — it's already done.
COPY --from=builder /opt/venv /opt/venv

# Create a non-root user. UID 1000 matches the default first user on most
# Linux distros and on WSL2, which keeps bind-mounted file ownership sane
# between host and container.
#   -r  = system user (no aging)
#   -u  = explicit UID so we can reason about file ownership on mounts
ARG UID=1000
ARG GID=1000
RUN groupadd -r -g ${GID} collector \
    && useradd -r -u ${UID} -g collector -m -d /home/collector -s /bin/bash collector

# Working directory inside the container. Anything we COPY below lands here
# unless otherwise specified. /app is the community convention for
# application code.
WORKDIR /app

# Copy the source. We only bring in what the collector actually needs at
# runtime — NOT the old Collector.py, NOT the venv, NOT the data folder
# (mounted at runtime), NOT the docs. .dockerignore enforces this.
COPY --chown=collector:collector Collectorv2.py ./
COPY --chown=collector:collector config.yaml ./
COPY --chown=collector:collector competitors.csv ./

# Ensure the bind-mount target dirs exist with the right ownership BEFORE
# the container first starts. Without this, a fresh `docker compose up` on
# a machine without ./data and ./logs would mount them as root and the
# collector (running as UID 1000) couldn't write.
RUN mkdir -p /app/data /app/logs /app/logs/quota \
    && chown -R collector:collector /app

# Drop privileges. Everything below this line runs as `collector`, not root.
USER collector

# ENTRYPOINT vs CMD split:
#   ENTRYPOINT fixes the binary we always run (Python + our script).
#   CMD provides the default args, which the user can override on the CLI.
# Effect: `docker run image` runs the collector with defaults,
#         `docker run image --format csv --max-videos 5` passes those args
#         straight into Collectorv2.py's argparse without needing `python`.
ENTRYPOINT ["python", "Collectorv2.py"]
CMD []

# No HEALTHCHECK. This container is a batch job, not a long-running service
# — it exits when the daily collection is done, so "health" is just "exit
# code 0". Airflow (Chapter 4) will read that exit code directly.
