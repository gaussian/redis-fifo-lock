# redis-fifo-lock — agent guide

Distributed FIFO lock using Redis Streams with strict ordering and crash
recovery. Published to PyPI.

## Repo shape

- Source: `redis_fifo_lock/`
- Tests: `tests/` (pytest, `asyncio_mode = "auto"`) — `uv run --all-extras pytest`
  - Integration tests require a running Redis, reachable via the `REDIS_URL`
    env var (defaults to `redis://localhost:6379/15`).
- Lint + format: `uv run --all-extras ruff check redis_fifo_lock/ tests/` and
  `ruff format --check redis_fifo_lock/ tests/`
- Default working branch: `develop`. Releases flow `develop` → `main`.

## Opening PRs & versioning

`main` is protected: PRs only, and checks (`lint`, `test`) must pass to merge.
The version is a static string in `pyproject.toml`, `redis_fifo_lock/__init__.py`,
and `uv.lock` and is **not** bumped automatically on merge — it must be bumped
deliberately, or no release is cut. Publishing to PyPI is automatic once a
`develop` → `main` PR merges.

**Follow the `create-merge-pr` skill** (`.agents/skills/create-merge-pr/`) for the
full PR workflow, including when and how to bump the version.
