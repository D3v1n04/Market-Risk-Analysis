# Part 01 — Environment & Repository Setup

## Outcome

At the end of this part, one command—`make check`—proves that the development
foundation is healthy. That is our first real milestone.

## 1. Repository and working tree

A **Git repository** is a project folder with a hidden `.git/` database. Git stores
snapshots called commits. The files you can currently edit are the **working tree**.

Try:

```bash
git status
git branch --show-current
```

After making the first commit from the checklist, try this small exercise: add a
harmless line to this guide, run `git diff`, then undo that line manually. Explain
why `git diff` is useful before the next commit. Before the first commit, these files
are still **untracked**, so ordinary `git diff` does not display their contents.

## 2. Python project and virtual environment

`pyproject.toml` is the project's machine-readable identity card. It declares the
Python version, package information, dependencies, and tool settings.

A **virtual environment** (`.venv/`) is an isolated box of Python packages for this
project. It prevents one project's dependency versions from silently changing
another project's behavior.

`uv` creates the environment and resolves dependencies:

```bash
make setup
```

Small exercise: run `uv run python --version`, then `python3 --version`. Are they the
same today? Why is the first command still safer for a project command?

## 3. Manifest versus lockfile

`pyproject.toml` is the **manifest**: it describes acceptable dependency ranges.
`uv.lock` is the **lockfile**: it records the exact versions selected for a repeatable
installation. Commit both. Do not commit `.venv/`; it is generated from them.

Small exercise: find `pytest` in both files. Which file expresses a range and which
records a specific version?

## 4. Source, tests, and configuration

The `src/` layout keeps application code separate from repository utilities and
tests. A function that calculates Value at Risk belongs under `src/`; evidence that
the function behaves correctly belongs under `tests/`; input observations belong
under `data/`.

The repository folder is `Market-Risk-Analysis`, the installable distribution uses
lowercase hyphens (`market-risk-analysis`), and Python imports use lowercase
underscores (`market_risk_analysis`). The Hatch configuration in
`pyproject.toml` explicitly connects those two names.

`Settings.from_environment(...)` demonstrates **externalized configuration**:
deployments can change a value without changing source code. `.env.example` documents
safe variable names, while `.env` is ignored because it may contain secrets.

Try:

```bash
uv run market-risk-check
MARKET_RISK_ENV=practice uv run market-risk-check
```

Small exercise: identify the one diagnostic line changed by the second command.

## 5. Tests, linting, and the quality gate

- A **test** executes code and checks an expected behavior.
- A **linter** reads code to find likely defects and consistency problems.
- A **formatter** rewrites layout consistently.
- A **quality gate** combines checks that must pass before code is accepted.

Try each stage separately, then together:

```bash
make lint
make test
make env
make check
```

Small exercise: temporarily change an assertion in `tests/test_config.py`, verify that
`make test` fails, then restore it and verify that the test passes. A useful test must
be capable of failing for the right reason.

## 6. Why data is ignored

Git is excellent for small text source files, not large generated datasets. Market
data can also be licensed or sensitive. `.gitignore` therefore excludes everything
under `data/` except its README. Later jobs will recreate the layer directories.

The planned layers are raw, bronze, silver, and gold. Think of them as progressively
more trustworthy forms of the same underlying information. Their detailed contracts
belong in a later part.

## Explain-back gate

Before Part 02, explain these in your own words without copying the definitions:

1. What is the difference between the repository, working tree, and a commit?
2. Why do we commit `uv.lock` but ignore `.venv/`?
3. What belongs in `src/`, `tests/`, and `data/`?
4. Why should secrets never be placed in `.env.example` or committed?
5. What different problems do tests and linting catch?
6. What evidence does `make check` provide, and what does it *not* prove?

## Troubleshooting

- `uv: command not found`: install `uv`, open a new terminal, and run `uv --version`.
- Wrong Python: install Python 3.12; `.python-version` tells compatible version
  managers what this repository expects.
- Failed dependency install: confirm network access, then retry `uv sync --all-groups`.
- Failed diagnostic: read the individual `FAIL` line before changing anything.
