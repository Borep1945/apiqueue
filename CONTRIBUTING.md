# Contributing

Use Python 3.11 or newer. From a clean checkout:

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e '.[dev]'
ruff check .
ruff format --check .
pytest -q
python -m build
pip-audit --progress-spinner off
```

Keep changes scoped, add a regression test for behavior changes, and explain
the user-visible result. Tests must run offline; never commit credentials or
private datasets. Open an issue for API changes before investing in a large patch.
The project currently has no promise of API stability before version 1.0.
