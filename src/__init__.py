# Marker file so `import src.*` works when repo root is on sys.path.

"""
Package marker for project modules under `src/`.

This allows imports like `from src.strategy import Strategy` to work
consistently across different entry points (e.g. future `main.py`).
"""

