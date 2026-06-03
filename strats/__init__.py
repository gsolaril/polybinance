"""
Strategy examples (`strats/`). Re-export names so `main.py` can import strategies explicitly.
"""

from src.strategy import Test
from strats.pedro import Pedro

__all__ = ["Test", "Pedro"]
