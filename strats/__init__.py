"""
Strategy examples (`strats/`). Re-export names so `main.py` can import strategies explicitly.
"""

from src.strategy import Strategy, StateStrategy
from strats.pedro import Pedro
from strats.test import Test

__all__ = ["Test", "Pedro"]
