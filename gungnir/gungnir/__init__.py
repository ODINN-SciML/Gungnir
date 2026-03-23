"""
Set of tools for the Gungnir
"""

__version__ = "0.0.1"
__all__ = ["utils", "preprocessing", "era5_climate"]

from .utils import *
from .preprocessing import *


def ensure_era5_file_for_gdir(*args, **kwargs):
    from .era5_climate import ensure_era5_file_for_gdir as _ensure_era5_file_for_gdir

    return _ensure_era5_file_for_gdir(*args, **kwargs)

