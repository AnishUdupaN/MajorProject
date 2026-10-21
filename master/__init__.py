"""Master package."""
from __future__ import annotations

import sys
from pathlib import Path

# Ensure master directory is in sys.path
_master_dir = str(Path(__file__).resolve().parent)
if _master_dir not in sys.path:
    sys.path.insert(0, _master_dir)

try:
    from master.master import split_active_and_spare_addresses
except ImportError:
    pass
