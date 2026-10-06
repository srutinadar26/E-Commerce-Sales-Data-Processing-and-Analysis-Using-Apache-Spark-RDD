"""
conftest.py
-----------
Pytest root configuration.
Sets PYTHONPATH so all test imports resolve correctly.
"""

import os
import sys

# Ensure project root is importable from tests
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
