"""Configuration module for shared app settings."""

import json
import os
from app.constants import GRAPH_INFO_PATH

# Initialize graph_info
try:
    with open(GRAPH_INFO_PATH) as f:
        graph_info = json.load(f)
except Exception as e:
    print(f"Could not load graph info: {e}")
    graph_info = {}
