# config.py
import os
import json

# Load config.json
cfg_path = os.path.join(os.path.dirname(__file__), "config.json")
with open(cfg_path, 'r', encoding='utf-8') as f:
    cfg = json.load(f)

# Expand any environment variables in paths
REPO_DIR = os.path.expandvars(cfg.get("repo_dir", ""))
DATA_DIR = os.path.expandvars(cfg.get("data_dir", ""))
API_KEY  = cfg.get("api_key", "")

# Ensure directories exist
if REPO_DIR:
    os.makedirs(REPO_DIR, exist_ok=True)
if DATA_DIR:
    os.makedirs(DATA_DIR, exist_ok=True)
