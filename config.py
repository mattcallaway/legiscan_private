import os
import json

# Load and expand environment variables in config.json
cfg_path = os.path.join(os.path.dirname(__file__), "config.json")
with open(cfg_path, 'r') as f:
    raw = f.read()
# Allows use of $VAR or %VAR% in JSON file
expanded = os.path.expandvars(raw)
cfg = json.loads(expanded)

# Expose configuration values
REPO_DIR = cfg.get("repo_dir")
DATA_DIR = cfg.get("data_dir")
API_KEY = cfg.get("api_key")

# Ensure directories exist
os.makedirs(REPO_DIR, exist_ok=True)
os.makedirs(DATA_DIR, exist_ok=True)
