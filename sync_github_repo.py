
import os
import subprocess
import platform

REPO_URL = "https://github.com/mattcallaway/legiscan_storage.git"
REPO_NAME = "legiscan_storage"
DEFAULT_PATHS = {
    "Windows": os.path.join(os.environ.get("USERPROFILE", ""), "Documents"),
    "Linux": os.path.expanduser("~/Documents"),
    "Darwin": os.path.expanduser("~/Documents")
}
BASE_DIR = DEFAULT_PATHS.get(platform.system(), os.getcwd())
REPO_DIR = os.path.join(BASE_DIR, REPO_NAME)

def ensure_repo():
    if not os.path.exists(REPO_DIR):
        print(f"Cloning {REPO_NAME} into {BASE_DIR}...")
        subprocess.run(["git", "clone", REPO_URL], cwd=BASE_DIR)
    return REPO_DIR

def sync_with_remote():
    try:
        subprocess.run(["git", "pull"], cwd=REPO_DIR, check=True)
        subprocess.run(["git", "add", "."], cwd=REPO_DIR, check=True)
        subprocess.run(["git", "commit", "-am", "Auto update"], cwd=REPO_DIR, check=False)
        subprocess.run(["git", "push"], cwd=REPO_DIR, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Git sync failed: {e}")
