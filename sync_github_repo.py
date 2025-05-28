# sync_github_repo.py

import os
import subprocess
import logging
from config import REPO_DIR

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def ensure_repo():
    """
    Ensure that REPO_DIR exists and is a git repo, then pull latest changes.
    """
    # Create the directory if it doesn't exist
    os.makedirs(REPO_DIR, exist_ok=True)

    # Verify it is a git repository
    git_dir = os.path.join(REPO_DIR, ".git")
    if not os.path.isdir(git_dir):
        raise RuntimeError(
            f"{REPO_DIR} is not a valid git repository (no .git folder). "
            "Please clone the repo manually into this location or add 'git_url' to config.json and enable auto-clone."
        )

    # Pull any upstream changes
    try:
        logger.info(f"Pulling latest changes in {REPO_DIR}")
        subprocess.check_call(["git", "pull"], cwd=REPO_DIR)
    except subprocess.CalledProcessError as e:
        logger.error(f"Git pull failed: {e}")
        raise


def sync_with_remote():
    """
    Commit & push any local changes back to your remote.
    Only commits when there is something staged.
    """
    try:
        # Stage all changes
        subprocess.check_call(["git", "add", "--all"], cwd=REPO_DIR)

        # Check if there is anything to commit
        res = subprocess.run(
            ["git", "diff", "--cached", "--quiet"],
            cwd=REPO_DIR
        )
        if res.returncode == 0:
            logger.info("No changes to commit.")
            return

        # Commit & push
        subprocess.check_call(
            ["git", "commit", "-m", "Automated sync by LegiScan tooling"],
            cwd=REPO_DIR
        )
        subprocess.check_call(["git", "push"], cwd=REPO_DIR)
        logger.info("Changes committed and pushed successfully.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to sync to remote: {e}")
        # Optional: re-raise if you want to fail hard
        # raise


if __name__ == "__main__":
    try:
        ensure_repo()
        sync_with_remote()
        print("✅ Repository is up‐to‐date and in sync.")
    except Exception as e:
        logger.error(f"sync_github_repo encountered an error: {e}")
        raise
