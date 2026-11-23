# sync_github_repo.py

import os
import subprocess
from pathlib import Path
from threading import RLock
import logging
from config import DATA_DIR, REPO_DIR

logger = logging.getLogger(__name__)
_git_lock = RLock()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def ensure_repo():
    """
    Confirm that the publishing directory is an existing Git checkout.
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

def sync_with_remote(paths=None):
    """Publish local changes. Return False when Git publication fails."""
    if not REPO_DIR or not (Path(REPO_DIR) / ".git").exists():
        return False
    repo = Path(REPO_DIR).resolve()

    def git(*args, allowed=(0,)):
        result = subprocess.run(
            ["git", *args], cwd=repo, capture_output=True, timeout=30,
        )
        if result.returncode not in allowed:
            raise subprocess.CalledProcessError(result.returncode, ["git", args[0]])
        return result

    try:
        with _git_lock:
            public_paths = {
                path.resolve().relative_to(repo).as_posix()
                for name in ("LegiScan_Enhanced_Full_Tracker.csv", "keywords.json")
                if (path := Path(DATA_DIR, name)).resolve().is_relative_to(repo)
            }
            files = sorted(public_paths) if paths is None else [
                Path(path).resolve().relative_to(repo).as_posix() for path in paths
            ]
            if not public_paths or not set(files).issubset(public_paths):
                return False

            # Include the names from each pending commit, even if a later commit deletes a file.
            pending = git("log", "--format=", "--name-only", "-z", "@{upstream}..HEAD").stdout
            changed = {path.strip("\n") for path in pending.decode("utf-8").split("\0") if path.strip("\n")}
            if changed - public_paths:
                logger.warning("Git publication paused because pending commits include private or unmanaged files.")
                return False
            files = [path for path in files if (repo / path).is_file()]
            if files:
                git("add", "--", *files)
                if git("diff", "HEAD", "--quiet", "--", *files, allowed=(0, 1)).returncode == 1:
                    git("commit", "-m", "Publish legislative snapshot", "--only", "--", *files)
            git("push")
        return True
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        logger.warning("Git publication failed: %s", error)
        return False


if __name__ == "__main__":
    try:
        ensure_repo()
        if not sync_with_remote():
            raise RuntimeError("Git publication failed.")
        print("✅ Repository is up‐to‐date and in sync.")
    except Exception as e:
        logger.error(f"sync_github_repo encountered an error: {e}")
        raise
