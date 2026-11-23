"""
auth.py  —  Multi-user authentication for UFtW Bill Tracker
============================================================
Design principles:
  - Browser registration follows the deployment's account enrollment policy.
  - First account created automatically becomes admin.
  - Passwords hashed with bcrypt (never stored in plaintext).
  - New workspaces use opaque IDs; existing accounts keep their legacy paths.
  - Per-user optional LegiScan API key override.
  - Admins can view all users and delete / reset-password any user.
"""
import os
import json
import logging
import sqlite3
from contextlib import closing
from functools import wraps
from hashlib import sha256
from time import time
from tempfile import NamedTemporaryFile, TemporaryDirectory
from uuid import uuid4
from datetime import datetime, timezone

import bcrypt
import streamlit as st

from config import DATA_DIR

logger = logging.getLogger(__name__)
LOGIN_WINDOW_SECONDS = 300
LOGIN_ATTEMPT_LIMIT = 8
# Used for constant-cost comparison when an account does not exist.
_UNKNOWN_PASSWORD_HASH = "$2b$12$xq.nT1VViaemG0NDAwWJhumWOxioGR/IfPGXJHUVLLNdM9a6PzoEi"

# ── Paths ─────────────────────────────────────────────────────────────────────
USERS_FILE   = os.path.join(DATA_DIR, "users.json")
USERS_SUBDIR = os.path.join(DATA_DIR, "users")


# ── User data directory ───────────────────────────────────────────────────────
def get_user_data_dir(username: str) -> str:
    """Keep existing workspace paths while assigning new accounts opaque IDs."""
    user = _load_users().get(username)
    if user is None:
        raise ValueError("Account is no longer available.")
    workspace_id = user.get("workspace_id")
    base = os.path.join(DATA_DIR, "workspaces") if workspace_id else USERS_SUBDIR
    directory = os.path.join(base, workspace_id or username)
    os.makedirs(os.path.join(directory, "uploads"), exist_ok=True)
    return directory


def get_guest_data_dir() -> str:
    if "guest_workspace" not in st.session_state:
        st.session_state.guest_workspace = TemporaryDirectory(prefix="legislation-guest-")
    return st.session_state.guest_workspace.name


def _serialize_account_write(function):
    @wraps(function)
    def serialized(*args, **kwargs):
        try:
            os.makedirs(DATA_DIR, exist_ok=True)
            # All writers lock the same file even when users.json is atomically replaced.
            with closing(sqlite3.connect(f"{USERS_FILE}.lock.sqlite3", timeout=10)) as lock, lock:
                lock.execute("BEGIN IMMEDIATE")
                return function(*args, **kwargs)
        except (OSError, RuntimeError, sqlite3.Error):
            return False, "Account storage is unavailable."
    return serialized


# ── Users store (flat JSON) ───────────────────────────────────────────────────
def _load_users() -> dict:
    if not os.path.exists(USERS_FILE):
        return {}
    try:
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            users = json.load(f)
        if not isinstance(users, dict) or not all(isinstance(record, dict) for record in users.values()):
            raise ValueError("Invalid account storage")
        return users
    except (OSError, ValueError) as error:
        logger.error("Account storage could not be read: %s", type(error).__name__)
        raise RuntimeError("Account storage is unavailable.") from error


def _save_users(users: dict) -> bool:
    pending = None
    try:
        with NamedTemporaryFile(mode="w", encoding="utf-8", dir=DATA_DIR, delete=False) as file:
            pending = file.name
            json.dump(users, file, indent=2)
            file.flush()
            os.fsync(file.fileno())
        os.replace(pending, USERS_FILE)
        pending = None
        return True
    except (OSError, ValueError) as error:
        logger.error("Account storage could not be written: %s", type(error).__name__)
        return False
    finally:
        if pending:
            try:
                os.unlink(pending)
            except OSError:
                logger.warning("Could not remove temporary account file.")


# ── Password helpers ──────────────────────────────────────────────────────────
def _password_error(password):
    if len(password) < 6:
        return "Password must be at least 6 characters."
    if len(password.encode("utf-8")) > 72:
        return "Password must be at most 72 bytes in UTF-8."
    return None


def _hash_password(plaintext: str) -> str:
    return bcrypt.hashpw(plaintext.encode(), bcrypt.gensalt()).decode()


def _check_password(plaintext: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(plaintext.encode(), hashed.encode())
    except Exception:
        return False


# ── Public API ────────────────────────────────────────────────────────────────
def get_current_user() -> dict | None:
    """Return the logged-in user dict from session state, or None."""
    active = st.session_state.get("auth_user")
    if not active or active.get("is_guest"):
        return active
    try:
        stored = _load_users().get(active["username"])
    except RuntimeError:
        stored = None
    if stored is None or active.get("workspace_id") != stored.get("workspace_id"):
        logout()
        return None
    revision = stored.get("credential_revision")
    if active.get("credential_revision") != revision:
        logout()
        return None
    active["role"] = stored.get("role", "user")
    active["api_key"] = stored.get("api_key", "")
    return active


def is_admin() -> bool:
    u = get_current_user()
    return u is not None and u.get("role") == "admin"


def logout():
    guest = st.session_state.pop("guest_workspace", None)
    if guest is not None:
        guest.cleanup()
    st.session_state.clear()


def registration_mode():
    """Read the enrollment policy. Invalid values disable browser account creation."""
    mode = os.environ.get("TRACKER_REGISTRATION_MODE", "open").strip().lower()
    return mode if mode in {"open", "admin", "disabled"} else "disabled"


@_serialize_account_write
def register_user(username: str, password: str, api_key: str = "") -> tuple[bool, str]:
    """Provision an account from a trusted local operator process."""
    return _create_account(username, password, api_key)


@_serialize_account_write
def register_public_user(username, password, api_key=""):
    if registration_mode() != "open":
        return False, "Public registration is disabled."
    return _create_account(username, password, api_key, public=True)


@_serialize_account_write
def create_managed_user(username, password):
    if registration_mode() == "disabled" or not is_admin():
        return False, "Administrator account creation is unavailable."
    return _create_account(username, password, "", managed=True)


def _create_account(username, password, api_key, *, public=False, managed=False):
    username = username.strip().lower()
    if not username:
        return False, "Username cannot be empty."
    if len(username) < 3:
        return False, "Username must be at least 3 characters."
    if not all(c.isalnum() or c in "-_" for c in username):
        return False, "Username may only contain letters, numbers, hyphens and underscores."
    if error := _password_error(password):
        return False, error

    users = _load_users()
    if username in users:
        if public:
            return False, "Could not create the account with these details."
        return False, "That username is already in use."

    role = "admin" if not users and not managed else "user"
    users[username] = {
        "password_hash": _hash_password(password),
        "workspace_id":  uuid4().hex,
        "role":          role,
        "api_key":       api_key.strip(),
        "created_at":    datetime.now(timezone.utc).isoformat(),
    }
    if not _save_users(users):
        return False, "Failed to write users file — check data directory permissions."

    get_user_data_dir(username)   # create personal dir
    logger.info(f"auth: new user '{username}' registered (role={role})")
    return True, "Account created."


def _reserve_login_attempt(identity, now):
    """Charge an attempt before bcrypt, including competing processes."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with closing(sqlite3.connect(f"{USERS_FILE}.login.sqlite3", timeout=5)) as connection, connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS attempts (identity TEXT PRIMARY KEY, started REAL NOT NULL, count INTEGER NOT NULL)"
        )
        connection.execute("CREATE INDEX IF NOT EXISTS attempts_started ON attempts(started)")
        connection.execute("DELETE FROM attempts WHERE started <= ?", (now - LOGIN_WINDOW_SECONDS,))
        row = connection.execute("SELECT count FROM attempts WHERE identity = ?", (identity,)).fetchone()
        if row and row[0] >= LOGIN_ATTEMPT_LIMIT:
            return False
        connection.execute(
            """INSERT INTO attempts (identity, started, count) VALUES (?, ?, 1)
               ON CONFLICT(identity) DO UPDATE SET count = count + 1""",
            (identity, now),
        )
        return True


def _clear_login_attempts(identity):
    with closing(sqlite3.connect(f"{USERS_FILE}.login.sqlite3", timeout=5)) as connection, connection:
        connection.execute("DELETE FROM attempts WHERE identity = ?", (identity,))


def login_user(username: str, password: str) -> tuple[bool, str]:
    """Read one account snapshot and verify credentials without holding the write lock."""
    username = username.strip().lower()
    invalid = (False, "Invalid username or password.")
    if not username or len(password.encode("utf-8")) > 72:
        return invalid
    try:
        users = _load_users()
        u = users.get(username)
        identity = sha256(username.encode()).hexdigest() if u else "unknown"
        if not _reserve_login_attempt(identity, time()):
            return invalid
        valid = _check_password(password, u.get("password_hash", _UNKNOWN_PASSWORD_HASH) if u else _UNKNOWN_PASSWORD_HASH)
        if not u or not u.get("password_hash") or not valid:
            return invalid
        _clear_login_attempts(identity)
    except (OSError, RuntimeError, sqlite3.Error):
        return False, "Account storage is unavailable."

    st.session_state.auth_user = {
        "username":  username,
        "workspace_id": u.get("workspace_id"),
        "credential_revision": u.get("credential_revision"),
        "role":      u.get("role", "user"),
        "api_key":   u.get("api_key", ""),
        "is_guest":  False,
    }
    return True, "Logged in."


def login_as_guest():
    """Set a guest session that uses the shared DATA_DIR (no personal storage)."""
    st.session_state.auth_user = {
        "username":  "guest",
        "role":      "guest",
        "api_key":   "",
        "is_guest":  True,
    }


def is_guest() -> bool:
    u = get_current_user()
    return u is not None and u.get("is_guest", False)


@_serialize_account_write
def update_password(username: str, new_password: str) -> tuple[bool, str]:
    if error := _password_error(new_password):
        return False, error
    users = _load_users()
    if username not in users:
        return False, "User not found."
    users[username]["password_hash"] = _hash_password(new_password)
    users[username]["credential_revision"] = uuid4().hex
    ok = _save_users(users)
    if ok:
        active = st.session_state.get("auth_user")
        if active and active.get("username") == username:
            active["credential_revision"] = users[username]["credential_revision"]
    return (True, "Password updated.") if ok else (False, "Failed to save.")


@_serialize_account_write
def update_api_key(username: str, api_key: str) -> tuple[bool, str]:
    users = _load_users()
    if username not in users:
        return False, "User not found."
    users[username]["api_key"] = api_key.strip()
    ok = _save_users(users)
    if ok:
        # Refresh session state so the live key is used immediately
        if st.session_state.get("auth_user", {}).get("username") == username:
            st.session_state.auth_user["api_key"] = api_key.strip()
    return (True, "API key updated.") if ok else (False, "Failed to save.")


def admin_list_users() -> list[dict]:
    users = _load_users()
    return [
        {
            "username":   u,
            "role":       v.get("role", "user"),
            "api_key_set": bool(v.get("api_key", "")),
            "created_at": v.get("created_at", "")[:10],
        }
        for u, v in users.items()
    ]


@_serialize_account_write
def admin_delete_user(target: str) -> tuple[bool, str]:
    users = _load_users()
    if target not in users:
        return False, "User not found."
    if users[target].get("role") == "admin" and sum(u.get("role") == "admin" for u in users.values()) <= 1:
        return False, "Keep at least one administrator."
    del users[target]
    ok = _save_users(users)
    return (True, f"User '{target}' deleted.") if ok else (False, "Failed to save.")


@_serialize_account_write
def admin_set_role(target: str, role: str) -> tuple[bool, str]:
    users = _load_users()
    if target not in users:
        return False, "User not found."
    if role not in {"user", "admin"}:
        return False, "Unknown role."
    if role != "admin" and users[target].get("role") == "admin" and sum(u.get("role") == "admin" for u in users.values()) <= 1:
        return False, "Keep at least one administrator."
    users[target]["role"] = role
    ok = _save_users(users)
    return (True, f"Role updated to '{role}'.") if ok else (False, "Failed to save.")


# ── Streamlit UI ──────────────────────────────────────────────────────────────
def render_auth_page() -> bool:
    """
    Renders the login / register page.
    Returns True if the user is already authenticated (or entered as guest).
    If not authenticated, renders the UI and returns False (caller should st.stop()).
    """
    if get_current_user() is not None:
        return True

    st.set_page_config(page_title="UFtW Bill Tracker — Sign In", layout="centered")

    # ── Branding header ──────────────────────────────────────────────────────
    st.markdown(
        """
        <div style='text-align:center; padding: 2rem 0 1rem;'>
            <span style='font-size:3rem;'>🏛️</span>
            <h1 style='margin:0; font-size:2rem;'>UFtW Bill Tracker</h1>
            <p style='color:#888; margin-top:.25rem;'>Track legislation under your own profile</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Guest button (prominent, above the tabs) ──────────────────────────────
    if st.button(
        "👁️  Browse as Guest  (no account needed)",
        use_container_width=True,
        help="View bills and shared data without creating an account. " 
             "Your tracked bills and notes won't be saved between sessions.",
    ):
        login_as_guest()
        st.rerun()

    allow_registration = registration_mode() == "open"
    if allow_registration:
        tab_login, tab_register = st.tabs(["🔐 Sign In", "📝 Create Account"])
    else:
        tab_login = st.container()

    # ── Login ──────────────────────────────────────────────────────────────
    with tab_login:
        with st.form("login_form"):
            lu = st.text_input("Username", placeholder="your-username")
            lp = st.text_input("Password", type="password")
            submitted = st.form_submit_button("Sign In", use_container_width=True, type="primary")

        if submitted:
            ok, msg = login_user(lu, lp)
            if ok:
                st.rerun()
            else:
                st.error(msg)

    if not allow_registration:
        return False

    # ── Register ───────────────────────────────────────────────────────────
    with tab_register:
        users_exist = bool(_load_users())
        if not users_exist:
            st.info(
                "👋 No accounts exist yet. The first account you create will automatically "
                "become the **admin**."
            )

        with st.form("register_form"):
            ru = st.text_input("Choose a username", placeholder="only letters, numbers, - or _")
            rp = st.text_input("Create a password (min 6 chars)", type="password")
            rp2 = st.text_input("Confirm password", type="password")
            rk = st.text_input(
                "LegiScan API Key (optional — you can add this later)",
                type="password",
                placeholder="Paste your key, or leave blank",
                help="Get a free key at legiscan.com. You can also set this later in Account Settings.",
            )
            reg_submitted = st.form_submit_button(
                "Create Account", use_container_width=True, type="primary"
            )

        if reg_submitted:
            if rp != rp2:
                st.error("Passwords do not match.")
            else:
                ok, msg = register_public_user(ru, rp, rk)
                if ok:
                    # Auto-login after registration
                    login_user(ru, rp)
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)

    return False   # not yet authenticated


# ── Account Settings widget (embedded in sidebar) ─────────────────────────────
def render_account_settings_sidebar():
    """
    Renders a sidebar expander with password-change and API-key fields.
    For guests, shows a prompt to sign in or create an account instead.
    """
    user = get_current_user()
    if not user:
        return

    if user.get("is_guest"):
        with st.sidebar.expander("👤 Guest Session"):
            st.caption("You are browsing as a guest. Your tracked bills and notes are saved only for this session.")
            st.write("Create a free account to save your work permanently.")
            if st.button("🔐 Sign In / Create Account", use_container_width=True, key="guest_signin_btn"):
                logout()
                st.rerun()
        return

    with st.sidebar.expander("👤 Account Settings"):
        username = user["username"]
        st.caption(f"Signed in as **{username}** ({user.get('role','user')})")

        st.subheader("🔑 Change Password")
        with st.form("chpw_form"):
            new_pw  = st.text_input("New password", type="password", key="chpw_new")
            new_pw2 = st.text_input("Confirm", type="password", key="chpw_conf")
            if st.form_submit_button("Update Password"):
                if new_pw != new_pw2:
                    st.error("Passwords do not match.")
                else:
                    ok, msg = update_password(username, new_pw)
                    st.success(msg) if ok else st.error(msg)

        st.divider()
        st.subheader("🗝️ LegiScan API Key")
        current_key = user.get("api_key", "")
        st.caption(
            "Your personal key overrides the shared key for all API calls you trigger. "
            "Leave blank to use the shared key. Get a free key at **legiscan.com**."
        )
        with st.form("apikey_form"):
            new_key = st.text_input(
                "API Key", value=current_key,
                type="password" if current_key else "default",
                placeholder="Paste your LegiScan API key",
                key="apikey_input"
            )
            if st.form_submit_button("Save API Key"):
                ok, msg = update_api_key(username, new_key)
                st.success(msg) if ok else st.error(msg)
                if ok:
                    st.rerun()


# ── Admin user management widget ──────────────────────────────────────────────
def render_admin_user_management():
    """
    Renders a user management section to be placed inside the admin tools expander.
    Only meaningful to call if is_admin() is True.
    """
    if not is_admin():
        return
    st.subheader("👥 User Management")
    if registration_mode() != "disabled":
        with st.form("admin_create_user", clear_on_submit=True):
            username = st.text_input("New username")
            password = st.text_input("Temporary password", type="password")
            if st.form_submit_button("Create user account"):
                ok, message = create_managed_user(username, password)
                if ok:
                    st.toast(message)
                    st.rerun()
                else:
                    st.error(message)
    users_list = admin_list_users()

    if not users_list:
        st.caption("No users found.")
        return

    import pandas as pd
    df = pd.DataFrame(users_list)
    df.columns = ["Username", "Role", "API Key Set", "Created"]
    st.dataframe(df, hide_index=True, use_container_width=True)

    st.divider()

    col_a, col_b = st.columns(2)
    usernames = [u["username"] for u in users_list]
    current_user = get_current_user()

    with col_a:
        st.markdown("**Reset a user's password**")
        with st.form("admin_reset_pw"):
            target_reset = st.selectbox("User", usernames, key="admin_reset_target")
            reset_pw     = st.text_input("New password", type="password", key="admin_reset_pw_val")
            if st.form_submit_button("Reset Password"):
                ok, msg = update_password(target_reset, reset_pw)
                st.success(msg) if ok else st.error(msg)

    with col_b:
        st.markdown("**Delete a user account**")
        with st.form("admin_del_user"):
            target_del = st.selectbox("User", usernames, key="admin_del_target")
            if st.form_submit_button("Delete User", type="primary"):
                if target_del == current_user.get("username"):
                    st.warning("You cannot delete your own account while logged in.")
                else:
                    ok, msg = admin_delete_user(target_del)
                    st.success(msg) if ok else st.error(msg)
                    if ok:
                        st.rerun()

    st.divider()
    st.markdown("**Change a user's role**")
    with st.form("admin_role_form"):
        target_role = st.selectbox("User", usernames, key="admin_role_target")
        new_role    = st.selectbox("New role", ["user", "admin"], key="admin_role_val")
        if st.form_submit_button("Update Role"):
            ok, msg = admin_set_role(target_role, new_role)
            st.success(msg) if ok else st.error(msg)
            if ok:
                st.rerun()
