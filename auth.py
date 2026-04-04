"""
auth.py  —  Multi-user authentication for UFtW Bill Tracker
============================================================
Design principles:
  - Open self-registration: anyone can create an account.
  - First account created automatically becomes admin.
  - Passwords hashed with bcrypt (never stored in plaintext).
  - Per-user data stored under DATA_DIR/users/<username>/
  - Per-user optional LegiScan API key override.
  - Admins can view all users and delete / reset-password any user.
"""
import os
import json
import logging
from datetime import datetime, timezone

import bcrypt
import streamlit as st

from config import DATA_DIR

logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
USERS_FILE   = os.path.join(DATA_DIR, "users.json")
USERS_SUBDIR = os.path.join(DATA_DIR, "users")


# ── User data directory ───────────────────────────────────────────────────────
def get_user_data_dir(username: str) -> str:
    """Return (and create) the personal data directory for a user."""
    d = os.path.join(USERS_SUBDIR, username)
    os.makedirs(os.path.join(d, "uploads"), exist_ok=True)
    return d


# ── Users store (flat JSON) ───────────────────────────────────────────────────
def _load_users() -> dict:
    if not os.path.exists(USERS_FILE):
        return {}
    try:
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"auth: failed to load users file: {e}")
        return {}


def _save_users(users: dict) -> bool:
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        tmp = USERS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(users, f, indent=2)
        os.replace(tmp, USERS_FILE)
        return True
    except Exception as e:
        logger.error(f"auth: failed to save users file: {e}")
        return False


# ── Password helpers ──────────────────────────────────────────────────────────
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
    return st.session_state.get("auth_user")


def is_admin() -> bool:
    u = get_current_user()
    return u is not None and u.get("role") == "admin"


def logout():
    st.session_state.pop("auth_user", None)


def register_user(username: str, password: str, api_key: str = "") -> tuple[bool, str]:
    """
    Create a new user account.
    Returns (success, message).
    The first user ever created is automatically admin.
    """
    username = username.strip().lower()
    if not username:
        return False, "Username cannot be empty."
    if len(username) < 3:
        return False, "Username must be at least 3 characters."
    if not all(c.isalnum() or c in "-_" for c in username):
        return False, "Username may only contain letters, numbers, hyphens and underscores."
    if len(password) < 6:
        return False, "Password must be at least 6 characters."

    users = _load_users()
    if username in users:
        return False, f"Username '{username}' is already taken."

    role = "admin" if not users else "user"   # first user → admin
    users[username] = {
        "password_hash": _hash_password(password),
        "role":          role,
        "api_key":       api_key.strip(),
        "created_at":    datetime.now(timezone.utc).isoformat(),
    }
    if not _save_users(users):
        return False, "Failed to write users file — check data directory permissions."

    get_user_data_dir(username)   # create personal dir
    logger.info(f"auth: new user '{username}' registered (role={role})")
    return True, f"Account created! You are now logged in as '{username}'."


def login_user(username: str, password: str) -> tuple[bool, str]:
    """Verify credentials and set session state. Returns (success, message)."""
    username = username.strip().lower()
    users = _load_users()
    u = users.get(username)
    if u is None or not _check_password(password, u.get("password_hash", "")):
        return False, "Invalid username or password."

    st.session_state.auth_user = {
        "username":  username,
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


def update_password(username: str, new_password: str) -> tuple[bool, str]:
    if len(new_password) < 6:
        return False, "Password must be at least 6 characters."
    users = _load_users()
    if username not in users:
        return False, "User not found."
    users[username]["password_hash"] = _hash_password(new_password)
    ok = _save_users(users)
    return (True, "Password updated.") if ok else (False, "Failed to save.")


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


def admin_delete_user(target: str) -> tuple[bool, str]:
    users = _load_users()
    if target not in users:
        return False, "User not found."
    del users[target]
    ok = _save_users(users)
    return (True, f"User '{target}' deleted.") if ok else (False, "Failed to save.")


def admin_set_role(target: str, role: str) -> tuple[bool, str]:
    users = _load_users()
    if target not in users:
        return False, "User not found."
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

    st.caption("— or sign in / create an account below —")
    st.write("")

    tab_login, tab_register = st.tabs(["🔐 Sign In", "📝 Create Account"])

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
                ok, msg = register_user(ru, rp, rk)
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
    st.subheader("👥 User Management")
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
