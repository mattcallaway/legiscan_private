"""Launch an isolated tracker and exercise its guest workflows in Chromium."""

import os
import site
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
from tempfile import TemporaryDirectory
import time
from contextlib import closing
from urllib.error import URLError
from urllib.request import urlopen

from playwright.sync_api import expect, sync_playwright

ROOT = Path(__file__).resolve().parents[1]


def wait_for_health(url, process):
    for _ in range(100):
        if process.poll() is not None:
            raise RuntimeError("Streamlit exited before it was ready.")
        try:
            with urlopen(f"{url}/_stcore/health", timeout=1) as response:
                if response.status == 200:
                    return
        except (URLError, OSError):
            pass
        time.sleep(0.25)
    raise RuntimeError("Streamlit did not become healthy.")


def main():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    with TemporaryDirectory() as temporary:
        base = Path(temporary)
        env = {
            **os.environ,
            "DATA_DIR": str(base / "data"),
            "REPO_DIR": str(base / "publication"),
            "API_KEY": "",
            "TRACKER_REGISTRATION_MODE": "admin",
            "CONGRESS_API_KEY": "",
            "LEGISCAN_BACKGROUND_JOBS": "0",
            "NOTIFICATION_DATABASE_URL": "",
            "RESEND_API_KEY": "",
            "RESEND_FROM_EMAIL": "",
        }
        executable = sys.executable
        if os.name == "nt" and sys.prefix != sys.base_prefix:
            # Launch the real interpreter so terminating it also releases SQLite files.
            executable = sys._base_executable
            env["PYTHONPATH"] = os.pathsep.join(site.getsitepackages())
        command = [
            executable, "-m", "streamlit", "run", "legiscan_git_sync_update_8_7.py",
            "--server.address", "127.0.0.1", "--server.port", str(port),
            "--server.headless", "true", "--browser.gatherUsageStats", "false",
        ]
        bootstrap = (
            "from auth import register_user; "
            "ok, message = register_user('fixture-admin', 'fixture-smoke-password'); "
            "assert ok, message"
        )
        subprocess.run([executable, "-B", "-c", bootstrap], cwd=ROOT, env=env, check=True)
        with (base / "streamlit.log").open("w+", encoding="utf-8", errors="replace") as log:
            process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
            try:
                wait_for_health(url, process)
                with sync_playwright() as playwright:
                    browser = playwright.chromium.launch(channel=os.environ.get("TRACKER_BROWSER_CHANNEL") or None)
                    page = browser.new_page()
                    page.goto(url)
                    page.get_by_role("button", name="Browse as Guest", exact=False).click(timeout=20_000)
                    expect(page.get_by_role("heading", name="Your legislative workspace")).to_be_visible()
                    page.get_by_role("button", name="Historical Coverage").click()
                    expect(page.get_by_role("heading", name="Historical Coverage")).to_be_visible()
                    expect(page.get_by_text("No California or congressional sessions have been discovered yet.")).to_be_visible()
                    page.get_by_text("Dashboard", exact=True).click()
                    page.get_by_role("button", name="Hearing Calendar", exact=True).click()
                    expect(page.get_by_role("heading", name="Hearing Calendar")).to_be_visible()
                    expect(page.get_by_text("Track a bill to see its published hearings and scheduled events here.")).to_be_visible()
                    page.get_by_text("Advanced Filters", exact=False).click()
                    page.get_by_role("textbox", name="Custom Status Code").fill("99")
                    page.get_by_role("textbox", name="Custom Status Label").fill("Needs research")
                    page.get_by_role("button", name="Save Status Label").click()
                    expect(page.get_by_text("Status label saved.", exact=True)).to_be_visible(timeout=15_000)
                    status_filter = page.get_by_test_id("stMultiSelect").filter(has_text="Status Stage")
                    status_input = status_filter.get_by_role("combobox")
                    status_input.click()
                    status_input.fill("Needs research")
                    expect(page.get_by_text("Needs research [99]", exact=True)).to_be_visible(timeout=15_000)
                    expect(page.locator('[data-testid="stException"]')).to_have_count(0)
                    print("Browser smoke passed: guest dashboard, coverage, calendar, and custom status labels.")
                    admin_page = browser.new_page()
                    admin_page.goto(url)
                    sign_in = admin_page.get_by_role("button", name="Sign In", exact=True)
                    expect(sign_in).to_be_visible(timeout=20_000)
                    expect(admin_page.get_by_role("button", name="Create Account", exact=True)).to_have_count(0)
                    admin_page.get_by_role("textbox", name="Username", exact=True).fill("fixture-admin")
                    admin_page.get_by_label("Password", exact=True).fill("fixture-smoke-password")
                    sign_in.click()
                    expect(admin_page.get_by_role("heading", name="Your legislative workspace")).to_be_visible()
                    admin_page.get_by_text("Admin & Database Tools", exact=False).first.click()
                    expect(admin_page.get_by_role("button", name="Create user account", exact=True)).to_be_visible()
                    print("Browser smoke passed: public signup hidden and administrator enrollment available.")

                    with closing(sqlite3.connect(base / "data" / "bills.db")) as fixture, fixture:
                        fixture.execute("INSERT INTO sessions (session_id,jurisdiction,session_name) VALUES (88,'CA','Fixture')")
                        fixture.executemany(
                            """INSERT INTO bills (bill_id, session_id, jurisdiction, bill_number, title, status_date)
                               VALUES (?,88,'CA',?,?,'2025-01-02')""",
                            ((i, f"AB {i}", "Water policy unique hearing" if i == 1 else "Water policy")
                             for i in range(1, 52)),
                        )
                    admin_page.get_by_role("button", name="All Bills", exact=False).click()
                    expect(admin_page.get_by_role("heading", name="All Bills", exact=True)).to_be_visible(timeout=15_000)
                    search = admin_page.get_by_role("textbox", name="Global Search", exact=False)
                    search.fill("Water policy")
                    search.press("Enter")
                    expect(admin_page.get_by_text("Bills 1–50 of 51", exact=True)).to_be_visible(timeout=15_000)
                    expect(admin_page.get_by_text("Suggested civic topics: Water", exact=True)).to_be_visible()

                    number = admin_page.get_by_test_id("stNumberInput").filter(has_text="Results page").locator("input")
                    number.fill("2")
                    number.press("Enter")
                    expect(admin_page.get_by_text("Bills 51–51 of 51", exact=True)).to_be_visible(timeout=15_000)
                    search.fill("unique hearing")
                    search.press("Enter")
                    expect(admin_page.get_by_text("Bills 1–1 of 1", exact=True)).to_be_visible(timeout=15_000)
                    search.fill("no such title")
                    search.press("Enter")
                    expect(admin_page.get_by_text("0 matching bills", exact=False)).to_be_visible(timeout=15_000)
                    expect(admin_page.locator('[data-testid="stException"]')).to_have_count(0)
                    print("Browser smoke passed: search results, pagination, page clamping and empty results.")
                    admin_page.get_by_text("Topic Inbox", exact=True).click()
                    expect(admin_page.get_by_role("heading", name="Topic Inbox")).to_be_visible()
                    admin_page.get_by_role("textbox", name="Topic name", exact=True).fill("My hearing portfolio")
                    admin_page.get_by_role("textbox", name="Search terms", exact=True).fill("unique hearing")
                    admin_page.get_by_role("button", name="Save topic", exact=True).click()
                    expect(admin_page.get_by_text("My matching tags", exact=True)).to_be_visible(timeout=15_000)
                    expect(admin_page.locator('[data-testid="stException"]')).to_have_count(0)
                    print("Browser smoke passed: private topic created and shown as a tag on an indexed bill.")
                    subprocess.run(
                        [executable, "-B", "-c",
                         "from civic_index import rebuild_civic_index; import sys; rebuild_civic_index(sys.argv[1])",
                         str(base / "data" / "bills.db")],
                        cwd=ROOT, env=env, check=True,
                    )
                    admin_page.get_by_role("button", name="Browse Water bills", exact=True).click()
                    expect(admin_page.get_by_role("heading", name="Civic Tags")).to_be_visible()
                    expect(admin_page.get_by_text("Last indexed", exact=False)).to_be_visible()
                    expect(admin_page.get_by_text("Tag results page", exact=True)).to_be_visible()
                    expect(admin_page.get_by_text("Suggested civic topics: Water", exact=True).first).to_be_visible()
                    expect(admin_page.get_by_text("Water · Bills 1–50 of 51", exact=True)).to_be_visible()
                    print("Browser smoke passed: a bill tag opens the corresponding shared results.")
                    admin_page.get_by_text("View Details & Edit Notes", exact=False).click()
                    admin_page.get_by_role("button", name="Draft a private rule", exact=True).click()
                    expect(admin_page.get_by_role("heading", name="Topic Inbox")).to_be_visible()
                    expect(admin_page.get_by_role("textbox", name="Topic name", exact=True)).to_have_value("Water policy")
                    expect(admin_page.get_by_role("textbox", name="Search terms", exact=True)).to_have_value("water policy")
                    expect(admin_page.get_by_text("My hearing portfolio", exact=True)).to_be_visible()
                    expect(admin_page.locator('[data-testid="stException"]')).to_have_count(0)
                    print("Browser smoke passed: drafting from evidence opens a prefilled form without saving.")

                    admin_page.get_by_role("button", name="⭐ Track", exact=True).click()
                    expect(admin_page.get_by_role("button", name="❌ Untrack", exact=True)).to_be_visible()
                    admin_page.get_by_text("Hearing Calendar", exact=True).click()
                    expect(admin_page.get_by_text("No source calendars are available yet for your tracked bills.")).to_be_visible()
                    admin_page.get_by_text("Refresh tracked-bill calendars", exact=True).click()
                    admin_page.get_by_role("button", name="Check up to 25 tracked bills").click()
                    warning = admin_page.get_by_test_id("stAlertContentWarning")
                    expect(warning.get_by_text("No LegiScan API key is configured", exact=False)).to_be_visible()
                    expect(admin_page.get_by_text("Calendar snapshots saved", exact=False)).to_have_count(0)
                    expect(admin_page.locator('[data-testid="stException"]')).to_have_count(0)
                    print("Browser smoke passed: the calendar explains a missing key without showing a success message.")
                    browser.close()
            except Exception:
                log.seek(0)
                print(log.read(), file=sys.stderr)
                raise
            finally:
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
