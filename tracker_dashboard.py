"""Dashboard, workspace navigation, and shared current-session refresh controls."""

from datetime import date, datetime, timedelta, timezone
import sqlite3

import streamlit as st

from hearing_calendar import PACIFIC, legislative_today, upcoming_calendar_events
from job_manager import CORPUS_JOB_TYPES
from job_runner import current_session, sync_selected_sessions


JURISDICTIONS = {"CA": "California", "US": "U.S. Congress"}
SEARCH_SCOPES = {
    "California and Congress": tuple(JURISDICTIONS.values()),
    **{name: (name,) for name in JURISDICTIONS.values()},
}
QUICK_ACTIONS = ("⭐ Tracked Bills", "Hearing Calendar", "Topic Inbox")
WORKFLOWS = {
    "Discover bills": {
        "🔍 All Bills": ("All Bills", "Search bills across current and historical sessions."),
        "🏷️ Keyword Matches": ("Keyword Matches", "Browse bills that match the shared keyword list."),
        "Civic Tags": ("Civic Tags", "Explore related bills by subject."),
    },
    "Track and review": {
        "⭐ Tracked Bills": ("Tracked Bills", "Save notes, set follow-up dates, and compare bills."),
        "Hearing Calendar": ("Hearing Calendar", "See published events and download calendar dates."),
        "Topic Inbox": ("Topic Inbox", "Create private tags and track phrases across bills."),
        "Daily Review": ("Daily Review", "Review changes, follow-ups, and new topic matches."),
        "Email Notifications": ("Email Notifications", "Set up alerts for bills you track."),
    },
    "Research people and sessions": {
        "Historical Coverage": ("Historical Coverage", "Check which sessions, bill texts, and votes are indexed."),
        "👔 Legislator Directory": ("Legislator Directory", "Look up California legislators, staff, and their work."),
        "Federal Voting Profiles": ("Federal Voting Profiles", "Explore recorded congressional votes by member."),
        "🛠️ Staff Analytics": ("Staff Analytics", "Explore staff assignments and roster changes."),
    },
    "Administration": {
        "API Quota": ("API Quota", "Review LegiScan usage and adjust the shared request budget."),
    },
}
DESTINATIONS = {
    view: (name, section, hint)
    for section, views in WORKFLOWS.items() for view, (name, hint) in views.items()
}


def render_workspace_breadcrumb(view, navigate):
    if view not in DESTINATIONS:
        return
    label, group, _ = DESTINATIONS[view]
    with st.container(horizontal=True, vertical_alignment="center"):
        st.button("Dashboard", key="workspace_home", type="tertiary", on_click=navigate, args=("Dashboard",))
        st.caption(f"› {group} › {label}")


def _show_queued_refreshes(job_manager, key):
    queued = st.session_state.get(key, {})
    if not queued:
        return
    st.button("Check refresh status", key=f"{key}:check")
    for code, job_id in queued.items():
        job = job_manager.get_job(job_id)
        if not job:
            continue
        status = job["status"].title()
        details = job["error_summary"] or job["progress_message"]
        if job["status"] == "SUCCESS":
            details = f'{job["new_items"]:,} new · {job["updated_items"]:,} updated'
        st.caption(" · ".join(filter(None, (JURISDICTIONS[code], status, details))))


def render_current_bill_sync(jurisdictions, *, corpus, job_manager, username, background, has_key, key, label):
    """Share the current-session workflow between the dashboard and the admin tools."""
    if not corpus or not job_manager:
        st.info("Bill refresh is temporarily unavailable.")
        return

    year = legislative_today().year
    sessions = corpus.get_cached_sessions()
    for code in jurisdictions:
        session = current_session((item for item in sessions if item["jurisdiction"] == code), year)
        if session:
            action = "Refresh updates" if session.get("last_bootstrap") else "First-time bulk import"
            st.caption(f'{JURISDICTIONS[code]} · {session["session_name"]} · {action}')
        else:
            st.caption(f"{JURISDICTIONS[code]} · Finds the current session when you refresh")
    if not has_key:
        st.info("A LegiScan key is required to refresh bills.")
    busy = not background and any(job["job_type"] in CORPUS_JOB_TYPES for job in job_manager.get_running_jobs())
    if busy:
        st.info("A sync is running. Check Recent Jobs for progress.")

    if st.button(label, key=f"{key}:run", type="primary", width="stretch",
                 disabled=not jurisdictions or not has_key or busy):
        if background:
            queued = st.session_state.setdefault(f"{key}:jobs", {})
            for code in jurisdictions:
                try:
                    queued[code] = job_manager.enqueue_job(
                        "current_session_sync", code, "CURRENT", {"year": year}, initiated_by=username,
                    )
                except (ValueError, sqlite3.Error):
                    st.error(f"Could not queue the refresh for {JURISDICTIONS[code]}.")
        else:
            progress = st.progress(0, text="Preparing session sync…")
            outcomes = sync_selected_sessions(
                corpus, jurisdictions, year, job_manager,
                lambda fraction, message: progress.progress(min(fraction, 1.0), text=message),
            )
            progress.empty()
            for code, outcome in outcomes.items():
                if "error" in outcome:
                    st.error(f'{JURISDICTIONS[code]} · {outcome["error"]}')
                    continue
                stats = outcome["stats"]
                action = "Bulk import" if outcome["importing"] else "Refresh"
                result = (f'{JURISDICTIONS[code]} · {outcome["session"]} · {action} '
                          f'({stats["new"]:,} new, {stats["updated"]:,} updated)')
                if stats["errors"]:
                    reason = stats.get("request_stop_reason") or f'Incomplete with {stats["errors"]:,} errors'
                    st.warning(f"{result}. {reason}.")
                else:
                    st.success(f"{result} completed.")
    if background:
        _show_queued_refreshes(job_manager, f"{key}:jobs")


def _render_at_a_glance(corpus, tracked_count, topic_count, today):
    st.subheader("At a glance")
    stats = corpus.get_corpus_stats() if corpus else {}
    sessions = corpus.get_cached_sessions() if corpus else []
    tracked, topics, california, federal = st.columns(4)
    tracked.metric("Tracked bills", f"{tracked_count:,}", border=True)
    topics.metric("Saved topics", f"{topic_count:,}", border=True)
    for code, label, column in (("CA", "California", california), ("US", "Federal", federal)):
        count = stats.get(f"{code.lower()}_bills")
        with column:
            st.metric(f"{label} bills indexed", f"{count:,}" if count is not None else "—", border=True)
            session = current_session((item for item in sessions if item["jurisdiction"] == code), today.year)
            stamp = max(session.get("last_bootstrap") or "", session.get("last_masterlist") or "") if session else ""
            if stamp:
                synced_at = datetime.fromisoformat(stamp)
                if synced_at.tzinfo is None:
                    synced_at = synced_at.replace(tzinfo=timezone.utc)
                st.caption(f'Current session synced {synced_at.astimezone(PACIFIC):%b %d, %Y · %I:%M %p %Z}')
            elif corpus:
                st.caption("No completed sync recorded for the current session.")


def _due_follow_ups(tracked_ids, notes, today, days):
    cutoff = today + timedelta(days=days - 1)
    follow_ups = []
    for bill_id in tracked_ids:
        raw_date = notes.get(bill_id, {}).get("follow_up")
        if not raw_date:
            continue
        try:
            due = date.fromisoformat(raw_date)
        except (ValueError, TypeError):
            continue
        if due <= cutoff:
            follow_ups.append((due, bill_id))
    return sorted(follow_ups)


def _render_happening_soon(corpus, tracked_bills, notes, today, load_tracked_rows, open_bill):
    st.subheader("Happening soon")
    if not tracked_bills:
        st.info("Track a bill to see its published events here. Add a follow-up date in its notes to set your own deadline.")
        return

    days = 14
    limit = 4
    st.caption("Next 14 days, including today. Dates use Pacific time. Overdue follow-ups appear first.")
    events_column, follow_ups_column = st.columns(2)
    with events_column, st.container(border=True):
        st.markdown("#### Published events")
        ids = [bill for bill in tracked_bills if bill.isascii() and bill.isdecimal()]
        calendars = corpus.get_bill_calendars(ids) if corpus else {}
        events = upcoming_calendar_events(calendars, today, days)
        if not calendars:
            st.caption("Source calendars are unavailable for the tracked bills. Open Hearing Calendar to check coverage.")
        else:
            st.caption(f"Source calendars available for {len(calendars):,} of {len(tracked_bills):,} tracked bills.")
            if not events:
                st.caption("No upcoming events appear in the available source calendars.")
        for event in events[:limit]:
            st.button(f'{event["bill_number"]} · {event["type"]}', key=f'dashboard:event:{event["event_id"]}',
                      on_click=open_bill, args=(event["bill_id"], event["bill_number"]), width="stretch")
            when = date.fromisoformat(event["date"]).strftime("%a, %b %d")
            time = f'{event["time"]} (source time)' if event["time"] else ""
            st.caption(" · ".join(filter(None, (when, time, event["location"]))))
        if len(events) > limit:
            st.caption(f"Showing {limit} of {len(events):,} events. Open Hearing Calendar for the full schedule.")

    with follow_ups_column, st.container(border=True):
        st.markdown("#### Your follow-ups")
        due = _due_follow_ups(tracked_bills, notes, today, days)
        if not due:
            st.caption("You have no dated follow-ups in this window. Add one in a tracked bill’s notes.")
        else:
            rows = load_tracked_rows([bill_id for _, bill_id in due[:limit]])
            numbers = {str(row["bill_id"]): row["bill_number"] for row in rows.to_dict("records")}
            for when, bill_id in due[:limit]:
                number = numbers.get(bill_id) or f"Bill {bill_id}"
                due_label = "Overdue" if when < today else "Due today" if when == today else "Due"
                st.button(f"{number} · {due_label} {when:%b %d}", key=f"dashboard:follow_up:{bill_id}",
                          on_click=open_bill, args=(bill_id, number), width="stretch")
        if len(due) > limit:
            st.caption(f"Showing {limit} of {len(due):,} follow-ups. Open Tracked Bills to see your notes.")


def render_dashboard(*, corpus, tracked_bills, notes, topic_count, destinations, navigate, find_bills,
                     load_tracked_rows, open_bill, is_guest, refresh_options):
    today = legislative_today()
    st.header("Your legislative workspace")
    if is_guest:
        st.caption("You’re browsing as a guest. Sign in to keep your watchlist, notes, and topics between visits.")

    with st.form("dashboard_bill_search", border=True):
        st.subheader("Find bills")
        query, scope, submit = st.columns([3, 2, 1], vertical_alignment="bottom")
        query.text_input("Bill number, issue, or sponsor", placeholder="e.g. AB 12, affordable housing",
                         key="dashboard_search_query")
        scope.selectbox("Search jurisdiction", list(SEARCH_SCOPES), key="dashboard_search_scope")

        def start_search():
            find_bills(st.session_state.dashboard_search_query.strip(),
                       SEARCH_SCOPES[st.session_state.dashboard_search_scope])

        submit.form_submit_button("Search bills", type="primary", width="stretch", on_click=start_search)

    actions = st.columns(len(QUICK_ACTIONS) + bool(refresh_options))
    for column, view in zip(actions, QUICK_ACTIONS):
        name, _, hint = DESTINATIONS[view]
        column.button(name, key=f"dashboard:{view}", help=hint, width="stretch", on_click=navigate, args=(view,))
    if refresh_options:
        popover = actions[-1].popover("Refresh bills", width="stretch", key="dashboard_refresh_menu", on_change="rerun")
        if popover.open:
            with popover:
                st.caption("Refresh the current session for one or both jurisdictions.")
                codes = st.multiselect("Jurisdictions to refresh", list(JURISDICTIONS), default=["CA"],
                                       format_func=JURISDICTIONS.__getitem__, key="dashboard_refresh_scope")
                render_current_bill_sync(codes, **refresh_options, key="dashboard_refresh", label="Refresh selected bills")

    with st.expander("New here? Start with these three steps"):
        st.markdown(
            "1. **Find a bill.** Search by bill number or issue, then narrow the results by session.\n"
            "2. **Track what matters.** Open a bill and select **Track**. Use its notes for your position, priority, and follow-up date.\n"
            "3. **Come back to review.** Check **Happening soon** for upcoming events and follow-ups, or open **Topic Inbox** to watch an issue."
        )

    try:
        _render_at_a_glance(corpus, len(tracked_bills), topic_count, today)
    except (sqlite3.Error, ValueError):
        st.warning("Bill counts or sync times are temporarily unavailable.")
    try:
        _render_happening_soon(corpus, tracked_bills, notes, today, load_tracked_rows, open_bill)
    except (sqlite3.Error, ValueError):
        st.warning("Upcoming events or follow-ups are temporarily unavailable.")

    st.subheader("Explore the workspace")
    groups = [(heading, choices) for heading, choices in WORKFLOWS.items() if any(view in destinations for view in choices)]
    for column, (heading, choices) in zip(st.columns(len(groups)), groups):
        with column, st.container(border=True):
            st.markdown(f"#### {heading}")
            for view, (label, hint) in choices.items():
                if view in QUICK_ACTIONS:
                    continue
                if view in destinations:
                    st.button(label, key=f"dashboard:{view}", width="stretch", on_click=navigate, args=(view,))
                    st.caption(hint)
                elif view == "Daily Review" and not is_guest:
                    st.caption("Enable Daily Review in the sidebar for a queue of updates and follow-ups.")
            if heading == "Track and review":
                st.caption("Open Tracked Bills to compare legislation, or Topic Inbox to follow an issue.")
