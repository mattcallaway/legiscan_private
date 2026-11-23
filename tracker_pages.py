"""Independent views rendered by the tracker entry point."""

from datetime import datetime
import logging
import os
import sqlite3

import pandas as pd

import streamlit as st

from hearing_calendar import PACIFIC, calendar_refresh_failure_summary, export_calendar, legislative_today, upcoming_calendar_events
from job_runner import run_calendar_refresh
from legiscanner import US_STATES
from notifications import ACTIVITY_LABELS, Notifications, account_owner, enabled_config
from request_budget import quota_for_directory

logger = logging.getLogger(__name__)


def render_legiscan_quota(data_dir, server_key):
    st.subheader("API Quota")
    try:
        quota = quota_for_directory(data_dir)
        status = quota.summary(server_key)
    except (OSError, sqlite3.Error):
        st.error("LegiScan usage and quota controls are temporarily unavailable.")
        return

    st.caption(f'{status["month"]} · Server key · Calendar month in Pacific time')
    if not server_key.strip():
        st.info("Configure the server LegiScan key to see its usage.")
    tracked, remaining, reserve = st.columns(3)
    tracked.metric("Requests tracked by this app", f'{status["sent"]:,}')
    remaining.metric("Monthly budget left", f'{max(0, status["monthly_limit"] - status["used"]):,}')
    reserve.metric("Reserved for bill syncs", f'{status["reserve"]:,}')
    st.progress(min(1.0, status["used"] / status["monthly_limit"]),
                text=f'{status["used"]:,} of {status["monthly_limit"]:,} budgeted requests used')
    if status["external"]:
        st.caption(f'Includes {status["external"]:,} additional requests based on the reported LegiScan total.')
    if status["pause"]:
        pause = status["pause"]
        until = datetime.fromisoformat(pause["until_utc"]).astimezone(PACIFIC)
        st.warning(f'{pause["reason"]}. The server key is paused until {until:%b %d at %I:%M %p %Z}.')
    elif status["used"] >= status["monthly_limit"]:
        st.warning("The monthly budget is exhausted. Local search and previously saved data remain available.")
    elif status["used"] >= status["monthly_limit"] - status["reserve"]:
        st.warning("The remaining allowance is reserved for bill imports and refreshes.")

    if status["breakdown"]:
        st.markdown("#### Requests by workflow")
        st.dataframe(pd.DataFrame([
            {"Workflow": purpose, "API operation": operation, "Requests": count}
            for (purpose, operation), count in status["breakdown"].most_common()
        ]), hide_index=True)
        st.markdown("#### Daily usage")
        st.bar_chart(pd.DataFrame.from_dict(status["daily"], orient="index", columns=["Requests"]))

    with st.form("legiscan_quota_policy"):
        st.markdown("#### Budget and schedule")
        monthly_limit = st.number_input("Monthly request limit per API key", min_value=1, step=1,
                                        value=status["monthly_limit"])
        reserved = st.number_input("Requests reserved for bill syncs", min_value=0, step=1, value=status["reserve"])
        paused = st.checkbox("Pause scheduled weekday refreshes", value=bool(status["pause_schedule"]))
        st.caption("Manual imports and refreshes remain available during a scheduled pause, subject to the budget.")
        if st.form_submit_button("Save quota settings"):
            try:
                quota.set_policy(monthly_limit, reserved, paused)
            except (ValueError, OSError, sqlite3.Error) as error:
                st.error(str(error) if isinstance(error, ValueError) else "Could not save quota settings.")
            else:
                st.toast("Quota settings saved")
                st.rerun()

    st.link_button("Open LegiScan API profile", "https://legiscan.com/legiscan/")
    with st.form("legiscan_provider_usage"):
        st.markdown("#### Account for earlier or external usage")
        st.caption("Enter the total used shown by LegiScan for the server key and current period. "
                   "Use this when first enabling the tracker, rotating a key on the same account, "
                   "or using the key from another tool.")
        used = st.number_input("Total queries used, as reported by LegiScan", min_value=0, step=1, value=status["used"])
        if st.form_submit_button("Record provider total", disabled=not server_key.strip()):
            try:
                quota.record_provider_usage(server_key, used)
            except (ValueError, OSError, sqlite3.Error):
                st.error("Could not record the provider total.")
            else:
                st.toast("Provider total recorded")
                st.rerun()


def render_email_notifications(username, current_user):
    st.subheader("Email Notifications")
    st.caption("Choose which updates to receive for the bills you track.")
    if not enabled_config():
        st.info("Email alerts are unavailable until PostgreSQL and Resend are configured on the host.")
    else:
        owner = account_owner(username, current_user)
        try:
            notifications = Notifications.from_environment()
            preferences = notifications.get_preferences(owner)
        except Exception as error:
            logger.warning("Notification preferences unavailable: %s", type(error).__name__)
            st.error("Email preferences could not be loaded. Contact the tracker administrator.")
        else:
            verified = preferences.get("email")
            if verified:
                status = "Active" if preferences["enabled"] else "Paused"
                st.write(f"{status} | {verified}")
                label = "Pause alerts" if preferences["enabled"] else "Resume alerts"
                if st.button(label, key="toggle_status_emails"):
                    try:
                        notifications.set_enabled(owner, not preferences["enabled"])
                    except Exception as error:
                        logger.warning("Notification preference update failed: %s", type(error).__name__)
                        st.error("Could not update your alert preference.")
                    else:
                        st.rerun()

                with st.form("notification_activity_form"):
                    kinds = st.multiselect(
                        "Additional alerts", list(ACTIVITY_LABELS),
                        default=preferences.get("activity_types", []), format_func=ACTIVITY_LABELS.__getitem__,
                    )
                    if st.form_submit_button("Save alert types"):
                        try:
                            notifications.set_activity_types(owner, kinds)
                        except ValueError as error:
                            st.warning(str(error))
                        except Exception as error:
                            logger.warning("Alert types could not be changed: %s", type(error).__name__)
                            st.error("Could not save your alert types.")
                        else:
                            st.toast("Alert types saved")
                            st.rerun()

            with st.form("notification_address_form"):
                destination = st.text_input("Email address", value=verified or "", max_chars=254)
                if st.form_submit_button("Send verification code"):
                    try:
                        notifications.request_verification(owner, destination)
                    except ValueError as error:
                        st.warning(str(error))
                    except Exception as error:
                        logger.warning("Notification verification email failed: %s", type(error).__name__)
                        st.error("Could not send the verification email. Try again in 10 minutes.")
                    else:
                        st.toast("Verification email requested")
                        st.rerun()

            if preferences.get("pending_email"):
                with st.form("notification_code_form", clear_on_submit=True):
                    code = st.text_input("Verification code", max_chars=80)
                    if st.form_submit_button("Verify and enable alerts"):
                        try:
                            notifications.verify(owner, code)
                        except ValueError as error:
                            st.warning(str(error))
                        except Exception as error:
                            logger.warning("Notification code verification failed: %s", type(error).__name__)
                            st.error("Could not verify the email address.")
                        else:
                            st.toast("Bill status alerts enabled")
                            st.rerun()
            st.caption("Alerts begin after the first notification pass establishes a baseline.")


def render_historical_coverage(corpus):
    st.subheader("Historical Coverage")
    if corpus is None:
        st.info("The corpus database is unavailable.")
    else:
        coverage = corpus.get_historical_coverage()
        if coverage.empty:
            st.info("No California or congressional sessions have been discovered yet.")
        else:
            jurisdictions = st.multiselect("Jurisdictions", ["California", "U.S. Congress"],
                                           default=["California", "U.S. Congress"], key="coverage_scope")
            coverage["Jurisdiction"] = coverage["jurisdiction"].map({"CA": "California", "US": "U.S. Congress"})
            coverage = coverage[coverage["Jurisdiction"].isin(jurisdictions)].copy()
            coverage["Last complete import"] = coverage["last_bootstrap"]
            coverage["Last complete refresh"] = coverage["last_masterlist"]
            columns = {
                "session_name": "Session", "bills": "Bills indexed",
                "bills_with_text": "Bills with text reference",
                "bills_with_cached_text": "Bills with cached text or PDF link",
                "roll_calls": "Roll calls",
                "with_individual_votes": "Roll calls with individual votes",
            }
            st.dataframe(coverage.rename(columns=columns)[[
                "Jurisdiction", "Session", "Last complete import", "Last complete refresh",
                "Bills indexed", "Bills with text reference", "Bills with cached text or PDF link",
                "Roll calls", "Roll calls with individual votes",
            ]], hide_index=True)
        st.caption("Admins can add older sessions under Manual session import and refresh.")


def render_hearing_calendar(corpus, tracked_bills, job_manager, username, *,
                            is_admin, has_personal_key, load_tracked_rows, open_bill):
    st.subheader("Hearing Calendar")
    st.button("Reload calendar", key="reload_hearing_calendar")
    refresh_notice = st.session_state.pop(f"hearing_refresh_notice:{username}", None)
    if refresh_notice is not None:
        failure = calendar_refresh_failure_summary(refresh_notice)
        if failure:
            st.warning(f"{failure} Existing snapshots were kept for failed or deferred checks.")
        saved = refresh_notice["new"] + refresh_notice["updated"]
        if saved:
            st.success(f"Calendar snapshots saved for {saved:,} tracked {'bill' if saved == 1 else 'bills'}.")
        if refresh_notice["unavailable"]:
            st.info(f'{refresh_notice["unavailable"]:,} bills supplied no usable schedule.')
    if not corpus:
        st.info("The calendar database is unavailable.")
    elif not tracked_bills:
        st.info("Track a bill to see its published hearings and scheduled events here.")
    else:
        ids = [bill for bill in tracked_bills if bill.isascii() and bill.isdecimal()]
        calendars = corpus.get_bill_calendars(ids)
        st.caption(f"Source calendar snapshots available for {len(calendars):,} of {len(ids):,} tracked bills.")
        current = legislative_today()
        horizon = st.selectbox("Show events in the next", [7, 30, 60, 90], index=1,
                               format_func=lambda days: f"{days} days", key="hearing_window")
        include_cancelled = st.checkbox("Show explicitly canceled events", key="show_canceled_hearings")
        events = upcoming_calendar_events(calendars, current, horizon, include_cancelled=include_cancelled)
        if events:
            jurisdictions = sorted({event["jurisdiction"] or "Unknown" for event in events})
            scope = st.multiselect("Jurisdictions", jurisdictions, key="hearing_jurisdiction")
            visible = [event for event in events if not scope or (event["jurisdiction"] or "Unknown") in scope]
            table = pd.DataFrame([{
                "Date": event["date"], "Source time": event["time"] or "Unspecified",
                "Bill": event["bill_number"], "Jurisdiction": event["jurisdiction"],
                "Event": event["type"], "Location": event["location"], "Details": event["description"],
                "Canceled in source": event["cancelled"],
                "Snapshot (UTC)": event["fetched_at"][:16].replace("T", " "),
            } for event in visible])
            st.dataframe(table, hide_index=True)
            choices = {event["event_id"]: event for event in visible}
            selected = st.selectbox(
                "Event details", list(choices), key="hearing_event",
                format_func=lambda key: f'{choices[key]["date"]} | {choices[key]["bill_number"]} | {choices[key]["type"]}',
            )
            if selected:
                event = choices[selected]
                if event["source_url"]:
                    st.link_button("Open source bill", event["source_url"])
                st.button("Open bill in tracker", on_click=open_bill,
                          args=(event["bill_id"], event["bill_number"]), key="open_hearing_bill")
            if any(not event["cancelled"] for event in visible):
                st.download_button("Download calendar (.ics)", export_calendar(visible),
                                   "tracked-bill-events.ics", "text/calendar")
                st.caption("Calendar exports use all-day markers. Any reported time is included in the description because the feed does not specify a time zone.")
        elif calendars:
            st.info("No upcoming events appear in the available snapshots for this date range.")
        else:
            st.info("No source calendars are available yet for your tracked bills.")
        st.caption("Check the source bill for schedule changes. When an event drops out of a source update, it leaves this calendar without a cancellation label.")

        if job_manager and (is_admin or has_personal_key):
            with st.expander("Refresh tracked-bill calendars"):
                tracked = load_tracked_rows(ids)
                labels = {label: code for code, label in US_STATES.items()}
                labels["U.S. Congress"] = "US"
                bills = {
                    str(row["bill_id"]): {
                        "bill_id": str(row["bill_id"]),
                        "jurisdiction": labels.get(row.get("jurisdiction_name"), ""),
                        **{key: row[key] if isinstance(row.get(key), str) else "" for key in ("bill_number", "title", "url")},
                    }
                    for row in tracked.to_dict("records")
                }
                batch = corpus.calendar_refresh_candidates(bills)
                if not batch:
                    st.caption("Calendars checked within the past three hours are skipped.")
                if st.button("Check up to 25 tracked bills", key="refresh_hearing_calendars", disabled=not batch):
                    use_worker = is_admin and os.environ.get("LEGISCAN_BACKGROUND_JOBS", "").lower() in {"1", "true", "yes"}
                    try:
                        if use_worker:
                            job_manager.enqueue_job(
                                "calendar_refresh", "", "WATCHLIST", {"bills": batch}, initiated_by=username,
                            )
                            st.success("Calendar refresh queued. Check Recent Jobs in the admin tools for progress.")
                        else:
                            status = st.progress(0, text="Checking source calendars...")
                            stats = run_calendar_refresh(
                                corpus, batch, job_manager,
                                lambda fraction, message: status.progress(fraction, text=message), initiated_by=username,
                            )
                            status.empty()
                            st.session_state[f"hearing_refresh_notice:{username}"] = stats
                            st.rerun()
                    except Exception as error:
                        st.error(f"Calendar refresh could not be completed: {error}")
