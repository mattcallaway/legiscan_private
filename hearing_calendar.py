"""Normalize source calendar events and export date-only iCalendar entries."""
from datetime import date, datetime, timezone
from hashlib import sha256
import json
import re
from urllib.parse import urlparse


def _text(value):
    return " ".join(value.split()) if isinstance(value, str) else ""


def source_link(*values):
    for value in values:
        if isinstance(value, str):
            try:
                parsed = urlparse(value)
                if parsed.scheme in {"https", "http"} and parsed.hostname and not parsed.username and not parsed.password:
                    return value
            except ValueError:
                continue
    return ""


def normalize_calendar(bill_id, entries):
    """None signals unavailable or malformed feed data. An explicit [] is authoritative."""
    if not isinstance(entries, list):
        return None
    events = {}
    for entry in entries:
        if not isinstance(entry, dict):
            return None
        day = _text(entry.get("date"))
        try:
            day = date.fromisoformat(day).isoformat()
        except ValueError:
            return None
        kind = _text(entry.get("type")) or "Legislative event"
        description = _text(entry.get("description"))
        details = {
            "date": day, "time": _text(entry.get("time")), "type": kind,
            "location": _text(entry.get("location")), "description": description,
        }
        identifier = sha256(json.dumps([str(bill_id), details], sort_keys=True).encode()).hexdigest()
        cancelled = kind.casefold() in {"canceled", "cancelled", "cancellation"} or bool(
            re.match(r"^(?:(?:hearing|meeting|event)\s+)?cancel(?:ed|led)\b", description, re.IGNORECASE)
        )
        events[identifier] = {**details, "event_id": identifier, "cancelled": cancelled}
    return sorted(events.values(), key=lambda event: (event["date"], event["time"], event["event_id"]))


def _escape(text):
    return (str(text).replace("\r\n", "\n").replace("\r", "\n").replace("\\", "\\\\")
            .replace("\n", "\\n").replace(";", "\\;").replace(",", "\\,"))


def _fold(line):
    folded = []
    current = ""
    width = 0
    for character in line:
        size = len(character.encode("utf-8"))
        if width + size > 75:
            folded.append(current)
            current, width = " ", 1
        current += character
        width += size
    return [*folded, current]


def export_calendar(events, now=None):
    """Use date markers because source times do not carry a time zone."""
    stamp = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    lines = ["BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//Legislation Tracker//Tracked Bill Calendar//EN",
             "CALSCALE:GREGORIAN"]
    for event in events:
        if event["cancelled"]:
            continue
        summary = f'{event["bill_number"]} | {event["type"]}'
        time_detail = f'Source time: {event["time"]} (time zone unavailable)' if event["time"] else "No time supplied"
        details = "\n".join(filter(None, (
            event["title"], event["description"], time_detail, event["source_url"],
        )))
        lines.extend([
            "BEGIN:VEVENT", f'UID:{event["event_id"]}@legislation-tracker',
            f"DTSTAMP:{stamp}", f'DTSTART;VALUE=DATE:{event["date"].replace("-", "")}',
            f"SUMMARY:{_escape(summary)}", f"DESCRIPTION:{_escape(details)}",
            f'LOCATION:{_escape(event["location"])}', "TRANSP:TRANSPARENT", "END:VEVENT",
        ])
    lines.append("END:VCALENDAR")
    return "\r\n".join(part for line in lines for part in _fold(line)) + "\r\n"
