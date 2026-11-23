"""Helpers for review snapshots and comparing legislative text."""
from datetime import date
from difflib import unified_diff
from hashlib import sha256
import json
from html import escape
from html.parser import HTMLParser


def bill_revision(row):
    def clean(field, fallback=""):
        value = row.get(field)
        return fallback if str(value) in {"", "None", "nan", "<NA>"} else str(value)

    return {
        "Status": clean("status_stage"),
        "Last action": clean("last_action"),
        "Action date": clean("last_action_date"),
        "History": clean("history"),
        "Committee": clean("committees", clean("committee")),
        "Text version": clean("latest_doc_id").removesuffix(".0"),
    }


def changed_fields(previous, current):
    """Ignore data a source does not currently supply."""
    if not previous:
        return []
    return [field for field, value in current.items() if value and previous.get(field) != value]


class _BillTextParser(HTMLParser):
    _blocks = {"p", "div", "br", "li", "tr", "section", "h1", "h2", "h3", "h4", "pre"}
    _hidden = {"head", "script", "style"}

    def __init__(self):
        super().__init__()
        self.parts = []
        self.hidden = 0

    def handle_starttag(self, tag, attrs):
        if tag in self._hidden:
            self.hidden += 1
        elif tag in self._blocks and not self.hidden:
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in self._hidden and self.hidden:
            self.hidden -= 1
        elif tag in self._blocks and not self.hidden:
            self.parts.append("\n")

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)



class _ReadableMarkup(HTMLParser):
    _allowed = frozenset({
        "p", "div", "section", "article", "h1", "h2", "h3", "h4", "h5", "h6",
        "ul", "ol", "li", "dl", "dt", "dd", "blockquote", "pre", "code",
        "strong", "em", "b", "i", "s", "u", "ins", "del", "sup", "sub",
        "table", "caption", "thead", "tbody", "tfoot", "tr", "th", "td", "br", "hr",
    })
    _void = frozenset({"br", "hr"})
    _hidden = frozenset({"head", "script", "style", "template"})

    def __init__(self):
        super().__init__()
        self.parts = []
        self.hidden = None

    def handle_starttag(self, tag, attrs):
        if self.hidden is None:
            if tag in self._hidden:
                self.hidden = tag
            elif tag in self._allowed:
                self.parts.append(f"<{tag}>")

    def handle_endtag(self, tag):
        if self.hidden == tag:
            self.hidden = None
        elif self.hidden is None and tag in self._allowed and tag not in self._void:
            self.parts.append(f"</{tag}>")

    def handle_data(self, data):
        if self.hidden is None:
            self.parts.append(escape(data, quote=False))


def readable_bill_markup(markup):
    """Keep legislative structure and text while discarding all source attributes."""
    if len(markup) > 2_000_000:
        raise ValueError("This version is too large for the interactive reader.")
    parser = _ReadableMarkup()
    parser.feed(markup)
    parser.close()
    return "".join(parser.parts)


def amendment_diff(before, after):
    """Compare visible HTML text by line, with a bound on interactive work."""
    def lines(markup):
        if len(markup) > 1_000_000:
            raise ValueError("This version is too large for interactive comparison.")
        parser = _BillTextParser()
        parser.feed(markup)
        parser.close()
        result = [" ".join(line.split()) for line in "".join(parser.parts).splitlines()]
        return [line for line in result if line]

    old, new = lines(before), lines(after)
    if not old or not new:
        raise ValueError("Both versions need readable text.")
    if max(len(old), len(new)) > 5000:
        raise ValueError("This version is too long for interactive comparison.")
    return "\n".join(unified_diff(old, new, fromfile="Earlier version", tofile="Later version", lineterm=""))

def build_review_queue(tracked_rows, notes, topic_matches, preferences, today, calendar_events=()):
    """Build a daily list without mutating saved reviews or notes."""
    items = []

    def add(kind, bill, detail, version, *, topic=None, due=None):
        identity = [kind, str(bill["bill_id"]), topic, version]
        key = sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        state = preferences.get("items", {}).get(key, {})
        if state.get("reviewed") or state.get("snoozed_until", "") > today.isoformat():
            return
        items.append({
            "key": key, "kind": kind, "bill": bill,
            "detail": detail, "topic": topic, "due": due,
        })

    for bill in tracked_rows:
        bill_id = str(bill["bill_id"])
        note = notes.get(bill_id, {})
        due = note.get("follow_up")
        if due:
            try:
                due = date.fromisoformat(due)
            except (TypeError, ValueError):
                due = None
            if due and due <= today:
                add("Follow-up", bill, f"Due {due.isoformat()}", due.isoformat(), due=due)
        before = note.get("reviewed_revision")
        current = bill_revision(bill)
        changes = changed_fields(before, current)
        if changes:
            add("Bill changed", bill, ", ".join(changes), {field: current[field] for field in changes})

    for topic_key, topic, matches in topic_matches:
        seen = set(topic.get("seen", []))
        for bill in matches:
            if str(bill["bill_id"]) not in seen:
                explanation = bill.get("topic_match_reason")
                detail = f'{topic["name"]} | {explanation}' if explanation else topic["name"]
                rules = {key: topic.get(key, []) for key in ("terms", "required", "excluded")}
                revision = rules if rules["required"] or rules["excluded"] else rules["terms"]
                add("Topic match", bill, detail, revision, topic=topic_key)

    tracked = {str(row["bill_id"]): row for row in tracked_rows}
    for event in calendar_events:
        bill_id = str(event["bill_id"])
        if bill_id in tracked and not event["cancelled"]:
            when = date.fromisoformat(event["date"])
            detail = " | ".join(part for part in (event["date"], event["time"], event["type"], event["location"]) if part)
            add("Upcoming event", tracked[bill_id], detail, event["event_id"], due=when)

    order = {"Follow-up": 0, "Upcoming event": 1, "Bill changed": 2, "Topic match": 3}
    return sorted(items, key=lambda item: (order[item["kind"]], item["due"] or today))

TOPIC_SEARCH_FIELDS = {"title": "Title", "description": "Summary", "subjects": "Subjects"}


def parse_topic_rules(any_text, required_text, excluded_text):
    def phrases(text):
        return list(dict.fromkeys(value.strip().casefold() for value in text.split(",") if value.strip()))

    any_terms = phrases(any_text)
    required_terms = phrases(required_text)
    excluded_terms = phrases(excluded_text)
    if not any_terms and not required_terms:
        raise ValueError("Enter at least one matching or required phrase.")
    if sum(map(len, (any_terms, required_terms, excluded_terms))) > 20:
        raise ValueError("Use up to 20 phrases across these rules.")
    if (set(any_terms) | set(required_terms)) & set(excluded_terms):
        raise ValueError("A phrase cannot be both included and excluded.")
    return {"terms": any_terms, "required": required_terms, "excluded": excluded_terms}


def topic_match_reason(bill, topic):
    """Apply literal topic rules to each field and return what matched."""
    fields = {
        label: value.casefold() if isinstance(value := bill.get(key), str) else ""
        for key, label in TOPIC_SEARCH_FIELDS.items()
    }

    def hits(phrase):
        return [label for label, value in fields.items() if phrase.casefold() in value]

    any_terms = topic.get("terms", [])
    required_terms = topic.get("required", [])
    if not any_terms and not required_terms:
        return ""
    if any(hits(term) for term in topic.get("excluded", [])):
        return ""
    if any(not hits(term) for term in required_terms) or (any_terms and not any(hits(term) for term in any_terms)):
        return ""
    positive = dict.fromkeys([*required_terms, *any_terms])
    return " | ".join(f'{phrase} ({", ".join(found)})' for phrase in positive if (found := hits(phrase)))


def personal_topic_tags(bill, topics):
    """Match private rules against current bill metadata, regardless of review state."""
    return [
        topic["name"] for topic in topics.values()
        if topic.get("auto_tag") and topic_match_reason(bill, topic)
    ]


def update_personal_topic(entry, rules, *, auto_tag, track_matches):
    """Revisit previous bills once when the phrase rules have changed."""
    if any(entry.get(key, []) != value for key, value in rules.items()):
        entry["seen"] = []
    entry.update(rules, auto_tag=auto_tag, track_matches=track_matches)
