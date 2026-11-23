"""Record sourced legislative service without inventing missing dates."""
from datetime import date
import re
from urllib.parse import urlparse

import requests

CALIFORNIA_SOURCE = "https://www.senate.ca.gov/citizens-guide/faqs"
EVIDENCE_HOSTS = ("ca.gov", "congress.gov", "senate.gov", "house.gov", "govinfo.gov")


def service_chamber(value):
    value = str(value or "").strip().casefold()
    if value in {"s", "sen", "senate", "upper"}:
        return "Senate"
    if value in {"a", "assembly"}:
        return "Assembly"
    if value in {"h", "house", "house of representatives"}:
        return "House of Representatives"
    return ""


def is_official_term_source(url):
    parsed = urlparse(url)
    host = parsed.hostname or ""
    return (parsed.scheme == "https" and not parsed.username and not parsed.password
            and any(host == root or host.endswith("." + root) for root in EVIDENCE_HOSTS))


def validate_exact_term(jurisdiction, chamber, start, end, source):
    chamber = service_chamber(chamber)
    allowed = {"CA": {"Assembly", "Senate"}, "US": {"House of Representatives", "Senate"}}
    if chamber not in allowed.get(jurisdiction, set()):
        raise ValueError("Choose a chamber in this jurisdiction.")
    if not start or not end or start > end:
        raise ValueError("Enter both exact dates in chronological order.")
    if not is_official_term_source(source):
        raise ValueError("Enter an HTTPS link to an official legislative source.")
    return {
        "jurisdiction": jurisdiction, "chamber": chamber, "precision": "Exact dates",
        "start": start.isoformat(), "end": end.isoformat(), "source_url": source,
        "congress": None,
    }


def fetch_congress_member_terms(bioguide, api_key):
    identifier = bioguide.strip().upper()
    if not re.fullmatch(r"[A-Z][0-9]{6}", identifier):
        raise ValueError("Enter a Bioguide ID such as A000001.")
    if not api_key:
        raise ValueError("Set CONGRESS_API_KEY on the server to look up federal terms.")
    try:
        response = requests.get(
            f"https://api.congress.gov/v3/member/{identifier}",
            params={"format": "json", "api_key": api_key}, timeout=(5, 15), allow_redirects=False,
        )
        if response.status_code != 200:
            raise RuntimeError(f"Congress.gov returned HTTP {response.status_code}.")
        member = response.json()["member"]
    except (requests.RequestException, KeyError, TypeError, ValueError) as error:
        raise RuntimeError("Congress.gov did not return a usable member record.") from error
    if not isinstance(member, dict) or member.get("bioguideId") != identifier:
        raise RuntimeError("Congress.gov did not return the requested Bioguide member.")
    source_url = f"https://bioguide.congress.gov/search/bio/{identifier}"
    entries = member.get("terms") or []
    if isinstance(entries, dict):
        entries = entries.get("item") or []
    records = {}
    for term in entries:
        if not isinstance(term, dict):
            continue
        chamber = service_chamber(term.get("chamber"))
        if chamber not in {"Senate", "House of Representatives"}:
            continue
        try:
            start = int(term["startYear"])
            end = int(term["endYear"]) if term.get("endYear") not in (None, "") else None
            congress = int(term["congress"])
        except (KeyError, TypeError, ValueError):
            continue
        if start < 1789 or (end is not None and end < start) or congress < 1:
            continue
        records[(chamber, congress)] = {
            "jurisdiction": "US", "chamber": chamber, "precision": "Year only",
            "start": str(start), "end": str(end) if end is not None else "",
            "source_url": source_url, "congress": congress,
        }
    name = member.get("directOrderName") or " ".join(
        part for part in (member.get("firstName"), member.get("lastName")) if part
    )
    if not name or not records:
        raise RuntimeError("No usable legislative terms were returned for this member.")
    return {"bioguide": identifier, "name": name, "terms": list(records.values())}


def vote_service_status(vote, terms):
    """Report date precision and leave incomplete or unmatched evidence visible."""
    try:
        when = date.fromisoformat(str(vote.get("vote_date", ""))[:10])
    except ValueError:
        return "Vote date unavailable"
    chamber = service_chamber(vote.get("vote_chamber"))
    if not chamber:
        return "Vote chamber unavailable"
    candidates = [term for term in terms
                  if term["jurisdiction"] == vote.get("jurisdiction") and term["chamber"] == chamber]
    for term in candidates:
        if term["precision"] == "Exact dates" and term["start"] <= when.isoformat() <= term["end"]:
            return "Within exact dates"
    congress = re.search(r"\b(\d+)(?:st|nd|rd|th) Congress\b", str(vote.get("session", "")), re.IGNORECASE)
    for term in candidates:
        if term["precision"] != "Year only":
            continue
        if congress and term["congress"] != int(congress.group(1)):
            continue
        start = int(term["start"])
        if not term["end"] and not congress and when.year != start:
            continue
        if when.year >= start and (not term["end"] or when.year <= int(term["end"])):
            return "Consistent with year-only record"
    return "No matching saved term"
