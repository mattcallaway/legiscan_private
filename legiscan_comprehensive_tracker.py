
import requests
import json
import csv
import os
from datetime import datetime

API_KEY = "de58519e5e99b73f16756ad6d1e5e377"
KEYWORDS_FILE = "keywords.json"
STATE = "CA"
BASE_URL = "https://api.legiscan.com/"
CACHE_FILE = "legiscan_cache.json"
CSV_FILE = "LegiScan_Enhanced_Full_Tracker.csv"
RELEVANCE_THRESHOLD = 55

def load_keywords():
    if os.path.exists(KEYWORDS_FILE):
        with open(KEYWORDS_FILE, "r") as f:
            return json.load(f)
    return ["climate", "transportation", "PFAS", "water", "CEQA", "energy", "forest"]

if os.path.exists(CACHE_FILE):
    with open(CACHE_FILE, 'r') as cache_file:
        cache = json.load(cache_file)
else:
    cache = {}

export_rows = []

def log(message):
    print(f"[{datetime.now().isoformat()}] {message}")

def get_bill_details(bill_id):
    url = f"{BASE_URL}?key={API_KEY}&op=getBill&id={bill_id}"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if data.get("status") == "OK":
            return data.get("bill", {})
    return None

def fetch_bills_by_keyword(keyword):
    url = f"{BASE_URL}?key={API_KEY}&op=getSearchRaw&state={STATE}&query={keyword}"
    log(f"Requesting: {url}")
    try:
        response = requests.get(url)
        log(f"Status code: {response.status_code}")
        if response.status_code != 200:
            return []

        data = response.json()
        if data.get("status") != "OK":
            return []

        results = data.get("searchresult", {}).get("results", [])
        output = []

        for v in results:
            if v.get("relevance", 0) < RELEVANCE_THRESHOLD:
                continue

            bill_id = v["bill_id"]
            change_hash = v["change_hash"]
            cached_hash = cache.get(str(bill_id), {}).get("change_hash")

            if change_hash != cached_hash:
                bill_data = get_bill_details(bill_id)
                if bill_data:
                    sponsors = ", ".join([p.get("name") for p in bill_data.get("sponsors", [])])
                    committee_entries = bill_data.get("committees", [])
                    if committee_entries:
                        committees = ", ".join([
                            c.get("committee", {}).get("name", "") if isinstance(c.get("committee"), dict)
                            else c.get("name", "")
                            for c in committee_entries
                        ])
                        committees = committees if committees.strip() else "None"
                    else:
                        committees = "None"

                    history = bill_data.get("history", [])
                    status_stage = bill_data.get("status", "N/A")

                    milestones = {
                        "introduced_date": "",
                        "committee_date": "",
                        "passed_assembly_date": "",
                        "passed_senate_date": "",
                        "governor_action_date": ""
                    }
                    for h in history:
                        action = h.get("action", "").lower()
                        if "introduce" in action and not milestones["introduced_date"]:
                            milestones["introduced_date"] = h.get("date", "")
                        elif "committee" in action and not milestones["committee_date"]:
                            milestones["committee_date"] = h.get("date", "")
                        elif "passed assembly" in action and not milestones["passed_assembly_date"]:
                            milestones["passed_assembly_date"] = h.get("date", "")
                        elif "passed senate" in action and not milestones["passed_senate_date"]:
                            milestones["passed_senate_date"] = h.get("date", "")
                        elif "governor" in action and not milestones["governor_action_date"]:
                            milestones["governor_action_date"] = h.get("date", "")

                    votes = "; ".join([
                        f"{v.get('date', '')} ({v.get('motion', '')}): Yea {v.get('yea', 0)}, Nay {v.get('nay', 0)}, Absent {v.get('absent', 0)}"
                        for v in bill_data.get("votes", [])
                    ])
                    calendar = "; ".join([
                        f"{c.get('date', '')}: {c.get('type', '')} - {c.get('location', '')}" 
                        for c in bill_data.get("calendar", [])
                    ])
                    amendments = "; ".join([
                        f"{a.get('date', '')}: {a.get('description', '')}" 
                        for a in bill_data.get("amendments", [])
                    ])

                    output.append({
                        "keyword": keyword,
                        "bill_id": bill_id,
                        "bill_number": bill_data.get("bill_number", ""),
                        "title": bill_data.get("title", ""),
                        "description": bill_data.get("description", ""),
                        "url": bill_data.get("url", ""),
                        "status": status_stage,
                        "last_action": bill_data.get("last_action", "N/A"),
                        "last_action_date": str(bill_data.get("last_action_date", "N/A")),
                        "sponsors": sponsors,
                        "committees": committees,
                        "status_stage": status_stage,
                        "introduced_date": milestones["introduced_date"],
                        "committee_date": milestones["committee_date"],
                        "passed_assembly_date": milestones["passed_assembly_date"],
                        "passed_senate_date": milestones["passed_senate_date"],
                        "governor_action_date": milestones["governor_action_date"],
                        "votes": votes,
                        "calendar": calendar,
                        "amendments": amendments
                    })

                    cache[str(bill_id)] = {
                        "change_hash": change_hash,
                        "last_checked": datetime.now().isoformat()
                    }

        log(f"{len(output)} bills processed for keyword '{keyword}'")
        return output

    except Exception as e:
        log(f"Exception: {e}")
        return []

keywords = load_keywords()
for keyword in keywords:
    export_rows.extend(fetch_bills_by_keyword(keyword))

with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=[
        "keyword", "bill_id", "bill_number", "title", "description", "url",
        "status", "last_action", "last_action_date", "sponsors", "committees",
        "status_stage", "introduced_date", "committee_date",
        "passed_assembly_date", "passed_senate_date", "governor_action_date",
        "votes", "calendar", "amendments"
    ])
    writer.writeheader()
    for row in export_rows:
        writer.writerow({key: row.get(key, "") for key in writer.fieldnames})

with open(CACHE_FILE, 'w') as cache_file:
    json.dump(cache, cache_file, indent=2)

log(f"Done. {len(export_rows)} total rows written to {CSV_FILE}")
