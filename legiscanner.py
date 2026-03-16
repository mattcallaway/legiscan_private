# legi_scan_scanner.py
"""
Refactored LegiScan scanner with enhanced column mapping and full-jurisdiction support:
- Reads API_KEY and DATA_DIR from config.py
- Dynamically handles multiple states, all states, and federal (US)
- Centralizes all file paths under DATA_DIR
- Provides run_scan(states, data_dir) entrypoint
- Flattens full bill details into consistent CSV schema including:
  * jurisdiction level & name, bill_id, session, bill_number, title, description,
    status_date, status_stage, url, committee, keyword
  * sponsor_names, sponsors, referrals, history, subjects, last_action, last_action_date
- CLI support for ad-hoc runs; use `ALL` to scan all states + federal
"""
import os
import json
import csv
import logging
import requests
import time
import argparse
from datetime import datetime
from config import API_KEY, DATA_DIR

# Constants
BASE_URL = "https://api.legiscan.com/"
RELEVANCE_THRESHOLD = 55
CHAMBER_MAP = {'A': 'Assembly', 'S': 'Senate', 'H': 'House'}
US_STATES = {
    'AL':'Alabama','AK':'Alaska','AZ':'Arizona','AR':'Arkansas','CA':'California',
    'CO':'Colorado','CT':'Connecticut','DE':'Delaware','FL':'Florida','GA':'Georgia',
    'HI':'Hawaii','ID':'Idaho','IL':'Illinois','IN':'Indiana','IA':'Iowa',
    'KS':'Kansas','KY':'Kentucky','LA':'Louisiana','ME':'Maine','MD':'Maryland',
    'MA':'Massachusetts','MI':'Michigan','MN':'Minnesota','MS':'Mississippi','MO':'Missouri',
    'MT':'Montana','NE':'Nebraska','NV':'Nevada','NH':'New Hampshire','NJ':'New Jersey',
    'NM':'New Mexico','NY':'New York','NC':'North Carolina','ND':'North Dakota','OH':'Ohio',
    'OK':'Oklahoma','OR':'Oregon','PA':'Pennsylvania','RI':'Rhode Island','SC':'South Carolina',
    'SD':'South Dakota','TN':'Tennessee','TX':'Texas','UT':'Utah','VT':'Vermont',
    'VA':'Virginia','WA':'Washington','WV':'West Virginia','WI':'Wisconsin','WY':'Wyoming',
    'DC':'District of Columbia'
}
ALL_ALIAS = 'ALL'
FEDERAL_ALIAS = 'US'

# File paths under DATA_DIR
KEYWORDS_FILE = os.path.join(DATA_DIR, "keywords.json")
CACHE_FILE    = os.path.join(DATA_DIR, "legiscan_cache.json")
CSV_FILE      = os.path.join(DATA_DIR, "LegiScan_Enhanced_Full_Tracker.csv")

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def load_keywords(filepath=KEYWORDS_FILE):
    if not os.path.exists(filepath):
        logger.warning(f"Keywords file not found at {filepath}, using defaults.")
        return ["climate", "transportation", "PFAS", "water", "CEQA", "energy", "forest"]
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_cache(filepath=CACHE_FILE):
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_cache(cache, filepath=CACHE_FILE):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(cache, f, indent=2)
    logger.info(f"Cache written to {filepath}")


def fetch_search_results(jurisdiction, keyword):
    url = f"{BASE_URL}?key={API_KEY}&op=getSearchRaw&state={jurisdiction}&query={keyword}"
    logger.info(f"Searching {jurisdiction} for keyword '{keyword}'")
    try:
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logger.error(f"Error fetching search results for {jurisdiction}/{keyword}: {e}")
        return [], 0, 0, False

    if data.get('status') != 'OK':
        logger.warning(f"SearchRaw status not OK ({jurisdiction}): {data.get('status')}")
        return [], 0, 0, False

    results = data.get('searchresult', {}).get('results', [])
    total_found = len(results)
    filtered_results = [
        {'bill_id': r['bill_id'], 'change_hash': r['change_hash'], 'relevance': r.get('relevance', 0)}
        for r in results if r.get('relevance', 0) >= RELEVANCE_THRESHOLD
    ]
    filtered_count = total_found - len(filtered_results)
    return filtered_results, total_found, filtered_count, True


def get_bill_details(bill_id):
    url = f"{BASE_URL}?key={API_KEY}&op=getBill&id={bill_id}"
    try:
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        logger.error(f"Error fetching bill {bill_id}: {e}")
        return None

    if data.get('status') != 'OK':
        logger.warning(f"getBill status not OK for {bill_id}: {data.get('status')}")
        return None

    return data.get('bill', {})


def flatten_bill(details, jurisdiction, keyword):
    # Determine jurisdiction metadata
    if jurisdiction == FEDERAL_ALIAS:
        jur_level = 'Federal'
        jur_name = 'U.S. Congress'
    else:
        jur_level = 'State'
        jur_name = US_STATES.get(jurisdiction, jurisdiction)

    row = {
        'jurisdiction_level': jur_level,
        'jurisdiction_name': jur_name,
        'bill_id': details.get('bill_id', ''),
        'session': details.get('session', {}).get('session_name', ''),
        'bill_number': details.get('bill_number', ''),
        'title': details.get('title', '').replace('\n', ' '),
        'description': details.get('description', '').replace('\n', ' '),
        'status_date': details.get('status_date', ''),
        'status_stage': details.get('status', ''),
        'url': details.get('url', ''),
        'keyword': keyword, 
    }
    committee_info = details.get('committee')
    row['committee'] = committee_info.get('name', '') if isinstance(committee_info, dict) else ''
    # Sponsors
    sponsors = details.get('sponsors', [])
    row['sponsor_names'] = ", ".join(s.get('name', '') for s in sponsors)
    # Mirror for dashboard compatibility
    row['sponsors'] = row['sponsor_names']
    # Committee mirror
    row['committees'] = row['committee']
    # Last action fields
    history = details.get('history', [])
    hist_list = []
    for h in history:
        chamber = CHAMBER_MAP.get(h.get('chamber', ''), h.get('chamber', ''))
        action  = h.get('action', '').replace('\n', ' ')
        date    = h.get('date', '')
        hist_list.append(f"{chamber}: {action} ({date})")
    row['history'] = "; ".join(hist_list)
    row['last_action'] = hist_list[-1] if hist_list else ''
    row['last_action_date'] = details.get('last_action_date', details.get('status_date', ''))
    # Referrals
    referrals = details.get('referrals', [])
    ref_list = []
    for r in referrals:
        chamber = CHAMBER_MAP.get(r.get('chamber', ''), r.get('chamber', ''))
        name    = r.get('name', '')
        date    = r.get('date', '')
        ref_list.append(f"{chamber} - {name} on {date}")
    row['referrals'] = "; ".join(ref_list)
    # Subjects
    row['subjects'] = "; ".join(details.get('subjects', []))
    return row


def load_existing_csv(filepath=CSV_FILE):
    existing_bills = {}
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing_bills[str(row.get('bill_id', ''))] = row
        except Exception as e:
            logger.error(f"Error reading existing CSV: {e}")
    return existing_bills

def run_scan(states=None, data_dir=None):
    # Default to ALL if none
    if not states:
        states = [ALL_ALIAS]
    # Expand ALL
    if ALL_ALIAS in states:
        states = list(US_STATES.keys()) + [FEDERAL_ALIAS]
    data_dir = data_dir or DATA_DIR
    os.makedirs(data_dir, exist_ok=True)

    keywords = load_keywords()
    cache    = load_cache()
    existing_bills = load_existing_csv(CSV_FILE)
    
    stats = {
        "api_status": "ok",
        "total_found": 0,
        "filtered": 0,
        "new_bills": 0,
        "changed_status": 0
    }

    for jurisdiction in states:
        for keyword in keywords:
            searches, total_f, filtered_c, api_ok = fetch_search_results(jurisdiction, keyword)
            if not api_ok:
                stats["api_status"] = "error"
            
            stats["total_found"] += total_f
            stats["filtered"] += filtered_c

            time.sleep(0.2)
            for item in searches:
                bid      = str(item['bill_id'])
                new_hash = item['change_hash']
                old_hash = cache.get(bid, {}).get('change_hash')
                if new_hash != old_hash:
                    details = get_bill_details(bid)
                    if not details:
                        continue
                    row = flatten_bill(details, jurisdiction, keyword)
                    
                    if bid not in existing_bills:
                        stats["new_bills"] += 1
                    else:
                        stats["changed_status"] += 1
                        
                    existing_bills[bid] = row
                    cache[bid] = {'change_hash': new_hash, 'last_checked': datetime.now().isoformat()}
                    time.sleep(0.2)

    # Write CSV
    if existing_bills:
        fieldnames = [
            'jurisdiction_level','jurisdiction_name','bill_id','session','bill_number',
            'title','description','status_date','status_stage','url','committee',
            'keyword','sponsor_names','sponsors','committees','referrals','history',
            'last_action','last_action_date','subjects'
        ]
        # Ensure all existing rows have exactly these fieldnames (missing ones as empty string)
        clean_bills = []
        for bill in existing_bills.values():
            clean_row = {k: bill.get(k, '') for k in fieldnames}
            clean_bills.append(clean_row)
            
        with open(CSV_FILE, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(clean_bills)
        logger.info(f"Wrote {len(existing_bills)} rows to {CSV_FILE}")
    else:
        logger.info("No bills to write.")

    save_cache(cache)
    return stats


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run LegiScan comprehensive tracker")
    parser.add_argument('--states', nargs='+', help='Jurisdictions to scan (e.g., CA NY TX US ALL)')
    args = parser.parse_args()
    stats = run_scan(states=[s.upper() for s in (args.states or [])])
    print(f"Scan complete. Stats: {json.dumps(stats, indent=2)}")
