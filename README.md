## SCCA Bill Tracker

A Streamlit application for tracking, filtering, annotating, and exporting state and federal legislative bills using LegiScan data.

---

### Table of Contents

1. Features  
2. Prerequisites  
3. Installation  
4. Configuration  
5. Usage  
6. Data Files & Structure  
7. Application Flow  
8. Customization  
9. Development & Contribution  
10. License  

---

### Features

- **Interactive Dashboard**  
  Displays total bills, tracked bills, high-priority counts, support/oppose metrics, and jurisdiction breakdowns.

- **Full-Text Search**  
  Search across bill number, title, sponsors, description, and committees.

- **Rich Filtering**  
  – Jurisdiction (Federal vs State vs Unknown)  
  – Specific states or federal chamber  
  – Keyword categories (e.g. “climate”, “water”)  
  – Status stage, sponsors, committees  
  – Position (Support, Oppose, Watch) and Priority (High, Medium, Low)  
  – Introduction date range.

- **Tracking & Notes**  
  – Mark/unmark bills for tracking (stored in `tracked_bills.json`).  
  – Attach comments, related links, PDF uploads, and set position/priority.

- **Rescan & Update**  
  Pulls the latest CSV data via an external LegiScan script (`legiscan_comprehensive_tracker.py`).

- **GitHub Sync**  
  Automatically clones/pulls and commits changes to a remote repo (`legiscan_storage`) via `sync_github_repo.py`.

- **Export**  
  Download tracked bills as CSV.

---

### Prerequisites

- **Python 3.8+**  
- **Git** installed and configured  
- **Pip** or **Poetry** for package management  

---

### Installation

```bash
# Clone your fork of this repository (or add it via your own remote):
git clone https://github.com/your-org/legiscan_tracker.git
cd legiscan_tracker

# Install dependencies:
pip install streamlit pandas
```

---

### Configuration

1. **Repository Sync**  
   By default, data is stored/cloned under your Documents folder:  
   - Windows: `%USERPROFILE%\Documents\legiscan_storage`  
   - macOS/Linux: `~/Documents/legiscan_storage`  
   (Override by editing `DEFAULT_PATHS` in `sync_github_repo.py`.)

2. **GitHub Repository**  
   Update `REPO_URL` in `sync_github_repo.py` to point to your own storage repo if needed.

3. **Data Files**  
   Ensure the storage repo contains:  
   - `LegiScan_Enhanced_Full_Tracker.csv` (master data)  
   - `tracked_bills.json` (list of bill numbers you’re tracking)  
   - `keywords.json` (list of keyword filters)  
   - `bill_notes.json` (per-bill notes, comments, links, uploads)

---

### Usage

```bash
# From the project root:
streamlit run legiscan_git_sync_update_4.py
```

- The sidebar lets you:
  - **Add New Keyword** → updates `keywords.json`  
  - **Rescan** → runs `legiscan_comprehensive_tracker.py` to refresh CSV  
  - **Toggle “Show All Tracked Bills”**  
  - Set filters by jurisdiction, states, federal chamber, keywords, status, sponsors, committees, position, priority, introduced date.

- The main view presents:
  - **Dashboard** cards and charts  
  - **All Bills** tab (with expandable bill details and saveable notes)  
  - **Tracked Bills** tab (for quick access/export/removal)

---

### Data Files & Structure

- **`LegiScan_Enhanced_Full_Tracker.csv`**  
  Rows of bills with columns like `bill_number`, `title`, `sponsors`, `description`, `status_stage`, `last_action_date`, etc.

- **`tracked_bills.json`**  
  JSON array of bill numbers you’ve marked for tracking. Example:  
  ```json
  ["AB1243", "SB94", "AB663"]
  ```

- **`keywords.json`**  
  List of strings used for keyword filtering (default includes climate, water, PFAS, etc.)

- **`bill_notes.json`**  
  Object mapping bill numbers to note objects:
  ```json
  {
    "AB1234": {
        "comment": "Key environmental bill",
        "links": ["https://..."],
        "files": ["upload1.pdf"],
        "position": "Support",
        "priority": "High"
    }
  }
  ```

- **`Tracked_Bills_Export.csv`**  
  Generated on-demand CSV of tracked bills.

---

### Application Flow

1. **Startup**  
   - Clone/pull storage repo (`ensure_repo()`)  
   - Load CSV, keywords, tracked list, and notes with caching and error handling.

2. **Dashboard Rendering**  
   - Metrics (total, tracked, high priority, support count, federal count)  
   - Charts for jurisdiction breakdown, status, tracked positions.

3. **Search & Filters**  
   - Full-text search  
   - Sidebar filters applied to the DataFrame.

4. **Bill Detail & Annotation**  
   - Expand each bill to view metadata, summary, link to history  
   - Add/edit comments, links, position, priority, and upload PDFs  
   - Save back to `bill_notes.json` and push changes

5. **Tracking Controls**  
   - Toggle bills in/out of `tracked_bills.json`  
   - Export tracked bills list  
   - Remove selected tracked bills.

---

### Customization

- **Add New Filters**  
  Extend `search_bills()` or sidebar filter blocks in `legiscan_git_sync_update_4.py`.

- **Styling**  
  Modify Streamlit layout (e.g., `st.set_page_config`) and column arrangements.

- **Data Pipeline**  
  Replace or extend `legiscan_comprehensive_tracker.py` for custom data ingestion.

---

### Development & Contribution

- **Code Style**  
  – Follow PEP8 for Python modules.  
  – Log via Python’s `logging` module for consistency.

- **Testing**  
  Add unit tests for data-loading and filter logic (e.g., in `load_data()`, `search_bills()`).

- **Issues & PRs**  
  - Fork the repo and open a pull request.  
  - Please document any new config options or data-format changes.

---

### License

MIT License – see [LICENSE](LICENSE) file for details.

---

*Happy tracking!*