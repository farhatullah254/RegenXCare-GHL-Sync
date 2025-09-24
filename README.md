# RegenXCare GHL Sync

Automates daily sync from a Google Sheet to HighLevel (LeadConnector v2).  
It reads raw rows, computes **cumulative spending per Patient Account**, writes a CSV snapshot, and upserts Contacts in GHL with custom fields.

> Cumulative logic: group by **PATIENT ACCOUNT**, sum **TOTAL AMOUNT PAID** (or fallback headers), carry forward descriptive fields, then push to GHL.

---

## Features

- Pulls a Google Sheet tab via CSV export
- Cleans currency strings and scientific-notation account IDs
- Builds a **Cumulative Spending** table
- Saves `cumulative_spending.csv`
- Upserts Contacts in GHL v2 (`/contacts/upsert`) with:
  - `contact.patient_account`
  - `contact.total_amount_paid` (numeric)
  - Optional: `contact.patient_name`, `contact.location_name`, `contact.insurance_name`
- Runs **forever**, once every ~24h, with retries and jitter
- Minimal dependencies: `pandas`, `requests`

---

## Requirements

- Python 3.9+
- A Google Sheet with consistent column headers
- GHL **Sub-Account Private Integration token** (LeadConnector v2)
- Your **LOCATION_ID** (from the GHL URL)

---

## Quick Start

```bash
git clone <your-repo-url>
cd <your-repo>
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt  # see below
```

Set environment variables:

```bash
export GHL_TOKEN="your_subaccount_private_integration_token"
export LOCATION_ID="twb3X04ZwAGeLN8iXc1O"
```

Run:

```bash
python main.py
```

You’ll see:
- RAW preview (first 10 rows)
- CUMULATIVE preview (first 10 rows)
- `cumulative_spending.csv` written
- Upserts to GHL if `PUSH_TO_GHL=True`

---

## Configuration

Edit the constants at the top of `main.py` if needed:

```python
GOOGLE_SHEET_ID = "13fxvJjvSl4fHqNfekckoneZqGPOZL6rNy30eJygikNU"
RAW_TAB_GID = "0"  # numeric gid of the tab to read
CSV_URL = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&gid={RAW_TAB_GID}"

OUT_CSV = "cumulative_spending.csv"

COL_PATIENT_ACCOUNT = "PATIENT ACCOUNT"
COL_PATIENT_NAME    = "PATIENT NAME"
AMOUNT_CANDIDATES   = ["TOTAL AMOUNT PAID","AMOUNT PAID","PAID","TOTAL PAID"]
CARRY_FORWARD       = ["PATIENT NAME","LOCATION NAME","INSURANCE NAME"]

BASE        = "https://services.leadconnectorhq.com"
VERSION_HDR = "2021-07-28"

PUSH_TO_GHL   = True      # set False for dry-run (CSV only)
RUN_FOREVER   = True      # loop every ~24h
SLEEP_SECONDS = 24*3600   # base interval
MAX_RETRIES   = 3         # transient retry attempts
JITTER_MAX    = 120       # add 0..120s jitter to sleep
```

Environment variables (required):

- `GHL_TOKEN` — Sub-Account Private Integration token
- `LOCATION_ID` — as seen in the GHL URL `/v2/location/<ID>/...`

---

## Google Sheet Format

The script expects these headers:

- `PATIENT ACCOUNT` (identifier for grouping)
- One of the amount columns: `TOTAL AMOUNT PAID`, `AMOUNT PAID`, `PAID`, or `TOTAL PAID`
- Optional descriptive columns:
  - `PATIENT NAME` (format `LAST, FIRST` recommended)
  - `LOCATION NAME`
  - `INSURANCE NAME`

> Long numeric accounts are normalized, so `3.55103E+15` becomes `3551030000000000` etc.

---

## What Gets Pushed to GHL

- Contact upsert via `/contacts/upsert` with:
  - `email`: `${PATIENT_ACCOUNT}@patients.local` (deterministic anchor to prevent duplicates)
  - `firstName` and `lastName` from splitting `PATIENT NAME` if present
  - `customFields`:
    - `contact.patient_account` – the exact account string
    - `contact.total_amount_paid` – numeric cumulative total
    - Optional: `contact.patient_name`, `contact.location_name`, `contact.insurance_name`

> The script auto-resolves custom field IDs by key/label on each run. Ensure the fields exist in the sub-account and are **Contact** fields.

---

## Install as a Service (optional)

### Cron (simple)
Edit crontab to run once a day and let the script run once (set `RUN_FOREVER=False` in code):

```
0 2 * * * /path/to/.venv/bin/python /path/to/main.py >> /path/to/log.txt 2>&1
```

### Systemd (Linux, robust)
Keep `RUN_FOREVER=True` and create a unit:

`/etc/systemd/system/regenxcare-ghl-sync.service`
```ini
[Unit]
Description=RegenXCare GHL Sync

[Service]
WorkingDirectory=/opt/regenxcare
Environment=GHL_TOKEN=xxxx
Environment=LOCATION_ID=twb3X04ZwAGeLN8iXc1O
ExecStart=/opt/regenxcare/.venv/bin/python /opt/regenxcare/main.py
Restart=always

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable regenxcare-ghl-sync
sudo systemctl start regenxcare-ghl-sync
```

---

## Troubleshooting

### 401 Unauthorized
- Wrong token type. You need a **Sub-Account Private Integration token** for v2.
- Fix headers: must include `Version: 2021-07-28`.

### 403 Forbidden
- Token isn’t installed in that sub-account or lacks scope.
- `LOCATION_ID` is wrong. Confirm with:
  - App URL: `/v2/location/<ID>/...`
  - Or API: `GET /users/locations` to list access.

### Custom fields show blank in UI
- You wrote to the wrong field IDs. List them with `GET /locations/{LOCATION_ID}/customFields` and confirm the IDs match.
- You added the wrong column to Smart Lists (duplicates with same labels are common).
- You accidentally created **Company** fields instead of **Contact** fields.
- For numeric fields, send numbers, not strings with `$` or commas.

### Scientific notation account IDs in output
- Confirm `PATIENT ACCOUNT` is being normalized. The code uses `Decimal` to de-exponent.

### Rate limiting or flaky network
- The script retries transient errors with backoff and sleeps 0.1s between upserts. Increase if needed.

---

## Security

- Do not commit `GHL_TOKEN` to the repo. Use environment variables or a secret manager.
- Treat the data as sensitive (PII/PHI). Restrict access to the repo and runtime host.
- If you rotated tokens publicly before, rotate again.

---

## Sample Output

```
=== RAW HEAD ===
PATIENT NAME,...,INSURANCE NAME,...
CURATOLO, DAWN,3551034835596928,...,$59.22,...,MEDICARE OF FLORIDA...

=== CUMULATIVE HEAD ===
PATIENT ACCOUNT  TOTAL_AMOUNT_PAID_CUMULATIVE  PATIENT NAME  LOCATION NAME  INSURANCE NAME  firstName  lastName
3551034835596928                      55058.26  CURATOLO, DAWN  REGEN-X ...  MEDICARE ...    DAWN       CURATOLO

Saved: cumulative_spending.csv (rows: 8)

Resolving custom fields via v2…
Field IDs: {'total_amount_paid': '...', 'patient_account': '...', ...}

Pushed 8 contacts to GHL.
READBACK (sample): { ... }
```

---

## Project Structure

```
.
├── main.py
├── requirements.txt
└── README.md
```

`requirements.txt`:
```
pandas>=2.0
requests>=2.31
```

---

## Notes on GHL API Versions

- This project targets **LeadConnector v2**:
  - Host: `https://services.leadconnectorhq.com`
  - Header: `Version: 2021-07-28`
  - Token: **Sub-Account Private Integration** or OAuth access token
- If you insist on v1 (`rest.gohighlevel.com`) you must change endpoints and auth accordingly, and drop `locationId` from payloads.

---

## License

MIT. Add your name and year if you care.

---

## Roadmap

- Optional Opportunities creation for pipeline reporting
- Field ID caching (reduce one API call per run)
- Google Sheets write-back (new “Cumulative Spending” tab)
- Dockerfile and GitHub Actions nightly workflow
