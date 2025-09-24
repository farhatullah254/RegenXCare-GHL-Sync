#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import time
import json
import random
import traceback
from decimal import Decimal, InvalidOperation
from typing import Dict, Any
from dotenv import load_dotenv
import pandas as pd
import requests
load_dotenv() 
# ====================== CONFIG ======================
GOOGLE_SHEET_ID = "13fxvJjvSl4fHqNfekckoneZqGPOZL6rNy30eJygikNU"
RAW_TAB_GID = "0"  # change if your raw tab isn't the first
CSV_URL = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&gid={RAW_TAB_GID}"

OUT_CSV = "cumulative_spending.csv"

COL_PATIENT_ACCOUNT = "PATIENT ACCOUNT"
COL_PATIENT_NAME = "PATIENT NAME"
AMOUNT_CANDIDATES = ["TOTAL AMOUNT PAID", "AMOUNT PAID", "PAID", "TOTAL PAID"]
CARRY_FORWARD = ["PATIENT NAME", "LOCATION NAME", "INSURANCE NAME"]

# GHL v2 (LeadConnector)
BASE = "https://services.leadconnectorhq.com"
VERSION_HDR = "2021-07-28"

# Runtime controls
PUSH_TO_GHL = True          # set False to only build CSV
RUN_FOREVER = True          # loop every ~24h
SLEEP_SECONDS = 24 * 3600   # base sleep between runs
MAX_RETRIES = 3             # retries inside a run for transient errors
JITTER_MAX = 120            # add 0..JITTER_MAX seconds jitter to sleep
# ====================================================


# --------------- Helpers: data cleaning ---------------
_money_re = re.compile(r"[^0-9.\-]")

def to_amount(x) -> float:
    s = str(x or "").strip()
    if not s:
        return 0.0
    s = _money_re.sub("", s)
    try:
        return float(s)
    except Exception:
        return 0.0

def normalize_account(val) -> str:
    """Turn 3.55103E+15 or '3,551,034,835,596,928' into a clean integer string."""
    s = str(val).strip().replace(",", "")
    try:
        d = Decimal(s)
        return format(d, "f").split(".")[0]
    except InvalidOperation:
        return "".join(ch for ch in s if ch.isdigit())
# -----------------------------------------------------


# ----------------- Sheet processing ------------------
def detect_amount_col(df: pd.DataFrame) -> str:
    for c in AMOUNT_CANDIDATES:
        if c in df.columns:
            return c
    raise ValueError(f"Amount column not found. Tried {AMOUNT_CANDIDATES}. Got: {list(df.columns)}")

def load_raw_df() -> pd.DataFrame:
    df = pd.read_csv(CSV_URL, dtype=str).fillna("")
    if COL_PATIENT_ACCOUNT not in df.columns:
        raise ValueError(f"Missing '{COL_PATIENT_ACCOUNT}'. Columns: {list(df.columns)}")
    # normalize patient account column early to avoid sci-notation
    df[COL_PATIENT_ACCOUNT] = df[COL_PATIENT_ACCOUNT].map(normalize_account)
    return df

def build_cumulative(df: pd.DataFrame) -> pd.DataFrame:
    amt_col = detect_amount_col(df)
    df["_amt"] = df[amt_col].apply(to_amount)

    agg = df.groupby(COL_PATIENT_ACCOUNT)["_amt"].sum().reset_index()
    agg.rename(columns={"_amt": "TOTAL_AMOUNT_PAID_CUMULATIVE"}, inplace=True)

    for extra in CARRY_FORWARD:
        if extra in df.columns:
            tmp = df[[COL_PATIENT_ACCOUNT, extra]].replace("", pd.NA).dropna()
            tmp = tmp.groupby(COL_PATIENT_ACCOUNT).first().reset_index()
            agg = agg.merge(tmp, on=COL_PATIENT_ACCOUNT, how="left")

    if COL_PATIENT_NAME in agg.columns:
        def split_name(s):
            parts = [p.strip() for p in str(s).split(",")]
            last = parts[0] if parts else ""
            first = parts[1] if len(parts) > 1 else ""
            return pd.Series({"firstName": first, "lastName": last})
        agg = pd.concat([agg, agg[COL_PATIENT_NAME].apply(split_name)], axis=1)

    agg.sort_values("TOTAL_AMOUNT_PAID_CUMULATIVE", ascending=False, inplace=True)
    agg.reset_index(drop=True, inplace=True)
    return agg
# -----------------------------------------------------


# ------------------ GHL API (v2) ---------------------
def require_env():
    token = os.getenv("GHL_TOKEN", "")
    location_id = os.getenv("LOCATION_ID", "")
    if not token:
        raise SystemExit("Missing env var GHL_TOKEN (Sub-account Private Integration token).")
    if not location_id:
        raise SystemExit("Missing env var LOCATION_ID (copy from /v2/location/<ID>/ in the app).")
    return token, location_id

def h():
    token, _ = require_env()
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Version": VERSION_HDR,
    }

def get_location_id():
    _, location_id = require_env()
    return location_id

def get_custom_fields() -> Dict[str, Any]:
    location_id = get_location_id()
    url = f"{BASE}/locations/{location_id}/customFields"
    r = requests.get(url, headers=h(), timeout=30)
    if r.status_code == 403:
        raise SystemExit(
            f"403 Forbidden on {url}. Token lacks access to this location or wrong token type.\nBody: {r.text}"
        )
    r.raise_for_status()
    return r.json()

def resolve_field_ids_strict() -> Dict[str, str]:
    """
    Map your merge-tag-like keys to actual field IDs.
    If there are duplicates with the same label, prefer exact key matches.
    """
    wanted = {
        "total_amount_paid": "contact.total_amount_paid",
        "patient_account": "contact.patient_account",
        "patient_name": "contact.patient_name",
        "location_name": "contact.location_name",
        "insurance_name": "contact.insurance_name",
    }
    data = get_custom_fields()
    items = data.get("customFields") or data.get("items") or []
    out = {}
    for k, want in wanted.items():
        bare = want.split(".")[-1].lower().replace(" ", "_")
        pick = None
        for f in items:
            candidates = [f.get("key"), f.get("name"), f.get("label"), f.get("fieldKey")]
            normed = [str(x or "").strip() for x in candidates]
            if want in normed:
                pick = f
                break
            if any(str(x).lower().replace(" ", "_") == bare for x in normed):
                pick = f
        if not pick:
            raise RuntimeError(f"Custom field not found for '{want}'. Create it in GHL or check token/location.")
        out[k] = pick["id"]
    return out

def upsert_contact(row: pd.Series, field_ids: Dict[str, str]) -> dict:
    location_id = get_location_id()
    acct = normalize_account(row[COL_PATIENT_ACCOUNT])

    payload = {
        "locationId": location_id,
        "firstName": str(row.get("firstName", "")),
        "lastName": str(row.get("lastName", "")),
        "email": f"{acct}@patients.local",  # upsert anchor
        "customFields": [
            {"id": field_ids["patient_account"], "value": acct},
            {"id": field_ids["total_amount_paid"], "value": float(row["TOTAL_AMOUNT_PAID_CUMULATIVE"])},
        ],
    }
    # Optional descriptors
    loc_name = row.get("LOCATION NAME", "")
    if isinstance(loc_name, str) and loc_name:
        payload["customFields"].append({"id": field_ids.get("location_name"), "value": loc_name})
    ins_name = row.get("INSURANCE NAME", "")
    if isinstance(ins_name, str) and ins_name:
        payload["customFields"].append({"id": field_ids.get("insurance_name"), "value": ins_name})
    pname = row.get("PATIENT NAME", "")
    if isinstance(pname, str) and pname:
        payload["customFields"].append({"id": field_ids.get("patient_name"), "value": pname})

    # Debug one sample per run (first row)
    if row.name == 0:
        print("UPSERT PAYLOAD (sample):", json.dumps(payload, indent=2)[:1200])

    r = requests.post(f"{BASE}/contacts/upsert", headers=h(), data=json.dumps(payload), timeout=30)
    if r.status_code >= 300:
        raise requests.exceptions.HTTPError(f"Upsert failed {r.status_code}: {r.text}", response=r)
    return r.json()

def verify_contact(email: str) -> dict:
    location_id = get_location_id()
    url = f"{BASE}/contacts/?locationId={location_id}&query={email}"
    r = requests.get(url, headers=h(), timeout=20)
    r.raise_for_status()
    return r.json()
# -----------------------------------------------------


# -------------------- Main run -----------------------
def main_once():
    # 1) Load and show raw
    print("Reading raw sheet...")
    df = load_raw_df()
    print("\n=== RAW HEAD ===")
    print(df.head(10).to_string(index=False))

    # 2) Build cumulative
    dfc = build_cumulative(df)
    print("\n=== CUMULATIVE HEAD ===")
    print(dfc.head(10).to_string(index=False))

    # 3) Save CSV
    dfc.to_csv(OUT_CSV, index=False)
    print(f"\nSaved: {OUT_CSV} (rows: {len(dfc)})")

    if not PUSH_TO_GHL:
        print("\nPUSH_TO_GHL=False. Skipping CRM push until you confirm the CSV.")
        return

    # 4) Resolve field IDs and push
    print("\nResolving custom fields via v2…")
    field_ids = resolve_field_ids_strict()
    print("Field IDs:", field_ids)

    pushed = 0
    for _, row in dfc.iterrows():
        try:
            upsert_contact(row, field_ids)
            pushed += 1
            time.sleep(0.1)  # be polite
        except Exception as e:
            print(f"[error] Failed [{row[COL_PATIENT_ACCOUNT]}]: {e}")
    print(f"\nPushed {pushed} contacts to GHL.")

    # 5) Verify first contact
    if len(dfc):
        acct0 = normalize_account(dfc.iloc[0][COL_PATIENT_ACCOUNT])
        email0 = f"{acct0}@patients.local"
        try:
            chk = verify_contact(email0)
            print("READBACK (sample):", json.dumps(chk, indent=2)[:1200])
        except Exception as e:
            print("[warn] readback failed:", e)

def run_once_with_retries():
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            main_once()
            return
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            wait = min(60 * attempt, 300)  # 1m, 2m, 3m up to 5m
            print(f"[warn] transient {type(e).__name__}: {e}. retrying in {wait}s [{attempt}/{MAX_RETRIES}]")
            time.sleep(wait)
        except requests.exceptions.HTTPError as e:
            code = getattr(e.response, "status_code", None)
            if code and 500 <= code < 600:
                wait = min(60 * attempt, 300)
                print(f"[warn] server {code}: retrying in {wait}s [{attempt}/{MAX_RETRIES}]")
                time.sleep(wait)
            else:
                print("[error] non-retryable HTTP error. bailing this run.")
                traceback.print_exc()
                return
        except Exception as e:
            print("[error] unexpected exception. bailing this run.")
            traceback.print_exc()
            return

def main():
    if not RUN_FOREVER:
        run_once_with_retries()
        return
    try:
        while True:
            print("\n========== RUN START ==========")
            run_once_with_retries()
            jitter = random.randint(0, JITTER_MAX)
            sleep_for = SLEEP_SECONDS + jitter
            print(f"Sleeping for {sleep_for}s (~24h + {jitter}s jitter). Ctrl-C to stop.")
            time.sleep(sleep_for)
    except KeyboardInterrupt:
        print("\nStopped by user. Goodbye.")

if __name__ == "__main__":
    main()
