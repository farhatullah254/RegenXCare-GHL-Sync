#!/usr/bin/env python3
import os, re, time, json
from typing import Dict, Any
import pandas as pd
import requests

# ===== CONFIG =====
GOOGLE_SHEET_ID = "13fxvJjvSl4fHqNfekckoneZqGPOZL6rNy30eJygikNU"
RAW_TAB_GID = "0"  # change if your raw tab isn't the first
CSV_URL = f"https://docs.google.com/spreadsheets/d/{GOOGLE_SHEET_ID}/export?format=csv&gid={RAW_TAB_GID}"

OUT_CSV = "cumulative_spending.csv"

COL_PATIENT_ACCOUNT = "PATIENT ACCOUNT"
COL_PATIENT_NAME    = "PATIENT NAME"
AMOUNT_CANDIDATES   = ["TOTAL AMOUNT PAID","AMOUNT PAID","PAID","TOTAL PAID"]
CARRY_FORWARD       = ["PATIENT NAME","LOCATION NAME","INSURANCE NAME"]

# GHL v2
BASE = "https://services.leadconnectorhq.com"
GHL_TOKEN    = "pit-acbc3bae-3dd2-4aff-9aca-aaa4c8b22cfe"
LOCATION_ID  = "twb3X04ZwAGeLN8iXc1O"  # e.g. the id visible in the app
VERSION_HDR  = "2021-07-28"

PUSH_TO_GHL = True  # flip to True after you eyeball the CSV
# ==================

_money = re.compile(r"[^0-9.\-]")
def to_amount(x) -> float:
    s = str(x or "").strip()
    if not s: return 0.0
    s = _money.sub("", s)
    try: return float(s)
    except: return 0.0

def detect_amount_col(df: pd.DataFrame) -> str:
    for c in AMOUNT_CANDIDATES:
        if c in df.columns: return c
    raise ValueError(f"Amount column not found. Tried {AMOUNT_CANDIDATES}. Got: {list(df.columns)}")

def load_raw_df() -> pd.DataFrame:
    df = pd.read_csv(CSV_URL, dtype=str).fillna("")
    if COL_PATIENT_ACCOUNT not in df.columns:
        raise ValueError(f"Missing '{COL_PATIENT_ACCOUNT}'. Columns: {list(df.columns)}")
    return df

def build_cumulative(df: pd.DataFrame) -> pd.DataFrame:
    amt_col = detect_amount_col(df)
    df["_amt"] = df[amt_col].apply(to_amount)
    agg = df.groupby(COL_PATIENT_ACCOUNT)["_amt"].sum().reset_index()
    agg.rename(columns={"_amt":"TOTAL_AMOUNT_PAID_CUMULATIVE"}, inplace=True)

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

    return agg.sort_values("TOTAL_AMOUNT_PAID_CUMULATIVE", ascending=False).reset_index(drop=True)

def h():
    return {
        "Authorization": f"Bearer {GHL_TOKEN}",
        "Content-Type": "application/json",
        "Version": VERSION_HDR
    }

def get_custom_fields():
    url = f"{BASE}/locations/{LOCATION_ID}/customFields"
    r = requests.get(url, headers=h(), timeout=30)
    r.raise_for_status()
    return r.json()

def upsert_contact(row: pd.Series, field_ids: Dict[str,str]):
    payload = {
        "locationId": LOCATION_ID,
        "firstName": str(row.get("firstName","")),
        "lastName":  str(row.get("lastName","")),
        "email": f"{str(row[COL_PATIENT_ACCOUNT]).strip()}@patients.local",  # upsert anchor
        "customFields": [
            {"id": field_ids["patient_account"], "value": str(row[COL_PATIENT_ACCOUNT]).strip()},
            {"id": field_ids["total_amount_paid"], "value": float(row["TOTAL_AMOUNT_PAID_CUMULATIVE"])},
        ]
    }
    # Optional descriptors
    if "LOCATION NAME" in row and isinstance(row["LOCATION NAME"], str) and row["LOCATION NAME"]:
        payload["customFields"].append({"id": field_ids.get("location_name"), "value": row["LOCATION NAME"]})
    if "INSURANCE NAME" in row and isinstance(row["INSURANCE NAME"], str) and row["INSURANCE NAME"]:
        payload["customFields"].append({"id": field_ids.get("insurance_name"), "value": row["INSURANCE NAME"]})
    if "PATIENT NAME" in row and isinstance(row["PATIENT NAME"], str) and row["PATIENT NAME"]:
        payload["customFields"].append({"id": field_ids.get("patient_name"), "value": row["PATIENT NAME"]})

    r = requests.post(f"{BASE}/contacts/upsert", headers=h(), data=json.dumps(payload), timeout=30)
    if r.status_code >= 300:
        raise RuntimeError(f"Upsert failed {r.status_code}: {r.text}")
    return r.json()

def resolve_field_ids() -> Dict[str,str]:
    # Pull all, match by key/name/label like "contact.total_amount_paid"
    wanted = {
        "total_amount_paid": "contact.total_amount_paid",
        "patient_account":   "contact.patient_account",
        "patient_name":      "contact.patient_name",
        "location_name":     "contact.location_name",
        "insurance_name":    "contact.insurance_name",
    }
    data = get_custom_fields()  # v2 endpoint
    items = data.get("customFields") or data.get("items") or []
    out = {}
    for k, want in wanted.items():
        pick = None
        bare = want.split(".")[-1].lower().replace(" ", "_")
        for f in items:
            # fields often carry key/name/label/fieldKey in v2
            for cand in (f.get("key"), f.get("name"), f.get("label"), f.get("fieldKey")):
                s = str(cand or "").strip()
                if s == want or s.lower().replace(" ", "_") == bare:
                    pick = f; break
            if pick: break
        if not pick:
            raise RuntimeError(f"Custom field not found for '{want}'. Create it in GHL.")
        out[k] = pick["id"]
    return out

def main():
    # Raw preview
    df = load_raw_df()
    print("\n=== RAW HEAD ===")
    print(df.head(10).to_string(index=False))

    # Cumulative
    dfc = build_cumulative(df)
    print("\n=== CUMULATIVE HEAD ===")
    print(dfc.head(10).to_string(index=False))

    # Save CSV
    dfc.to_csv(OUT_CSV, index=False)
    print(f"\nSaved: {OUT_CSV} (rows: {len(dfc)})")

    if not PUSH_TO_GHL:
        print("\nPUSH_TO_GHL=False. Skipping CRM push until you confirm the CSV.")
        return

    # Push
    print("\nResolving custom fields via v2…")
    field_ids = resolve_field_ids()
    print("Field IDs:", field_ids)

    pushed = 0
    for _, row in dfc.iterrows():
        upsert_contact(row, field_ids)
        pushed += 1
        time.sleep(0.1)
    print(f"\nPushed {pushed} contacts to GHL.")

if __name__ == "__main__":
    main()
