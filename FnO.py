# ═══════════════════════════════════════════════════════════════════════════
#  FnO.py — Chartink FnO Scan → Google Sheets
#  (Copy of LongTerm.py — only CONFIG lines below are changed)
# ═══════════════════════════════════════════════════════════════════════════

# ── CONFIG — only these 3 lines change per scan file ──────────────────────
SCAN_NAME         = "FnO"                         # → Google Sheet tab name
CONDITIONS_FOLDER = "Files/Conditions_FnO"         # → folder with .txt files
SHEET_ENV_VAR     = "GOOGLE_SHEET_URL_SCANS"       # → same sheet, different tab
# ──────────────────────────────────────────────────────────────────────────

# === paste the rest of LongTerm.py exactly from here down — nothing changes ===
import os
import sys
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime

base_dir         = os.path.dirname(os.path.abspath(__file__))
support_code_dir = os.path.join(base_dir, "Files", "support_code")
conditions_dir   = os.path.join(base_dir, CONDITIONS_FOLDER)
error_log_dir    = os.path.join(base_dir, "Files", "error_logs")
date_string      = datetime.now().strftime("%Y%m%d_%H%M")

sys.path.append(support_code_dir)
from chartink_utils import get_data_from_chartink, ChartinkFetchError
from formatter import add_condition_headers

os.makedirs(error_log_dir, exist_ok=True)

print(f"\n{'='*60}")
print(f"  {SCAN_NAME} Scan  |  {datetime.now().strftime('%Y-%m-%d %H:%M')} UTC")
print(f"{'='*60}")

dataframes        = []
failed_conditions = []

for condition_file in sorted(os.listdir(conditions_dir)):
    if not condition_file.endswith(".txt"):
        continue
    filepath = os.path.join(conditions_dir, condition_file)
    with open(filepath, 'r') as f:
        condition = f.read().strip()
    try:
        df = get_data_from_chartink(condition)
        if not df.empty:
            df['condition'] = condition_file.replace('.txt', '')
            dataframes.append(df)
            print(f"  ✓  {condition_file:<40}  {len(df)} stocks")
        else:
            print(f"  ○  {condition_file:<40}  no results")
    except ChartinkFetchError as e:
        failed_conditions.append((condition_file, str(e)))
        print(f"  ✗  {condition_file:<40}  fetch error: {e}")
    except Exception as e:
        failed_conditions.append((condition_file, f"Unexpected: {str(e)}"))
        print(f"  ✗  {condition_file:<40}  unexpected: {e}")

if dataframes:
    combined  = pd.concat(dataframes, ignore_index=True)
    formatted = add_condition_headers(combined)
    formatted['nsecode'] = (formatted['nsecode']
                            .str.replace('&', '_', regex=False)
                            .str.replace('-', '_', regex=False))
    stock_series = formatted['nsecode'].apply(
        lambda x: "NSE:" + x if not x.startswith('#') else x
    )
    sheet_rows = [[v] for v in stock_series.tolist()]
    print(f"\n  Total rows to upload: {len(sheet_rows)}")
else:
    sheet_rows = [["No data found"]]
    print("\n  No data returned from any condition.")

sheet_url  = os.environ.get(SHEET_ENV_VAR)
creds_path = os.environ.get("GOOGLE_CREDENTIALS_PATH", "/tmp/google_credentials.json")

if sheet_url:
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds  = Credentials.from_service_account_file(creds_path, scopes=scopes)
        client = gspread.authorize(creds)
        sh     = client.open_by_url(sheet_url)
        try:
            ws = sh.worksheet(SCAN_NAME)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=SCAN_NAME, rows=3000, cols=5)
        ws.clear()
        ws.update("A1", [[f"{SCAN_NAME}  |  Run: {datetime.now().strftime('%Y-%m-%d %H:%M')} UTC"]])
        ws.update("A2", sheet_rows)
        print(f"  ✓ Uploaded to Google Sheets → tab '{SCAN_NAME}'")
    except Exception as e:
        print(f"  ✗ Google Sheets upload failed: {e}")
        raise
else:
    local_file = os.path.join(error_log_dir, f"{SCAN_NAME}_Scan_{date_string}.txt")
    pd.Series([r[0] for r in sheet_rows]).to_csv(local_file, index=False, header=False)
    print(f"  No SHEET_ENV_VAR set. Saved locally → {local_file}")

if failed_conditions:
    error_log_path = os.path.join(error_log_dir, f"error_log_{SCAN_NAME}_{date_string}.txt")
    with open(error_log_path, 'w') as lf:
        for cfile, err in failed_conditions:
            lf.write(f"{cfile}: {err}\n")
    print(f"  Failed conditions logged → {error_log_path}")
else:
    print(f"  No errors.")

print(f"{'='*60}\n")