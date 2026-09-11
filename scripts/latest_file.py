"""Print the current monthly "Full" archive file for a fiscal year and award
type, as `<file_name>\t<url>`, using USAspending's list_monthly_files API.

    python scripts/latest_file.py 2024 contracts
"""
import json
import sys
import urllib.request

API = "https://api.usaspending.gov/api/v2/bulk_download/list_monthly_files/"

fiscal_year, award_type = int(sys.argv[1]), sys.argv[2]
req = urllib.request.Request(
    API,
    data=json.dumps({"agency": "all", "fiscal_year": fiscal_year, "type": award_type}).encode(),
    headers={"Content-Type": "application/json", "User-Agent": "labordata-usaspending/0.1"},
)
with urllib.request.urlopen(req, timeout=60) as resp:
    files = json.load(resp)["monthly_files"]

full = [f for f in files if f["file_name"].startswith(f"FY{fiscal_year}_") and "_Full_" in f["file_name"]]
if not full:
    sys.exit(f"no Full file for FY{fiscal_year} {award_type}: {[f['file_name'] for f in files]}")
f = max(full, key=lambda f: f["updated_date"])
print(f["file_name"], f["url"], sep="\t")
