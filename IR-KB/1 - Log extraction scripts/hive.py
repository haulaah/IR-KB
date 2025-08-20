import requests
import json
import csv
from datetime import datetime
from requests.auth import HTTPBasicAuth

# === Configuration ===
HIVE_URL = "http://localhost:9000"
CASE_ID = "~28792"
USERNAME = "admin2@irorg.com"
PASSWORD = "KinPass&01"
OUTPUT_CSV_FILE = "caseexport.csv"

auth = HTTPBasicAuth(USERNAME, PASSWORD)

# === Timestamp formatting ===
def format_timestamp(ms):
    try:
        return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S") if ms else ""
    except Exception:
        return ""

def convert_timestamps_in_dict(obj, keys):
    """Convert specific timestamp fields in a dict to readable string format"""
    for k in keys:
        if k in obj and isinstance(obj[k], (int, float)):
            obj[k] = format_timestamp(obj[k])
    return obj

def clean_and_format_tasks(tasks):
    clean = []
    for task in tasks:
        # Remove technical fields
        task = {k: v for k, v in task.items() if k not in ["_id", "_type"]}
        # Format date fields
        task = convert_timestamps_in_dict(task, ["createdAt", "updatedAt", "startDate", "endDate"])
        clean.append(task)
    return clean

def clean_and_format_observables(observables):
    clean = []
    for obs in observables:
        # Remove technical fields
        obs = {k: v for k, v in obs.items() if k not in ["_id", "_type", "stats", "reports", "ignoreSimilarity", "tags"]}
        # Format date fields
        obs = convert_timestamps_in_dict(obs, ["createdAt", "updatedAt", "startDate"])
        clean.append(obs)
    return clean

# === API calls ===
def fetch_case():
    url = f"{HIVE_URL}/api/case/{CASE_ID}"
    r = requests.get(url, auth=auth)
    r.raise_for_status()
    return r.json()

def fetch_tasks():
    url = f"{HIVE_URL}/api/case/task/_search"
    query = {
        "query": {
            "_parent": {
                "_type": "case",
                "_query": {
                    "_id": CASE_ID
                }
            }
        }
    }
    r = requests.post(url, auth=auth, headers={"Content-Type": "application/json"}, data=json.dumps(query))
    r.raise_for_status()
    return r.json()

def fetch_observables():
    url = f"{HIVE_URL}/api/case/artifact/_search"
    query = {
        "query": {
            "_parent": {
                "_type": "case",
                "_query": {
                    "_id": CASE_ID
                }
            }
        }
    }
    r = requests.post(url, auth=auth, headers={"Content-Type": "application/json"}, data=json.dumps(query))
    r.raise_for_status()
    return r.json()

# === Main process ===
def main():
    print("Fetching case data...")
    case = fetch_case()

    print("Fetching tasks...")
    tasks_raw = fetch_tasks()
    if isinstance(tasks_raw, dict):
        tasks = [hit["_source"] for hit in tasks_raw.get("hits", {}).get("hits", [])]
    elif isinstance(tasks_raw, list):
        tasks = tasks_raw
    else:
        tasks = []
    tasks_clean = clean_and_format_tasks(tasks)

    print("Fetching observables...")
    observables_raw = fetch_observables()
    if isinstance(observables_raw, dict):
        observables = [hit["_source"] for hit in observables_raw.get("hits", {}).get("hits", [])]
    elif isinstance(observables_raw, list):
        observables = observables_raw
    else:
        observables = []
    observables_clean = clean_and_format_observables(observables)

    # === Fields to export ===
    fields = [
        "id", "createdBy", "updatedBy", "createdAt", "updatedAt",
        "caseId", "title", "description", "severity", "startDate", "endDate",
        "impactStatus", "status", "extendedStatus", "stage", "summary", "owner",
        "tasks", "observables"
    ]

    # === Build CSV row ===
    row = {}
    for field in fields:
        if field in ["createdAt", "updatedAt", "startDate", "endDate"]:
            row[field] = format_timestamp(case.get(field))
        elif field == "tasks":
            row["tasks"] = json.dumps(tasks_clean, ensure_ascii=False)
        elif field == "observables":
            row["observables"] = json.dumps(observables_clean, ensure_ascii=False)
        else:
            row[field] = case.get(field, "")

    # === Write CSV ===
    with open(OUTPUT_CSV_FILE, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        writer.writerow(row)

    print(f" Exported case data to '{OUTPUT_CSV_FILE}'")

if __name__ == "__main__":
    main()
