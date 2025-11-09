import requests
import json
import csv
from datetime import datetime
from requests.auth import HTTPBasicAuth

# TheHive API Configuration
HIVE_URL = "http://localhost:9000"
CASE_ID = "~8264"
USERNAME = "#######################"
PASSWORD = "#######################"
OUTPUT_FILE = "case.csv"

auth = HTTPBasicAuth(USERNAME, PASSWORD)

# Format timestamps as DD/MM/YYYY HH:MM
def format_timestamp_ddmmyyyy(ms):
    try:
        return datetime.fromtimestamp(ms / 1000).strftime("%d/%m/%Y %H:%M") if ms else ""
    except Exception:
        return ""

# Remove multi-line from strings
def remove_multiline(text):
    if not text:
        return ""
    # Replace all types of line breaks with a single space
    return text.replace("\n", " ").replace("\r", " ").strip()

# Sanitize incident ID by removing ~
def sanitize_incident_id(incident_id):
    if not incident_id:
        return ""
    return incident_id.lstrip("~")

# Convert selected timestamps 
def convert_timestamps_in_dict(obj, keys):
    for k in keys:
        if k in obj and isinstance(obj[k], (int, float)):
            obj[k] = format_timestamp_ddmmyyyy(obj[k])
    return obj

# Clean tasks to include only selected fields and sanitize description
def clean_and_format_tasks(tasks):
    clean = []
    for task in tasks:
        task = {k: task[k] for k in ["createdBy", "createdAt", "title", "description", "owner", "status", "startDate", "endDate"] if k in task}
        task = convert_timestamps_in_dict(task, ["createdAt", "startDate", "endDate"])
        if "description" in task:
            task["description"] = remove_multiline(task["description"])
        clean.append(task)
    return clean

# Clean observables to include only selected fields and sanitize data
def clean_and_format_observables(observables):
    clean = []
    for obs in observables:
        obs = {k: obs[k] for k in ["createdBy", "createdAt", "dataType", "data"] if k in obs}
        obs = convert_timestamps_in_dict(obs, ["createdAt"])
        if "data" in obs:
            obs["data"] = remove_multiline(obs["data"])
        clean.append(obs)
    return clean

def fetch_case():
    url = f"{HIVE_URL}/api/case/{CASE_ID}"
    r = requests.get(url, auth=auth)
    r.raise_for_status()
    return r.json()

def fetch_tasks():
    url = f"{HIVE_URL}/api/case/task/_search"
    query = {"query": {"_parent": {"_type": "case", "_query": {"_id": CASE_ID}}}}
    r = requests.post(url, auth=auth, headers={"Content-Type": "application/json"}, data=json.dumps(query))
    r.raise_for_status()
    return r.json()

def fetch_observables():
    url = f"{HIVE_URL}/api/case/artifact/_search"
    query = {"query": {"_parent": {"_type": "case", "_query": {"_id": CASE_ID}}}}
    r = requests.post(url, auth=auth, headers={"Content-Type": "application/json"}, data=json.dumps(query))
    r.raise_for_status()
    return r.json()

def main():
    print("Fetching case data...")
    case = fetch_case()

    # Sanitize incident ID
    case["id"] = sanitize_incident_id(case.get("id", ""))

    print("Fetching tasks...")
    tasks_raw = fetch_tasks()
    if isinstance(tasks_raw, dict):
        tasks = [hit["_source"] for hit in tasks_raw.get("hits", {}).get("hits", [])]
    else:
        tasks = tasks_raw
    tasks_clean = clean_and_format_tasks(tasks)

    print("Fetching observables...")
    observables_raw = fetch_observables()
    if isinstance(observables_raw, dict):
        observables = [hit["_source"] for hit in observables_raw.get("hits", {}).get("hits", [])]
    else:
        observables = observables_raw
    observables_clean = clean_and_format_observables(observables)

    # Selected CSV Fields
    fields = [
        "id", "createdBy", "createdAt", "title", "description",
        "severity", "startDate", "endDate", "status", "stage",
        "tasks", "observables"
    ]

    row = {}
    for field in fields:
        if field in ["createdAt", "startDate", "endDate"]:
            row[field] = format_timestamp_ddmmyyyy(case.get(field))
        elif field == "tasks":
            row["tasks"] = json.dumps(tasks_clean, ensure_ascii=False)
        elif field == "observables":
            row["observables"] = json.dumps(observables_clean, ensure_ascii=False)
        else:
            row[field] = case.get(field, "")

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        writer.writerow(row)

    print(f"Exported case into: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()

