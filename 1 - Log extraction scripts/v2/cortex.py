import requests
import csv
import json
from datetime import datetime

# Cortex API Configuration
API_KEY = "iOFZcVonA1FjdrFlwthnhpNhbOrhr6q7"
CORTEX_URL = "http://localhost:9001/api"
OUTPUT_FILE = "analysis.csv"

headers = {"Authorization": f"Bearer {API_KEY}"}


def format_timestamp(ms):
    
    try:
        return datetime.fromtimestamp(ms / 1000).strftime("%d/%m/%Y %H:%M") if ms else ""
    except Exception:
        return ""


def flatten_report(report, parent_key=""):

    items = []

    if isinstance(report, dict):
        for k, v in report.items():
            new_key = f"{parent_key}.{k}" if parent_key else k
            items.extend(flatten_report(v, new_key))
    elif isinstance(report, list):
        for v in report:
            items.extend(flatten_report(v, parent_key))
    else:
        items.append(f"{parent_key}={report}")

    return items


def extract_flat_report(report_json):

    try:
        flat_parts = flatten_report(report_json)
        return " | ".join(flat_parts)
    except Exception:
        return ""


# Fetch all jobs
response = requests.get(f"{CORTEX_URL}/job", headers=headers)
if response.status_code != 200:
    print(f"Failed to fetch jobs list: {response.status_code}")
    exit(1)

jobs = response.json()

all_observables = []

for job in jobs:
    job_id = job.get("id")
    try:
        report_response = requests.get(f"{CORTEX_URL}/job/{job_id}/report", headers=headers)
        report_response.raise_for_status()
        report_json = report_response.json()
    except Exception:
        report_json = {}

    report_excerpt = extract_flat_report(report_json)

    observable_obj = {
        "data": job.get("data", ""),
        "dataType": job.get("dataType", ""),
        "createdBy": job.get("createdBy", ""),
        "createdAt": format_timestamp(job.get("createdAt")),
        "report": report_excerpt
    }

    all_observables.append(observable_obj)

# Write to CSV 
fieldname = "Observables Analysis"

with open(OUTPUT_FILE, "w", newline='', encoding="utf-8") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=[fieldname], quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()

    observable_json_str = json.dumps(all_observables, ensure_ascii=False, separators=(',', ':'))
    writer.writerow({fieldname: observable_json_str})

print(f" Exported analysis into: {OUTPUT_FILE}")

