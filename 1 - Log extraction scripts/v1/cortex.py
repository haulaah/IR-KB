import requests
import csv
from datetime import datetime

# === Configuration ===
API_KEY = "iOFZcVonA1FjdrFlwthnhpNhbOrhr6q7"
CORTEX_URL = "http://localhost:9001/api/job/0c2L9JcB0IBvl8ZzxL_6/report"
OUTPUT_FILE = "cortexexports.csv"

# === Step 1: Set headers and fetch data ===
headers = {
    "Authorization": f"Bearer {API_KEY}"
}

response = requests.get(CORTEX_URL, headers=headers)

if response.status_code != 200:
    print(f"Failed to fetch job report: {response.status_code}")
    print(response.text)
    exit(1)

job = response.json()

# === Step 2: Format timestamps ===
def format_timestamp(ms):
    try:
        return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S") if ms else ""
    except Exception:
        return ""

# === Step 3: Extract and format report taxonomies ===
def extract_taxonomies_summary(report_section):
    try:
        taxonomies = report_section.get("summary", {}).get("taxonomies", [])
        formatted_list = [
            f"level={t.get('level', '')}, namespace={t.get('namespace', '')}, predicate={t.get('predicate', '')}, value={t.get('value', '')}"
            for t in taxonomies
        ]
        return " | ".join(formatted_list)
    except Exception:
        return ""

# === Step 4: Define CSV fields ===
fields = [
    "analyzerName", "data", "dataType", "status",
    "createdBy", "createdAt", "startDate", "endDate", "updatedAt",
    "report"
]

# === Step 5: Write to CSV ===
with open(OUTPUT_FILE, "w", newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fields)
    writer.writeheader()

    writer.writerow({
        "analyzerName": job.get("analyzerName", ""),
        "data": job.get("data", ""),
        "dataType": job.get("dataType", ""),
        "status": job.get("status", ""),
        "createdBy": job.get("createdBy", ""),
        "createdAt": format_timestamp(job.get("createdAt")),
        "startDate": format_timestamp(job.get("startDate")),
        "endDate": format_timestamp(job.get("endDate")),
        "updatedAt": format_timestamp(job.get("updatedAt")),
        "report": extract_taxonomies_summary(job.get("report", {})),
    })

print(f"Exported job report to '{OUTPUT_FILE}'")
