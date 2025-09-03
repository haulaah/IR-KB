import requests
import json
import csv

# MISP API Configuration
MISP_URL = "http://localhost:8080"
API_KEY = "eR3pmNjAct0fhdAhIxFF4UOrrlEOvjkGFGaTVjO8"
EVENT_ID = 4778
OUTPUT_FILE = f"event.csv"

headers = {
    "Authorization": API_KEY,
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# Event actions audit logs for the event 
response = requests.get(
    f"{MISP_URL}/audit_logs/eventIndex/{EVENT_ID}.json",
    headers=headers,
    verify=False 
)

if response.status_code != 200:
    print(f"Failed to fetch audit logs: {response.status_code} - {response.text}")
    exit(1)

data = response.json()
audit_logs = data if isinstance(data, list) else []

# Event actions audit logs into JSON array with selected fields 
output_logs = []
for item in audit_logs:
    log = item.get("AuditLog", {})
    user = item.get("User", {})

    row = {
        "created": log.get("created", ""),        
        "user_id": str(user.get("id", "")),
        "user_email": user.get("email", ""),
        "action": log.get("action", ""),
        "event_id": str(log.get("event_id", "")),
        "title": log.get("title", "")
    }

    output_logs.append(row)

# Write single row CSV with JSON array in one field 
with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["Event"])
    writer.writeheader()
    
    writer.writerow({"Event": json.dumps(output_logs)})

print(f" Exported events into: {OUTPUT_FILE} ")

