import requests
import csv

# === Configuration ===
MISP_URL = "http://localhost:8080"
API_KEY = "eR3pmNjAct0fhdAhIxFF4UOrrlEOvjkGFGaTVjO8"
EVENT_ID = 3035
CSV_FILE = f"audit_logs_event_{EVENT_ID}.csv"

headers = {
    "Authorization": API_KEY,
    "Accept": "application/json",
    "Content-Type": "application/json"
}

# === Request audit logs for the event ===
response = requests.get(
    f"{MISP_URL}/audit_logs/eventIndex/{EVENT_ID}.json",
    headers=headers,
    verify=False  # Optional: skip TLS verification
)

if response.status_code != 200:
    print(f"Failed to fetch audit logs: {response.status_code} - {response.text}")
    exit(1)

data = response.json()
audit_logs = data if isinstance(data, list) else []

# === Extract required fields ===
output_rows = []
for item in audit_logs:
    log = item.get("AuditLog", {})
    user = item.get("User", {})
    org = item.get("Organisation", {})

    row = {
        "created": log.get("created", ""),
        "user_id": user.get("id", ""),
        "user_email": user.get("email", ""),
        "org_id": org.get("id", ""),
        "org_name": org.get("name", ""),
        "action": log.get("action", ""),
        "model": log.get("model", ""),
        "model_id": log.get("model_id", ""),
        "model_title": log.get("model_title", ""),
        "event_id": log.get("event_id", ""),
        "title": log.get("title", "")
    }

    output_rows.append(row)

# === Write to CSV ===
fieldnames = [
    "created", "user_id", "user_email", "org_id", "org_name",
    "action", "model", "model_id", "model_title", "event_id", "title"
]

with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(output_rows)

print(f" Exported {len(output_rows)} audit log entries to: {CSV_FILE}")
