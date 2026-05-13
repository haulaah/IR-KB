import requests
import json
import pandas as pd
import csv
import time
from datetime import datetime
from requests.auth import HTTPBasicAuth


# TheHive logs extraction
HIVE_URL = "http://#.#.#.#:9000"
HIVE_CASE_ID = "~###"
HIVE_USERNAME = "######"
HIVE_PASSWORD = "######"

hive_auth = HTTPBasicAuth(HIVE_USERNAME, HIVE_PASSWORD)


def hive_format_time(ms):
    try:
        return datetime.fromtimestamp(ms / 1000).strftime("%d/%m/%Y %H:%M") if ms else ""
    except:
        return ""


def hive_remove_multiline(text):
    if not text:
        return ""
    return text.replace("\n", " ").replace("\r", " ").strip()


def hive_sanitize_case_id(cid):
    return cid.lstrip("~") if cid else ""


def hive_convert_timestamps(obj, keys):
    for k in keys:
        if k in obj and isinstance(obj[k], (int, float)):
            obj[k] = hive_format_time(obj[k])
    return obj


def get_task_activity_logs(task_id):
    """Fetches activity logs and formats ID, Creator, Date, and Type into a string."""
    log_query = {
        "query": {
            "_parent": {
                "_type": "task",
                "_query": { "_id": task_id }
            }
        }
    }
    try:
        resp = requests.post(f"{HIVE_URL}/api/case/task/log/_search", auth=hive_auth, json=log_query)
        if resp.status_code == 200:
            logs = resp.json()
            hits = logs if isinstance(logs, list) else logs.get('hits', {}).get('hits', [])

            formatted_entries = []
            for hit in hits:
                data = hit if '_type' in hit else hit.get('_source', {})

                log_id = data.get('id', 'N/A')
                author = data.get('createdBy', 'N/A')
                date = hive_format_time(data.get('createdAt'))
                l_type = data.get('_type', 'case_task_log')
                message = data.get('message', '')


                entry_block = (
                    f"\n--- LOG ENTRY ---\n"
                    f"ID: {log_id}\n"
                    f"Author: {author}\n"
                    f"Date: {date}\n"
                    f"Type: {l_type}\n"
                    f"Message: {message}\n"
                )
                formatted_entries.append(entry_block)

            if formatted_entries:
                return "\n\n=== TASK ACTIVITY HISTORY ===\n" + "\n".join(formatted_entries)
    except Exception as e:
        print(f"Error fetching logs for task {task_id}: {e}")
    return ""


def hive_clean_tasks(tasks):
    clean = []
    for t in tasks:

        d = {k: t[k] for k in ["createdBy", "createdAt", "title", "description", "owner",
                               "status", "startDate", "endDate"] if k in t}


        t_id = t.get('id') or t.get('_id')

        # Fetch activity logs
        activity_text = get_task_activity_logs(t_id)

        # Merge logs into description:

        original_desc = d.get("description", "")
        d["description"] = f"{original_desc}{activity_text}".strip()


        hive_convert_timestamps(d, ["createdAt", "startDate", "endDate"])
        clean.append(d)
    return clean


def hive_clean_observables(obs):
    clean = []
    for o in obs:
        d = {k: o[k] for k in ["createdBy", "createdAt", "dataType", "data"] if k in o}
        hive_convert_timestamps(d, ["createdAt"])
        d["data"] = hive_remove_multiline(d.get("data", ""))
        clean.append(d)
    return clean


def extract_hive():
    print("Extracting log data from The Hive...")

    case = requests.get(f"{HIVE_URL}/api/case/{HIVE_CASE_ID}", auth=hive_auth).json()
    case["id"] = hive_sanitize_case_id(case.get("id"))

    # Fetch Tasks
    task_query = {"query": {"_parent": {"_type": "case", "_query": {"_id": HIVE_CASE_ID}}}}
    tasks_raw = requests.post(f"{HIVE_URL}/api/case/task/_search", auth=hive_auth, json=task_query).json()

    tasks = [hit["_source"] for hit in tasks_raw.get("hits", {}).get("hits", [])] if isinstance(tasks_raw, dict) else tasks_raw
    tasks_clean = hive_clean_tasks(tasks)

    # Fetch Observables
    obs_query = {"query": {"_parent": {"_type": "case", "_query": {"_id": HIVE_CASE_ID}}}}
    obs_raw = requests.post(f"{HIVE_URL}/api/case/artifact/_search", auth=hive_auth, json=obs_query).json()
    observables = [hit["_source"] for hit in obs_raw.get("hits", {}).get("hits", [])] if isinstance(obs_raw, dict) else obs_raw
    observables_clean = hive_clean_observables(observables)

    fields = [
        "id", "createdBy", "createdAt", "title", "description",
        "severity", "startDate", "endDate", "status", "stage",
        "tasks", "observables"
    ]

    row = {}
    for f in fields:
        if f in ["createdAt", "startDate", "endDate"]:
            row[f] = hive_format_time(case.get(f))
        elif f == "tasks":
            row[f] = json.dumps(tasks_clean, ensure_ascii=False)
        elif f == "observables":
            row[f] = json.dumps(observables_clean, ensure_ascii=False)
        else:
            row[f] = case.get(f, "")

    return pd.DataFrame([row])


# MISP logs extraction

MISP_URL = "http://#.#.#.#:8080"
MISP_API_KEY = "#####"
MISP_EVENT_ID = ###

misp_headers = {
    "Authorization": MISP_API_KEY,
    "Accept": "application/json",
    "Content-Type": "application/json"
}


def extract_misp():
    print("Extracting log data from MISP...")

    r = requests.get(
        f"{MISP_URL}/audit_logs/eventIndex/{MISP_EVENT_ID}.json",
        headers=misp_headers,
        verify=False
    )
    r.raise_for_status()

    data = r.json()
    logs = data if isinstance(data, list) else []

    simplified = []
    for item in logs:
        log = item.get("AuditLog", {})
        usr = item.get("User", {})
        simplified.append({
            "created": log.get("created", ""),
            "user_id": str(usr.get("id", "")),
            "user_email": usr.get("email", ""),
            "action": log.get("action", ""),
            "event_id": str(log.get("event_id", "")),
            "title": log.get("title", "")
        })

    return pd.DataFrame({
        "Event": [json.dumps(simplified)]
    })


# Cortex logs extraction


CORTEX_API_KEY = "######"
CORTEX_URL = "http://#.#.#.#:9001/api"

cortex_headers = {"Authorization": f"Bearer {CORTEX_API_KEY}"}


def cortex_format_timestamp(ms):
    try:
        return datetime.fromtimestamp(ms / 1000).strftime("%d/%m/%Y %H:%M") if ms else ""
    except:
        return ""


def cortex_extract_summary_only(report_json):
    fields_to_keep = [
        "organization", "updatedAt", "tlp", "pap", "endDate", "createdAt",
        "createdBy", "updatedBy", "startDate", "status", "data", "dataType",
        "workerName", "workerId", "workerDefinitionId", "analyzerName",
        "analyzerId", "analyzerDefinitionId", "id", "cacheTag", "type"
    ]

    flat_items = []
    for key in fields_to_keep:
        if key in report_json:
            flat_items.append(f"{key}={report_json[key]}")

    try:
        taxonomies = report_json.get("report", {}).get("summary", {}).get("taxonomies", [])
        for t in taxonomies:
            ns = t.get("namespace", "")
            pred = t.get("predicate", "")
            val = t.get("value", "")
            level = t.get("level", "")
            if ns or pred or val:
                flat_items.append(f"taxonomy.namespace={ns}")
                flat_items.append(f"taxonomy.predicate={pred}")
                flat_items.append(f"taxonomy.value={val}")
                flat_items.append(f"taxonomy.level={level}")
    except Exception:
        pass

    return " | ".join(flat_items)


def extract_cortex():
    print("Extracting log data from Cortex...")

    response = requests.get(f"{CORTEX_URL}/job", headers=cortex_headers)
    response.raise_for_status()
    jobs = response.json()

    trimmed_reports = []
    for job in jobs:
        job_id = job.get("id")
        try:
            r = requests.get(f"{CORTEX_URL}/job/{job_id}/report", headers=cortex_headers)
            r.raise_for_status()
            report_json = r.json()
        except:
            report_json = {}
        summary_text = cortex_extract_summary_only(report_json)
        trimmed_reports.append({
            "data": job.get("data", ""),
            "dataType": job.get("dataType", ""),
            "createdBy": job.get("createdBy", ""),
            "createdAt": cortex_format_timestamp(job.get("createdAt")),
            "report": summary_text
        })

    return pd.DataFrame({
        "Observables Analysis": [json.dumps(trimmed_reports, ensure_ascii=False)]
    })



# Mattermost logs extraction


BASE_URL = "http://#.#.#.#:8065"
ACCESS_TOKEN = "#####"
TEAM_ID = "#####"
PER_PAGE = 200

HEADERS = {"Authorization": f"Bearer {ACCESS_TOKEN}"}

def api_get(path, params=None):
    r = requests.get(f"{BASE_URL}{path}", headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()

def format_timestamp_ddmmyyyy(ms):
    try:
        return datetime.fromtimestamp(ms / 1000).strftime("%d/%m/%Y %H:%M") if ms else ""
    except:
        return ""

def extract_mattermost():
    print("Extracting log data from Mattermost...")

    user_cache = {}
    responder_chat = []

    ch = api_get(f"/api/v4/teams/{TEAM_ID}/channels/name/ransomware-incident")
    channels = [ch]

    for ch in channels:
        page = 0
        while True:
            
            params = {"page": page, "per_page": PER_PAGE}
            posts_data = api_get(f"/api/v4/channels/{ch['id']}/posts", params=params)

            order = posts_data.get("order", [])
            if not order:                
                break

            for pid in order:
                p = posts_data["posts"][pid]

                uid = p.get("user_id")
                if uid not in user_cache:
                    u = api_get(f"/api/v4/users/{uid}")
                    user_cache[uid] = {
                        "id": u["id"],
                        "email": u["email"]
                    }

                responder_chat.append({
                    "channel_id": ch["id"],
                    "channel_name": ch["name"],
                    "post_id": p["id"],
                    "created_at": format_timestamp_ddmmyyyy(p["create_at"]),
                    "message": p["message"],
                    "user": {
                        "id": user_cache[uid]["id"],
                        "email": user_cache[uid]["email"]
                    }
                })

            if len(order) < PER_PAGE:   
                break
            page += 1

    return pd.DataFrame({
        "ResponderChat": [json.dumps(responder_chat, ensure_ascii=False)]
    })


# Dataset CSV Generation


if __name__ == "__main__":
    print("\nRunning Incident Response (IR) Logs Extraction Tool:\n")

    hive_df = extract_hive()
    misp_df = extract_misp()
    cortex_df = extract_cortex()
    matter_df = extract_mattermost()

    final_df = pd.concat([hive_df, misp_df, cortex_df, matter_df], axis=1, ignore_index=False)
    final_df.to_csv("irlog.csv", index=False)

    print("\n- Extraction complete")
    print("- Merged IR Log Dataset saved as: irlog.csv\n")

