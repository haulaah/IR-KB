import requests
import json
import pandas as pd
from datetime import datetime
from requests.auth import HTTPBasicAuth


#TheHive logs extraction

HIVE_URL = "http://localhost:9000"
HIVE_CASE_ID = "~8192"
HIVE_USERNAME = "analyst00002@ir-org.com"
HIVE_PASSWORD = "K7RYpTon*202#"

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


def hive_clean_tasks(tasks):
    clean = []
    for t in tasks:
        d = {k: t[k] for k in ["createdBy", "createdAt", "title", "description", "owner",
                               "status", "startDate", "endDate"] if k in t}

        hive_convert_timestamps(d, ["createdAt", "startDate", "endDate"])
        d["description"] = hive_remove_multiline(d.get("description", ""))

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


    task_query = {"query": {"_parent": {"_type": "case", "_query": {"_id": HIVE_CASE_ID}}}}
    tasks_raw = requests.post(
        f"{HIVE_URL}/api/case/task/_search",
        auth=hive_auth, json=task_query
    ).json()

    if isinstance(tasks_raw, dict):
        tasks = [hit["_source"] for hit in tasks_raw.get("hits", {}).get("hits", [])]
    else:
        tasks = tasks_raw  

    tasks_clean = hive_clean_tasks(tasks)


    obs_query = {"query": {"_parent": {"_type": "case", "_query": {"_id": HIVE_CASE_ID}}}}
    obs_raw = requests.post(
        f"{HIVE_URL}/api/case/artifact/_search",
        auth=hive_auth, json=obs_query
    ).json()

    if isinstance(obs_raw, dict):
        observables = [hit["_source"] for hit in obs_raw.get("hits", {}).get("hits", [])]
    else:
        observables = obs_raw

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


#MISP logs extraction

MISP_URL = "http://localhost:8080"
MISP_API_KEY = "YgmAjZKETLRQJ7ItSlwOe4PHCjsvRtk2M4cQ3maI"
MISP_EVENT_ID = 4836

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

#Cortex logs extraction

CORTEX_API_KEY = "iMZMDxPhhE02serRS8dnUEtAd4V4H/nj"
CORTEX_URL = "http://localhost:9001/api"

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
        taxonomies = report_json.get("report", {}) \
                                .get("summary", {}) \
                                .get("taxonomies", [])

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


#Dataset CSV File Generation

if __name__ == "__main__":
    print("\nRunning Incident Response (IR) Logs Extraction Tool:\n")

    hive_df = extract_hive()
    misp_df = extract_misp()
    cortex_df = extract_cortex()

    final_df = pd.concat([hive_df, misp_df, cortex_df], axis=1)
    final_df.to_csv("irlog.csv", index=False)

    print("\n- Extraction complete")
    print("- Merged IR Log Dataset saved as: irlog.csv\n")

