# dashboard.py

import streamlit as st
import json
import os
import pandas as pd
import time
import zipfile
import requests
import gspread
import subprocess
import base64
import re
from oauth2client.service_account import ServiceAccountCredentials
from google.auth.exceptions import RefreshError
from itertools import islice

# NOTE: Assuming fetch_videos.py and its function fetch_all are available
# from fetch_videos import fetch_all as fetch_videos_main

# --- UI Config and Session State Initialization (MUST BE FIRST) ---
st.set_page_config(page_title="YouTube Video Dashboard", layout="wide")

# Initialize session state keys safely and immediately
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False
if "login_time" not in st.session_state:
    st.session_state["login_time"] = 0

# --- Constants & Auth Config ---
CORRECT_PASSWORD = "DemoUp2025!"
LOGIN_TIMEOUT = 2 * 60 * 60  # 7200 seconds (2 hours)

# --- Secrets (MUST exist in Streamlit secrets) ---
GOOGLE_SHEET_ID = st.secrets.get("GOOGLE_SHEET_ID")

ZENDESK_SUBDOMAIN = st.secrets.get("ZENDESK_SUBDOMAIN", "")
ZENDESK_EMAIL = st.secrets.get("ZENDESK_EMAIL", "")
ZENDESK_API_TOKEN = st.secrets.get("ZENDESK_API_TOKEN", "TO_BE_ADDED_BY_ADMIN")

# New: Zendesk allocation config
def _to_int(v, default=0):
    try:
        return int(str(v).strip())
    except Exception:
        return default

ZENDESK_VIEW_ID = _to_int(st.secrets.get("ZENDESK_VIEW_ID", 0))
ZENDESK_LIGHT_AGENT_FIELD_ID = _to_int(st.secrets.get("ZENDESK_LIGHT_AGENT_FIELD_ID", 0))
# Comma-separated list of agent IDs in secrets, e.g. "123,456,789"
ZENDESK_AGENT_IDS = [
    _to_int(x) for x in str(st.secrets.get("ZENDESK_AGENT_IDS", "")).split(",") if str(x).strip().isdigit()
]

# FIX: Add missing secrets for the Zendesk Solve section
ZENDESK_FEEDBACK_VIEW_ID = _to_int(st.secrets.get("ZENDESK_FEEDBACK_VIEW_ID", 0))
ZENDESK_SOLVE_SUBJECT_PREFIX = st.secrets.get("ZENDESK_SOLVE_SUBJECT_PREFIX", "Video Review:")


# --- Sheet names ---
QUICKWATCH_SHEET = "quickwatch"
NOT_RELEVANT_SHEET = "not_relevant"
ALREADY_DOWNLOADED_SHEET = "already downloaded"
TICKETS_CREATED_SHEET = "tickets_created"

# --- Google Sheets Authorization (CACHED) ---
@st.cache_resource
def authorize_gspread_client():
    """Initializes and caches the gspread client."""
    
    # FIX: Remove json.loads as st.secrets already loads the TOML structure into a dictionary
    try:
        SERVICE_ACCOUNT_SECRET = st.secrets["gcp_service_account"]
        if not isinstance(SERVICE_ACCOUNT_SECRET, dict):
            # Fallback for if the key was stored as a JSON string
            SERVICE_ACCOUNT_SECRET = json.loads(SERVICE_ACCOUNT_SECRET)
    except json.JSONDecodeError:
        st.error("Error: 'gcp_service_account' secret is not valid JSON string.")
        return None
    except KeyError:
        st.error("Error: 'gcp_service_account' key not found in Streamlit secrets.")
        return None

    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    try:
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_SECRET, scope)
        return gspread.authorize(credentials)
    except Exception as e:
        st.error(f"Gspread Authorization Failed. Check Service Account details. Error: {e}")
        return None

gs_client = authorize_gspread_client()
# If the client is None, stop execution immediately before loading sheets
if gs_client is None:
    st.stop()

# --- Zendesk Helpers ---
def create_zendesk_ticket(subject, description):
# ... (Zendesk helpers remain the same) ...
    if not ZENDESK_SUBDOMAIN or not ZENDESK_EMAIL or ZENDESK_API_TOKEN == "TO_BE_ADDED_BY_ADMIN":
        return False, "Zendesk API token not set. Please ask your admin."

    url = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/tickets.json"
    auth_str = f"{ZENDESK_EMAIL}/token:{ZENDESK_API_TOKEN}"
    auth_bytes = base64.b64encode(auth_str.encode("utf-8")).decode("utf-8")

    headers = {"Content-Type": "application/json", "Authorization": f"Basic {auth_bytes}"}
    payload = {
        "ticket": {
            "subject": subject,
            "comment": {"body": description},
            "priority": "normal"
        }
    }

    response = requests.post(url, headers=headers, json=payload, timeout=30)
    if response.status_code == 201:
        return True, response.json()
    else:
        return False, response.text

# Round-robin assignment helpers
def _zd_auth():
    return (f"{ZENDESK_EMAIL}/token", ZENDESK_API_TOKEN)

def _zd_base():
    return f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2"

def zd_get_tickets_from_view(view_id: int) -> list[int]:
    """Return a list of ticket IDs from the view (handles pagination)."""
    url = f"{_zd_base()}/views/{view_id}/tickets.json?per_page=100"
    ids = []
    while url:
        r = requests.get(url, auth=_zd_auth(), timeout=30)
        if r.status_code in (401, 403):
            raise RuntimeError(f"Auth error {r.status_code}: {r.text[:200]}")
        r.raise_for_status()
        data = r.json()
        ids.extend([t["id"] for t in data.get("tickets", [])])
        url = data.get("next_page")
    return ids

def _chunks(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            return
        yield chunk

def zd_update_group(ids_chunk: list[int], field_id: int, agent_id: int) -> str | None:
    """Update a chunk of ticket IDs to set the Light Agent custom field to agent_id. Returns job_status URL."""
    ids_param = ",".join(map(str, ids_chunk))
    url = f"{_zd_base()}/tickets/update_many.json?ids={ids_param}"
    payload = {
        "ticket": {
            "custom_fields": [
                {"id": field_id, "value": agent_id}
            ]
        }
    }
    r = requests.put(
        url,
        auth=_zd_auth(),
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=60
    )
    r.raise_for_status()
    return r.json().get("job_status", {}).get("url")

def zd_mass_assign_light_agent_round_robin(view_id: int, field_id: int, agent_ids: list[int]) -> dict:
    """
    Runs round-robin assignment and returns a result dict:
    {
      "total": int,
      "distribution": [{ "agent_id": int, "count": int }],
      "jobs": [{"agent_id": int, "url": str}],
    }
    """
    if not (ZENDESK_SUBDOMAIN and ZENDESK_EMAIL and ZENDESK_API_TOKEN):
        raise RuntimeError("Zendesk secrets not configured. Ask admin to set ZENDESK_* in secrets.")
    if not view_id or not field_id or not agent_ids:
        raise RuntimeError("Missing view_id/field_id/agent_ids configuration.")

    ticket_ids = zd_get_tickets_from_view(view_id)
    if not ticket_ids:
        return {"total": 0, "distribution": [], "jobs": []}

    # bucket round-robin
    buckets = {aid: [] for aid in agent_ids}
    for idx, tid in enumerate(ticket_ids):
        aid = agent_ids[idx % len(agent_ids)]
        buckets[aid].append(tid)

    # update per agent in chunks
    jobs = []
    for aid, ids in buckets.items():
        for chunk in _chunks(ids, 100):
            time.sleep(0.5)  # gentle rate limiting
            try:
                job_url = zd_update_group(chunk, ZENDESK_LIGHT_AGENT_FIELD_ID, aid)
                if job_url:
                    jobs.append({"agent_id": aid, "url": job_url})
            except requests.exceptions.RequestException as e:
                jobs.append({"agent_id": aid, "url": f"ERROR: {e}"})

    # build distribution
    base = len(ticket_ids) // len(agent_ids)
    rem = len(ticket_ids) % len(agent_ids)
    distribution = []
    for i, aid in enumerate(agent_ids):
        count = base + (1 if i < rem else 0)
        distribution.append({"agent_id": aid, "count": count})

    return {"total": len(ticket_ids), "distribution": distribution, "jobs": jobs}

# --- Download Archives (Ensures files exist before loading) ---
RAW_ZIP_URL_OFFICIAL = "https://raw.githubusercontent.com/gauravshindee/youtube-dashboard/main/data/archive.csv.zip"
RAW_ZIP_URL_THIRD_PARTY = "https://raw.githubusercontent.com/gauravshindee/youtube-dashboard/main/data/archive_third_party.csv.zip"

def download_and_extract_zip(url):
    zip_path = "temp.zip"
    try:
        r = requests.get(url, timeout=30)
        if r.status_code == 200:
            with open(zip_path, "wb") as f:
                f.write(r.content)
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall("data")
            os.remove(zip_path)
        # FIX: Added a more informative error for non-200 status
        else:
            st.error(f"Failed to download archive from GitHub. Status Code: {r.status_code}")
    except Exception as e:
        st.error(f"Failed to download or extract archive from GitHub: {e}")

os.makedirs("data", exist_ok=True)
if not os.path.exists("data/archive.csv"):
    download_and_extract_zip(RAW_ZIP_URL_OFFICIAL)
if not os.path.exists("data/archive_third_party.csv"):
    download_and_extract_zip(RAW_ZIP_URL_THIRD_PARTY)

# --- Archive Data Loaders (Robust CSV Loading and Link/Video_ID Correction) ---
def extract_youtube_id(url):
# ... (extract_youtube_id remains the same) ...
    """Extracts YouTube ID from various URL formats."""
    if pd.isna(url):
        return None
    match_watch = re.search(r'(?<=v=)[\w-]+', str(url))
    if match_watch:
        return match_watch.group(0)
    match_short = re.search(r'(?:youtu\.be\/|embed\/)([\w-]+)', str(url))
    if match_short:
        return match_short.group(1)
    return None

def clean_and_normalize_df(df):
# ... (clean_and_normalize_df remains the same) ...
    """Helper to clean column names, fix link mapping, and ensure 'video_id' exists."""
    if df.empty:
        return df
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    if "video_link" in df.columns and "link" not in df.columns:
        df.rename(columns={"video_link": "link"}, inplace=True)
    if "link" in df.columns and "video_id" not in df.columns:
        df["video_id"] = df["link"].apply(extract_youtube_id)
    return df

@st.cache_data
def load_official_archive():
# ... (load_official_archive remains the same) ...
    file_path = "data/archive.csv"
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(
                file_path,
                encoding='latin-1',
                sep=',',
                quotechar='"',
                doublequote=True,
                on_bad_lines='warn'
            )
            return clean_and_normalize_df(df)
        except Exception as e:
            st.error(f"Failed to read archive.csv: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

@st.cache_data
def load_third_party_archive():
# ... (load_third_party_archive remains the same) ...
    file_path = "data/archive_third_party.csv"
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(
                file_path,
                encoding='latin-1',
                sep=',',
                quotechar='"',
                doublequote=True,
                on_bad_lines='warn'
            )
            return clean_and_normalize_df(df)
        except Exception as e:
            st.error(f"Failed to read archive_third_party.csv: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

# --- Sheet Helpers (with RefreshError check) ---
def load_sheet(name):
    """Loads a specific worksheet, handling gspread connection errors."""
    try:
        if gs_client is None:
            raise Exception("Google Sheets client is not authorized.")
        # FIX: Check for GOOGLE_SHEET_ID existence
        if GOOGLE_SHEET_ID is None:
            st.error("Error: GOOGLE_SHEET_ID is not set in Streamlit secrets.")
            st.stop()
            
        return gs_client.open_by_key(GOOGLE_SHEET_ID).worksheet(name)
    except RefreshError as e:
        st.error("Google Sheets Connection Error: No access token in response. "
                 "Please check the 'gcp_service_account' secret and permissions. "
                 f"Error: {e}")
        st.stop()
    except Exception:
        # NOTE: APIError (gspread's main exception) will land here and return None.
        # The main code will then handle the empty DataFrame return.
        return None

def normalize_df(records):
# ... (normalize_df remains the same) ...
    df = pd.DataFrame(records)
    if df.empty:
        return df
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df

def load_quickwatch():
    sheet = load_sheet(QUICKWATCH_SHEET)
    return normalize_df(sheet.get_all_records()) if sheet else pd.DataFrame()

def load_not_relevant():
    sheet = load_sheet(NOT_RELEVANT_SHEET)
    return normalize_df(sheet.get_all_records()) if sheet else pd.DataFrame()

def load_already_downloaded():
    sheet = load_sheet(ALREADY_DOWNLOADED_SHEET)
    return normalize_df(sheet.get_all_records()) if sheet else pd.DataFrame()

def load_tickets_created():
    sheet = load_sheet(TICKETS_CREATED_SHEET)
    return normalize_df(sheet.get_all_records()) if sheet else pd.DataFrame()
# ... (rest of the script remains the same) ...
