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
import re  # Import the regex module
from oauth2client.service_account import ServiceAccountCredentials
from google.auth.exceptions import RefreshError

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
# Ensure these secrets are defined in your Streamlit secrets.toml file
GOOGLE_SHEET_ID = st.secrets.get("GOOGLE_SHEET_ID")
ZENDESK_SUBDOMAIN = st.secrets.get("ZENDESK_SUBDOMAIN", "")
ZENDESK_EMAIL = st.secrets.get("ZENDESK_EMAIL", "")
ZENDESK_API_TOKEN = st.secrets.get("ZENDESK_API_TOKEN", "TO_BE_ADDED_BY_ADMIN")

QUICKWATCH_SHEET = "quickwatch"
NOT_RELEVANT_SHEET = "not_relevant"
ALREADY_DOWNLOADED_SHEET = "already downloaded"
TICKETS_CREATED_SHEET = "tickets_created"

# --- Google Sheets Authorization (CACHED) ---
@st.cache_resource
def authorize_gspread_client():
    """Initializes and caches the gspread client."""
    try:
        SERVICE_ACCOUNT_SECRET = json.loads(st.secrets["gcp_service_account"])
    except json.JSONDecodeError:
        st.error("Error: 'gcp_service_account' secret is not valid JSON.")
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

if gs_client is None:
    st.stop()

# --- Zendesk Helper (Remains the same) ---
def create_zendesk_ticket(subject, description):
    if not ZENDESK_SUBDOMAIN or not ZENDESK_EMAIL or ZENDESK_API_TOKEN == "TO_BE_ADDED_BY_ADMIN":
        return False, "Zendesk API token not set. Please ask your admin."

    url = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/tickets.json"
    auth_str = f"{ZENDESK_EMAIL}/token:{ZENDESK_API_TOKEN}" 
    auth_bytes = base64.b64encode(auth_str.encode("utf-8")).decode("utf-8")

    headers = {"Content-Type": "application/json", "Authorization": f"Basic {auth_bytes}"}
    payload = {
        "ticket": {
            "subject": subject,
            "comment": { "body": description },
            "priority": "normal"
        }
    }

    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 201:
        return True, response.json()
    else:
        return False, response.text

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
    except Exception as e:
        st.error(f"Failed to download or extract archive from GitHub: {e}")

os.makedirs("data", exist_ok=True)
if not os.path.exists("data/archive.csv"):
    download_and_extract_zip(RAW_ZIP_URL_OFFICIAL)
if not os.path.exists("data/archive_third_party.csv"):
    download_and_extract_zip(RAW_ZIP_URL_THIRD_PARTY)

# --- Archive Data Loaders (Robust CSV Loading and Link/Video_ID Correction) ---
def extract_youtube_id(url):
    """Extracts YouTube ID from various URL formats."""
    if pd.isna(url):
        return None
    
    # Standard watch link
    match_watch = re.search(r'(?<=v=)[\w-]+', str(url))
    if match_watch:
        return match_watch.group(0)

    # Shortened/Embed link
    match_short = re.search(r'(?:youtu\.be\/|embed\/)([\w-]+)', str(url))
    if match_short:
        return match_short.group(1)
        
    return None

def clean_and_normalize_df(df):
    """Helper to clean column names, fix link mapping, and ensure 'video_id' exists."""
    if df.empty:
        return df
        
    # 1. Normalize column names (strip whitespace, lowercase, replace spaces)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    
    # 2. Fix the link column mapping (if CSV uses 'video_link' instead of 'link')
    if "video_link" in df.columns and "link" not in df.columns:
        df.rename(columns={"video_link": "link"}, inplace=True)

    # 3. Create the missing 'video_id' column from the 'link' column (FIX for KeyError: 'video_id')
    if "link" in df.columns and "video_id" not in df.columns:
        df["video_id"] = df["link"].apply(extract_youtube_id)
        
    return df

@st.cache_data
def load_official_archive():
    """Load the official archive CSV with robust parsing and column correction."""
    file_path = "data/archive.csv"
    if os.path.exists(file_path):
        try:
            # Robust parsing for malformed CSVs
            df = pd.read_csv(file_path, 
                             encoding='latin-1', 
                             sep=',', 
                             quotechar='"', 
                             doublequote=True,
                             on_bad_lines='warn') 
            return clean_and_normalize_df(df)
        except Exception as e:
            st.error(f"Failed to read archive.csv: {e}") 
            return pd.DataFrame()
    return pd.DataFrame()

@st.cache_data
def load_third_party_archive():
    """Load the third party archive CSV with robust parsing and column correction."""
    file_path = "data/archive_third_party.csv"
    if os.path.exists(file_path):
        try:
            # Robust parsing for malformed CSVs
            df = pd.read_csv(file_path, 
                             encoding='latin-1', 
                             sep=',', 
                             quotechar='"', 
                             doublequote=True,
                             on_bad_lines='warn')
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
            
        return gs_client.open_by_key(GOOGLE_SHEET_ID).worksheet(name)
    except RefreshError as e:
        # Cited error: No access token in response
        st.error(f"Google Sheets Connection Error: No access token in response. Please check the 'gcp_service_account' secret and permissions. Error: {e}") 
        st.stop()
    except Exception as e:
        return None

def normalize_df(records):
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

# --- Action Helpers (Remains the same) ---
def save_ticket_marker(video, ticket_id, ticket_url):
    try:
        sh = gs_client.open_by_key(GOOGLE_SHEET_ID)
        try:
            t_sheet = sh.worksheet(TICKETS_CREATED_SHEET)
        except gspread.exceptions.WorksheetNotFound:
            t_sheet = sh.add_worksheet(title=TICKETS_CREATED_SHEET, rows="1000", cols="7")
            t_sheet.append_row(["video_id", "title", "channel_name", "publish_date", "link", "ticket_created", "ticket_url"])
        t_sheet.append_row([
            str(video.get("video_id", "")),
            video.get("title", ""),
            video.get("channel_name", ""),
            str(video.get("publish_date", "")),
            video.get("link", ""),
            str(ticket_id),
            ticket_url
        ])
    except Exception as e:
        st.error(f"❌ Failed to mark ticket: {e}")

def move_to_sheet(video, sheet_name):
    try:
        sh = gs_client.open_by_key(GOOGLE_SHEET_ID)
        try:
            target_sheet = sh.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            target_sheet = sh.add_worksheet(title=sheet_name, rows="1000", cols="5")
            target_sheet.append_row(["video_id", "title", "channel_name", "publish_date", "link"])
        target_sheet.append_row([
            str(video.get("video_id", "")),
            video.get("title", ""),
            video.get("channel_name", ""),
            str(video.get("publish_date", "")),
            video.get("link", "")
        ])
    except Exception as e:
        st.error(f"❌ Failed to save to {sheet_name} tab: {e}")

def remove_from_quickwatch(video_id):
    try:
        sh = gs_client.open_by_key(GOOGLE_SHEET_ID)
        qsheet = sh.worksheet(QUICKWATCH_SHEET)
        all_rows = qsheet.get_all_records()
        row_to_delete = None
        for i, row in enumerate(all_rows):
            if str(row.get("video_id")) == str(video_id):
                row_to_delete = i + 2  
                break
        
        if row_to_delete:
            qsheet.delete_rows(row_to_delete)
        else:
            st.warning(f"Video ID {video_id} not found in QuickWatch sheet.")

    except Exception as e:
        st.error(f"❌ Failed to remove from quickwatch: {e}")

# --- Common UI Components ---

def apply_quickwatch_filters(df, prefix):
    """Applies search, channel, and date/ticket filters to a DataFrame."""
    if df.empty:
        return df, None, None

    # Ensure date column exists and is datetime
    if "publish_date" not in df.columns:
        df["publish_date"] = pd.NaT 
    else:
        df["publish_date"] = pd.to_datetime(df["publish_date"], errors="coerce")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        q = st.text_input("🔍 Search title", key=f"{prefix}_search")
    with col2:
        ch_options = ["All"] + sorted(df["channel_name"].dropna().unique())
        ch = st.selectbox("🎞 Channel", ch_options, key=f"{prefix}_channel")
    with col3:
        # Define safe min/max dates
        min_date_df = df["publish_date"].min()
        max_date_df = df["publish_date"].max()
        
        min_date = min_date_df.date() if not pd.isnull(min_date_df) else pd.Timestamp('2000-01-01').date()
        max_date = max_date_df.date() if not pd.isnull(max_date_df) else pd.Timestamp('2030-01-01').date()
        
        try:
            start_date, end_date = st.date_input("📅 Date range", [min_date, max_date], key=f"{prefix}_date")
        except ValueError:
            st.warning("Please select a valid date range.")
            return pd.DataFrame(), None, None
        
    with col4:
        ticket_filter = st.selectbox("🎫 Ticket Status", ["All", "Ticket Created", "No Ticket"], key=f"{prefix}_ticket_filter")

    filtered = df.copy()
    if q:
        filtered = filtered[filtered["title"].str.contains(q, case=False, na=False)]
    if ch != "All":
        filtered = filtered[filtered["channel_name"] == ch]
    
    # Filter by date range
    filtered = filtered[
        (filtered["publish_date"].dt.date >= start_date) & 
        (filtered["publish_date"].dt.date <= end_date)
    ]
    
    # Apply Ticket Filter 
    if ticket_filter in ["Ticket Created", "No Ticket"]:
        tickets_df = load_tickets_created()
        ticketed_ids = set(tickets_df["video_id"].astype(str)) if not tickets_df.empty else set()
        
        # This check is now safe because 'video_id' is created in clean_and_normalize_df
        if "video_id" in filtered.columns:
            if ticket_filter == "Ticket Created":
                filtered = filtered[filtered["video_id"].astype(str).isin(ticketed_ids)]
            elif ticket_filter == "No Ticket":
                filtered = filtered[~filtered["video_id"].astype(str).isin(ticketed_ids)]
        else:
             st.warning("Cannot filter by ticket status: 'video_id' could not be determined from video link.")
             # Fall back to showing all videos if the required column for filtering is missing

    return filtered, start_date, end_date

def display_quickwatch_style_list(df, view_name, prefix, tickets_df):
    """Displays a list of videos with player, top/bottom pagination, and action buttons."""
    
    if df.empty:
        st.info(f"No results found in {view_name}.")
        return

    ticketed_ids = set(tickets_df["video_id"].astype(str)) if not tickets_df.empty else set()
    
    # --- Pagination Setup ---
    st.markdown(f"**🔎 {len(df)} results**")
    per_page = 10
    total_pages = max(1, (len(df) - 1) // per_page + 1)
    
    # --- Top Pagination ---
    top_page_col1, top_page_col2 = st.columns([1, 10])
    with top_page_col1:
        page = st.number_input("Page", 1, total_pages, 1, key=f"{prefix}_page_top")
    with top_page_col2:
        st.markdown(f"Page {page} of {total_pages}")
    
    # Slice the DataFrame for the current page
    videos_to_display = df.iloc[(page-1)*per_page:page*per_page].to_dict("records")

    # --- Video Display Loop ---
    for i, video in enumerate(videos_to_display):
        # 'video_id' is now present due to the fix in the loader functions
        vid = str(video.get("video_id", f"no_id_{page}_{i}")) 
        unique_key_base = f"{prefix}_{vid}_{page}_{i}" 

        st.subheader(video.get("title", "No Title"))
        st.caption(f"{video.get('channel_name', 'Unknown Channel')} • {video.get('publish_date', 'Unknown Date')}")
        
        # Check for link existence and validity before calling st.video (Fix for 'about:blank' error)
        video_link = video.get("link")
        if video_link and isinstance(video_link, str) and video_link.startswith(("http", "https")):
            st.video(video_link)
        else:
            st.warning("⚠️ Video link is missing or invalid. Cannot display player.")


        col1, col2, col3 = st.columns(3)
        
        # --- Action Buttons ---
        with col1:
            if st.button("⬇️ Download", key=f"dl_{unique_key_base}"):
                move_to_sheet(video, ALREADY_DOWNLOADED_SHEET)
                if view_name == "⚡ QuickWatch":
                    remove_from_quickwatch(vid)
                st.success(f"Video {vid} marked as downloaded and moved to '{ALREADY_DOWNLOADED_SHEET}'.")
                st.rerun()

        with col2:
            if st.button("🚫 Not Relevant", key=f"nr_{unique_key_base}"):
                move_to_sheet(video, NOT_RELEVANT_SHEET)
                if view_name == "⚡ QuickWatch":
                    remove_from_quickwatch(vid)
                st.success(f"Video {vid} marked as not relevant and moved to '{NOT_RELEVANT_SHEET}'.")
                st.rerun()
        
        with col3:
            if vid in ticketed_ids:
                # Safely retrieve ticket row
                ticket_row = tickets_df[tickets_df["video_id"].astype(str) == vid]
                if not ticket_row.empty:
                    ticket_row = ticket_row.iloc[0]
                    st.success(f"🎫 Ticket Created: [#{ticket_row['ticket_created']}]({ticket_row['ticket_url']})")
                else:
                    st.warning("Ticket status mismatch.")
            else:
                if st.button("🎫 Create Ticket", key=f"ticket_{unique_key_base}"):
                    subject = f"Video Review: {video.get('title', 'Unknown Title')}"
                    description = (
                        f"Video ID: {vid}\n"
                        f"Title: {video.get('title', 'Unknown Title')}\n"
                        f"Channel: {video.get('channel_name', 'Unknown Channel')}\n"
                        f"Date: {video.get('publish_date', 'Unknown Date')}\n"
                        f"Link: {video.get('link', 'No Link')}"
                    )
                    success, result = create_zendesk_ticket(subject, description)
                    if success:
                        ticket_id = result["ticket"]["id"]
                        ticket_url = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/agent/tickets/{ticket_id}"
                        save_ticket_marker(video, ticket_id, ticket_url)
                        st.success(f"✅ Ticket #{ticket_id} created!")
                        st.rerun()
                    else:
                        st.error(f"❌ Failed to create ticket: {result}")
        
        st.markdown("---") 
    
    # --- Bottom Pagination ---
    bottom_page_col1, bottom_page_col2 = st.columns([1, 10])
    with bottom_page_col1:
        st.number_input("Page", 1, total_pages, page, key=f"{prefix}_page_bottom", label_visibility="collapsed")
    with bottom_page_col2:
        st.markdown(f"Page {page} of {total_pages}")


# --- Authentication Check (Centralized) ---
def check_authentication():
    """Checks session state and handles login/timeout."""
    
    auth_time = st.session_state["login_time"]
    time_since_login = time.time() - auth_time
    
    if st.session_state["authenticated"] and time_since_login <= LOGIN_TIMEOUT:
        return True
    
    st.session_state["authenticated"] = False
    
    st.markdown("## 🔐 Welcome to DemoUp Dashboard")
    password = st.text_input("Password", type="password")
    
    if password == CORRECT_PASSWORD:
        st.session_state["authenticated"] = True
        st.session_state["login_time"] = time.time()
        st.success("Access granted. Loading dashboard...")
        st.rerun()
    elif password:
        st.error("❌ Incorrect password.")
        
    st.stop() 

check_authentication()

# ----------------------------------------------------------------------
# Main Dashboard UI (Only executes if authenticated)
# ----------------------------------------------------------------------

st.title("📺 YouTube Video Dashboard")

view = st.sidebar.radio("📂 Select View", ["⚡ QuickWatch", "🚫 Not Relevant", "📥 Already Downloaded", "📦 Archive (Official)", "📦 Archive (Third-Party)"])

tickets_df = load_tickets_created()

# --- ⚡ QuickWatch ---
if view == "⚡ QuickWatch":
    with st.expander("📡 Run Manual Video Fetch (Admin Only)"):
        if st.text_input("Admin Password", type="password", key="qw_admin_pw") == "demoup123":
            if st.button("🔁 Fetch Now", key="qw_fetch_btn"):
                with st.spinner("Fetching..."):
                    try: 
                        # fetch_videos_main() # Placeholder/Admin Action
                        st.success("✅ Fetched successfully. (Placeholder)")
                        st.rerun()
                    except Exception as e:
                        st.error("Fetch failed.")
                        st.exception(e)

    df = load_quickwatch()
    if df.empty:
        st.warning("⚠️ QuickWatch sheet is empty or failed to load.")
        st.stop()

    filtered_df, start, end = apply_quickwatch_filters(df, "qw")
    display_quickwatch_style_list(filtered_df, "⚡ QuickWatch", "qw", tickets_df)


# --- 🚫 Not Relevant ---
elif view == "🚫 Not Relevant":
    st.header("🚫 Not Relevant Videos")
    df = load_not_relevant()
    if df.empty:
        st.info("No videos marked as Not Relevant in the Google Sheet.")
        st.stop()
        
    filtered_df, start, end = apply_quickwatch_filters(df, "nr")
    display_quickwatch_style_list(filtered_df, "🚫 Not Relevant", "nr", tickets_df)

# --- 📥 Already Downloaded ---
elif view == "📥 Already Downloaded":
    st.header("📥 Already Downloaded Videos")
    df = load_already_downloaded()
    if df.empty:
        st.info("No videos marked as Already Downloaded in the Google Sheet.")
        st.stop()
        
    filtered_df, start, end = apply_quickwatch_filters(df, "ad")
    display_quickwatch_style_list(filtered_df, "📥 Already Downloaded", "ad", tickets_df)

# --- 📦 Archive (Official) ---
elif view == "📦 Archive (Official)":
    st.header("📦 Official Video Archive")
    df = load_official_archive()

    if df.empty:
        st.warning("⚠️ Official Archive is empty or failed to load.")
        st.stop()
    
    filtered_df, start, end = apply_quickwatch_filters(df, "arch_off")
    display_quickwatch_style_list(filtered_df, "📦 Archive (Official)", "arch_off", tickets_df)


# --- 📦 Archive (Third-Party) ---
elif view == "📦 Archive (Third-Party)":
    st.header("📦 Third-Party Video Archive")
    df = load_third_party_archive()

    if df.empty:
        st.warning("⚠️ Third-Party Archive is empty or failed to load.")
        st.stop()

    filtered_df, start, end = apply_quickwatch_filters(df, "arch_tp")
    display_quickwatch_style_list(filtered_df, "📦 Archive (Third-Party)", "arch_tp", tickets_df)
