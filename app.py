import streamlit as st
import psycopg2
import pandas as pd
from datetime import datetime
import datetime as dt
import folium
from streamlit_folium import st_folium
import os
import extra_streamlit_components as stx
import time
import math
from supabase import create_client
import streamlit.components.v1 as components

# --- PAGE CONFIG ---
st.set_page_config(page_title="WorkPulse Platform", layout="wide")

# --- SECURE CLOUD CONNECTION ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
DB_URL = os.environ.get("DB_URL")

if not SUPABASE_URL or not DB_URL:
    try:
        SUPABASE_URL = st.secrets["SUPABASE_URL"]
        SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
        DB_URL = st.secrets["DB_URL"]
    except Exception:
        st.error("⚠️ Connection Keys not found! Please check your Render Environment Variables.")
        st.stop()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_connection():
    return psycopg2.connect(DB_URL)

# --- SILENT DATABASE MIGRATION FOR SHIFT HOURS ---
def ensure_db_updates():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("ALTER TABLE branches ADD COLUMN IF NOT EXISTS shift_hours NUMERIC DEFAULT 8.0;")
        conn.commit()
        conn.close()
    except Exception:
        pass

ensure_db_updates()

@st.cache_data(ttl=15, show_spinner=False)
def get_df(query, params=None):
    conn = get_connection()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

# =========================================================
# 🔴 THE BROWSER-LEVEL GPS FIX (SINGLE-LINE TO PREVENT TEXT SPILL)
# =========================================================
def get_url_coords():
    try:
        params = st.query_params
        if 'lat' in params and 'lon' in params:
            return float(params['lat']), float(params['lon'])
    except Exception:
        pass
    return None, None

NATIVE_GPS_IFRAME = """<iframe srcdoc="<html><head><style>body{margin:0;padding:0;font-family:sans-serif;}button{background-color:#1484A6;color:white;padding:12px 20px;border:none;border-radius:8px;font-size:16px;font-weight:bold;cursor:pointer;width:100%;box-shadow:0px 4px 6px rgba(0,0,0,0.1);transition:0.3s;}button:active{background-color:#0e607a;}</style></head><body><button id='btn' onclick='getLoc()'>📍 TAP HERE TO GET GPS LOCATION</button><script>function getLoc(){var btn = document.getElementById('btn');btn.innerText = '⏳ Locating... Please check for a pop-up...';btn.style.backgroundColor = '#E2E8F0';btn.style.color = '#1A202C';if(navigator.geolocation){navigator.geolocation.getCurrentPosition(function(pos){var lat = pos.coords.latitude;var lon = pos.coords.longitude;var currentUrl = window.parent.location.href.split('?')[0];window.parent.location.href = currentUrl + '?lat=' + lat + '&lon=' + lon;},function(err){alert('GPS Error: You must tap ALLOW when the browser asks for your location.');btn.innerText = '📍 TAP HERE TO GET GPS LOCATION';btn.style.backgroundColor = '#1484A6';btn.style.color = 'white';},{enableHighAccuracy:true, timeout:10000, maximumAge:0});}else{alert('Geolocation not supported.');}}</script></body></html>" width="100%" height="70px" style="border:none;" allow="geolocation"></iframe>"""

# =========================================================
# VISIBILITY FIX
# =========================================================
st.markdown("""
<style>
div[data-baseweb="input"] { background-color: #FFFFFF !important; border: 1px solid #CBD5E0 !important; border-radius: 8px !important; }
div[data-baseweb="input"] input { color: #1A202C !important; -webkit-text-fill-color: #1A202C !important; caret-color: #1A202C !important; font-size: 16px !important; }
div[data-baseweb="input"] input::placeholder { color: #A0AEC0 !important; -webkit-text-fill-color: #A0AEC0 !important; }
[data-testid="stIconMaterial"] { color: #1F4E79 !important; }
div[data-testid="InputInstructions"] { display: none !important; }
</style>
""", unsafe_allow_html=True)

# --- STRICT GEOFENCING FUNCTION ---
def calculate_distance(lat1, lon1, lat2, lon2):
    if None in [lat1, lon1, lat2, lon2] or 0.0 in [lat1, lon1, lat2, lon2]:
        return float('inf') 
    
    lat1, lon1, lat2, lon2 = float(lat1), float(lon1), float(lat2), float(lon2)
    
    R = 6371000 
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# --- Database Helper Functions ---
def authenticate_user(phone, password):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id, full_name, role, branch_id, performance_status FROM users WHERE phone_number=%s AND password=%s", (phone, password))
    user = cursor.fetchone()
    conn.close()
    return user

def get_user_by_id(user_id):
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, full_name, role, branch_id, performance_status FROM users WHERE user_id=%s", (user_id,))
        user = cursor.fetchone()
        conn.close()
        return user
    except Exception:
        return None

def get_full_user_details(user_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT full_name, phone_number, password, role, branch_id FROM users WHERE user_id=%s", (user_id,))
    res = cursor.fetchone()
    conn.close()
    return res

def log_notification(sender_id, target_role, target_branch_id, target_user_id, message, file_path=None, file_name=None):
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    s_id = sender_id if sender_id != 0 else None
    t_b_id = target_branch_id if target_branch_id != 0 else None
    cursor.execute("INSERT INTO notifications (sender_id, target_role, target_branch_id, target_user_id, message, created_at, is_read, file_path, file_name) VALUES (%s, %s, %s, %s, %s, %s, 0, %s, %s)", 
                   (s_id, target_role, t_b_id, target_user_id, message, now, file_path, file_name))
    conn.commit()
    conn.close()

def log_meeting(branch_id, organizer, title, date_str, time_str, desc):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO meetings (branch_id, organizer_name, title, date, time, description) VALUES (%s, %s, %s, %s, %s, %s)", 
                   (branch_id, organizer, title, date_str, time_str, desc))
    conn.commit()
    conn.close()

def get_attendance_record(user_id):
    conn = get_connection()
    cursor = conn.cursor()
    date_today = datetime.now().strftime("%Y-%m-%d")
    cursor.execute("SELECT check_in_time, check_out_time, on_break, break_start_time, break_seconds, checkout_status, record_id, checkin_status, check_in_lat, check_in_lon FROM attendance WHERE user_id=%s AND date=%s", (user_id, date_today))
    record = cursor.fetchone()
    conn.close()
    return record

def update_performance_status(user_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT break_seconds FROM attendance WHERE user_id=%s", (user_id,))
    records = cursor.fetchall()
    violations = sum(1 for r in records if r[0] and int(r[0]) > 3600)
            
    if violations == 0: status = '🟢 Green'
    elif violations <= 2: status = '🟡 Yellow'
    else: status = '🔴 Red'
        
    cursor.execute("UPDATE users SET performance_status=%s WHERE user_id=%s", (status, user_id))
    conn.commit()
    conn.close()
    return status

def log_attendance(user_id, lat, lon):
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_today = datetime.now().strftime("%Y-%m-%d")
    cursor.execute("INSERT INTO attendance (user_id, date, check_in_time, check_in_lat, check_in_lon, checkin_status) VALUES (%s, %s, %s, %s, %s, %s)", (user_id, date_today, now, lat, lon, 'Approved'))
    conn.commit()
    conn.close()
    update_performance_status(user_id) 

def request_check_out(user_id, role):
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_today = datetime.now().strftime("%Y-%m-%d")
    if role in ['Marketer', 'Driver']:
        status = 'Pending GM'
    elif role in ['Branch Manager', 'General Manager', 'Operations Manager', 'CEO', 'HR', 'System Admin']:
        status = 'Approved'
    else:
        status = 'Pending Manager'
    cursor.execute("UPDATE attendance SET checkout_status=%s WHERE user_id=%s AND date=%s", (status, user_id, date_today))
    conn.commit()
    conn.close()

def start_break(user_id):
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_today = datetime.now().strftime("%Y-%m-%d")
    cursor.execute("UPDATE attendance SET on_break=1, break_start_time=%s WHERE user_id=%s AND date=%s", (now, user_id, date_today))
    conn.commit()
    conn.close()

def end_break(user_id, break_start_time_str):
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now()
    break_start = datetime.strptime(break_start_time_str, "%Y-%m-%d %H:%M:%S")
    elapsed_seconds = int((now - break_start).total_seconds())
    date_today = datetime.now().strftime("%Y-%m-%d")
    cursor.execute("UPDATE attendance SET on_break=0, break_seconds = break_seconds + %s, break_start_time=NULL WHERE user_id=%s AND date=%s", (elapsed_seconds, user_id, date_today))
    conn.commit()
    conn.close()
    update_performance_status(user_id) 

def get_branch_coordinates(branch_id):
    if not branch_id: return None
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT latitude, longitude FROM branches WHERE branch_id=%s", (branch_id,))
    coords = cursor.fetchone()
    conn.close()
    return coords

def get_branch_name(branch_id):
    if not branch_id: return "Headquarters"
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT branch_name FROM branches WHERE branch_id=%s", (branch_id,))
    res = cursor.fetchone()
    conn.close()
    return res[0] if res else "Unknown"

def get_branch_shift_hours(branch_id):
    if not branch_id: return 8.0
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT shift_hours FROM branches WHERE branch_id=%s", (branch_id,))
        res = cursor.fetchone()
        conn.close()
        return float(res[0]) if res and res[0] else 8.0
    except Exception:
        return 8.0

def get_active_journey(driver_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT journey_id FROM driver_journeys WHERE driver_id=%s AND date=%s AND end_time IS NULL", (driver_id, datetime.now().strftime("%Y-%m-%d")))
    res = cursor.fetchone()
    conn.close()
    return res[0] if res else None

def start_journey(driver_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO driver_journeys (driver_id, date, start_time) VALUES (%s, %s, %s)", (driver_id, datetime.now().strftime("%Y-%m-%d"), datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    conn.commit()
    conn.close()

def end_journey(journey_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE driver_journeys SET end_time=%s WHERE journey_id=%s", (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), journey_id))
    conn.commit()
    conn.close()

def log_delivery(journey_id, lat, lon):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO deliveries (journey_id, delivery_time, latitude, longitude) VALUES (%s, %s, %s, %s)", (journey_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), lat, lon))
    conn.commit()
    conn.close()

def submit_leave_request(user_id, start_date, end_date, reason):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO leave_requests (user_id, start_date, end_date, reason) VALUES (%s, %s, %s, %s)", (user_id, start_date, end_date, reason))
    conn.commit()
    conn.close()

def update_leave_status(request_id, new_status):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE leave_requests SET status=%s WHERE request_id=%s", (new_status, request_id))
    conn.commit()
    conn.close()

def log_daily_sales(branch_id, amount):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO daily_sales (branch_id, date, total_sales) VALUES (%s, %s, %s)", (branch_id, datetime.now().strftime("%Y-%m-%d"), amount))
    conn.commit()
    conn.close()
    
def log_expense(branch_id, amount, description):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO expenses (branch_id, date, amount, description) VALUES (%s, %s, %s, %s)", (branch_id, datetime.now().strftime("%Y-%m-%d"), amount, description))
    conn.commit()
    conn.close()

def get_weekly_rankings_df():
    query = '''
        SELECT b.branch_name AS "Branch_Name", 
               COALESCE(SUM(ds.total_sales), 0) AS "Weekly Sales (KES)"
        FROM branches b
        LEFT JOIN daily_sales ds ON b.branch_id = ds.branch_id AND CAST(ds.date AS DATE) >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY b.branch_name
        ORDER BY "Weekly Sales (KES)" DESC
    '''
    df = get_df(query)
    if not df.empty:
        df['Rank'] = df['Weekly Sales (KES)'].rank(method='min', ascending=False).astype(int)
    return df

def get_monthly_sales_rankings_df():
    query = '''
        SELECT b.branch_name AS "Branch_Name", 
               COALESCE(SUM(ds.total_sales), 0) AS "Monthly Sales (KES)"
        FROM branches b
        LEFT JOIN daily_sales ds ON b.branch_id = ds.branch_id AND CAST(ds.date AS DATE) >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY b.branch_name
        ORDER BY "Monthly Sales (KES)" DESC
    '''
    df = get_df(query)
    if not df.empty:
        df['Rank'] = df['Monthly Sales (KES)'].rank(method='min', ascending=False).astype(int)
    return df

def get_directory_df(branch_id=None):
    if branch_id:
        query = "SELECT full_name AS \"Name\", role AS \"Role\", phone_number AS \"Phone_Number\", performance_status AS \"Performance_Status\" FROM users WHERE role IN ('Worker', 'Driver', 'Marketer') AND branch_id = %s"
        df = get_df(query, (branch_id,))
    else:
        query = "SELECT u.full_name AS \"Name\", u.role AS \"Role\", b.branch_name AS \"Branch\", u.phone_number AS \"Phone_Number\", u.performance_status AS \"Performance_Status\" FROM users u LEFT JOIN branches b ON u.branch_id = b.branch_id WHERE u.role IN ('Worker', 'Driver', 'Marketer')"
        df = get_df(query)
    
    if not df.empty:
        rank_map = {'🟢 Green': 1, '🟡 Yellow': 2, '🔴 Red': 3}
        df['Rank_Order'] = df['Performance_Status'].map(rank_map).fillna(4)
        df = df.sort_values('Rank_Order').drop(columns=['Rank_Order'])
        df['Action'] = "tel:" + df['Phone_Number']
    return df

def get_monthly_attendance_ranking():
    current_month = datetime.now().strftime("%Y-%m")
    days_in_month_so_far = datetime.now().day
    query = f"""
        SELECT u.full_name AS "Employee", u.role AS "Role", COALESCE(b.branch_name, 'Corporate') AS "Branch",
               COUNT(DISTINCT a.date) AS "Days_Worked"
        FROM users u
        LEFT JOIN branches b ON u.branch_id = b.branch_id
        LEFT JOIN attendance a ON u.user_id = a.user_id AND a.date LIKE '{current_month}-%%'
        WHERE u.role NOT IN ('CEO', 'System Admin')
        GROUP BY u.user_id, u.full_name, u.role, b.branch_name
        ORDER BY "Days_Worked" DESC
    """
    att_rank_df = get_df(query)
    if not att_rank_df.empty:
        att_rank_df['Possible Days'] = days_in_month_so_far
        att_rank_df['Attendance Rate'] = (att_rank_df['Days_Worked'] / days_in_month_so_far * 100).apply(lambda x: f"{x:.1f}%")
    return att_rank_df

def add_branch(name, lat, lon, shift_hours=8.0):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO branches (branch_name, latitude, longitude, shift_hours) VALUES (%s, %s, %s, %s)", (name, lat, lon, shift_hours))
    except Exception:
        conn.rollback()
        cursor.execute("INSERT INTO branches (branch_name, latitude, longitude) VALUES (%s, %s, %s)", (name, lat, lon))
    conn.commit()
    conn.close()

def delete_branch(branch_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE users SET branch_id=NULL WHERE branch_id=%s", (branch_id,))
    cursor.execute("DELETE FROM daily_sales WHERE branch_id=%s", (branch_id,))
    cursor.execute("DELETE FROM expenses WHERE branch_id=%s", (branch_id,))
    cursor.execute("DELETE FROM meetings WHERE branch_id=%s", (branch_id,))
    cursor.execute("DELETE FROM notifications WHERE target_branch_id=%s", (branch_id,))
    cursor.execute("DELETE FROM branches WHERE branch_id=%s", (branch_id,))
    conn.commit()
    conn.close()

def add_user(name, phone, password, role, branch_id):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO users (full_name, phone_number, password, role, branch_id, performance_status) VALUES (%s, %s, %s, %s, %s, %s)", (name, phone, password, role, branch_id, '🟢 Green'))
        conn.commit()
        success = True
        msg = f"User '{name}' created successfully!"
    except psycopg2.IntegrityError:
        success = False
        msg = "⚠️ Error: That Phone Number is already registered to another account."
    finally:
        conn.close()
    return success, msg

def update_user_full(user_id, name, phone, password, role, branch_id):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("UPDATE users SET full_name=%s, phone_number=%s, password=%s, role=%s, branch_id=%s WHERE user_id=%s", (name, phone, password, role, branch_id, user_id))
        conn.commit()
        success = True
        msg = f"User '{name}' updated successfully!"
    except psycopg2.IntegrityError:
        success = False
        msg = "⚠️ Error: That Phone Number is already in use by someone else."
    finally:
        conn.close()
    return success, msg

def delete_user(user_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM attendance WHERE user_id=%s", (user_id,))
    cursor.execute("DELETE FROM leave_requests WHERE user_id=%s", (user_id,))
    cursor.execute("DELETE FROM driver_journeys WHERE driver_id=%s", (user_id,))
    cursor.execute("DELETE FROM notifications WHERE sender_id=%s OR target_user_id=%s", (user_id, user_id))
    cursor.execute("DELETE FROM users WHERE user_id=%s", (user_id,))
    conn.commit()
    conn.close()

def get_all_branches_df():
    try:
        return get_df("SELECT branch_id AS \"Branch_ID\", branch_name AS \"Branch_Name\", latitude AS \"Latitude\", longitude AS \"Longitude\", COALESCE(shift_hours, 8.0) AS \"Shift_Hours\" FROM branches")
    except Exception:
        return get_df("SELECT branch_id AS \"Branch_ID\", branch_name AS \"Branch_Name\", latitude AS \"Latitude\", longitude AS \"Longitude\" FROM branches")

def get_all_users_df():
    return get_df("SELECT u.user_id AS \"User_ID\", u.full_name AS \"Full_Name\", u.role AS \"Role\", u.phone_number AS \"Phone_Number\", COALESCE(b.branch_name, 'None') AS \"Branch\" FROM users u LEFT JOIN branches b ON u.branch_id = b.branch_id")

def get_unread_count(my_role, my_branch, my_id):
    conn = get_connection()
    query = """
        SELECT COUNT(*) FROM notifications 
        WHERE (target_user_id = %s 
        OR target_role = %s 
        OR target_role = 'Entire Company'
        OR (target_role = 'My Branch' AND target_branch_id = %s))
        AND is_read = 0
    """
    cursor = conn.cursor()
    cursor.execute(query, (my_id, my_role, my_branch if my_branch else 0))
    count = cursor.fetchone()[0]
    conn.close()
    return count

# --- CALLBACKS TO PREVENT TAB JUMPING ---
def cb_mark_read(notif_id):
    conn = get_connection()
    conn.cursor().execute("UPDATE notifications SET is_read=1 WHERE notif_id=%s", (notif_id,))
    conn.commit()
    conn.close()
    st.cache_data.clear()

def cb_mark_all_read(my_id, my_role, my_branch):
    conn = get_connection()
    conn.cursor().execute("UPDATE notifications SET is_read=1 WHERE (target_user_id=%s OR target_role=%s OR target_role='Entire Company' OR (target_role='My Branch' AND target_branch_id=%s)) AND is_read=0", (my_id, my_role, my_branch if my_branch else 0))
    conn.commit()
    conn.close()
    st.cache_data.clear()

def cb_send_reply(r_id, s_id, key, my_id):
    msg = st.session_state.get(key, "")
    if msg:
        log_notification(my_id, None, None, s_id, f"↪️ REPLY: {msg}")
        cb_mark_read(r_id)
        st.session_state[key] = ""

def render_inbox(my_role, my_branch, my_id):
    query = """
        SELECT n.notif_id AS "Notif_ID", n.message AS "Message", n.created_at AS "Created_At", n.file_path AS "File_Path", n.file_name AS "File_Name", n.sender_id AS "Sender_ID", COALESCE(u.full_name, 'System') as "Sender_Name" 
        FROM notifications n LEFT JOIN users u ON n.sender_id = u.user_id
        WHERE (n.target_user_id = %s OR n.target_role = %s OR n.target_role = 'Entire Company' OR (n.target_role = 'My Branch' AND n.target_branch_id = %s))
        AND n.is_read = 0 ORDER BY n.created_at DESC
    """
    unread_inbox = get_df(query, (my_id, my_role, my_branch if my_branch else 0))
    
    st.write("### 📬 My Inbox & Alerts")
    
    if not unread_inbox.empty:
        st.button("✅ Mark All as Read", on_click=cb_mark_all_read, args=(my_id, my_role, my_branch), type="primary")
        st.write(f"**You have {len(unread_inbox)} unread messages:**")
        
        for _, row in unread_inbox.iterrows():
            with st.container():
                st.warning(f"**From {row['Sender_Name']}** ({row['Created_At']})\n\n{row['Message']}")
                
                if row['File_Path'] and pd.notna(row['File_Path']):
                    try:
                        file_data = supabase.storage.from_("uploads").download(row['File_Path'])
                        st.download_button(label=f"📎 Download {row['File_Name']}", data=file_data, file_name=row['File_Name'], key=f"dl_u_{row['Notif_ID']}")
                    except Exception:
                        st.caption("⚠️ File missing or deleted from server.")
                
                col_b1, col_b2 = st.columns([1, 4])
                col_b1.button("Mark as Read", key=f"read_{row['Notif_ID']}", on_click=cb_mark_read, args=(row['Notif_ID'],))
                    
                if pd.notna(row['Sender_ID']) and row['Sender_ID'] != 0:
                    with st.expander("Reply"):
                        key_name = f"rep_msg_{row['Notif_ID']}"
                        st.text_input("Type your reply...", key=key_name)
                        st.button("Send Reply", key=f"rep_btn_{row['Notif_ID']}", on_click=cb_send_reply, args=(row['Notif_ID'], row['Sender_ID'], key_name, my_id))
    else:
        st.success("No new unread messages.")
        
    with st.expander("View Previously Read Messages"):
        read_query = """
            SELECT n.notif_id AS "Notif_ID", n.message AS "Message", n.created_at AS "Created_At", n.file_path AS "File_Path", n.file_name AS "File_Name", COALESCE(u.full_name, 'System') as "Sender_Name" 
            FROM notifications n LEFT JOIN users u ON n.sender_id = u.user_id
            WHERE (n.target_user_id = %s OR n.target_role = %s OR n.target_role = 'Entire Company' OR (n.target_role = 'My Branch' AND n.target_branch_id = %s))
            AND n.is_read = 1 ORDER BY n.created_at DESC LIMIT 15
        """
        read_inbox = get_df(read_query, (my_id, my_role, my_branch if my_branch else 0))
        if not read_inbox.empty:
            for _, row in read_inbox.iterrows():
                st.info(f"**From {row['Sender_Name']}** ({row['Created_At']})\n\n{row['Message']}")
                if row['File_Path'] and pd.notna(row['File_Path']):
                    try:
                        file_data = supabase.storage.from_("uploads").download(row['File_Path'])
                        st.download_button(label=f"📎 Download {row['File_Name']}", data=file_data, file_name=row['File_Name'], key=f"dl_r_{row['Notif_ID']}")
                    except Exception:
                        pass
        else:
            st.caption("No read messages.")
            
    if my_branch:
        meet_query = "SELECT title AS \"Title\", date AS \"Date\", time AS \"Time\", description AS \"Description\", organizer_name AS \"Organizer_Name\" FROM meetings WHERE branch_id = %s ORDER BY date DESC LIMIT 3"
        my_meetings = get_df(meet_query, (my_branch,))
        if not my_meetings.empty:
            st.write("---")
            st.write("#### 📅 Scheduled Branch Meetings")
            for _, row in my_meetings.iterrows():
                st.warning(f"🗣️ **{row['Title']}** with {row['Organizer_Name']} | 🗓️ {row['Date']} at {row['Time']}\n\n_{row['Description']}_")

# =========================================================
# 🔴 COOKIE MANAGER & SESSION STATE LOGIC
# =========================================================
cookie_manager = stx.CookieManager()

if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False
if 'logout_clicked' not in st.session_state:
    st.session_state['logout_clicked'] = False

cookie_uid = cookie_manager.get(cookie="wp_user_id")

if cookie_uid and not st.session_state['logged_in'] and not st.session_state['logout_clicked']:
    user_data = get_user_by_id(int(float(str(cookie_uid))))
    
    if user_data:
        st.session_state['logged_in'] = True
        st.session_state['user_id'] = user_data[0]
        st.session_state['name'] = user_data[1]
        st.session_state['role'] = user_data[2]
        st.session_state['branch_id'] = user_data[3]
        st.session_state['status'] = user_data[4]
    else:
        st.session_state['logout_clicked'] = True

# =========================================================
# THE LOGIN SYSTEM
# =========================================================
if not st.session_state['logged_in']:
    
    st.markdown("""
    <style>
    .stApp { background-color: #F4F7F8 !important; }
    header { visibility: hidden; }
    [data-testid="stForm"] { background-color: #FFFFFF !important; border: none !important; border-radius: 30px !important; box-shadow: 0px 15px 50px rgba(0, 0, 0, 0.08) !important; padding: 40px 40px !important; margin-top: 50px !important; }
    .welcome-text { text-align: center; font-size: 32px; font-weight: 900; color: #1A202C !important; margin-bottom: 30px; line-height: 1.2; font-family: 'Arial Black', sans-serif; }
    .welcome-subtext { font-size: 16px; color: #1484A6; font-weight: 700; font-family: sans-serif; }
    .stTextInput label { color: #4A5568 !important; font-weight: 700 !important; font-size: 15px !important; margin-bottom: 7px; }
    .stTextInput div[data-baseweb="input"] { background-color: #EDF2F7 !important; border: 2px solid transparent !important; border-radius: 12px !important; transition: all 0.3s; }
    .stTextInput div[data-baseweb="input"]:focus-within { border: 2px solid #1484A6 !important; }
    .stTextInput input { color: #1A202C !important; -webkit-text-fill-color: #1A202C !important; caret-color: #1A202C !important; padding: 14px !important; font-size: 16px !important; }
    .stTextInput input::placeholder { color: #A0AEC0 !important; -webkit-text-fill-color: #A0AEC0 !important; }
    [data-testid="stFormSubmitButton"] button { background-color: #111111 !important; color: #FFFFFF !important; border-radius: 12px !important; font-weight: 900 !important; font-size: 16px !important; padding: 10px 30px !important; border: none !important; transition: all 0.3s ease; margin-top: 15px; }
    [data-testid="stFormSubmitButton"] button:hover { background-color: #333333 !important; transform: translateY(-2px); }
    .robot-container { display: flex; justify-content: center; align-items: end; height: 80px; margin-bottom: -70px; position: relative; z-index: 10; }
    .robot-face { width: 90px; height: 70px; background-color: #D1D5DB; border-radius: 20px 20px 5px 5px; position: relative; display: flex; justify-content: center; align-items: center; gap: 15px; box-shadow: 0px -5px 15px rgba(0,0,0,0.05); }
    .eye { width: 26px; height: 26px; background-color: #FFFFFF; border-radius: 50%; position: relative; overflow: hidden; display: flex; justify-content: center; align-items: center; border: 2px solid #A0AEC0; }
    .pupil { width: 12px; height: 12px; background-color: #1A202C; border-radius: 50%; position: absolute; transition: transform 0.1s ease-out; }
    .eyelid { position: absolute; top: 0; left: 0; width: 100%; height: 0%; background-color: #A0AEC0; transition: height 0.2s ease-in-out; z-index: 2; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("""
    <div class="robot-container">
        <div class="robot-face">
            <div class="eye">
                <div class="pupil pupil-left"></div>
                <div class="eyelid eyelid-left"></div>
            </div>
            <div class="eye">
                <div class="pupil pupil-right"></div>
                <div class="eyelid eyelid-right"></div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    components.html("""
    <script>
    document.addEventListener("DOMContentLoaded", function() {
        const parentDoc = window.parent.document;
        function attachInteractions() {
            const pupils = parentDoc.querySelectorAll('.pupil');
            const eyelids = parentDoc.querySelectorAll('.eyelid');
            const inputs = parentDoc.querySelectorAll('input');
            if (inputs.length < 2 || pupils.length === 0) { setTimeout(attachInteractions, 300); return; }
            const passwordInput = inputs[1];
            parentDoc.addEventListener('mousemove', (e) => {
                if (parentDoc.activeElement === passwordInput) return; 
                pupils.forEach(pupil => {
                    const rect = pupil.getBoundingClientRect();
                    const x = Math.max(-6, Math.min(6, (e.clientX - rect.left) / 30));
                    const y = Math.max(-6, Math.min(6, (e.clientY - rect.top) / 30));
                    pupil.style.transform = `translate(${x}px, ${y}px)`;
                });
            });
            passwordInput.addEventListener('focus', () => {
                eyelids.forEach(el => el.style.height = '100%');
                pupils.forEach(pupil => pupil.style.transform = `translate(0px, 0px)`);
            });
            passwordInput.addEventListener('blur', () => { eyelids.forEach(el => el.style.height = '0%'); });
        }
        attachInteractions();
    });
    </script>
    """, height=0, width=0)

    col1, col2, col3 = st.columns([1, 1.2, 1])
    
    with col2:
        with st.form("login_form"):
            st.markdown("""
            <div class="welcome-text">
                Welcome back<br>
                <span class="welcome-subtext">to Mediocare Pharmaceutical Ltd</span>
            </div>
            """, unsafe_allow_html=True)
            
            phone = st.text_input("Username", placeholder="e.g. 0700000001")
            password = st.text_input("Password", type="password", placeholder="••••••••")
            
            submitted = st.form_submit_button("Log In", use_container_width=False)
            
            if submitted:
                user = authenticate_user(phone, password)
                if user:
                    st.session_state['logged_in'] = True
                    st.session_state['logout_clicked'] = False
                    st.session_state['user_id'] = user[0]
                    st.session_state['name'] = user[1]
                    st.session_state['role'] = user[2]
                    st.session_state['branch_id'] = user[3]
                    st.session_state['status'] = user[4]
                    cookie_manager.set("wp_user_id", str(user[0]), key="set_u")
                    st.rerun()
                else:
                    st.error("Invalid Credentials.")
            
        with st.expander("Don't have an account? Request Access"):
            with st.form("signup_request_form"):
                st.caption("Submit your details. The System Admin will configure your official account.")
                req_name = st.text_input("Your Full Name")
                req_phone = st.text_input("Your Phone Number")
                req_role = st.selectbox("Requested Role", ["Worker", "Marketer", "Driver", "Branch Manager", "Operations Manager", "HR"])
                
                if st.form_submit_button("Send Request to Admin", type="primary"):
                    if req_name.strip() and req_phone.strip():
                        log_notification(None, 'System Admin', None, None, f"🆕 Account Request: {req_name} ({req_phone}) is requesting a {req_role} role.")
                        st.success("Request sent securely! The System Admin will reach out to you once your account is active.")
                    else:
                        st.error("Please enter your name and phone number.")

# =========================================================
# MAIN APP DASHBOARDS (Logged In)
# =========================================================
else:
    my_notif_count = get_unread_count(st.session_state['role'], st.session_state['branch_id'], st.session_state['user_id'])
    
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT performance_status FROM users WHERE user_id=%s", (st.session_state['user_id'],))
    result = cursor.fetchone()
    fresh_status = result[0] if result else "Unknown"
    conn.close()

    st.sidebar.title(f"👤 {st.session_state['name']}")
    st.sidebar.write(f"**Role:** {st.session_state['role']}")
    
    if st.session_state['role'] not in ['System Admin', 'CEO', 'General Manager', 'Operations Manager']:
        st.sidebar.write(f"**Status:** {fresh_status}")
    st.sidebar.write("---")

    if st.session_state['role'] != 'System Admin':
        with st.sidebar.expander("✉️ Direct Message & Files"):
            st.caption("Send a private message or file to anyone.")
            if st.session_state['role'] == 'Branch Manager':
                users_df = get_df("SELECT user_id AS \"User_ID\", full_name AS \"Full_Name\", role AS \"Role\" FROM users WHERE user_id != %s AND branch_id = %s", (st.session_state['user_id'], st.session_state['branch_id']))
            else:
                users_df = get_df("SELECT user_id AS \"User_ID\", full_name AS \"Full_Name\", role AS \"Role\" FROM users WHERE user_id != %s", (st.session_state['user_id'],))
            
            user_options = {"Select Recipient...": None}
            for _, row in users_df.iterrows():
                user_options[f"{row['Full_Name']} ({row['Role']})"] = row['User_ID']
                
            selected_user = st.selectbox("To:", list(user_options.keys()))
            dm_msg = st.text_area("Message...", height=100)
            uploaded_file = st.file_uploader("Attach File", type=['pdf', 'png', 'jpg', 'jpeg', 'docx', 'xlsx', 'csv'])
            
            if st.button("Send", use_container_width=True, type="primary"):
                target_id = user_options[selected_user]
                if target_id is None:
                    st.error("Select a recipient.")
                elif not dm_msg.strip() and not uploaded_file:
                    st.error("Enter a message or attach a file.")
                else:
                    file_path, file_name = None, None
                    if uploaded_file:
                        try:
                            file_name = uploaded_file.name
                            safe_name = f"{datetime.now().strftime('%H%M%S')}_{file_name}"
                            file_bytes = uploaded_file.getvalue()
                            supabase.storage.from_("uploads").upload(safe_name, file_bytes)
                            file_path = safe_name
                        except Exception as e:
                            st.error(f"Upload failed! Error: {e}")
                            st.stop()
                    
                    msg_content = dm_msg.strip() if dm_msg.strip() else "📎 Sent an attached file."
                    log_notification(st.session_state['user_id'], None, None, target_id, msg_content, file_path, file_name)
                    st.success(f"Sent to {selected_user.split(' (')[0]}!")
        
        with st.sidebar.expander("🔐 Request Password Change"):
            with st.form("pwd_req"):
                new_pwd = st.text_input("New Password Desired")
                if st.form_submit_button("Send Request to Admin"):
                    if new_pwd.strip():
                        log_notification(st.session_state['user_id'], 'System Admin', None, None, f"🔐 Password Change Request from {st.session_state['name']} ({st.session_state['role']}). Requested new password: {new_pwd}")
                        st.success("Request sent! The Admin will update it soon.")
                    else:
                        st.error("Enter a valid password.")
        st.sidebar.write("---")

    # 🔴 BULLETPROOF LOGOUT FIX
    if st.sidebar.button("Logout", type="secondary"):
        st.session_state['logout_clicked'] = True
        st.session_state['logged_in'] = False
        
        try:
            cookie_manager.delete("wp_user_id", key="del_u")
        except: pass
            
        time.sleep(1)
        st.rerun()

    # =========================================================
    # UNIVERSAL LEAVE REQUEST
    # =========================================================
    if st.session_state['role'] not in ['CEO', 'System Admin']:
        with st.sidebar.expander("🏖️ Request Time Off"):
            with st.form("leave_form"):
                start_date = st.date_input("Start Date", min_value=dt.date.today())
                end_date = st.date_input("End Date", min_value=start_date)
                reason = st.text_area("Reason for Leave")
                if st.form_submit_button("Submit Request"):
                    submit_leave_request(st.session_state['user_id'], start_date, end_date, reason)
                    st.cache_data.clear()
                    st.success("Request sent to HR!")

    # =========================================================
    # SYSTEM ADMIN DASHBOARD
    # =========================================================
    if st.session_state['role'] == "System Admin":
        st.title("⚙️ System Configuration Portal")
        st.write("Welcome, Developer. This portal controls the core database records for WorkPulse.")
        
        tab1, tab2, tab3 = st.tabs(["🏢 Manage Branches", "👤 Manage Users", f"🔔 Access Requests ({my_notif_count})"])
        
        with tab1:
            st.write("### Add a New Branch")
            with st.form("add_branch_form"):
                new_b_name = st.text_input("Branch Name (e.g., Kisumu Hub)")
                new_b_lat = st.number_input("GPS Latitude (e.g., -0.0917)", format="%.6f")
                new_b_lon = st.number_input("GPS Longitude (e.g., 34.7680)", format="%.6f")
                new_b_shift = st.number_input("Shift Duration (Hours)", min_value=1.0, max_value=24.0, value=8.0, step=0.5)
                
                if st.form_submit_button("Register Branch"):
                    if new_b_name:
                        add_branch(new_b_name, new_b_lat, new_b_lon, new_b_shift)
                        st.cache_data.clear()
                        st.success(f"Branch '{new_b_name}' successfully added to the database.")
                        st.rerun()
                    else:
                        st.error("Branch Name is required.")
            
            st.write("---")
            st.write("### Manage Existing Branches")
            branches_df = get_all_branches_df()
            
            if not branches_df.empty:
                col_b1, col_b2 = st.columns(2)
                
                with col_b1:
                    st.write("**Update Shift Duration**")
                    edit_branch_options = {"-- Select Branch --": None}
                    for _, row in branches_df.iterrows():
                        edit_branch_options[row['Branch_Name']] = row['Branch_ID']
                        
                    selected_edit_branch = st.selectbox("Select Branch to Edit", list(edit_branch_options.keys()))
                    if selected_edit_branch != "-- Select Branch --":
                        b_id = edit_branch_options[selected_edit_branch]
                        current_hours = get_branch_shift_hours(b_id)
                        new_hours = st.number_input("New Shift Duration (Hours)", min_value=1.0, max_value=24.0, value=float(current_hours), step=0.5)
                        if st.button("Update Shift Time", type="primary"):
                            conn = get_connection()
                            try:
                                conn.cursor().execute("UPDATE branches SET shift_hours=%s WHERE branch_id=%s", (new_hours, b_id))
                                conn.commit()
                            except Exception:
                                pass
                            finally:
                                conn.close()
                            st.cache_data.clear()
                            st.success(f"Updated {selected_edit_branch} shift to {new_hours} hours.")
                            time.sleep(1)
                            st.rerun()

                with col_b2:
                    st.write("**Remove Branch**")
                    delete_branch_options = {"-- Select Branch --": None}
                    for _, row in branches_df.iterrows():
                        delete_branch_options[row['Branch_Name']] = row['Branch_ID']
                        
                    selected_del_branch = st.selectbox("Select Branch to Delete", list(delete_branch_options.keys()))
                    
                    if selected_del_branch != "-- Select Branch --":
                        st.warning(f"⚠️ Deleting {selected_del_branch} reassigns workers to Corporate and drops sales data.")
                        if st.button("🗑️ Delete Branch", type="primary"):
                            delete_branch(delete_branch_options[selected_del_branch])
                            st.cache_data.clear()
                            st.success(f"{selected_del_branch} deleted successfully!")
                            time.sleep(1)
                            st.rerun()
                        
                st.write("---")
                st.dataframe(branches_df, hide_index=True, use_container_width=True)
            else:
                st.info("No branches currently exist.")

        with tab2:
            st.write("### Register a New Employee")
            branches_df = get_all_branches_df()
            branch_options = {"None (Corporate)": None}
            if not branches_df.empty:
                for _, row in branches_df.iterrows():
                    branch_options[row['Branch_Name']] = row['Branch_ID']

            with st.form("add_user_form"):
                new_u_name = st.text_input("Full Name")
                new_u_phone = st.text_input("Phone Number (Used for Login)")
                new_u_pass = st.text_input("Temporary Password", value="pass123")
                new_u_role = st.selectbox("Assign Role", ["Worker", "Marketer", "Driver", "Branch Manager", "Operations Manager", "General Manager", "HR", "CEO"])
                new_u_branch = st.selectbox("Assign to Branch", list(branch_options.keys()))
                
                if st.form_submit_button("Create User Account"):
                    if new_u_name and new_u_phone and new_u_pass:
                        selected_branch_id = branch_options[new_u_branch]
                        success, message = add_user(new_u_name, new_u_phone, new_u_pass, new_u_role, selected_branch_id)
                        if success:
                            st.cache_data.clear()
                            st.success(message)
                        else:
                            st.error(message)
                    else:
                        st.error("Name, Phone, and Password are all required fields.")
            
            st.write("---")
            st.write("### 🛠️ Edit / Manage Existing Employees")
            user_list_df = get_all_users_df()
            
            if not user_list_df.empty:
                edit_user_options = {}
                for _, row in user_list_df.iterrows():
                    edit_user_options[f"{row['Full_Name']} ({row['Role']}) - {row['Phone_Number']}"] = row['User_ID']
                
                selected_edit_user = st.selectbox("Select Employee to Modify or Remove", ["-- Select Employee --"] + list(edit_user_options.keys()))
                
                if selected_edit_user != "-- Select Employee --":
                    edit_user_id = edit_user_options[selected_edit_user]
                    
                    user_info = get_full_user_details(edit_user_id)
                    if user_info:
                        with st.form("edit_full_user_form"):
                            st.write("**Edit Account Details**")
                            e_name = st.text_input("Full Name", value=user_info[0])
                            e_phone = st.text_input("Phone Number", value=user_info[1])
                            e_pass = st.text_input("Password", value=user_info[2])
                            
                            roles = ["Worker", "Marketer", "Driver", "Branch Manager", "Operations Manager", "General Manager", "HR", "CEO", "System Admin"]
                            current_role_index = roles.index(user_info[3]) if user_info[3] in roles else 0
                            e_role = st.selectbox("Role", roles, index=current_role_index)
                            
                            branch_list = list(branch_options.keys())
                            branch_values = list(branch_options.values())
                            current_branch_index = branch_values.index(user_info[4]) if user_info[4] in branch_values else 0
                            e_branch = st.selectbox("Branch", branch_list, index=current_branch_index)
                            
                            if st.form_submit_button("💾 Save Changes", type="primary"):
                                succ, msg = update_user_full(edit_user_id, e_name, e_phone, e_pass, e_role, branch_options[e_branch])
                                if succ:
                                    st.cache_data.clear()
                                    st.success(msg)
                                    time.sleep(1)
                                    st.rerun()
                                else:
                                    st.error(msg)
                                    
                        st.write("---")
                        with st.form("delete_user_form"):
                            st.write("**Remove Employee**")
                            st.caption("Warning: This action permanently deletes their login.")
                            if st.form_submit_button("🗑️ Delete User", type="secondary"):
                                delete_user(edit_user_id)
                                st.cache_data.clear()
                                st.success("User deleted successfully!")
                                time.sleep(1)
                                st.rerun()

            st.write("---")
            st.write("### Current Registered Users")
            search_users = st.text_input("🔍 Search Users...", key="admin_user_search")
            if search_users:
                user_list_df = user_list_df[user_list_df.astype(str).apply(lambda x: x.str.contains(search_users, case=False, na=False)).any(axis=1)]
            st.dataframe(user_list_df, hide_index=True, use_container_width=True)

        with tab3:
            st.write("### 🔔 Account Access Requests")
            
            admin_notifs = get_df("SELECT notif_id AS \"Notif_ID\", message AS \"Message\", created_at AS \"Created_At\" FROM notifications WHERE target_role='System Admin' AND is_read=0 ORDER BY created_at DESC")
            
            if not admin_notifs.empty:
                for _, row in admin_notifs.iterrows():
                    st.info(f"{row['Message']}\n\n*{row['Created_At']}*")
                    if st.button("Mark as Handled", key=f"admin_req_{row['Notif_ID']}", type="primary"):
                        conn = get_connection()
                        conn.cursor().execute("UPDATE notifications SET is_read=1 WHERE notif_id=%s", (row['Notif_ID'],))
                        conn.commit()
                        conn.close()
                        st.cache_data.clear()
                        st.rerun()
            else:
                st.success("No pending requests.")

    # =========================================================
    # UNIVERSAL TIME TRACKER & WORKFLOW
    # =========================================================
    elif st.session_state['role'] not in ['CEO', 'General Manager', 'Operations Manager', 'HR']:
        att_record = get_attendance_record(st.session_state['user_id'])
        
        if att_record:
            check_in_time_str, check_out_time_str, on_break, break_start_time_str, break_seconds, checkout_status, current_record_id, checkin_status, check_in_lat, check_in_lon = att_record

        # 1. NOT CHECKED IN
        if att_record is None:
            st.title(f"{st.session_state['role']} Dashboard ⚙️")
            st.write("Welcome to your shift. Please check in below to begin working.")
            branch_coords = get_branch_coordinates(st.session_state['branch_id'])
            
            if branch_coords:
                b_lat, b_lon = branch_coords
                col_left, col_map, col_right = st.columns([1, 2, 1])
                with col_map:
                    st.write("### 📍 Step 1: Get GPS Location")
                    
                    worker_lat, worker_lon = get_url_coords()
                    
                    if worker_lat and worker_lon:
                        st.success("✅ GPS Coordinates Locked! You may now press Check In.")
                        if st.button("🔄 Re-Scan Location", type="secondary"):
                            try: st.query_params.clear()
                            except: pass
                            st.rerun()
                    else:
                        st.markdown(NATIVE_GPS_IFRAME, unsafe_allow_html=True)
                    
                    st.write("### 📍 Step 2: Verify on Map & Check In")
                    m = folium.Map(location=[b_lat, b_lon], zoom_start=19)
                    
                    folium.Circle(location=[b_lat, b_lon], radius=50, color="blue", fill=True, fill_opacity=0.2).add_to(m)
                    
                    folium.Marker([b_lat, b_lon], tooltip="Branch", icon=folium.Icon(color="green", icon="building", prefix='fa')).add_to(m)
                    if worker_lat and worker_lon:
                        folium.Marker([worker_lat, worker_lon], tooltip="You", icon=folium.Icon(color="red", icon="user", prefix='fa')).add_to(m)
                    st_folium(m, width=700, height=400)
                    
                    if st.button("✅ PRESS TO CHECK IN", use_container_width=True, type="primary"):
                        if not worker_lat or not worker_lon:
                            st.error("⚠️ GPS Error: Location missing. Please tap the blue button above.")
                        else:
                            if st.session_state['role'] in ['Marketer', 'Driver']:
                                log_attendance(st.session_state['user_id'], worker_lat, worker_lon)
                                if st.session_state['role'] == 'Driver':
                                    log_notification(None, 'General Manager', None, None, f"🌍 {st.session_state['name']} (Driver) checked in from the field.")
                                    log_notification(None, 'Operations Manager', None, None, f"🌍 {st.session_state['name']} (Driver) checked in from the field.")
                                    log_notification(None, 'CEO', None, None, f"🌍 {st.session_state['name']} (Driver) checked in from the field.")
                                else:
                                    log_notification(None, 'General Manager', None, None, f"🌍 {st.session_state['name']} (Marketer) checked in from the field.")
                                    log_notification(None, 'Operations Manager', None, None, f"🌍 {st.session_state['name']} (Marketer) checked in from the field.")
                                st.cache_data.clear()
                                try: st.query_params.clear()
                                except: pass
                                st.success("Location recorded. Shift started!")
                                st.rerun()
                            else:
                                distance_to_branch = calculate_distance(b_lat, b_lon, worker_lat, worker_lon)
                                
                                if distance_to_branch <= 50:
                                    log_attendance(st.session_state['user_id'], worker_lat, worker_lon)
                                    st.cache_data.clear()
                                    try: st.query_params.clear()
                                    except: pass
                                    st.success(f"Shift started! Verified on-site ({int(distance_to_branch)}m away).")
                                    st.rerun()
                                else:
                                    st.error(f"❌ Security Block: You are {int(distance_to_branch)} meters away from the branch. You must be within 50 meters to check in.")
                            
            else:
                st.info("You operate across all branches. Click below to start your shift.")
                if st.button("✅ PRESS TO CHECK IN", use_container_width=True, type="primary"):
                    log_attendance(st.session_state['user_id'], 0.0, 0.0)
                    st.cache_data.clear()
                    st.rerun()
            
            st.write("---")
            render_inbox(st.session_state['role'], st.session_state['branch_id'], st.session_state['user_id'])
            st.stop() 

        is_working = True

        # 2. CHECKED OUT AND APPROVED BY MANAGER
        if checkout_status == 'Approved':
            st.success("🏁 You have completed your shift for today. Have a good evening!")
            is_working = False
            
        # 3. CHECKED OUT BUT PENDING MANAGER APPROVAL
        elif checkout_status in ['Pending Manager', 'Pending GM']:
            st.warning("⏳ Your checkout request is pending management approval. Your departure time is securely recorded.")
            is_working = False
            
        # 4. ACTIVELY WORKING
        else:
            st.title(f"{st.session_state['role']} Workspace ⏱️")
            check_in_dt = datetime.strptime(check_in_time_str, "%Y-%m-%d %H:%M:%S")
            now_dt = datetime.now()
            
            total_break_sec = break_seconds
            if on_break == 1:
                break_start_dt = datetime.strptime(break_start_time_str, "%Y-%m-%d %H:%M:%S")
                current_break_elapsed = (now_dt - break_start_dt).total_seconds()
                total_break_sec += current_break_elapsed
                
                break_remaining = (3600) - total_break_sec
                if break_remaining > 0:
                    st.warning(f"🍱 **ON LUNCH BREAK:** {int(break_remaining // 60)} minutes {int(break_remaining % 60)} seconds remaining.")
                else:
                    st.error(f"⚠️ **BREAK OVERDUE:** You are {int(abs(break_remaining) // 60)} minutes late returning from lunch!")
                
                if st.button("▶️ END BREAK & RESUME WORK", use_container_width=True, type="primary"):
                    end_break(st.session_state['user_id'], break_start_time_str)
                    st.cache_data.clear()
                    st.rerun()
                
                st.write("---")
                render_inbox(st.session_state['role'], st.session_state['branch_id'], st.session_state['user_id'])
                st.stop() 
            
            else:
                worked_seconds = (now_dt - check_in_dt).total_seconds() - total_break_sec
                hours = int(worked_seconds // 3600)
                minutes = int((worked_seconds % 3600) // 60)
                
                col1, col2 = st.columns([4, 1])
                with col1:
                    branch_shift_hours = get_branch_shift_hours(st.session_state.get('branch_id'))
                    shift_target_seconds = branch_shift_hours * 3600
                    
                    progress = min(worked_seconds / shift_target_seconds, 1.0)
                    st.progress(progress)
                    st.caption(f"**Active Time Worked:** {hours} Hours, {minutes} Minutes (Shift Goal: {branch_shift_hours} Hours)")
                with col2:
                    if break_seconds < 3600:
                        btn_label = "🍱 Take 1h Lunch Break" if break_seconds == 0 else f"🍱 Resume Break ({int((3600 - break_seconds)//60)}m left)"
                        if st.button(btn_label, use_container_width=True):
                            start_break(st.session_state['user_id'])
                            st.cache_data.clear()
                            st.rerun()
                    else:
                        st.caption("✅ 1h Lunch break fully used.")

        st.write("---")
        render_inbox(st.session_state['role'], st.session_state['branch_id'], st.session_state['user_id'])
        
        # =========================================================
        # ROLE-SPECIFIC TASKS 
        # =========================================================
        if is_working or st.session_state['role'] in ['Branch Manager']:
            
            if st.session_state['role'] == "Marketer" and is_working:
                st.write("### 💸 Log Daily Field Expenses")
                st.caption("Submit your travel, meal, or operational expenses for today.")
                with st.form("marketer_expense_form"):
                    exp_desc = st.text_input("Expense Description (e.g., Client Lunch, Fuel)")
                    exp_amount = st.number_input("Expense Amount (KES)", min_value=0, step=100)
                    if st.form_submit_button("Submit Expense", type="primary"):
                        if exp_desc.strip() != "":
                            b_id = st.session_state['branch_id'] if st.session_state['branch_id'] else 1
                            log_expense(b_id, exp_amount, exp_desc)
                            log_notification(None, 'General Manager', None, None, f"💸 Marketer {st.session_state['name']} logged a field expense: KES {exp_amount} for {exp_desc}.")
                            log_notification(None, 'Operations Manager', None, None, f"💸 Marketer {st.session_state['name']} logged a field expense: KES {exp_amount} for {exp_desc}.")
                            st.cache_data.clear()
                            st.success("Expense logged securely to finance!")
                        else:
                            st.error("Please enter a description.")
            
            elif st.session_state['role'] == "Driver" and is_working:
                active_journey = get_active_journey(st.session_state['user_id'])
                if not active_journey:
                    st.info("Start a journey when leaving for deliveries.")
                    if st.button("🚀 Start Journey", use_container_width=True, type="primary"):
                        start_journey(st.session_state['user_id'])
                        log_notification(None, 'General Manager', None, None, f"🚀 {st.session_state['name']} started a delivery journey.")
                        log_notification(None, 'Operations Manager', None, None, f"🚀 {st.session_state['name']} started a delivery journey.")
                        log_notification(None, 'CEO', None, None, f"🚀 {st.session_state['name']} started a delivery journey.")
                        st.cache_data.clear()
                        st.rerun()
                else:
                    st.success(f"🟢 Journey Active (ID: {active_journey})")
                    
                    st.write("### 📍 Step 1: Pinpoint Delivery Location")
                    
                    d_lat, d_lon = get_url_coords()
                    
                    if d_lat and d_lon:
                        st.success("✅ Delivery Location Locked!")
                        if st.button("🔄 Re-Scan Location", key="driver_rescan"):
                            try: st.query_params.clear()
                            except: pass
                            st.rerun()
                    else:
                        st.markdown(NATIVE_GPS_IFRAME, unsafe_allow_html=True)
                    
                    if st.button("📦 Log Delivery at Current Location", use_container_width=True):
                        if d_lat and d_lon:
                            log_delivery(active_journey, d_lat, d_lon)
                            
                            map_link = f"https://www.google.com/maps/search/?api=1&query={d_lat},{d_lon}"
                            msg = f"📦 {st.session_state['name']} logged a delivery stop. [📍 View Location]({map_link})"
                            
                            log_notification(None, 'General Manager', None, None, msg)
                            log_notification(None, 'Operations Manager', None, None, msg)
                            log_notification(None, 'CEO', None, None, msg)
                            st.cache_data.clear()
                            st.success("Delivery logged!")
                            try: st.query_params.clear()
                            except: pass
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("Click the blue button above to get your location first!")
                            
                    if st.button("🏁 Return to Branch & End Journey", use_container_width=True, type="secondary"):
                        end_journey(active_journey)
                        log_notification(None, 'General Manager', None, None, f"🏁 {st.session_state['name']} completed their journey and returned.")
                        log_notification(None, 'Operations Manager', None, None, f"🏁 {st.session_state['name']} completed their journey and returned.")
                        log_notification(None, 'CEO', None, None, f"🏁 {st.session_state['name']} completed their journey and returned.")
                        st.cache_data.clear()
                        st.rerun()

            elif st.session_state['role'] == "Branch Manager":
                tab1, tab2, tab3, tab4 = st.tabs(["⚙️ Operations", "📅 Schedule Meeting", "📊 Performance", "📞 Directory"])
                
                with tab1:
                    col1, col2 = st.columns([2, 1])
                    with col1:
                        st.write("### Today's Attendance")
                        query = "SELECT u.full_name AS \"Name\", u.phone_number AS \"Phone\", a.check_in_time AS \"Check_In\", a.on_break AS \"On_Break\", u.performance_status AS \"Status\" FROM attendance a JOIN users u ON a.user_id = u.user_id WHERE u.branch_id = %s AND u.role != 'Driver' AND a.date = %s"
                        df = get_df(query, (st.session_state['branch_id'], datetime.now().strftime("%Y-%m-%d")))
                        if not df.empty:
                            df['On_Break'] = df['On_Break'].apply(lambda x: '🍱 On Lunch' if x == 1 else '⚙️ Working')
                            st.dataframe(df, use_container_width=True, hide_index=True)
                        else:
                            st.info("No workers have checked in today.")
                            
                        st.write("---")
                        st.write("### 🛑 Pending Checkouts")
                        pending_out_query = "SELECT a.record_id AS \"Record_ID\", u.full_name AS \"Name\", a.check_out_time AS \"Check_Out_Time\" FROM attendance a JOIN users u ON a.user_id = u.user_id WHERE u.branch_id = %s AND a.checkout_status = 'Pending Manager' AND u.role != 'Driver'"
                        pending_out_df = get_df(pending_out_query, (st.session_state['branch_id'],))
                        
                        if not pending_out_df.empty:
                            for _, row in pending_out_df.iterrows():
                                col_p1, col_p2, col_p3 = st.columns([3, 1, 1])
                                col_p1.warning(f"**{row['Name']}** requested checkout at {row['Check_Out_Time']}")
                                if col_p2.button("Approve", key=f"app_co_{row['Record_ID']}", type="primary"):
                                    conn = get_connection()
                                    conn.cursor().execute("UPDATE attendance SET checkout_status='Approved' WHERE record_id=%s", (row['Record_ID'],))
                                    conn.commit()
                                    conn.close()
                                    st.cache_data.clear()
                                    st.rerun()
                                if col_p3.button("Deny", key=f"den_co_{row['Record_ID']}"):
                                    conn = get_connection()
                                    conn.cursor().execute("UPDATE attendance SET checkout_status='Active', check_out_time=NULL WHERE record_id=%s", (row['Record_ID'],))
                                    conn.commit()
                                    conn.close()
                                    st.cache_data.clear()
                                    st.rerun()
                        else:
                            st.caption("No pending checkouts.")

                        st.write("---")
                        today_str = datetime.now().strftime("%Y-%m-%d")
                        sales_check = get_df("SELECT COUNT(*) FROM daily_sales WHERE branch_id=%s AND date=%s", (st.session_state['branch_id'], today_str))
                        if sales_check.iloc[0,0] == 0:
                            st.write("### 📈 End of Day Sales & Expenses")
                            daily_sales = st.number_input("Enter Total Daily Sales (KES)", min_value=0, step=1000)
                            exp_desc = st.text_input("Expense Description (e.g., Supplies, Fuel)")
                            exp_amount = st.number_input("Expense Amount (KES)", min_value=0, step=500)
                            
                            if st.button("Submit Daily Financials", type="primary"):
                                log_daily_sales(st.session_state['branch_id'], daily_sales)
                                log_notification(None, 'General Manager', None, None, f"💰 Branch {st.session_state['branch_id']} submitted Daily Sales: KES {daily_sales}.")
                                log_notification(None, 'Operations Manager', None, None, f"💰 Branch {st.session_state['branch_id']} submitted Daily Sales: KES {daily_sales}.")
                                log_notification(None, 'CEO', None, None, f"💰 Branch {st.session_state['branch_id']} submitted Daily Sales: KES {daily_sales}.")
                                
                                if exp_desc.strip() != "":
                                    log_expense(st.session_state['branch_id'], exp_amount, exp_desc)
                                st.cache_data.clear()
                                st.success("Financials submitted successfully.")
                                st.rerun()
                        else:
                            st.success("✅ End of Day financials have already been submitted for today.")

                    with col2:
                        st.write("### 🔔 Branch Operations & Alerts")
                        notif_query = "SELECT message AS \"Message\", created_at AS \"Created_At\" FROM notifications WHERE target_role='Branch Manager' AND target_branch_id=%s ORDER BY created_at DESC LIMIT 6"
                        notifs = get_df(notif_query, (st.session_state['branch_id'],))
                        if not notifs.empty:
                            for _, row in notifs.iterrows():
                                st.info(f"{row['Message']}\n\n*{row['Created_At']}*")
                        else:
                            st.write("No recent alerts.")
                            
                with tab2:
                    st.write("### 📅 Schedule a Branch Meeting")
                    with st.form("meet_form"):
                        m_title = st.text_input("Meeting Title")
                        m_date = st.date_input("Meeting Date", min_value=dt.date.today())
                        m_time = st.time_input("Meeting Time")
                        m_desc = st.text_area("Agenda / Description")
                        if st.form_submit_button("Schedule & Notify Workers", type="primary"):
                            log_meeting(st.session_state['branch_id'], st.session_state['name'], m_title, str(m_date), str(m_time), m_desc)
                            st.cache_data.clear()
                            st.success("Meeting scheduled! Your workers will see this in their inbox.")

                with tab3:
                    st.write("### 🏆 Your Branch Ranking (Past 7 Days)")
                    rank_df = get_weekly_rankings_df()
                    if not rank_df.empty:
                        my_branch_name = get_branch_name(st.session_state['branch_id'])
                        my_rank_data = rank_df[rank_df['Branch_Name'] == my_branch_name]
                        if not my_rank_data.empty:
                            rank = my_rank_data['Rank'].values[0]
                            sales = my_rank_data['Weekly Sales (KES)'].values[0]
                            st.metric(label="Your Weekly Rank", value=f"#{rank} out of {len(rank_df)}")
                            st.metric(label="Your Total Weekly Sales", value=f"KES {sales:,.2f}")
                            st.write("---")
                            st.dataframe(rank_df[['Rank', 'Branch_Name', 'Weekly Sales (KES)']], use_container_width=True, hide_index=True)
                        else:
                            st.info("Your branch hasn't registered sales this week.")
                    else:
                        st.info("No weekly sales data available for ranking.")
                        
                    st.write("---")
                    st.write("### 🏆 Your Branch Ranking (Past 30 Days)")
                    m_rank_df = get_monthly_sales_rankings_df()
                    if not m_rank_df.empty:
                        my_branch_name = get_branch_name(st.session_state['branch_id'])
                        my_m_rank_data = m_rank_df[m_rank_df['Branch_Name'] == my_branch_name]
                        if not my_m_rank_data.empty:
                            m_rank = my_m_rank_data['Rank'].values[0]
                            m_sales = my_m_rank_data['Monthly Sales (KES)'].values[0]
                            st.metric(label="Your Monthly Rank", value=f"#{m_rank} out of {len(m_rank_df)}")
                            st.metric(label="Your Total Monthly Sales", value=f"KES {m_sales:,.2f}")
                            st.write("---")
                            st.dataframe(m_rank_df[['Rank', 'Branch_Name', 'Monthly Sales (KES)']], use_container_width=True, hide_index=True)
                        else:
                            st.info("Your branch hasn't registered sales this month.")
                    else:
                        st.info("No monthly sales data available for ranking.")
                        
                with tab4:
                    st.write("### 📞 Employee Contact Directory")
                    dir_df = get_directory_df(st.session_state['branch_id'])
                    if not dir_df.empty:
                        search_bm_dir = st.text_input("🔍 Search Directory...", key="bm_dir_search")
                        if search_bm_dir:
                            dir_df = dir_df[dir_df.astype(str).apply(lambda x: x.str.contains(search_bm_dir, case=False, na=False)).any(axis=1)]
                        st.dataframe(dir_df, column_config={"Call": st.column_config.LinkColumn("Action", display_text="📞 Call Now")}, hide_index=True, use_container_width=True)
                    else:
                        st.info("No active employees found.")

            # --- UNIVERSAL CHECKOUT BUTTON ---
            if is_working:
                st.write("---")
                col_c1, col_c2, col_c3 = st.columns([1, 2, 1])
                with col_c2:
                    st.write("### End Your Shift")
                    if st.button("🛑 REQUEST CHECKOUT", use_container_width=True, type="secondary"):
                        request_check_out(st.session_state['user_id'], st.session_state['role'])
                            
                        if st.session_state['role'] == 'Driver':
                            log_notification(None, 'General Manager', None, None, f"🛑 {st.session_state['name']} (Driver) has requested to check out.")
                            log_notification(None, 'Operations Manager', None, None, f"🛑 {st.session_state['name']} (Driver) has requested to check out.")
                            log_notification(None, 'CEO', None, None, f"🛑 {st.session_state['name']} (Driver) has requested to check out.")
                        elif st.session_state['role'] == 'Worker':
                            log_notification(None, 'Branch Manager', st.session_state['branch_id'], None, f"🛑 {st.session_state['name']} has requested to check out.")
                                
                        st.cache_data.clear()
                        st.success("Checkout requested!")
                        st.rerun()

    # =========================================================
    # HR DASHBOARD
    # =========================================================
    elif st.session_state['role'] == "HR":
        st.title("HR Operations Portal")
        tab1, tab2, tab3, tab4, tab5 = st.tabs(["📝 Leaves", "🚨 Live Status", "🏆 Rankings", "📞 Directory", f"🔔 Inbox ({my_notif_count})"])
        
        with tab1:
            hr_leaves = get_df("SELECT lr.request_id AS \"Request_ID\", u.full_name as \"Employee\", lr.start_date AS \"Start_Date\", lr.end_date AS \"End_Date\", lr.reason AS \"Reason\", lr.status AS \"Status\" FROM leave_requests lr JOIN users u ON lr.user_id = u.user_id WHERE lr.status='Pending HR'")
            if not hr_leaves.empty:
                for index, row in hr_leaves.iterrows():
                    with st.expander(f"Request from {row['Employee']} ({row['Start_Date']} to {row['End_Date']})"):
                        st.write(f"**Reason:** {row['Reason']}")
                        col1, col2 = st.columns(2)
                        if col1.button(f"Approve & Send to CEO", key=f"approve_{row['Request_ID']}", type="primary"):
                            update_leave_status(row['Request_ID'], 'Pending CEO')
                            log_notification(None, 'CEO', None, None, f"📝 HR Approved leave for {row['Employee']}. Awaiting your final confirmation.")
                            st.cache_data.clear()
                            st.rerun()
                        if col2.button(f"Reject", key=f"reject_{row['Request_ID']}"):
                            update_leave_status(row['Request_ID'], 'Rejected by HR')
                            st.cache_data.clear()
                            st.rerun()
            else:
                st.info("No pending requests.")
                
        with tab2:
            st.write("### Live Workforce Roster")
            date_today = datetime.now().strftime("%Y-%m-%d")
            all_users_query = f'''
                SELECT u.full_name AS "Name", u.role AS "Role", COALESCE(b.branch_name, 'Corporate') AS "Branch",
                       a.check_in_time AS "Check_In_Time", a.check_out_time AS "Check_Out_Time", a.on_break AS "On_Break", a.checkout_status AS "Checkout_Status", a.checkin_status AS "Checkin_Status", a.check_in_lat AS "Check_In_Lat", a.check_in_lon AS "Check_In_Lon"
                FROM users u
                LEFT JOIN branches b ON u.branch_id = b.branch_id
                LEFT JOIN attendance a ON u.user_id = a.user_id AND a.date = '{date_today}'
                WHERE u.role NOT IN ('CEO', 'System Admin')
            '''
            all_df = get_df(all_users_query)
            
            if not all_df.empty:
                def get_live_status(row):
                    if pd.isna(row['Check_In_Time']): return "⚪ Not Logged In"
                    if row['Checkin_Status'] in ['Pending GM', 'Pending Manager']: return "⏳ Check-In Pending"
                    if not pd.isna(row['Checkout_Status']):
                        if row['Checkout_Status'] == 'Approved': return "🛑 Checked Out"
                        if row['Checkout_Status'] in ['Pending Manager', 'Pending GM']: return "⏳ Pending Checkout"
                    if row['On_Break'] == 1: return "🍱 On Lunch"
                    return "🟢 Working Active"
                
                def get_loc_link(row):
                    if pd.notna(row['Check_In_Lat']) and pd.notna(row['Check_In_Lon']):
                        return f"https://www.google.com/maps/search/?api=1&query={row['Check_In_Lat']},{row['Check_In_Lon']}"
                    return None

                all_df['Live Status'] = all_df.apply(get_live_status, axis=1)
                all_df['Location'] = all_df.apply(get_loc_link, axis=1)
                display_df = all_df[['Name', 'Role', 'Branch', 'Live Status', 'Location']]
                
                search_hr_roster = st.text_input("🔍 Search Roster...", key="hr_roster_search")
                if search_hr_roster:
                    display_df = display_df[display_df.astype(str).apply(lambda x: x.str.contains(search_hr_roster, case=False, na=False)).any(axis=1)]
                
                st.dataframe(
                    display_df, 
                    use_container_width=True, 
                    hide_index=True,
                    column_config={"Location": st.column_config.LinkColumn("GPS Map", display_text="🌍 TAP TO OPEN MAP")}
                )
            else:
                st.info("No employees found in system.")
                
        with tab3:
            st.write("### 🏆 Monthly Attendance Ranking")
            st.caption("Ranks employees by the number of days they have actively worked this month.")
            att_rank_df = get_monthly_attendance_ranking()
            if not att_rank_df.empty:
                st.dataframe(att_rank_df, use_container_width=True, hide_index=True)
            else:
                st.info("No attendance data recorded yet this month.")
                
            st.write("---")
            st.write("### Weekly Branch Leaderboard")
            rank_df = get_weekly_rankings_df()
            if not rank_df.empty:
                st.dataframe(rank_df[['Rank', 'Branch_Name', 'Weekly Sales (KES)']], use_container_width=True, hide_index=True)
            else:
                st.info("No sales data available.")
        
        with tab4:
            st.write("### 📞 Global Contact Directory")
            dir_df = get_directory_df()
            if not dir_df.empty:
                search_hr_dir = st.text_input("🔍 Search Directory...", key="hr_dir_search")
                if search_hr_dir:
                    dir_df = dir_df[dir_df.astype(str).apply(lambda x: x.str.contains(search_hr_dir, case=False, na=False)).any(axis=1)]
                st.dataframe(dir_df, column_config={"Call": st.column_config.LinkColumn("Action", display_text="📞 Call Now")}, hide_index=True, use_container_width=True)

        with tab5:
            render_inbox(st.session_state['role'], st.session_state['branch_id'], st.session_state['user_id'])

    # =========================================================
    # GENERAL MANAGER & OPERATIONS MANAGER DASHBOARD
    # =========================================================
    elif st.session_state['role'] in ["General Manager", "Operations Manager"]:
        st.title(f"{st.session_state['role']} Operations")
        
        gm_tab1, gm_tab2, gm_tab3, gm_tab4, gm_tab5, gm_tab6, gm_tab7 = st.tabs(["🌍 Field Approvals", "💰 Daily", "🏆 Leaderboards", "📅 Calendar", "📜 Finance", "📞 Directory", f"🔔 Inbox ({my_notif_count})"])
        
        with gm_tab1:
            st.write("### 🌍 Field Marketer & Driver Approvals")
            st.caption("Review live GPS check-ins and approve field checkouts.")
            
            pending_out_query = "SELECT a.record_id AS \"Record_ID\", u.full_name AS \"Name\", a.check_out_time AS \"Check_Out_Time\" FROM attendance a JOIN users u ON a.user_id = u.user_id WHERE a.checkout_status = 'Pending GM'"
            pending_out_df = get_df(pending_out_query)
            
            if not pending_out_df.empty:
                st.write("#### 🛑 Pending Check-Outs")
                for _, row in pending_out_df.iterrows():
                    col_o1, col_o2, col_o3 = st.columns([3, 1, 1])
                    col_o1.warning(f"**{row['Name']}** requested checkout at {row['Check_Out_Time']}")
                    if col_o2.button("Approve Out", key=f"gm_app_out_{row['Record_ID']}", type="primary"):
                        conn = get_connection()
                        conn.cursor().execute("UPDATE attendance SET checkout_status='Approved' WHERE record_id=%s", (row['Record_ID'],))
                        conn.commit()
                        conn.close()
                        st.cache_data.clear()
                        st.rerun()
                    if col_o3.button("Deny", key=f"gm_den_{row['Record_ID']}"):
                        conn = get_connection()
                        conn.cursor().execute("UPDATE attendance SET checkout_status='Active', check_out_time=NULL WHERE record_id=%s", (row['Record_ID'],))
                        conn.commit()
                        conn.close()
                        st.cache_data.clear()
                        st.rerun()
            else:
                st.caption("No pending checkouts.")

        with gm_tab2:
            st.write("### Today's Branch Sales Rankings")
            date_today = datetime.now().strftime("%Y-%m-%d")
            daily_query = f"SELECT b.branch_name AS \"Branch\", SUM(ds.total_sales) AS \"Total Sales (KES)\" FROM daily_sales ds JOIN branches b ON ds.branch_id = b.branch_id WHERE ds.date = '{date_today}' GROUP BY b.branch_name ORDER BY \"Total Sales (KES)\" DESC"
            daily_df = get_df(daily_query)
            if not daily_df.empty:
                daily_df.index = daily_df.index + 1 
                st.dataframe(daily_df, use_container_width=True)
            else:
                st.info("No sales reports submitted today.")
                
        with gm_tab3:
            st.write("### Weekly Branch Leaderboard")
            rank_df = get_weekly_rankings_df()
            if not rank_df.empty:
                st.dataframe(rank_df[['Rank', 'Branch_Name', 'Weekly Sales (KES)']], use_container_width=True, hide_index=True)
            else:
                st.info("No sales data available for the week.")
                
            st.write("---")
            st.write("### Monthly Branch Leaderboard")
            m_rank_df = get_monthly_sales_rankings_df()
            if not m_rank_df.empty:
                st.dataframe(m_rank_df[['Rank', 'Branch_Name', 'Monthly Sales (KES)']], use_container_width=True, hide_index=True)
            else:
                st.info("No sales data available for the month.")
                
        with gm_tab4:
            st.write("### 📅 Historical Data Explorer")
            view_date = st.date_input("Select a specific date to view:", dt.date.today(), key="gm_date")
            view_date_str = str(view_date)
            
            st.write(f"**Sales on {view_date_str}**")
            day_sales_query = f"SELECT b.branch_name AS \"Branch\", SUM(ds.total_sales) AS \"Total Sales (KES)\" FROM daily_sales ds JOIN branches b ON ds.branch_id = b.branch_id WHERE ds.date = '{view_date_str}' GROUP BY b.branch_name ORDER BY \"Total Sales (KES)\" DESC"
            day_sales_df = get_df(day_sales_query)
            if not day_sales_df.empty:
                st.dataframe(day_sales_df, use_container_width=True, hide_index=True)
            else:
                st.caption("No sales reported on this date.")
                
        with gm_tab5:
            st.write("### Master Transaction Log")
            history_query = "SELECT ds.date AS \"Date\", b.branch_name AS \"Branch_Name\", ds.total_sales AS \"Total_Sales\" FROM daily_sales ds JOIN branches b ON ds.branch_id = b.branch_id ORDER BY ds.date DESC"
            hist_df = get_df(history_query)
            if not hist_df.empty:
                st.dataframe(hist_df, use_container_width=True, hide_index=True)
            else:
                st.info("No history found.")
                
        with gm_tab6:
            st.write("### 📞 Global Contact Directory")
            dir_df = get_directory_df()
            if not dir_df.empty:
                search_gm_dir = st.text_input("🔍 Search Directory...", key="gm_dir_search")
                if search_gm_dir:
                    dir_df = dir_df[dir_df.astype(str).apply(lambda x: x.str.contains(search_gm_dir, case=False, na=False)).any(axis=1)]
                st.dataframe(dir_df, column_config={"Call": st.column_config.LinkColumn("Action", display_text="📞 Call Now")}, hide_index=True, use_container_width=True)
        
        with gm_tab7:
            render_inbox(st.session_state['role'], st.session_state['branch_id'], st.session_state['user_id'])


    # =========================================================
    # CEO DASHBOARD 
    # =========================================================
    elif st.session_state['role'] == "CEO":
        st.title("CEO Master Operations 📈")
        date_today = datetime.now().strftime("%Y-%m-%d")
        
        tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["📊 Data & Sales", "🚨 Live Status", "📅 History", "📢 Broadcast", "✅ Leaves", "📞 Directory", f"🔔 Inbox ({my_notif_count})"])
        
        with tab1:
            st.write("### 🧠 AI System Report & Recommendations")
            rank_query = f"SELECT b.branch_name, SUM(ds.total_sales) as sales FROM daily_sales ds JOIN branches b ON ds.branch_id=b.branch_id WHERE ds.date='{date_today}' GROUP BY b.branch_name ORDER BY sales DESC"
            report_df = get_df(rank_query)
            
            if not report_df.empty:
                top_branch = report_df.iloc[0]['branch_name']
                top_sales = report_df.iloc[0]['sales']
                st.success(f"🌟 **Top Performer Today:** {top_branch} is leading the company with KES {top_sales:,.2f} in sales.")
                
                if len(report_df) > 1:
                    bottom_branch = report_df.iloc[-1]['branch_name']
                    st.warning(f"⚠️ **Attention Needed:** {bottom_branch} is currently trailing in daily sales.")
                    st.write(f"💡 **AI Recommendation:** Consider reassigning Field Marketers to the {bottom_branch} region tomorrow to boost local engagement, or review expenses with the {bottom_branch} Branch Manager to optimize profit margins.")
            else:
                st.info("The AI System is waiting for branches to submit their end-of-day sales to generate today's report.")

            st.write("---")
            col_g1, col_g2 = st.columns(2)
            with col_g1:
                st.write("### 📊 Weekly Sales vs Expenses")
                perf_query = '''
                    SELECT b.branch_name AS "Branch_Name",
                        (SELECT COALESCE(SUM(total_sales), 0) FROM daily_sales WHERE branch_id = b.branch_id AND CAST(date AS DATE) >= CURRENT_DATE - INTERVAL '7 days') AS "Sales (KES)",
                        (SELECT COALESCE(SUM(amount), 0) FROM expenses WHERE branch_id = b.branch_id AND CAST(date AS DATE) >= CURRENT_DATE - INTERVAL '7 days') AS "Expenses (KES)"
                    FROM branches b
                '''
                perf_df = get_df(perf_query)
                if not perf_df.empty and (perf_df['Sales (KES)'].sum() > 0 or perf_df['Expenses (KES)'].sum() > 0):
                    chart_data = perf_df.set_index('Branch_Name')
                    st.bar_chart(chart_data, color=["#1484A6", "#EF4444"])
                else:
                    st.info("No data in last 7 days.")
                    
                st.write("---")
                st.write("### 📊 Monthly Sales vs Expenses")
                monthly_query = '''
                    SELECT b.branch_name AS "Branch_Name",
                        (SELECT COALESCE(SUM(total_sales), 0) FROM daily_sales WHERE branch_id = b.branch_id AND CAST(date AS DATE) >= CURRENT_DATE - INTERVAL '30 days') AS "Sales (KES)",
                        (SELECT COALESCE(SUM(amount), 0) FROM expenses WHERE branch_id = b.branch_id AND CAST(date AS DATE) >= CURRENT_DATE - INTERVAL '30 days') AS "Expenses (KES)"
                    FROM branches b
                '''
                monthly_df = get_df(monthly_query)
                if not monthly_df.empty and (monthly_df['Sales (KES)'].sum() > 0 or monthly_df['Expenses (KES)'].sum() > 0):
                    m_chart_data = monthly_df.set_index('Branch_Name')
                    st.bar_chart(m_chart_data, color=["#1484A6", "#EF4444"])
                else:
                    st.info("No data in last 30 days.")
                    
            with col_g2:
                st.write("### 🏆 Today's Rankings")
                if not report_df.empty:
                    report_df.index = report_df.index + 1 
                    st.dataframe(report_df.rename(columns={"branch_name": "Branch", "sales": "Total Sales (KES)"}), use_container_width=True)
                else:
                    st.warning("No sales reports today.")
                    
                st.write("---")
                st.write("### 🏆 Monthly Branch Rankings")
                m_rank_df = get_monthly_sales_rankings_df()
                if not m_rank_df.empty:
                    st.dataframe(m_rank_df[['Rank', 'Branch_Name', 'Monthly Sales (KES)']], use_container_width=True, hide_index=True)
                else:
                    st.warning("No sales data available for the month.")
                
        with tab2:
            st.write("### Live Workforce Roster")
            all_users_query = f'''
                SELECT u.full_name AS "Name", u.role AS "Role", COALESCE(b.branch_name, 'Corporate') AS "Branch",
                       a.check_in_time AS "Check_In_Time", a.check_out_time AS "Check_Out_Time", a.on_break AS "On_Break", a.checkout_status AS "Checkout_Status", a.checkin_status AS "Checkin_Status", a.check_in_lat AS "Check_In_Lat", a.check_in_lon AS "Check_In_Lon"
                FROM users u
                LEFT JOIN branches b ON u.branch_id = b.branch_id
                LEFT JOIN attendance a ON u.user_id = a.user_id AND a.date = '{date_today}'
                WHERE u.role NOT IN ('CEO', 'System Admin')
            '''
            all_df = get_df(all_users_query)
            
            if not all_df.empty:
                def get_live_status(row):
                    if pd.isna(row['Check_In_Time']): return "⚪ Not Logged In"
                    if row['Checkin_Status'] in ['Pending GM', 'Pending Manager']: return "⏳ Check-In Pending"
                    if not pd.isna(row['Checkout_Status']):
                        if row['Checkout_Status'] == 'Approved': return "🛑 Checked Out"
                        if row['Checkout_Status'] in ['Pending Manager', 'Pending GM']: return "⏳ Pending Checkout"
                    if row['On_Break'] == 1: return "🍱 On Lunch"
                    return "🟢 Working Active"
                
                def get_loc_link(row):
                    if pd.notna(row['Check_In_Lat']) and pd.notna(row['Check_In_Lon']):
                        return f"https://www.google.com/maps/search/?api=1&query={row['Check_In_Lat']},{row['Check_In_Lon']}"
                    return None

                all_df['Live Status'] = all_df.apply(get_live_status, axis=1)
                all_df['Location'] = all_df.apply(get_loc_link, axis=1)
                display_df = all_df[['Name', 'Role', 'Branch', 'Live Status', 'Location']]
                
                search_ceo_roster = st.text_input("🔍 Search Roster...", key="ceo_roster_search")
                if search_ceo_roster:
                    display_df = display_df[display_df.astype(str).apply(lambda x: x.str.contains(search_ceo_roster, case=False, na=False)).any(axis=1)]
                
                st.dataframe(
                    display_df, 
                    use_container_width=True, 
                    hide_index=True,
                    column_config={"Location": st.column_config.LinkColumn("GPS Map", display_text="🌍 TAP TO OPEN MAP")}
                )
            else:
                st.info("No employees found in system.")

        with tab3:
            st.write("### 🏆 Monthly Attendance Ranking")
            st.caption("Ranks employees by the number of days they have actively worked this month.")
            att_rank_df = get_monthly_attendance_ranking()
            if not att_rank_df.empty:
                st.dataframe(att_rank_df, use_container_width=True, hide_index=True)
            else:
                st.info("No attendance data recorded yet this month.")
                
            st.write("---")
            st.write("### 📅 Historical Data Explorer")
            view_date = st.date_input("Select a date to view history", dt.date.today())
            view_date_str = str(view_date)
            
            col_c1, col_c2 = st.columns(2)
            with col_c1:
                st.write(f"**Sales on {view_date_str}**")
                day_sales_query = f"SELECT b.branch_name AS \"Branch\", SUM(ds.total_sales) AS \"Total Sales (KES)\" FROM daily_sales ds JOIN branches b ON ds.branch_id = b.branch_id WHERE ds.date = '{view_date_str}' GROUP BY b.branch_name ORDER BY \"Total Sales (KES)\" DESC"
                day_sales_df = get_df(day_sales_query)
                if not day_sales_df.empty:
                    st.dataframe(day_sales_df, use_container_width=True, hide_index=True)
                else:
                    st.caption("No sales reported.")
                    
            with col_c2:
                st.write(f"**Attendance on {view_date_str}**")
                day_att_query = f"SELECT u.full_name AS \"Name\", a.check_in_time AS \"Check_In_Time\" FROM attendance a JOIN users u ON a.user_id = u.user_id WHERE a.date = '{view_date_str}'"
                day_att_df = get_df(day_att_query)
                if not day_att_df.empty:
                    st.dataframe(day_att_df, use_container_width=True, hide_index=True)
                else:
                    st.caption("No attendance logged.")
                    
        with tab4:
            st.write("### 📢 Corporate Broadcast")
            with st.form("broadcast_form"):
                audience = st.selectbox("Select Target Audience", ["Entire Company", "Branch Manager", "Operations Manager", "General Manager", "HR", "Worker", "Marketer", "Driver"])
                b_message = st.text_area("Broadcast Message")
                if st.form_submit_button("Send Broadcast", type="primary"):
                    log_notification(None, audience, None, None, f"📢 CEO ANNOUNCEMENT: {b_message}")
                    st.cache_data.clear()
                    st.success(f"Message successfully sent to: {audience}")

        with tab5:
            st.write("### Final Approval Required")
            ceo_leaves = get_df("SELECT lr.request_id AS \"Request_ID\", u.full_name AS \"Employee\", lr.start_date AS \"Start_Date\", lr.end_date AS \"End_Date\", lr.reason AS \"Reason\" FROM leave_requests lr JOIN users u ON lr.user_id = u.user_id WHERE lr.status='Pending CEO'")
            if not ceo_leaves.empty:
                for index, row in ceo_leaves.iterrows():
                    with st.expander(f"Review: {row['Employee']} ({row['Start_Date']} to {row['End_Date']})"):
                        st.write(f"**Reason:** {row['Reason']}")
                        col1, col2 = st.columns(2)
                        if col1.button(f"Confirm Approved", key=f"ceo_ok_{row['Request_ID']}", type="primary"):
                            update_leave_status(row['Request_ID'], 'Approved')
                            st.cache_data.clear()
                            st.rerun()
                        if col2.button(f"Veto & Reject", key=f"ceo_no_{row['Request_ID']}"):
                            update_leave_status(row['Request_ID'], 'Rejected by CEO')
                            st.cache_data.clear()
                            st.rerun()
            else:
                st.success("No pending leaves to confirm.")
                
        with tab6:
            st.write("### 📞 Global Contact Directory")
            dir_df = get_directory_df()
            if not dir_df.empty:
                search_ceo_dir = st.text_input("🔍 Search Directory...", key="ceo_dir_search")
                if search_ceo_dir:
                    dir_df = dir_df[dir_df.astype(str).apply(lambda x: x.str.contains(search_ceo_dir, case=False, na=False)).any(axis=1)]
                st.dataframe(
                    dir_df,
                    column_config={"Call": st.column_config.LinkColumn("Action", display_text="📞 Call Now")}, 
                    hide_index=True, 
                    use_container_width=True
                )
                
        with tab7:
            render_inbox(st.session_state['role'], st.session_state['branch_id'], st.session_state['user_id'])
