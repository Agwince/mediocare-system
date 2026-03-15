import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
import datetime as dt
import folium
from streamlit_folium import st_folium
from gps_logic import verify_location
from streamlit_geolocation import streamlit_geolocation
import streamlit.components.v1 as components
import os

# --- PAGE CONFIG (Responsive Wide Layout) ---
st.set_page_config(page_title="WorkPulse Platform", layout="wide")

# Ensure secure file upload directory exists
if not os.path.exists("uploads"):
    os.makedirs("uploads")

# =========================================================
# 🔴 VISIBILITY FIX: Forces text inside input boxes to be readable
# =========================================================
st.markdown("""
<style>
div[data-baseweb="input"] {
    background-color: #FFFFFF !important;
    border: 1px solid #CBD5E0 !important;
    border-radius: 8px !important;
}
div[data-baseweb="input"] input {
    color: #1A202C !important;
    -webkit-text-fill-color: #1A202C !important;
    caret-color: #1A202C !important;
    font-size: 16px !important;
}
div[data-baseweb="input"] input::placeholder {
    color: #A0AEC0 !important;
    -webkit-text-fill-color: #A0AEC0 !important;
}
[data-testid="stIconMaterial"] {
    color: #1F4E79 !important;
}
div[data-testid="InputInstructions"] {
    display: none !important;
}
</style>
""", unsafe_allow_html=True)


# --- Database Setup & Migration ---
def ensure_db_updates():
    conn = sqlite3.connect('workpulse.db')
    cursor = conn.cursor()
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Branches (
        Branch_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Branch_Name TEXT, Latitude REAL, Longitude REAL
    )''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS Users (
        User_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Full_Name TEXT, Phone_Number TEXT UNIQUE, Password TEXT,
        Role TEXT, Branch_ID INTEGER, Performance_Status TEXT,
        FOREIGN KEY (Branch_ID) REFERENCES Branches(Branch_ID)
    )''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS Attendance (
        Record_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        User_ID INTEGER, Date TEXT, Check_In_Time TEXT, Check_Out_Time TEXT,
        Check_In_Lat REAL, Check_In_Lon REAL,
        On_Break INTEGER DEFAULT 0, Break_Start_Time TEXT, Break_Seconds INTEGER DEFAULT 0,
        FOREIGN KEY (User_ID) REFERENCES Users(User_ID)
    )''')

    cursor.execute("PRAGMA table_info(Attendance)")
    columns = [info[1] for info in cursor.fetchall()]
    try:
        if 'Checkout_Status' not in columns:
            cursor.execute("ALTER TABLE Attendance ADD COLUMN Checkout_Status TEXT DEFAULT 'Active'")
        if 'Checkin_Status' not in columns:
            cursor.execute("ALTER TABLE Attendance ADD COLUMN Checkin_Status TEXT DEFAULT 'Approved'")
        if 'Check_In_Lat' not in columns:
            cursor.execute("ALTER TABLE Attendance ADD COLUMN Check_In_Lat REAL")
        if 'Check_In_Lon' not in columns:
            cursor.execute("ALTER TABLE Attendance ADD COLUMN Check_In_Lon REAL")
    except Exception:
        pass

    cursor.execute('''CREATE TABLE IF NOT EXISTS Driver_Journeys (
        Journey_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Driver_ID INTEGER, Date TEXT, Start_Time TEXT, End_Time TEXT,
        FOREIGN KEY (Driver_ID) REFERENCES Users(User_ID)
    )''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS Deliveries (
        Delivery_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Journey_ID INTEGER, Delivery_Time TEXT, Latitude REAL, Longitude REAL,
        FOREIGN KEY (Journey_ID) REFERENCES Driver_Journeys(Journey_ID)
    )''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS Daily_Sales (
        Sale_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Branch_ID INTEGER, Date TEXT, Total_Sales REAL,
        FOREIGN KEY (Branch_ID) REFERENCES Branches(Branch_ID)
    )''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS Leave_Requests (
        Request_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        User_ID INTEGER, Start_Date TEXT, End_Date TEXT, Reason TEXT, Status TEXT DEFAULT 'Pending HR',
        FOREIGN KEY (User_ID) REFERENCES Users(User_ID)
    )''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS Notifications (
        Notif_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Sender_ID INTEGER, Target_Role TEXT, Target_Branch_ID INTEGER, Target_User_ID INTEGER, 
        Message TEXT, Created_At TEXT, Is_Read INTEGER DEFAULT 0
    )''')
    
    cursor.execute("PRAGMA table_info(Notifications)")
    columns = [info[1] for info in cursor.fetchall()]
    try:
        if 'File_Path' not in columns:
            cursor.execute("ALTER TABLE Notifications ADD COLUMN File_Path TEXT DEFAULT NULL")
        if 'File_Name' not in columns:
            cursor.execute("ALTER TABLE Notifications ADD COLUMN File_Name TEXT DEFAULT NULL")
    except Exception:
        pass

    cursor.execute('''CREATE TABLE IF NOT EXISTS Billing (
        Payment_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        User_ID INTEGER, Amount REAL, Payment_Date TEXT, Valid_Until TEXT,
        FOREIGN KEY (User_ID) REFERENCES Users(User_ID)
    )''')

    cursor.execute('''CREATE TABLE IF NOT EXISTS Expenses (
        Expense_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Branch_ID INTEGER, Date TEXT, Amount REAL, Description TEXT,
        FOREIGN KEY (Branch_ID) REFERENCES Branches(Branch_ID)
    )''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Meetings (
        Meeting_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Branch_ID INTEGER, Organizer_Name TEXT, Title TEXT, Date TEXT, Time TEXT, Description TEXT
    )''')

    cursor.execute("INSERT OR IGNORE INTO Branches (Branch_ID, Branch_Name, Latitude, Longitude) VALUES (1, 'Nairobi HQ', -1.265000, 36.800000)")
    cursor.execute("INSERT OR IGNORE INTO Branches (Branch_ID, Branch_Name, Latitude, Longitude) VALUES (2, 'Mombasa Branch', -4.0435, 39.6682)")
    
    users_data = [
        (1, 'Agwince Kagali', '0700000001', 'pass123', 'Worker', 1, '🟢 Green'),
        (2, 'Delivery Driver', '0700000002', 'pass123', 'Driver', 1, '🟢 Green'),
        (3, 'Branch Manager HQ', '0700000003', 'pass123', 'Branch Manager', 1, '🟢 Green'),
        (4, 'Company CEO', '0700000004', 'pass123', 'CEO', None, '🟢 Green'),
        (5, 'Sarah HR', '0700000005', 'pass123', 'HR', 1, '🟢 Green'),
        (6, 'General Manager', '0700000006', 'pass123', 'General Manager', None, '🟢 Green'),
        (7, 'Branch Manager MSA', '0700000007', 'pass123', 'Branch Manager', 2, '🟢 Green'),
        (8, 'System Admin', 'admin', 'admin123', 'System Admin', None, '🟢 Green'),
        (9, 'Field Marketer', '0700000009', 'pass123', 'Marketer', 1, '🟢 Green')
    ]
    
    for u in users_data:
        cursor.execute("INSERT OR IGNORE INTO Users (User_ID, Full_Name, Phone_Number, Password, Role, Branch_ID, Performance_Status) VALUES (?, ?, ?, ?, ?, ?, ?)", u)

    conn.commit()
    conn.close()

ensure_db_updates()

# --- Database Helper Functions ---
def get_connection():
    return sqlite3.connect('workpulse.db')

def authenticate_user(phone, password):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT User_ID, Full_Name, Role, Branch_ID, Performance_Status FROM Users WHERE Phone_Number=? AND Password=?", (phone, password))
    user = cursor.fetchone()
    conn.close()
    return user

def log_notification(sender_id, target_role, target_branch_id, target_user_id, message, file_path=None, file_name=None):
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute("INSERT INTO Notifications (Sender_ID, Target_Role, Target_Branch_ID, Target_User_ID, Message, Created_At, Is_Read, File_Path, File_Name) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)", 
                   (sender_id, target_role, target_branch_id, target_user_id, message, now, file_path, file_name))
    conn.commit()
    conn.close()

def log_meeting(branch_id, organizer, title, date_str, time_str, desc):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO Meetings (Branch_ID, Organizer_Name, Title, Date, Time, Description) VALUES (?, ?, ?, ?, ?, ?)", 
                   (branch_id, organizer, title, date_str, time_str, desc))
    conn.commit()
    conn.close()

def check_subscription(user_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT Valid_Until FROM Billing WHERE User_ID=? ORDER BY Payment_Date DESC LIMIT 1", (user_id,))
    result = cursor.fetchone()
    conn.close()
    if result and datetime.now().strftime("%Y-%m-%d") <= result[0]:
        return True, result[0]
    return False, None

def process_payment(user_id, role, user_name):
    amount = 100 if role in ['Worker', 'Driver', 'Marketer'] else 200
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    valid_until = (datetime.now() + dt.timedelta(days=30)).strftime("%Y-%m-%d")
    cursor.execute("INSERT INTO Billing (User_ID, Amount, Payment_Date, Valid_Until) VALUES (?, ?, ?, ?)", (user_id, amount, now, valid_until))
    conn.commit()
    conn.close()
    log_notification(0, 'CEO', None, None, f"💳 M-Pesa Payment: {user_name} ({role}) paid KES {amount}.")

def get_attendance_record(user_id):
    conn = get_connection()
    cursor = conn.cursor()
    date_today = datetime.now().strftime("%Y-%m-%d")
    try:
        cursor.execute("SELECT Check_In_Time, Check_Out_Time, On_Break, Break_Start_Time, Break_Seconds, Checkout_Status, Record_ID, Checkin_Status, Check_In_Lat, Check_In_Lon FROM Attendance WHERE User_ID=? AND Date=?", (user_id, date_today))
        record = cursor.fetchone()
    except sqlite3.OperationalError:
        record = None
    conn.close()
    return record

def update_performance_status(user_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT Check_In_Time FROM Attendance WHERE User_ID=?", (user_id,))
    records = cursor.fetchall()
    violations = sum(1 for r in records if r[0].split(" ")[1] > "08:30:00")
            
    if violations == 0: status = '🟢 Green'
    elif violations <= 2: status = '🟡 Yellow'
    else: status = '🔴 Red'
        
    cursor.execute("UPDATE Users SET Performance_Status=? WHERE User_ID=?", (status, user_id))
    conn.commit()
    conn.close()
    return status

def approve_checkin(record_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE Attendance SET Checkin_Status='Approved' WHERE Record_ID=?", (record_id,))
    cursor.execute("SELECT User_ID FROM Attendance WHERE Record_ID=?", (record_id,))
    user_id = cursor.fetchone()[0]
    conn.commit()
    conn.close()
    
    status = update_performance_status(user_id)
    if status != '🟢 Green':
        log_notification(0, 'CEO', None, None, f"⚠️ Attendance Violation: User {user_id} dropped to {status} status.")

def log_attendance(user_id, lat, lon, checkin_status='Approved'):
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_today = datetime.now().strftime("%Y-%m-%d")
    cursor.execute("INSERT INTO Attendance (User_ID, Date, Check_In_Time, Check_In_Lat, Check_In_Lon, Checkin_Status) VALUES (?, ?, ?, ?, ?, ?)", (user_id, date_today, now, lat, lon, checkin_status))
    conn.commit()
    conn.close()
    if checkin_status == 'Approved':
        status = update_performance_status(user_id) 
        if status != '🟢 Green':
            log_notification(0, 'CEO', None, None, f"⚠️ Attendance Violation: User {user_id} dropped to {status} status.")

def request_check_out(user_id, role):
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_today = datetime.now().strftime("%Y-%m-%d")
    
    if role in ['Marketer', 'Driver']:
        status = 'Pending GM'
    elif role in ['Branch Manager', 'General Manager', 'CEO', 'HR', 'System Admin']:
        status = 'Approved'
    else:
        status = 'Pending Manager'
        
    cursor.execute("UPDATE Attendance SET Check_Out_Time=?, Checkout_Status=? WHERE User_ID=? AND Date=?", (now, status, user_id, date_today))
    conn.commit()
    conn.close()

def start_break(user_id):
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_today = datetime.now().strftime("%Y-%m-%d")
    cursor.execute("UPDATE Attendance SET On_Break=1, Break_Start_Time=? WHERE User_ID=? AND Date=?", (now, user_id, date_today))
    conn.commit()
    conn.close()

def end_break(user_id, break_start_time_str):
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.now()
    break_start = datetime.strptime(break_start_time_str, "%Y-%m-%d %H:%M:%S")
    elapsed_seconds = int((now - break_start).total_seconds())
    date_today = datetime.now().strftime("%Y-%m-%d")
    
    cursor.execute("UPDATE Attendance SET On_Break=0, Break_Seconds = Break_Seconds + ?, Break_Start_Time=NULL WHERE User_ID=? AND Date=?", (elapsed_seconds, user_id, date_today))
    conn.commit()
    conn.close()

def get_branch_coordinates(branch_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT Latitude, Longitude FROM Branches WHERE Branch_ID=?", (branch_id,))
    coords = cursor.fetchone()
    conn.close()
    return coords

def get_branch_name(branch_id):
    if not branch_id: return "Headquarters"
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT Branch_Name FROM Branches WHERE Branch_ID=?", (branch_id,))
    res = cursor.fetchone()
    conn.close()
    return res[0] if res else "Unknown"

def get_active_journey(driver_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT Journey_ID FROM Driver_Journeys WHERE Driver_ID=? AND Date=? AND End_Time IS NULL", (driver_id, datetime.now().strftime("%Y-%m-%d")))
    res = cursor.fetchone()
    conn.close()
    return res[0] if res else None

def start_journey(driver_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO Driver_Journeys (Driver_ID, Date, Start_Time) VALUES (?, ?, ?)", (driver_id, datetime.now().strftime("%Y-%m-%d"), datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    conn.commit()
    conn.close()

def end_journey(journey_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE Driver_Journeys SET End_Time=? WHERE Journey_ID=?", (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), journey_id))
    conn.commit()
    conn.close()

def log_delivery(journey_id, lat, lon):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO Deliveries (Journey_ID, Delivery_Time, Latitude, Longitude) VALUES (?, ?, ?, ?)", (journey_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), lat, lon))
    conn.commit()
    conn.close()

def submit_leave_request(user_id, start_date, end_date, reason):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO Leave_Requests (User_ID, Start_Date, End_Date, Reason) VALUES (?, ?, ?, ?)", (user_id, start_date, end_date, reason))
    conn.commit()
    conn.close()

def update_leave_status(request_id, new_status):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE Leave_Requests SET Status=? WHERE Request_ID=?", (new_status, request_id))
    conn.commit()
    conn.close()

def log_daily_sales(branch_id, amount):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO Daily_Sales (Branch_ID, Date, Total_Sales) VALUES (?, ?, ?)", (branch_id, datetime.now().strftime("%Y-%m-%d"), amount))
    conn.commit()
    conn.close()
    
def log_expense(branch_id, amount, description):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO Expenses (Branch_ID, Date, Amount, Description) VALUES (?, ?, ?, ?)", (branch_id, datetime.now().strftime("%Y-%m-%d"), amount, description))
    conn.commit()
    conn.close()

def get_weekly_rankings_df():
    conn = get_connection()
    perf_query = '''
        SELECT b.Branch_Name, 
               COALESCE(SUM(ds.Total_Sales), 0) as "Weekly Sales (KES)"
        FROM Branches b
        LEFT JOIN Daily_Sales ds ON b.Branch_ID = ds.Branch_ID AND ds.Date >= date('now', '-7 days')
        GROUP BY b.Branch_Name
        ORDER BY "Weekly Sales (KES)" DESC
    '''
    df = pd.read_sql_query(perf_query, conn)
    conn.close()
    if not df.empty:
        df['Rank'] = df['Weekly Sales (KES)'].rank(method='min', ascending=False).astype(int)
    return df

def get_directory_df(branch_id=None):
    conn = get_connection()
    if branch_id:
        query = f"SELECT Full_Name as Name, Role, Phone_Number, Performance_Status FROM Users WHERE Role IN ('Worker', 'Driver', 'Marketer') AND Branch_ID = {branch_id}"
    else:
        query = "SELECT u.Full_Name as Name, u.Role, b.Branch_Name as Branch, u.Phone_Number, u.Performance_Status FROM Users u LEFT JOIN Branches b ON u.Branch_ID = b.Branch_ID WHERE u.Role IN ('Worker', 'Driver', 'Marketer')"
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    if not df.empty:
        rank_map = {'🟢 Green': 1, '🟡 Yellow': 2, '🔴 Red': 3}
        df['Rank_Order'] = df['Performance_Status'].map(rank_map).fillna(4)
        df = df.sort_values('Rank_Order').drop(columns=['Rank_Order'])
        df['Action'] = "tel:" + df['Phone_Number']
    return df

def get_monthly_attendance_ranking():
    conn = get_connection()
    current_month = datetime.now().strftime("%Y-%m")
    days_in_month_so_far = datetime.now().day
    
    att_rank_query = f"""
        SELECT u.Full_Name as Employee, u.Role, COALESCE(b.Branch_Name, 'Corporate') as Branch,
               COUNT(DISTINCT a.Date) as Days_Worked
        FROM Users u
        LEFT JOIN Branches b ON u.Branch_ID = b.Branch_ID
        LEFT JOIN Attendance a ON u.User_ID = a.User_ID AND a.Date LIKE '{current_month}-%'
        WHERE u.Role NOT IN ('CEO', 'System Admin')
        GROUP BY u.User_ID
        ORDER BY Days_Worked DESC
    """
    att_rank_df = pd.read_sql_query(att_rank_query, conn)
    conn.close()
    
    if not att_rank_df.empty:
        att_rank_df['Possible Days'] = days_in_month_so_far
        att_rank_df['Attendance Rate'] = (att_rank_df['Days_Worked'] / days_in_month_so_far * 100).apply(lambda x: f"{x:.1f}%")
    return att_rank_df

def add_branch(name, lat, lon):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT INTO Branches (Branch_Name, Latitude, Longitude) VALUES (?, ?, ?)", (name, lat, lon))
    conn.commit()
    conn.close()

def add_user(name, phone, password, role, branch_id):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO Users (Full_Name, Phone_Number, Password, Role, Branch_ID, Performance_Status) VALUES (?, ?, ?, ?, ?, ?)", (name, phone, password, role, branch_id, '🟢 Green'))
        conn.commit()
        success = True
        msg = f"User '{name}' added successfully."
    except sqlite3.IntegrityError:
        success = False
        msg = "Error: Phone number already exists in the system."
    finally:
        conn.close()
    return success, msg

# 🔴 NEW HELPER: Updates User's Branch
def update_user_branch(user_id, new_branch_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("UPDATE Users SET Branch_ID=? WHERE User_ID=?", (new_branch_id, user_id))
    conn.commit()
    conn.close()

# 🔴 NEW HELPER: Deletes User
def delete_user(user_id):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM Users WHERE User_ID=?", (user_id,))
    conn.commit()
    conn.close()

def get_all_branches_df():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM Branches", conn)
    conn.close()
    return df

def get_all_users_df():
    conn = get_connection()
    df = pd.read_sql_query("SELECT u.User_ID, u.Full_Name, u.Role, u.Phone_Number, COALESCE(b.Branch_Name, 'None') as Branch FROM Users u LEFT JOIN Branches b ON u.Branch_ID = b.Branch_ID", conn)
    conn.close()
    return df

def get_unread_count(my_role, my_branch, my_id):
    conn = get_connection()
    query = f"""
        SELECT COUNT(*) FROM Notifications 
        WHERE (Target_User_ID = {my_id} 
        OR Target_Role = '{my_role}' 
        OR Target_Role = 'Entire Company'
        OR (Target_Role = 'My Branch' AND Target_Branch_ID = {my_branch if my_branch else 0}))
        AND Is_Read = 0
    """
    cursor = conn.cursor()
    cursor.execute(query)
    count = cursor.fetchone()[0]
    conn.close()
    return count

def render_inbox(my_role, my_branch, my_id):
    conn = get_connection()
    
    unread_query = f"""
        SELECT n.Notif_ID, n.Message, n.Created_At, n.File_Path, n.File_Name, n.Sender_ID, COALESCE(u.Full_Name, 'System') as Sender_Name 
        FROM Notifications n LEFT JOIN Users u ON n.Sender_ID = u.User_ID
        WHERE (n.Target_User_ID = {my_id} 
        OR n.Target_Role = '{my_role}' 
        OR n.Target_Role = 'Entire Company'
        OR (n.Target_Role = 'My Branch' AND n.Target_Branch_ID = {my_branch if my_branch else 0}))
        AND n.Is_Read = 0
        ORDER BY n.Created_At DESC
    """
    unread_inbox = pd.read_sql_query(unread_query, conn)
    
    st.write("### 📬 My Inbox & Alerts")
    
    if not unread_inbox.empty:
        st.write(f"**You have {len(unread_inbox)} unread messages:**")
        for _, row in unread_inbox.iterrows():
            with st.container():
                st.warning(f"**From {row['Sender_Name']}** ({row['Created_At']})\n\n{row['Message']}")
                
                if row['File_Path'] and pd.notna(row['File_Path']):
                    try:
                        with open(row['File_Path'], "rb") as f:
                            st.download_button(label=f"📎 Download {row['File_Name']}", data=f, file_name=row['File_Name'], key=f"dl_u_{row['Notif_ID']}")
                    except Exception:
                        st.caption("⚠️ File missing or deleted from server.")
                
                col_b1, col_b2 = st.columns([1, 4])
                if col_b1.button("Mark as Read", key=f"read_{row['Notif_ID']}"):
                    conn.execute("UPDATE Notifications SET Is_Read=1 WHERE Notif_ID=?", (row['Notif_ID'],))
                    conn.commit()
                    st.rerun()
                    
                if row['Sender_ID'] and row['Sender_ID'] != 0:
                    with st.expander("Reply"):
                        reply_msg = st.text_input("Type your reply...", key=f"rep_msg_{row['Notif_ID']}")
                        if st.button("Send Reply", key=f"rep_btn_{row['Notif_ID']}"):
                            log_notification(my_id, None, None, row['Sender_ID'], f"↪️ REPLY: {reply_msg}")
                            conn.execute("UPDATE Notifications SET Is_Read=1 WHERE Notif_ID=?", (row['Notif_ID'],))
                            conn.commit()
                            st.success("Reply sent!")
                            st.rerun()
    else:
        st.success("No new unread messages.")
        
    with st.expander("View Previously Read Messages"):
        read_query = f"""
            SELECT n.Notif_ID, n.Message, n.Created_At, n.File_Path, n.File_Name, COALESCE(u.Full_Name, 'System') as Sender_Name 
            FROM Notifications n LEFT JOIN Users u ON n.Sender_ID = u.User_ID
            WHERE (n.Target_User_ID = {my_id} 
            OR n.Target_Role = '{my_role}' 
            OR n.Target_Role = 'Entire Company'
            OR (n.Target_Role = 'My Branch' AND n.Target_Branch_ID = {my_branch if my_branch else 0}))
            AND n.Is_Read = 1
            ORDER BY n.Created_At DESC LIMIT 15
        """
        read_inbox = pd.read_sql_query(read_query, conn)
        if not read_inbox.empty:
            for _, row in read_inbox.iterrows():
                st.info(f"**From {row['Sender_Name']}** ({row['Created_At']})\n\n{row['Message']}")
                if row['File_Path'] and pd.notna(row['File_Path']):
                    try:
                        with open(row['File_Path'], "rb") as f:
                            st.download_button(label=f"📎 Download {row['File_Name']}", data=f, file_name=row['File_Name'], key=f"dl_r_{row['Notif_ID']}")
                    except Exception:
                        pass
        else:
            st.caption("No read messages.")
            
    if my_branch:
        meet_query = f"SELECT Title, Date, Time, Description, Organizer_Name FROM Meetings WHERE Branch_ID = {my_branch} ORDER BY Date DESC LIMIT 3"
        my_meetings = pd.read_sql_query(meet_query, conn)
        if not my_meetings.empty:
            st.write("---")
            st.write("#### 📅 Scheduled Branch Meetings")
            for _, row in my_meetings.iterrows():
                st.warning(f"🗣️ **{row['Title']}** with {row['Organizer_Name']} | 🗓️ {row['Date']} at {row['Time']}\n\n_{row['Description']}_")
    conn.close()

# --- Session State Setup ---
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False

# =========================================================
# 🔴 THE "MOVING EYES" LOGIN WITH ADMIN SIGN-UP SYSTEM
# =========================================================
if not st.session_state['logged_in']:
    
    st.markdown("""
    <style>
    .stApp { background-color: #F4F7F8 !important; }
    header { visibility: hidden; }
    
    [data-testid="stForm"] {
        background-color: #FFFFFF !important;
        border: none !important;
        border-radius: 30px !important;
        box-shadow: 0px 15px 50px rgba(0, 0, 0, 0.08) !important;
        padding: 40px 40px !important;
        margin-top: 50px !important;
    }
    
    .welcome-text {
        text-align: center;
        font-size: 32px;
        font-weight: 900;
        color: #1A202C !important;
        margin-bottom: 30px;
        line-height: 1.2;
        font-family: 'Arial Black', sans-serif;
    }
    .welcome-subtext {
        font-size: 16px;
        color: #1484A6;
        font-weight: 700;
        font-family: sans-serif;
    }

    .stTextInput label {
        color: #4A5568 !important;
        font-weight: 700 !important;
        font-size: 15px !important;
        margin-bottom: 7px;
    }
    
    .stTextInput div[data-baseweb="input"] {
        background-color: #EDF2F7 !important; 
        border: 2px solid transparent !important;
        border-radius: 12px !important;
        transition: all 0.3s;
    }
    .stTextInput div[data-baseweb="input"]:focus-within {
        border: 2px solid #1484A6 !important;
    }
    
    .stTextInput input {
        color: #1A202C !important;
        -webkit-text-fill-color: #1A202C !important;
        caret-color: #1A202C !important;
        padding: 14px !important;
        font-size: 16px !important;
    }
    .stTextInput input::placeholder {
        color: #A0AEC0 !important;
        -webkit-text-fill-color: #A0AEC0 !important;
    }
    
    [data-testid="stIconMaterial"] {
        color: #4A5568 !important;
    }
    
    [data-testid="stFormSubmitButton"] button {
        background-color: #111111 !important;
        color: #FFFFFF !important;
        border-radius: 12px !important;
        font-weight: 900 !important;
        font-size: 16px !important;
        padding: 10px 30px !important;
        border: none !important;
        transition: all 0.3s ease;
        margin-top: 15px;
    }
    [data-testid="stFormSubmitButton"] button:hover {
        background-color: #333333 !important;
        transform: translateY(-2px);
    }
    
    .robot-container {
        display: flex;
        justify-content: center;
        align-items: end;
        height: 80px;
        margin-bottom: -70px;
        position: relative;
        z-index: 10;
    }
    .robot-face {
        width: 90px;
        height: 70px;
        background-color: #D1D5DB; 
        border-radius: 20px 20px 5px 5px; 
        position: relative;
        display: flex;
        justify-content: center;
        align-items: center;
        gap: 15px;
        box-shadow: 0px -5px 15px rgba(0,0,0,0.05);
    }
    .eye {
        width: 26px;
        height: 26px;
        background-color: #FFFFFF;
        border-radius: 50%;
        position: relative;
        overflow: hidden;
        display: flex;
        justify-content: center;
        align-items: center;
        border: 2px solid #A0AEC0;
    }
    .pupil {
        width: 12px;
        height: 12px;
        background-color: #1A202C;
        border-radius: 50%;
        position: absolute;
        transition: transform 0.1s ease-out; 
    }
    .eyelid {
        position: absolute;
        top: 0; left: 0;
        width: 100%; height: 0%;
        background-color: #A0AEC0; 
        transition: height 0.2s ease-in-out; 
        z-index: 2;
    }
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
            if (inputs.length < 2 || pupils.length === 0) {
                setTimeout(attachInteractions, 300);
                return;
            }
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
            passwordInput.addEventListener('blur', () => {
                eyelids.forEach(el => el.style.height = '0%');
            });
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
                    st.session_state['user_id'] = user[0]
                    st.session_state['name'] = user[1]
                    st.session_state['role'] = user[2]
                    st.session_state['branch_id'] = user[3]
                    st.session_state['status'] = user[4]
                    st.rerun()
                else:
                    st.error("Invalid Credentials.")
            
        with st.expander("Don't have an account? Request Access"):
            with st.form("signup_request_form"):
                st.caption("Submit your details. The System Admin will configure your official account.")
                req_name = st.text_input("Your Full Name")
                req_phone = st.text_input("Your Phone Number")
                req_role = st.selectbox("Requested Role", ["Worker", "Marketer", "Driver", "Branch Manager", "HR"])
                
                if st.form_submit_button("Send Request to Admin", type="primary"):
                    if req_name.strip() and req_phone.strip():
                        log_notification(0, 'System Admin', None, None, f"🆕 Account Request: {req_name} ({req_phone}) is requesting a {req_role} role.")
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
    cursor.execute("SELECT Performance_Status FROM Users WHERE User_ID=?", (st.session_state['user_id'],))
    result = cursor.fetchone()
    fresh_status = result[0] if result else "Unknown"
    conn.close()

    st.sidebar.title(f"👤 {st.session_state['name']}")
    st.sidebar.write(f"**Role:** {st.session_state['role']}")
    
    if st.session_state['role'] not in ['System Admin', 'CEO', 'General Manager']:
        st.sidebar.write(f"**Status:** {fresh_status}")
    st.sidebar.write("---")

    if st.session_state['role'] != 'System Admin':
        with st.sidebar.expander("✉️ Direct Message & Files"):
            st.caption("Send a private message or file to anyone in the company.")
            conn = get_connection()
            users_df = pd.read_sql_query(f"SELECT User_ID, Full_Name, Role FROM Users WHERE User_ID != {st.session_state['user_id']}", conn)
            conn.close()
            
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
                        file_name = uploaded_file.name
                        safe_name = f"{datetime.now().strftime('%H%M%S')}_{file_name}"
                        file_path = os.path.join("uploads", safe_name)
                        with open(file_path, "wb") as f:
                            f.write(uploaded_file.getbuffer())
                    
                    msg_content = dm_msg.strip() if dm_msg.strip() else "📎 Sent an attached file."
                    log_notification(st.session_state['user_id'], None, None, target_id, msg_content, file_path, file_name)
                    st.success(f"Sent to {selected_user.split(' (')[0]}!")
        st.sidebar.write("---")

    if st.sidebar.button("Logout", type="secondary"):
        st.session_state['logged_in'] = False
        st.rerun()

    # =========================================================
    # THE PAYWALL LOGIC 
    # =========================================================
    if st.session_state['role'] not in ['CEO', 'System Admin']:
        has_active_sub, valid_until_date = check_subscription(st.session_state['user_id'])
        if not has_active_sub:
            st.error("🔒 Account Locked: Payment Required")
            amount_due = 100 if st.session_state['role'] in ['Worker', 'Driver', 'Marketer'] else 200
            st.info(f"**Account Role:** {st.session_state['role']} \n\n **Monthly Fee:** KES {amount_due}")
            if st.button(f"🟢 Pay KES {amount_due} via M-Pesa", use_container_width=True):
                process_payment(st.session_state['user_id'], st.session_state['role'], st.session_state['name'])
                st.success("Payment successful!")
                st.rerun() 
            st.stop() 
        else:
            st.sidebar.success(f"Sub Active until: {valid_until_date}")

    # =========================================================
    # SYSTEM ADMIN DASHBOARD
    # =========================================================
    if st.session_state['role'] == "System Admin":
        st.title("⚙️ System Configuration Portal")
        st.write("Welcome, Developer. This portal controls the core database records for WorkPulse.")
        
        conn = get_connection()
        admin_req_cursor = conn.cursor()
        admin_req_cursor.execute("SELECT COUNT(*) FROM Notifications WHERE Target_Role='System Admin' AND Is_Read=0")
        admin_req_count = admin_req_cursor.fetchone()[0]
        
        tab1, tab2, tab3 = st.tabs(["🏢 Manage Branches", "👤 Manage Users", f"🔔 Access Requests ({admin_req_count})"])
        
        with tab1:
            st.write("### Add a New Branch")
            with st.form("add_branch_form"):
                new_b_name = st.text_input("Branch Name (e.g., Kisumu Hub)")
                new_b_lat = st.number_input("GPS Latitude (e.g., -0.0917)", format="%.6f")
                new_b_lon = st.number_input("GPS Longitude (e.g., 34.7680)", format="%.6f")
                if st.form_submit_button("Register Branch"):
                    if new_b_name:
                        add_branch(new_b_name, new_b_lat, new_b_lon)
                        st.success(f"Branch '{new_b_name}' successfully added to the database.")
                    else:
                        st.error("Branch Name is required.")
            
            st.write("---")
            st.write("### Current Active Branches")
            st.dataframe(get_all_branches_df(), hide_index=True, use_container_width=True)

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
                new_u_role = st.selectbox("Assign Role", ["Worker", "Marketer", "Driver", "Branch Manager", "General Manager", "HR", "CEO"])
                new_u_branch = st.selectbox("Assign to Branch", list(branch_options.keys()))
                
                if st.form_submit_button("Create User Account"):
                    if new_u_name and new_u_phone and new_u_pass:
                        selected_branch_id = branch_options[new_u_branch]
                        success, message = add_user(new_u_name, new_u_phone, new_u_pass, new_u_role, selected_branch_id)
                        if success:
                            st.success(message)
                        else:
                            st.error(message)
                    else:
                        st.error("Name, Phone, and Password are all required fields.")
            
            # 🔴 NEW: The Admin User Editing / Removal Tools
            st.write("---")
            st.write("### Manage Existing Employees")
            user_list_df = get_all_users_df()
            
            if not user_list_df.empty:
                edit_user_options = {}
                for _, row in user_list_df.iterrows():
                    edit_user_options[f"{row['Full_Name']} ({row['Role']}) - {row['Phone_Number']}"] = row['User_ID']
                
                selected_edit_user = st.selectbox("Select Employee to Modify or Remove", ["-- Select Employee --"] + list(edit_user_options.keys()))
                
                if selected_edit_user != "-- Select Employee --":
                    edit_user_id = edit_user_options[selected_edit_user]
                    col_u1, col_u2 = st.columns(2)
                    
                    with col_u1:
                        with st.form("update_branch_form"):
                            st.write("**Reassign Branch**")
                            new_assign_branch = st.selectbox("Select New Branch", list(branch_options.keys()))
                            if st.form_submit_button("Update Branch", type="primary"):
                                update_user_branch(edit_user_id, branch_options[new_assign_branch])
                                st.success(f"Branch reassigned to {new_assign_branch} successfully!")
                                st.rerun()
                                
                    with col_u2:
                        with st.form("delete_user_form"):
                            st.write("**Remove Employee**")
                            st.caption("Warning: This action permanently deletes their login.")
                            if st.form_submit_button("Delete User", type="primary"):
                                delete_user(edit_user_id)
                                st.success("User deleted successfully!")
                                st.rerun()

            st.write("---")
            st.write("### Current Registered Users")
            search_users = st.text_input("🔍 Search Users...", key="admin_user_search")
            if search_users:
                user_list_df = user_list_df[user_list_df.astype(str).apply(lambda x: x.str.contains(search_users, case=False, na=False)).any(axis=1)]
            st.dataframe(user_list_df, hide_index=True, use_container_width=True)

        with tab3:
            st.write("### 🔔 Account Access Requests")
            st.caption("Review new requests below. Once you create their account in the 'Manage Users' tab, click 'Mark as Handled' here.")
            
            admin_notifs = pd.read_sql_query("SELECT Notif_ID, Message, Created_At FROM Notifications WHERE Target_Role='System Admin' AND Is_Read=0 ORDER BY Created_At DESC", conn)
            
            if not admin_notifs.empty:
                for _, row in admin_notifs.iterrows():
                    st.info(f"{row['Message']}\n\n*{row['Created_At']}*")
                    if st.button("Mark as Handled", key=f"admin_req_{row['Notif_ID']}", type="primary"):
                        conn.execute("UPDATE Notifications SET Is_Read=1 WHERE Notif_ID=?", (row['Notif_ID'],))
                        conn.commit()
                        st.rerun()
            else:
                st.success("No pending access requests.")
        
        conn.close()


    # =========================================================
    # UNIVERSAL TIME TRACKER & WORKFLOW
    # =========================================================
    elif st.session_state['role'] != 'CEO':
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
                    st.write("### 📍 Live Location Verification")
                    location = streamlit_geolocation()
                    worker_lat = location.get('latitude') if location else None
                    worker_lon = location.get('longitude') if location else None
                    
                    m = folium.Map(location=[b_lat, b_lon], zoom_start=18)
                    folium.Circle(location=[b_lat, b_lon], radius=15, color="blue", fill=True, fill_opacity=0.2).add_to(m)
                    folium.Marker([b_lat, b_lon], tooltip="Branch", icon=folium.Icon(color="green", icon="building", prefix='fa')).add_to(m)
                    if worker_lat and worker_lon:
                        folium.Marker([worker_lat, worker_lon], tooltip="You", icon=folium.Icon(color="red", icon="user", prefix='fa')).add_to(m)
                    st_folium(m, width=700, height=400)
                    
                    if st.button("✅ PRESS TO CHECK IN", use_container_width=True, type="primary"):
                        if st.session_state['role'] in ['Marketer', 'Driver']:
                            if worker_lat and worker_lon:
                                log_attendance(st.session_state['user_id'], worker_lat, worker_lon, checkin_status='Pending GM')
                                log_notification(0, 'General Manager', None, None, f"🌍 {st.session_state['name']} ({st.session_state['role']}) is requesting check-in approval.")
                                st.success("Location recorded. Waiting for GM approval.")
                                st.rerun()
                            else:
                                st.error(f"{st.session_state['role']}s MUST click the GPS icon to record their location before checking in!")
                        
                        elif st.session_state['role'] == 'Worker':
                            success, message = True, "Developer Override Active"
                            if success:
                                log_attendance(st.session_state['user_id'], worker_lat or 0.0, worker_lon or 0.0, checkin_status='Pending Manager')
                                log_notification(0, 'Branch Manager', st.session_state['branch_id'], None, f"🟢 {st.session_state['name']} (Worker) is requesting check-in approval.")
                                st.success("Check-in requested. Waiting for Branch Manager approval.")
                                st.rerun()
                            else:
                                st.error(message)
                        
                        else:
                            log_attendance(st.session_state['user_id'], worker_lat or 0.0, worker_lon or 0.0, checkin_status='Approved')
                            st.success("Shift started!")
                            st.rerun()
                            
            else:
                st.info("You operate across all branches. Click below to start your shift.")
                if st.button("✅ PRESS TO CHECK IN", use_container_width=True, type="primary"):
                    log_attendance(st.session_state['user_id'], 0.0, 0.0, checkin_status='Approved')
                    st.rerun()
            
            if st.session_state['role'] in ['Worker', 'Driver', 'Marketer']:
                st.write("---")
                render_inbox(st.session_state['role'], st.session_state['branch_id'], st.session_state['user_id'])
            st.stop() 

        # 1.5 PENDING CHECK-IN APPROVAL
        elif checkin_status in ['Pending GM', 'Pending Manager']:
            st.warning("⏳ Waiting for your supervisor to approve your check-in. Please hold on.")
            if st.session_state['role'] in ['Worker', 'Driver', 'Marketer']:
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
                
                break_remaining = (3600) - current_break_elapsed
                if break_remaining > 0:
                    st.warning(f"🍱 **ON LUNCH BREAK:** {int(break_remaining // 60)} minutes {int(break_remaining % 60)} seconds remaining.")
                else:
                    st.error(f"⚠️ **BREAK OVERDUE:** You are {int(abs(break_remaining) // 60)} minutes late returning from lunch!")
                
                if st.button("▶️ END BREAK & RESUME WORK", use_container_width=True, type="primary"):
                    end_break(st.session_state['user_id'], break_start_time_str)
                    st.rerun()
                
                if st.session_state['role'] in ['Worker', 'Driver', 'Marketer']:
                    st.write("---")
                    render_inbox(st.session_state['role'], st.session_state['branch_id'], st.session_state['user_id'])
                st.stop() 
            
            else:
                worked_seconds = (now_dt - check_in_dt).total_seconds() - total_break_sec
                hours = int(worked_seconds // 3600)
                minutes = int((worked_seconds % 3600) // 60)
                
                col1, col2 = st.columns([4, 1])
                with col1:
                    progress = min(worked_seconds / 28800.0, 1.0)
                    st.progress(progress)
                    st.caption(f"**Active Time Worked:** {hours} Hours, {minutes} Minutes")
                with col2:
                    if st.button("🍱 Take 1h Lunch Break", use_container_width=True):
                        start_break(st.session_state['user_id'])
                        st.rerun()

        if st.session_state['role'] in ['Worker', 'Driver', 'Marketer']:
            st.write("---")
            render_inbox(st.session_state['role'], st.session_state['branch_id'], st.session_state['user_id'])
        
        # =========================================================
        # ROLE-SPECIFIC TASKS 
        # =========================================================
        if is_working or st.session_state['role'] in ['Branch Manager', 'General Manager', 'HR']:
            
            if st.session_state['role'] == "Worker":
                st.write("### Request Time Off")
                with st.form("leave_form"):
                    start_date = st.date_input("Start Date", min_value=dt.date.today())
                    end_date = st.date_input("End Date", min_value=start_date)
                    reason = st.text_area("Reason for Leave")
                    submitted = st.form_submit_button("Submit Request to HR")
                    if submitted:
                        submit_leave_request(st.session_state['user_id'], start_date, end_date, reason)
                        st.success("Request sent to HR for approval.")
                        
            elif st.session_state['role'] == "Marketer" and is_working:
                st.write("### 💸 Log Daily Field Expenses")
                st.caption("Submit your travel, meal, or operational expenses for today.")
                with st.form("marketer_expense_form"):
                    exp_desc = st.text_input("Expense Description (e.g., Client Lunch, Fuel)")
                    exp_amount = st.number_input("Expense Amount (KES)", min_value=0, step=100)
                    if st.form_submit_button("Submit Expense", type="primary"):
                        if exp_desc.strip() != "":
                            b_id = st.session_state['branch_id'] if st.session_state['branch_id'] else 1
                            log_expense(b_id, exp_amount, exp_desc)
                            log_notification(0, 'General Manager', None, None, f"💸 Marketer {st.session_state['name']} logged a field expense: KES {exp_amount} for {exp_desc}.")
                            st.success("Expense logged securely to finance!")
                        else:
                            st.error("Please enter a description.")
            
            elif st.session_state['role'] == "Driver" and is_working:
                active_journey = get_active_journey(st.session_state['user_id'])
                if not active_journey:
                    st.info("Start a journey when leaving for deliveries.")
                    if st.button("🚀 Start Journey", use_container_width=True, type="primary"):
                        start_journey(st.session_state['user_id'])
                        log_notification(0, 'General Manager', None, None, f"🚀 {st.session_state['name']} started a delivery journey.")
                        log_notification(0, 'HR', None, None, f"🚀 {st.session_state['name']} started a delivery journey.")
                        st.rerun()
                else:
                    st.success(f"🟢 Journey Active (ID: {active_journey})")
                    location = streamlit_geolocation()
                    d_lat = location.get('latitude') if location else None
                    d_lon = location.get('longitude') if location else None
                    
                    if st.button("📦 Log Delivery at Current Location", use_container_width=True):
                        if d_lat and d_lon:
                            log_delivery(active_journey, d_lat, d_lon)
                            map_link = f"https://www.google.com/maps?q={d_lat},{d_lon}"
                            msg = f"📦 {st.session_state['name']} logged a delivery stop. [📍 View Location]({map_link})"
                            log_notification(0, 'General Manager', None, None, msg)
                            log_notification(0, 'HR', None, None, msg)
                            log_notification(0, 'CEO', None, None, msg)
                            st.success("Delivery logged!")
                        else:
                            st.error("Click the location target icon first!")
                    if st.button("🏁 Return to Branch & End Journey", use_container_width=True, type="secondary"):
                        end_journey(active_journey)
                        log_notification(0, 'General Manager', None, None, f"🏁 {st.session_state['name']} completed their journey and returned.")
                        log_notification(0, 'HR', None, None, f"🏁 {st.session_state['name']} completed their journey and returned.")
                        st.rerun()

            elif st.session_state['role'] == "Branch Manager":
                tab1, tab2, tab3, tab4, tab5 = st.tabs(["⚙️ Operations", "📅 Schedule Meeting", "📊 Performance", "📞 Directory", f"🔔 Inbox ({my_notif_count})"])
                
                with tab1:
                    col1, col2 = st.columns([2, 1])
                    with col1:
                        st.write("### Today's Attendance")
                        conn = get_connection()
                        query = f"SELECT u.Full_Name as Name, u.Phone_Number as Phone, a.Check_In_Time as Check_In, a.On_Break, u.Performance_Status as Status FROM Attendance a JOIN Users u ON a.User_ID = u.User_ID WHERE u.Branch_ID = {st.session_state['branch_id']} AND u.Role != 'Driver'"
                        df = pd.read_sql_query(query, conn)
                        if not df.empty:
                            df['On_Break'] = df['On_Break'].apply(lambda x: '🍱 On Lunch' if x == 1 else '⚙️ Working')
                            st.dataframe(df, use_container_width=True, hide_index=True)
                        else:
                            st.info("No workers have checked in today.")
                            
                        st.write("---")
                        st.write("### 📍 Pending Check-Ins")
                        pending_in_query = f"SELECT a.Record_ID, u.Full_Name as Name, a.Check_In_Time FROM Attendance a JOIN Users u ON a.User_ID = u.User_ID WHERE u.Branch_ID = {st.session_state['branch_id']} AND a.Checkin_Status = 'Pending Manager' AND u.Role = 'Worker'"
                        pending_in_df = pd.read_sql_query(pending_in_query, conn)
                        
                        if not pending_in_df.empty:
                            for _, row in pending_in_df.iterrows():
                                col_p1, col_p2 = st.columns([3, 1])
                                col_p1.warning(f"**{row['Name']}** requested check-in at {row['Check_In_Time']}")
                                if col_p2.button("Approve In", key=f"bm_app_in_{row['Record_ID']}", type="primary"):
                                    approve_checkin(row['Record_ID'])
                                    st.rerun()
                        else:
                            st.caption("No pending check-ins.")
                            
                        st.write("---")
                        st.write("### 🛑 Pending Checkouts")
                        pending_out_query = f"SELECT a.Record_ID, u.Full_Name as Name, a.Check_Out_Time FROM Attendance a JOIN Users u ON a.User_ID = u.User_ID WHERE u.Branch_ID = {st.session_state['branch_id']} AND a.Checkout_Status = 'Pending Manager' AND u.Role != 'Driver'"
                        pending_out_df = pd.read_sql_query(pending_out_query, conn)
                        
                        if not pending_out_df.empty:
                            for _, row in pending_out_df.iterrows():
                                col_p1, col_p2 = st.columns([3, 1])
                                col_p1.warning(f"**{row['Name']}** requested checkout at {row['Check_Out_Time']}")
                                if col_p2.button("Approve", key=f"app_co_{row['Record_ID']}", type="primary"):
                                    conn.execute("UPDATE Attendance SET Checkout_Status='Approved' WHERE Record_ID=?", (row['Record_ID'],))
                                    conn.commit()
                                    st.rerun()
                        else:
                            st.caption("No pending checkouts.")

                        st.write("---")
                        st.write("### 📈 End of Day Sales")
                        daily_sales = st.number_input("Enter Total Daily Sales (KES)", min_value=0, step=1000)
                        if st.button("Submit Sales Report", type="primary"):
                            log_daily_sales(st.session_state['branch_id'], daily_sales)
                            log_notification(0, 'General Manager', None, None, f"💰 Branch {st.session_state['branch_id']} submitted Daily Sales: KES {daily_sales}.")
                            log_notification(0, 'CEO', None, None, f"💰 Branch {st.session_state['branch_id']} submitted Daily Sales: KES {daily_sales}.")
                            st.success(f"KES {daily_sales} reported successfully.")
                            
                        st.write("---")
                        st.write("### 💸 Log Branch Expense")
                        exp_desc = st.text_input("Expense Description (e.g., Office Supplies, Fuel)")
                        exp_amount = st.number_input("Expense Amount (KES)", min_value=0, step=500)
                        if st.button("Submit Expense", type="secondary"):
                            if exp_desc.strip() != "":
                                log_expense(st.session_state['branch_id'], exp_amount, exp_desc)
                                st.success("Expense logged securely.")
                            else:
                                st.error("Please enter a description.")

                    with col2:
                        st.write("### 🔔 Branch Operations & Alerts")
                        notif_query = f"SELECT Message, Created_At FROM Notifications WHERE Target_Role='Branch Manager' AND Target_Branch_ID={st.session_state['branch_id']} ORDER BY Created_At DESC LIMIT 6"
                        notifs = pd.read_sql_query(notif_query, conn)
                        conn.close()
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
                            st.metric(label="Your Rank in Company", value=f"#{rank} out of {len(rank_df)}")
                            st.metric(label="Your Total Weekly Sales", value=f"KES {sales:,.2f}")
                            st.write("---")
                            st.dataframe(rank_df[['Rank', 'Branch_Name', 'Weekly Sales (KES)']], use_container_width=True, hide_index=True)
                        else:
                            st.info("Your branch hasn't registered sales yet.")
                    else:
                        st.info("No sales data available for ranking.")
                        
                with tab4:
                    st.write("### 📞 Employee Contact Directory")
                    st.caption("Click 'Call Now' to dial directly from your phone.")
                    dir_df = get_directory_df(st.session_state['branch_id'])
                    if not dir_df.empty:
                        search_bm_dir = st.text_input("🔍 Search Directory...", key="bm_dir_search")
                        if search_bm_dir:
                            dir_df = dir_df[dir_df.astype(str).apply(lambda x: x.str.contains(search_bm_dir, case=False, na=False)).any(axis=1)]
                        st.dataframe(dir_df, column_config={"Call": st.column_config.LinkColumn("Action", display_text="📞 Call Now")}, hide_index=True, use_container_width=True)
                    else:
                        st.info("No active employees found.")
                        
                with tab5:
                    render_inbox(st.session_state['role'], st.session_state['branch_id'], st.session_state['user_id'])

            elif st.session_state['role'] == "HR":
                tab1, tab2, tab3, tab4, tab5 = st.tabs(["📝 Leaves", "🚨 Live Status", "🏆 Rankings", "📞 Directory", f"🔔 Inbox ({my_notif_count})"])
                with tab1:
                    conn = get_connection()
                    hr_leaves = pd.read_sql_query("SELECT lr.Request_ID, u.Full_Name as Employee, lr.Start_Date, lr.End_Date, lr.Reason, lr.Status FROM Leave_Requests lr JOIN Users u ON lr.User_ID = u.User_ID WHERE lr.Status='Pending HR'", conn)
                    if not hr_leaves.empty:
                        for index, row in hr_leaves.iterrows():
                            with st.expander(f"Request from {row['Employee']} ({row['Start_Date']} to {row['End_Date']})"):
                                st.write(f"**Reason:** {row['Reason']}")
                                col1, col2 = st.columns(2)
                                if col1.button(f"Approve & Send to CEO", key=f"approve_{row['Request_ID']}", type="primary"):
                                    update_leave_status(row['Request_ID'], 'Pending CEO')
                                    log_notification(0, 'CEO', None, None, f"📝 HR Approved leave for {row['Employee']}. Awaiting your final confirmation.")
                                    st.rerun()
                                if col2.button(f"Reject", key=f"reject_{row['Request_ID']}"):
                                    update_leave_status(row['Request_ID'], 'Rejected by HR')
                                    st.rerun()
                    else:
                        st.info("No pending requests.")
                        
                with tab2:
                    pending_in_query = f"SELECT a.Record_ID, u.Full_Name as Name, a.Check_In_Time, a.Check_In_Lat, a.Check_In_Lon FROM Attendance a JOIN Users u ON a.User_ID = u.User_ID WHERE a.Checkin_Status = 'Pending GM'"
                    pending_in_df = pd.read_sql_query(pending_in_query, conn)
                    
                    if not pending_in_df.empty:
                        st.write("### 📍 Pending Check-Ins (Drivers & Marketers)")
                        for _, row in pending_in_df.iterrows():
                            col_f1, col_f2, col_f3 = st.columns([3, 1, 1])
                            col_f1.warning(f"**{row['Name']}** checking in at {row['Check_In_Time']}")
                            map_link = f"https://www.google.com/maps?q={row['Check_In_Lat']},{row['Check_In_Lon']}"
                            col_f2.markdown(f"[📍 View Location]({map_link})")
                            if col_f3.button("Approve In", key=f"hr_app_in_{row['Record_ID']}", type="primary"):
                                approve_checkin(row['Record_ID'])
                                log_notification(0, 'Worker', None, None, f"✅ HR approved your field check-in.")
                                st.rerun()
                        st.write("---")
                        
                    pending_out_query = f"SELECT a.Record_ID, u.Full_Name as Name, a.Check_Out_Time FROM Attendance a JOIN Users u ON a.User_ID = u.User_ID WHERE a.Checkout_Status = 'Pending GM'"
                    pending_out_df = pd.read_sql_query(pending_out_query, conn)
                    
                    if not pending_out_df.empty:
                        st.write("### 🛑 Pending Check-Outs (Drivers & Marketers)")
                        for _, row in pending_out_df.iterrows():
                            col_o1, col_o2 = st.columns([3, 1])
                            col_o1.warning(f"**{row['Name']}** requested checkout at {row['Check_Out_Time']}")
                            if col_o2.button("Approve Out", key=f"hr_app_out_{row['Record_ID']}", type="primary"):
                                conn.execute("UPDATE Attendance SET Checkout_Status='Approved' WHERE Record_ID=?", (row['Record_ID'],))
                                conn.commit()
                                st.rerun()
                        st.write("---")

                    st.write("### Live Workforce Roster")
                    date_today = datetime.now().strftime("%Y-%m-%d")
                    all_users_query = f'''
                        SELECT u.Full_Name as Name, u.Role, COALESCE(b.Branch_Name, 'Corporate') as Branch,
                               a.Check_In_Time, a.Check_Out_Time, a.On_Break, a.Checkout_Status, a.Checkin_Status, a.Check_In_Lat, a.Check_In_Lon
                        FROM Users u
                        LEFT JOIN Branches b ON u.Branch_ID = b.Branch_ID
                        LEFT JOIN Attendance a ON u.User_ID = a.User_ID AND a.Date = '{date_today}'
                        WHERE u.Role NOT IN ('CEO', 'System Admin')
                    '''
                    all_df = pd.read_sql_query(all_users_query, conn)
                    
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
                                return f"https://www.google.com/maps?q={row['Check_In_Lat']},{row['Check_In_Lon']}"
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
                            column_config={"Location": st.column_config.LinkColumn("GPS Map", display_text="📍 View Map")}
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

            elif st.session_state['role'] == "General Manager":
                st.write("### General Manager Operations")
                
                gm_tab1, gm_tab2, gm_tab3, gm_tab4, gm_tab5, gm_tab6, gm_tab7 = st.tabs(["🌍 Field Approvals", "💰 Daily", "🏆 Weekly", "📅 Calendar", "📜 Finance", "📞 Directory", f"🔔 Inbox ({my_notif_count})"])
                conn = get_connection()
                
                with gm_tab1:
                    st.write("### 🌍 Field Marketer & Driver Approvals")
                    st.caption("Review live GPS check-ins and approve field checkouts.")
                    
                    pending_in_query = f"SELECT a.Record_ID, u.Full_Name as Name, a.Check_In_Time, a.Check_In_Lat, a.Check_In_Lon FROM Attendance a JOIN Users u ON a.User_ID = u.User_ID WHERE a.Checkin_Status = 'Pending GM'"
                    pending_in_df = pd.read_sql_query(pending_in_query, conn)
                    
                    if not pending_in_df.empty:
                        st.write("#### 📍 Pending Check-Ins")
                        for _, row in pending_in_df.iterrows():
                            col_f1, col_f2, col_f3 = st.columns([3, 1, 1])
                            col_f1.warning(f"**{row['Name']}** checking in at {row['Check_In_Time']}")
                            map_link = f"https://www.google.com/maps?q={row['Check_In_Lat']},{row['Check_In_Lon']}"
                            col_f2.markdown(f"[📍 View Location]({map_link})")
                            if col_f3.button("Approve In", key=f"gm_app_in_{row['Record_ID']}", type="primary"):
                                approve_checkin(row['Record_ID'])
                                log_notification(0, 'Worker', None, None, f"✅ GM approved your field check-in.")
                                st.rerun()
                    else:
                        st.caption("No pending field check-ins.")
                        
                    st.write("---")
                    
                    pending_out_query = f"SELECT a.Record_ID, u.Full_Name as Name, a.Check_Out_Time FROM Attendance a JOIN Users u ON a.User_ID = u.User_ID WHERE a.Checkout_Status = 'Pending GM'"
                    pending_out_df = pd.read_sql_query(pending_out_query, conn)
                    
                    if not pending_out_df.empty:
                        st.write("#### 🛑 Pending Check-Outs")
                        for _, row in pending_out_df.iterrows():
                            col_o1, col_o2 = st.columns([3, 1])
                            col_o1.warning(f"**{row['Name']}** requested checkout at {row['Check_Out_Time']}")
                            if col_o2.button("Approve Out", key=f"gm_app_out_{row['Record_ID']}", type="primary"):
                                conn.execute("UPDATE Attendance SET Checkout_Status='Approved' WHERE Record_ID=?", (row['Record_ID'],))
                                conn.commit()
                                st.rerun()
                    else:
                        st.caption("No pending checkouts.")

                with gm_tab2:
                    st.write("### Today's Branch Sales Rankings")
                    date_today = datetime.now().strftime("%Y-%m-%d")
                    daily_query = f"SELECT b.Branch_Name as Branch, SUM(ds.Total_Sales) as 'Total Sales (KES)' FROM Daily_Sales ds JOIN Branches b ON ds.Branch_ID = b.Branch_ID WHERE ds.Date = '{date_today}' GROUP BY b.Branch_Name ORDER BY 'Total Sales (KES)' DESC"
                    daily_df = pd.read_sql_query(daily_query, conn)
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
                        
                with gm_tab4:
                    st.write("### 📅 Historical Data Explorer")
                    view_date = st.date_input("Select a specific date to view:", dt.date.today(), key="gm_date")
                    view_date_str = str(view_date)
                    
                    st.write(f"**Sales on {view_date_str}**")
                    day_sales_query = f"SELECT b.Branch_Name as Branch, SUM(ds.Total_Sales) as 'Total Sales (KES)' FROM Daily_Sales ds JOIN Branches b ON ds.Branch_ID = b.Branch_ID WHERE ds.Date = '{view_date_str}' GROUP BY b.Branch_Name ORDER BY 'Total Sales (KES)' DESC"
                    day_sales_df = pd.read_sql_query(day_sales_query, conn)
                    if not day_sales_df.empty:
                        st.dataframe(day_sales_df, use_container_width=True, hide_index=True)
                    else:
                        st.caption("No sales reported on this date.")
                        
                with gm_tab5:
                    st.write("### Master Transaction Log")
                    history_query = "SELECT ds.Date, b.Branch_Name, ds.Total_Sales FROM Daily_Sales ds JOIN Branches b ON ds.Branch_ID = b.Branch_ID ORDER BY ds.Date DESC"
                    hist_df = pd.read_sql_query(history_query, conn)
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
                conn.close()

            # --- UNIVERSAL CHECKOUT BUTTON ---
            if is_working:
                st.write("---")
                col_c1, col_c2, col_c3 = st.columns([1, 2, 1])
                with col_c2:
                    st.write("### End Your Shift")
                    if st.button("🛑 REQUEST CHECKOUT", use_container_width=True, type="secondary"):
                        success, message = True, "Developer Override Active"
                        if success:
                            request_check_out(st.session_state['user_id'], st.session_state['role'])
                            
                            if st.session_state['role'] == 'Driver':
                                log_notification(0, 'General Manager', None, None, f"🛑 {st.session_state['name']} (Driver) has requested to check out.")
                                log_notification(0, 'HR', None, None, f"🛑 {st.session_state['name']} (Driver) has requested to check out.")
                            elif st.session_state['role'] == 'Worker':
                                log_notification(0, 'Branch Manager', st.session_state['branch_id'], None, f"🛑 {st.session_state['name']} has requested to check out.")
                                
                            st.success("Checkout requested!")
                            st.rerun()
                        else:
                            st.error("You must be at the branch to check out!")

    # =========================================================
    # CEO DASHBOARD 
    # =========================================================
    elif st.session_state['role'] == "CEO":
        st.title("CEO Master Operations 📈")
        conn = get_connection()
        date_today = datetime.now().strftime("%Y-%m-%d")
        
        tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["📊 Data & Sales", "🚨 Live Status", "📅 History", "📢 Broadcast", "✅ Leaves", "📞 Directory", f"🔔 Inbox ({my_notif_count})"])
        
        with tab1:
            col_g1, col_g2 = st.columns(2)
            with col_g1:
                st.write("### 📊 Weekly Sales vs Expenses")
                perf_query = '''
                    SELECT b.Branch_Name,
                        (SELECT COALESCE(SUM(Total_Sales), 0) FROM Daily_Sales WHERE Branch_ID = b.Branch_ID AND Date >= date('now', '-7 days')) as "Sales (KES)",
                        (SELECT COALESCE(SUM(Amount), 0) FROM Expenses WHERE Branch_ID = b.Branch_ID AND Date >= date('now', '-7 days')) as "Expenses (KES)"
                    FROM Branches b
                '''
                perf_df = pd.read_sql_query(perf_query, conn)
                if not perf_df.empty and (perf_df['Sales (KES)'].sum() > 0 or perf_df['Expenses (KES)'].sum() > 0):
                    chart_data = perf_df.set_index('Branch_Name')
                    st.bar_chart(chart_data, color=["#1484A6", "#EF4444"])
                else:
                    st.info("No data in last 7 days.")
                    
            with col_g2:
                st.write("### 🏆 Today's Rankings")
                rank_query = f"SELECT b.Branch_Name as Branch, SUM(ds.Total_Sales) as 'Total Sales (KES)' FROM Daily_Sales ds JOIN Branches b ON ds.Branch_ID = b.Branch_ID WHERE ds.Date = '{date_today}' GROUP BY b.Branch_Name ORDER BY 'Total Sales (KES)' DESC"
                rank_df = pd.read_sql_query(rank_query, conn)
                if not rank_df.empty:
                    rank_df.index = rank_df.index + 1 
                    st.dataframe(rank_df, use_container_width=True)
                else:
                    st.warning("No sales reports today.")
                
        with tab2:
            st.write("### Live Workforce Roster")
            all_users_query = f'''
                SELECT u.Full_Name as Name, u.Role, COALESCE(b.Branch_Name, 'Corporate') as Branch,
                       a.Check_In_Time, a.Check_Out_Time, a.On_Break, a.Checkout_Status, a.Checkin_Status, a.Check_In_Lat, a.Check_In_Lon
                FROM Users u
                LEFT JOIN Branches b ON u.Branch_ID = b.Branch_ID
                LEFT JOIN Attendance a ON u.User_ID = a.User_ID AND a.Date = '{date_today}'
                WHERE u.Role NOT IN ('CEO', 'System Admin')
            '''
            all_df = pd.read_sql_query(all_users_query, conn)
            
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
                        return f"https://www.google.com/maps?q={row['Check_In_Lat']},{row['Check_In_Lon']}"
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
                    column_config={"Location": st.column_config.LinkColumn("GPS Map", display_text="📍 View Map")}
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
                day_sales_query = f"SELECT b.Branch_Name as Branch, SUM(ds.Total_Sales) as 'Total Sales (KES)' FROM Daily_Sales ds JOIN Branches b ON ds.Branch_ID = b.Branch_ID WHERE ds.Date = '{view_date_str}' GROUP BY b.Branch_Name ORDER BY 'Total Sales (KES)' DESC"
                day_sales_df = pd.read_sql_query(day_sales_query, conn)
                if not day_sales_df.empty:
                    st.dataframe(day_sales_df, use_container_width=True, hide_index=True)
                else:
                    st.caption("No sales reported.")
                    
            with col_c2:
                st.write(f"**Attendance on {view_date_str}**")
                day_att_query = f"SELECT u.Full_Name as Name, a.Check_In_Time FROM Attendance a JOIN Users u ON a.User_ID = u.User_ID WHERE a.Date = '{view_date_str}'"
                day_att_df = pd.read_sql_query(day_att_query, conn)
                if not day_att_df.empty:
                    st.dataframe(day_att_df, use_container_width=True, hide_index=True)
                else:
                    st.caption("No attendance logged.")
                    
        with tab4:
            st.write("### 📢 Corporate Broadcast")
            with st.form("broadcast_form"):
                audience = st.selectbox("Select Target Audience", ["Entire Company", "Branch Manager", "General Manager", "HR", "Worker", "Marketer", "Driver"])
                b_message = st.text_area("Broadcast Message")
                if st.form_submit_button("Send Broadcast", type="primary"):
                    log_notification(st.session_state['user_id'], audience, None, None, f"📢 CEO ANNOUNCEMENT: {b_message}")
                    st.success(f"Message successfully sent to: {audience}")

        with tab5:
            st.write("### Final Approval Required")
            ceo_leaves = pd.read_sql_query("SELECT lr.Request_ID, u.Full_Name as Employee, lr.Start_Date, lr.End_Date, lr.Reason FROM Leave_Requests lr JOIN Users u ON lr.User_ID = u.User_ID WHERE lr.Status='Pending CEO'", conn)
            if not ceo_leaves.empty:
                for index, row in ceo_leaves.iterrows():
                    with st.expander(f"Review: {row['Employee']} ({row['Start_Date']} to {row['End_Date']})"):
                        st.write(f"**Reason:** {row['Reason']}")
                        col1, col2 = st.columns(2)
                        if col1.button(f"Confirm Approved", key=f"ceo_ok_{row['Request_ID']}", type="primary"):
                            update_leave_status(row['Request_ID'], 'Approved')
                            st.rerun()
                        if col2.button(f"Veto & Reject", key=f"ceo_no_{row['Request_ID']}"):
                            update_leave_status(row['Request_ID'], 'Rejected by CEO')
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

        conn.close()