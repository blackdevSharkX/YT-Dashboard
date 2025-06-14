import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

# Set page config at the very top
st.set_page_config(page_title="YouTube Dashboard", layout="wide")

# --- DATABASE CONNECTION ---
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        dbname="defaultdb",
        user="avnadmin",
        password="AVNS_KJqeb-gewDBuBv86gqN",
        host="pg-14ef1716-navysuarez69-106e.b.aivencloud.com",
        port="24135",
        sslmode="require"
    )

conn = get_connection()

# --- FUNCTIONS ---
def get_total_records():
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM youtube_videos")
        return cur.fetchone()[0]

def fetch_data(offset, limit):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT * FROM youtube_videos
            ORDER BY published_at DESC
            OFFSET %s LIMIT %s
        """, (offset, limit))
        return cur.fetchall()

def insert_video(data):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO youtube_videos (title, channel_title, published_at, views)
            VALUES (%s, %s, %s, %s)
        """, (data['title'], data['channel_title'], data['published_at'], data['views']))
        conn.commit()

def update_video(video_id, data):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE youtube_videos
            SET title = %s, channel_title = %s, published_at = %s, views = %s
            WHERE id = %s
        """, (data['title'], data['channel_title'], data['published_at'], data['views'], video_id))
        conn.commit()

def delete_video(video_id):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM youtube_videos WHERE id = %s", (video_id,))
        conn.commit()

# --- STREAMLIT DASHBOARD ---
st.title("YouTube Scraped Video Dashboard")

# --- Pagination and limit options ---
st.sidebar.subheader("Pagination Options")
limit = st.sidebar.selectbox("Records per page", [10, 100, 200, 500], index=0)
total = get_total_records()
total_pages = (total + limit - 1) // limit
page = st.sidebar.number_input("Page", 1, total_pages, 1)
offset = (page - 1) * limit

# --- View Data ---
st.subheader("Videos")
videos = fetch_data(offset, limit)
df = pd.DataFrame(videos)
st.dataframe(df)

# --- Footer Info ---
st.caption(f"Total Records: {total} | Page {page} of {total_pages}")

# --- ADD / EDIT / DELETE FORM ---
st.sidebar.header("Manage Video")
action = st.sidebar.selectbox("Action", ["Add", "Edit", "Delete"])

if action == "Add":
    with st.sidebar.form("Add Video"):
        title = st.text_input("Title")
        channel = st.text_input("Channel Title")
        published = st.date_input("Published At")
        views = st.number_input("Views", min_value=0)
        submitted = st.form_submit_button("Add")
        if submitted:
            insert_video({
                "title": title,
                "channel_title": channel,
                "published_at": published,
                "views": views
            })
            st.success("Video added.")

elif action == "Edit":
    selected_id = st.sidebar.number_input("Video ID to Edit", min_value=1)
    with st.sidebar.form("Edit Video"):
        title = st.text_input("New Title")
        channel = st.text_input("New Channel Title")
        published = st.date_input("New Published At")
        views = st.number_input("New Views", min_value=0)
        submitted = st.form_submit_button("Update")
        if submitted:
            update_video(selected_id, {
                "title": title,
                "channel_title": channel,
                "published_at": published,
                "views": views
            })
            st.success("Video updated.")

elif action == "Delete":
    selected_id = st.sidebar.number_input("Video ID to Delete", min_value=1)
    if st.sidebar.button("Delete Video"):
        delete_video(selected_id)
        st.warning("Video deleted.")
