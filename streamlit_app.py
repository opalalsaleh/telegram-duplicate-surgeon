import streamlit as st
import asyncio
import gc
import hashlib
import io
import sqlite3
import tempfile
import time
import threading
from typing import Dict, List, Optional, Tuple

import pandas as pd
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, SessionPasswordNeededError
from telethon.tl.types import (
    MessageMediaDocument, MessageMediaPhoto, DocumentAttributeVideo, PhotoSize
)
from PIL import Image
try:
    import imagehash
    _HAS_IMAGEHASH = True
except ImportError:
    _HAS_IMAGEHASH = False

# ================== تهيئة الصفحة ==================
st.set_page_config(page_title="DupZap – مزيل المكررات", page_icon="✂️", layout="wide")

# ================== نظام التكرار الضبابي (Fuzzy Duplicate Detection) ==================
def make_bucket(v: Dict) -> Tuple[int, int]:
    """تقسيم الفيديوهات لمجموعات لتقليل عدد المقارنات."""
    return (
        v.get("duration", 0) // 3,
        v.get("size", 0) // 2_000_000
    )

def fuzzy_video_score(a: Dict, b: Dict) -> float:
    """حساب درجة التشابه بين فيديوهين (0-100) - نسخة خفيفة."""
    score = 0.0
    d1, d2 = a.get("duration", 0), b.get("duration", 0)
    diff = abs(d1 - d2)
    if diff == 0: score += 50
    elif diff <= 1: score += 40
    elif diff <= 2: score += 30
    elif diff <= 3: score += 15
    elif diff <= 5: score += 5
    else: return 0

    s1, s2 = a.get("size", 0), b.get("size", 0)
    if s1 > 0 and s2 > 0:
        ratio = abs(s1 - s2) / max(s1, s2)
        if ratio < 0.05: score += 30
        elif ratio < 0.12: score += 20
        elif ratio < 0.25: score += 10

    w1, h1 = a.get("width", 0), a.get("height", 0)
    w2, h2 = b.get("width", 0), b.get("height", 0)
    if w1 and h1 and w2 and h2:
        ar1 = max(w1, h1) / min(w1, h1)
        ar2 = max(w2, h2) / min(w2, h2)
        if abs(ar1 - ar2) < 0.05: score += 20
        elif abs(ar1 - ar2) < 0.1: score += 10
    return score

def group_videos_by_bucket(videos: List[Dict]) -> Dict[Tuple[int, int], List[Dict]]:
    buckets = {}
    for v in videos:
        key = make_bucket(v)
        if key not in buckets:
            buckets[key] = []
        buckets[key].append(v)
    return buckets

def find_fuzzy_duplicates(videos: List[Dict], threshold: float = 75) -> List[Dict]:
    duplicates = []
    seen = set()
    buckets = group_videos_by_bucket(videos)
    for bucket_videos in buckets.values():
        if len(bucket_videos) > 200:
            continue
        for i in range(len(bucket_videos)):
            for j in range(i + 1, len(bucket_videos)):
                a = bucket_videos[i]
                b = bucket_videos[j]
                if abs(a.get("duration", 0) - b.get("duration", 0)) > 5:
                    continue
                pair = tuple(sorted((a["msg_id"], b["msg_id"])))
                if pair in seen:
                    continue
                seen.add(pair)
                score = fuzzy_video_score(a, b)
                if score >= threshold:
                    duplicates.append({
                        "dup": b,
                        "original": a,
                        "score": score,
                        "match_type": f"fuzzy ({score:.0f})"
                    })
    return duplicates

# ================== التنسيق العام ==================
st.html("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Arabic:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"], .stApp {
        font-family: 'IBM Plex Sans Arabic', 'Segoe UI', system-ui, sans-serif;
    }

    .stApp {
        background: #f8fafc;
    }
    .main .block-container {
        padding-top: 2rem;
        max-width: 960px;
    }

    [data-testid="stSidebar"] {
        background: #0f172a !important;
    }
    [data-testid="stSidebar"] > div:first-child {
        padding: 0;
    }
    [data-testid="stSidebar"] * { color: #cbd5e1 !important; }
    [data-testid="stSidebar"] hr { border-color: #1e293b !important; margin: 0.75rem 0 !important; }

    [data-testid="stSidebar"] .stButton > button {
        background: transparent !important;
        border: 1px solid #1e293b !important;
        color: #94a3b8 !important;
        border-radius: 8px;
        font-weight: 500;
        font-size: 0.85rem;
        transition: all 0.15s;
    }
    [data-testid="stSidebar"] .stButton > button:hover {
        background: #1e293b !important;
        border-color: #334155 !important;
        color: #e2e8f0 !important;
        transform: none;
        box-shadow: none;
    }

    .sidebar-logo {
        text-align: center;
        padding: 28px 16px 20px;
        border-bottom: 1px solid #1e293b;
        margin-bottom: 8px;
    }
    .sidebar-logo .logo-icon {
        font-size: 2.6rem;
        line-height: 1;
        display: block;
        margin-bottom: 10px;
    }
    .sidebar-logo .logo-name {
        font-size: 1.45rem;
        font-weight: 700;
        background: linear-gradient(90deg, #38bdf8 0%, #818cf8 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        display: block;
        margin-bottom: 3px;
    }
    .sidebar-logo .logo-ver {
        font-size: 0.72rem;
        color: #475569 !important;
        letter-spacing: 0.04em;
    }

    .user-chip {
        background: #1e293b;
        border-radius: 10px;
        padding: 10px 14px;
        margin: 8px 0;
        display: flex;
        align-items: center;
        gap: 8px;
    }

    h1 {
        font-size: 2rem !important;
        font-weight: 700 !important;
        color: #0f172a !important;
        letter-spacing: -0.02em;
        margin-bottom: 0 !important;
    }
    h2, h3 { font-weight: 600 !important; color: #1e293b !important; }

    .stApp [data-testid="stCaptionContainer"] p {
        color: #64748b;
        font-size: 0.88rem;
    }

    .stButton > button {
        border-radius: 9px;
        font-weight: 600;
        font-size: 0.88rem;
        min-height: 42px;
        transition: all 0.18s cubic-bezier(.4,0,.2,1);
        border: 1.5px solid #e2e8f0 !important;
        background: #ffffff !important;
        color: #374151 !important;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    }
    .stButton > button:hover {
        border-color: #cbd5e1 !important;
        box-shadow: 0 4px 12px rgba(0,0,0,0.09) !important;
        transform: translateY(-1px);
        color: #0f172a !important;
    }
    .stButton > button:active { transform: translateY(0); }

    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #0ea5e9 0%, #6366f1 100%) !important;
        color: #ffffff !important;
        border: none !important;
        box-shadow: 0 3px 12px rgba(99,102,241,0.30) !important;
    }
    .stButton > button[kind="primary"]:hover {
        box-shadow: 0 6px 20px rgba(99,102,241,0.40) !important;
        transform: translateY(-2px);
        color: #ffffff !important;
    }

    [data-testid="metric-container"] {
        background: #ffffff;
        border-radius: 14px;
        padding: 20px 16px;
        border: 1px solid #e2e8f0;
        box-shadow: 0 1px 4px rgba(0,0,0,0.05);
        transition: box-shadow 0.2s;
    }
    [data-testid="metric-container"]:hover {
        box-shadow: 0 4px 16px rgba(0,0,0,0.08);
    }
    [data-testid="stMetricLabel"] { color: #64748b !important; font-size: 0.82rem !important; }
    [data-testid="stMetricValue"] { color: #0f172a !important; font-weight: 700 !important; }

    .stTextInput > div > div > input,
    .stNumberInput > div > div > input,
    .stSelectbox > div > div {
        border-radius: 9px !important;
        border: 1.5px solid #e2e8f0 !important;
        background: #ffffff !important;
        font-size: 0.9rem;
        transition: border-color 0.15s, box-shadow 0.15s;
    }
    .stTextInput > div > div > input:focus,
    .stNumberInput > div > div > input:focus {
        border-color: #6366f1 !important;
        box-shadow: 0 0 0 3px rgba(99,102,241,0.12) !important;
        outline: none !important;
    }
    .stTextInput label, .stNumberInput label,
    .stSelectbox label, .stMultiSelect label,
    .stCheckbox label, .stToggle label {
        font-size: 0.85rem !important;
        font-weight: 600 !important;
        color: #374151 !important;
    }

    hr { border-color: #f1f5f9 !important; margin: 1.5rem 0 !important; }

    [data-testid="stForm"] {
        background: #ffffff;
        border-radius: 16px;
        padding: 28px !important;
        border: 1px solid #e2e8f0;
        box-shadow: 0 2px 8px rgba(0,0,0,0.05);
    }

    .stDataEditor {
        border-radius: 12px !important;
        overflow: hidden;
        border: 1px solid #e2e8f0 !important;
        box-shadow: 0 1px 4px rgba(0,0,0,0.04);
    }

    .stProgress > div > div > div > div {
        background: linear-gradient(90deg, #0ea5e9, #6366f1) !important;
        border-radius: 99px;
    }

    [data-testid="stAlert"] {
        border-radius: 10px !important;
        border-width: 1px !important;
    }

    .footer-bar {
        text-align: center;
        padding: 20px;
        color: #94a3b8;
        font-size: 0.8rem;
        margin-top: 40px;
        border-top: 1px solid #f1f5f9;
    }
    .footer-bar strong { color: #64748b; font-weight: 600; }

    .match-badge {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 99px;
        font-size: 0.75rem;
        font-weight: 600;
    }

    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: #f8fafc; }
    ::-webkit-scrollbar-thumb { background: #cbd5e1; border-radius: 99px; }
    ::-webkit-scrollbar-thumb:hover { background: #94a3b8; }
</style>
""")

# ================== Loop ثابت في session_state ==================
if '_bg_loop' not in st.session_state:
    loop = asyncio.new_event_loop()
    t = threading.Thread(target=loop.run_forever, daemon=True, name="TelegramLoop")
    t.start()
    st.session_state._bg_loop = loop
    st.session_state._bg_thread = t

def run_sync(coro):
    future = asyncio.run_coroutine_threadsafe(coro, st.session_state._bg_loop)
    return future.result(timeout=120)

# ================== الثوابت ==================
BATCH_SCAN_SIZE   = 50
BATCH_DELETE_SIZE = 25
PAGE_SIZE         = 50
PHASH_SIZE_LIMIT  = 5 * 1024 * 1024
MD5_SIZE_LIMIT    = 5 * 1024 * 1024

# ================== دوال مساعدة ==================
def fmt_size(n: int) -> str:
    if n == 0: return "0 B"
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024: return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} PB"

def get_thumb(media):
    if isinstance(media, MessageMediaPhoto):
        if not media.photo: return None
        for size in media.photo.sizes:
            if isinstance(size, PhotoSize) and hasattr(size, 'type') and size.type == 'm':
                return size
        sizes = [s for s in media.photo.sizes if hasattr(s, 'size')]
        return min(sizes, key=lambda s: s.size) if sizes else None
    elif isinstance(media, MessageMediaDocument):
        doc = media.document
        if doc and doc.thumbs:
            return min(doc.thumbs, key=lambda t: getattr(t, 'size', 0))
    return None

# ================== دوال Telethon async ==================
async def _make_client(api_id, api_hash, session_string=None):
    session = StringSession(session_string) if session_string else StringSession()
    client = TelegramClient(session, int(api_id), api_hash)
    await client.connect()
    return client

async def _send_code(client, phone):
    return await client.send_code_request(phone)

async def _sign_in(client, phone, code, phone_code_hash):
    return await client.sign_in(phone, code, phone_code_hash=phone_code_hash)

async def _sign_in_password(client, password):
    return await client.sign_in(password=password)

async def _is_authorized(client):
    return await client.is_user_authorized()

async def _get_entity(client, channel_input):
    if "/+" in channel_input or "/joinchat/" in channel_input:
        from telethon.tl.functions.messages import CheckChatInviteRequest
        hash_part = channel_input.split("/+")[-1] if "/+" in channel_input else channel_input.split("/joinchat/")[-1]
        result = await client(CheckChatInviteRequest(hash_part))
        if hasattr(result, 'chat'):
            return result.chat
        raise Exception("تعذّر الوصول — تأكد أنك عضو في المجموعة أولاً")
    return await client.get_entity(channel_input)

async def _get_messages(client, channel, offset_id, limit):
    msgs = []
    async for msg in client.iter_messages(channel, offset_id=offset_id, limit=limit, reverse=False):
        msgs.append(msg)
    return msgs

async def _delete_messages(client, channel, ids):
    await client.delete_messages(channel, ids)

def get_session_string(client):
    return client.session.save()

# ================== استخراج معلومات الملف (بدون بوت) ==================
async def extract_file_info_async(client, msg, compute_md5: bool, compute_phash: bool) -> Optional[Dict]:
    media = msg.media
    if not media: return None

    info = {
        "id": msg.id, "file_id": None, "file_unique_id": None,
        "size": 0, "duration": 0, "width": 0, "height": 0,
        "mime": "", "type": "", "date": msg.date.isoformat(),
        "md5": None, "phash": None, "views": msg.views or 0, "name": None
    }

    if isinstance(media, MessageMediaDocument):
        doc = media.document
        info["file_id"] = f"{doc.id}:{doc.dc_id}"
        info["size"]    = doc.size or 0
        info["mime"]    = doc.mime_type or ""
        info["type"]    = ("video"    if info["mime"].startswith("video/")
                           else "image" if info["mime"].startswith("image/")
                           else "document")
        for attr in doc.attributes:
            if isinstance(attr, DocumentAttributeVideo):
                info["duration"] = attr.duration or 0
                info["width"]    = attr.w or 0
                info["height"]   = attr.h or 0
            if hasattr(attr, 'file_name'):
                info["name"] = attr.file_name

    elif isinstance(media, MessageMediaPhoto):
        photo = media.photo
        info["file_id"] = f"{photo.id}:{photo.dc_id}"
        info["type"]    = "photo"
        info["mime"]    = "image/jpeg"
        sizes = [s for s in getattr(photo, "sizes", []) if hasattr(s, "size") and s.size > 0]
        info["size"] = max(sizes, key=lambda s: s.size).size if sizes else 0
    else:
        return None

    # إنشاء file_unique_id احتياطي (بدون بوت)
    if info["type"] == "video" and info["size"] > 0:
        info["file_unique_id"] = f"v:{info['size']}:{int(info['duration'])}"
    elif info["type"] == "photo":
        info["file_unique_id"] = f"p:{media.photo.id}"
    else:
        info["file_unique_id"] = f"d:{media.document.id}"

    if compute_md5 or compute_phash:
        thumb = get_thumb(media) if compute_phash else None
        data = None
        try:
            if thumb:
                data = await client.download_media(thumb, file=bytes)
            else:
                limit = PHASH_SIZE_LIMIT if compute_phash else None
                data = await client.download_media(msg, file=bytes, size=limit)

            if compute_md5 and info["size"] <= MD5_SIZE_LIMIT:
                info["md5"] = hashlib.md5(data).hexdigest()

            if compute_phash and _HAS_IMAGEHASH and info["type"] in ("photo", "image"):
                try:
                    with io.BytesIO(data) as bio:
                        with Image.open(bio) as img:
                            info["phash"] = str(imagehash.phash(img))
                except Exception:
                    pass
        except Exception:
            pass
        finally:
            del data
            gc.collect()

    return info

# ================== قاعدة البيانات ==================
class Database:
    def __init__(self, path):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init_tables()

    def _init_tables(self):
        # إنشاء الجدول الأساسي
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS seen_files (
                channel_id INTEGER, msg_id INTEGER, file_id TEXT, file_unique_id TEXT,
                file_size INTEGER, duration INTEGER,
                md5_hash TEXT, phash TEXT,
                msg_date TEXT, file_type TEXT, mime_type TEXT, views INTEGER, file_name TEXT,
                PRIMARY KEY (channel_id, msg_id)
            ) WITHOUT ROWID
        """)
        # إضافة أعمدة width, height بأمان (للتوافق مع الإصدارات السابقة)
        for col in ("width", "height"):
            try:
                self.conn.execute(f"ALTER TABLE seen_files ADD COLUMN {col} INTEGER")
            except sqlite3.OperationalError:
                pass  # العمود موجود مسبقاً

        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS resume_meta (
                channel_id INTEGER PRIMARY KEY,
                last_scanned_id INTEGER DEFAULT 0,
                total_scanned INTEGER DEFAULT 0,
                files_saved INTEGER DEFAULT 0
            )
        """)
        self.conn.commit()

    def get_resume_state(self, channel_id):
        row = self.conn.execute(
            "SELECT last_scanned_id, total_scanned, files_saved FROM resume_meta WHERE channel_id=?",
            (channel_id,)
        ).fetchone()
        return (row[0], row[1], row[2]) if row else (0, 0, 0)

    def save_progress(self, channel_id, last_id, total_scanned, files_saved):
        self.conn.execute(
            "INSERT OR REPLACE INTO resume_meta VALUES (?,?,?,?)",
            (channel_id, last_id, total_scanned, files_saved)
        )
        self.conn.commit()

    def buffer_insert(self, record):
        # record يجب أن يحتوي على 15 قيمة
        self.conn.execute("INSERT OR REPLACE INTO seen_files VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", record)
        self.conn.commit()

    def delete_msg_records(self, channel_id, msg_ids):
        self.conn.executemany(
            "DELETE FROM seen_files WHERE channel_id=? AND msg_id=?",
            [(channel_id, mid) for mid in msg_ids]
        )
        self.conn.commit()

    def stream_duplicates(self, channel_id, keep_strategy, min_size=0, use_md5=False, use_phash=False, use_fuzzy=True, fuzzy_threshold=75):
        query = """
            SELECT msg_id, file_size, msg_date, file_id, file_unique_id, duration,
                   width, height, phash, file_type, mime_type, file_name, md5_hash
            FROM seen_files
            WHERE channel_id=? AND file_size>=?
        """
        rows = self.conn.execute(query, (channel_id, min_size)).fetchall()
        if not rows:
            return []

        videos = []
        for r in rows:
            videos.append({
                "msg_id": r[0], "size": r[1], "date": r[2], "file_id": r[3],
                "file_unique_id": r[4], "duration": r[5], "width": r[6], "height": r[7],
                "phash": r[8], "type": r[9], "mime": r[10], "name": r[11], "md5": r[12]
            })

        if keep_strategy == "oldest":
            videos.sort(key=lambda x: x["date"])
        elif keep_strategy == "newest":
            videos.sort(key=lambda x: x["date"], reverse=True)
        elif keep_strategy == "largest":
            videos.sort(key=lambda x: x["size"], reverse=True)

        duplicates_map = {}

        if use_fuzzy:
            fuzzy_dups = find_fuzzy_duplicates(videos, fuzzy_threshold)
            for d in fuzzy_dups:
                dup = d["dup"]
                orig = d["original"]
                mid = dup["msg_id"]
                if mid not in duplicates_map:
                    duplicates_map[mid] = {
                        "id": mid, "size": dup["size"], "date": dup["date"], "file_id": dup["file_id"],
                        "file_unique_id": dup["file_unique_id"], "duration": dup["duration"],
                        "phash": dup["phash"], "type": dup["type"], "mime": dup["mime"],
                        "name": dup["name"], "keeper_id": orig["msg_id"],
                        "match_type": d["match_type"]
                    }

        def add_duplicates_from_group(group_func, match_type):
            groups = {}
            for v in videos:
                key = group_func(v)
                if key is None: continue
                groups.setdefault(key, []).append(v)
            for key, group in groups.items():
                if len(group) < 2: continue
                if keep_strategy == "oldest":
                    group.sort(key=lambda x: x["date"])
                elif keep_strategy == "newest":
                    group.sort(key=lambda x: x["date"], reverse=True)
                elif keep_strategy == "largest":
                    group.sort(key=lambda x: x["size"], reverse=True)
                keeper = group[0]
                for dup in group[1:]:
                    mid = dup["msg_id"]
                    if mid not in duplicates_map:
                        duplicates_map[mid] = {
                            "id": mid, "size": dup["size"], "date": dup["date"],
                            "file_id": dup["file_id"], "file_unique_id": dup["file_unique_id"],
                            "duration": dup["duration"], "phash": dup["phash"], "type": dup["type"],
                            "mime": dup["mime"], "name": dup["name"], "keeper_id": keeper["msg_id"],
                            "match_type": match_type
                        }

        add_duplicates_from_group(lambda v: v["file_id"], "file_id")
        add_duplicates_from_group(lambda v: v["file_unique_id"], "file_unique_id")
        if use_md5:
            add_duplicates_from_group(lambda v: v["md5"], "md5")
        if use_phash and _HAS_IMAGEHASH:
            add_duplicates_from_group(lambda v: v["phash"], "phash")

        return list(duplicates_map.values())

    def clear_channel(self, channel_id):
        self.conn.execute("DELETE FROM seen_files WHERE channel_id=?", (channel_id,))
        self.conn.execute("DELETE FROM resume_meta WHERE channel_id=?", (channel_id,))
        self.conn.commit()

    def close(self): self.conn.close()

# ================== حالة الجلسة ==================
defaults = {
    'client': None, 'step': 'login', 'db_path': None, 'channel': None,
    'scan_params': {}, 'page': 0, 'selected_ids': set(), 'auto_mode': False,
    'total_scanned': 0, 'files_saved': 0, 'phone_code_hash': None,
    'session_string': None, 'api_id': None, 'api_hash': None, 'phone': None,
    'last_deleted_count': 0, 'last_deleted_failed': 0,
    'auto_scan_running': False, 'scan_speed': 0.0,
    'me': None, '_confirm_rescan': False,
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ================== الشريط الجانبي (Sidebar) ==================
with st.sidebar:
    st.html("""
    <div class='sidebar-logo'>
        <div class='logo-icon'>✂️</div>
        <div class='logo-name'>DupZap</div>
        <div class='logo-ver'>v4.0 · Telegram</div>
    </div>
    """)

    st.divider()

    if st.session_state.client and st.session_state.step not in ['login', 'verify_code']:
        try:
            if not st.session_state.me:
                st.session_state.me = run_sync(st.session_state.client.get_me())
            me = st.session_state.me
            st.markdown(f"**👤 {me.first_name}**")
            if me.username:
                st.markdown(f"@{me.username}")
        except:
            pass

        if st.session_state.session_string:
            with st.expander("🔑 Session String"):
                st.caption("احفظها واستخدمها للدخول المباشر بدون SMS")
                st.code(st.session_state.session_string, language=None)

    if st.session_state.channel:
        channel = st.session_state.channel
        st.markdown(f"**📢 {getattr(channel, 'title', 'قناة')}**")

    st.divider()

    current_step = st.session_state.step

    if current_step == 'verify_code':
        if st.button("⬅️ العودة لتسجيل الدخول", use_container_width=True, key="nav_back_login"):
            st.session_state.step = 'login'
            st.rerun()

    elif current_step == 'channel':
        if st.button("🚪 تسجيل الخروج", use_container_width=True, key="nav_logout"):
            for key in ['client', 'me', 'phone', 'api_id', 'api_hash', 'session_string']:
                if key in st.session_state:
                    del st.session_state[key]
            st.session_state.step = 'login'
            st.rerun()

    elif current_step == 'scanning':
        if st.button("⬅️ تغيير القناة", use_container_width=True, key="nav_back_channel"):
            st.session_state.step = 'channel'
            st.session_state.auto_scan_running = False
            st.rerun()

    elif current_step == 'results':
        col1, col2 = st.columns(2)
        with col1:
            if st.button("⬅️ مسح", use_container_width=True, key="nav_back_scan"):
                st.session_state.step = 'scanning'
                st.session_state.selected_ids = set()
                st.rerun()
        with col2:
            if st.button("📋 قناة", use_container_width=True, key="nav_back_channel2"):
                st.session_state.step = 'channel'
                st.session_state.selected_ids = set()
                st.rerun()

    st.divider()

    if current_step not in ['login', 'verify_code']:
        if st.button("🚪 تسجيل الخروج", use_container_width=True, key="nav_logout_bottom"):
            for key in ['client', 'me', 'phone', 'api_id', 'api_hash', 'session_string']:
                if key in st.session_state:
                    del st.session_state[key]
            st.session_state.step = 'login'
            st.rerun()

    st.markdown("---")
    st.markdown("<p style='text-align:center; font-size:0.8rem; color:#64748b;'>© F.ALSALEH</p>", unsafe_allow_html=True)

# ================== المحتوى الرئيسي ==================
st.html("""
<div style="padding: 8px 0 24px;">
  <div style="display:flex; align-items:center; gap:12px; margin-bottom:4px;">
    <span style="font-size:2rem;">✂️</span>
    <span style="font-size:1.9rem; font-weight:700; color:#0f172a; letter-spacing:-0.02em;">DupZap</span>
    <span style="background:#f1f5f9; color:#64748b; font-size:0.72rem; font-weight:600;
                 padding:3px 10px; border-radius:99px; letter-spacing:0.04em; margin-top:4px;">v4.0</span>
  </div>
  <p style="color:#64748b; font-size:0.88rem; margin:0;">
    كشف المكررات عبر: File ID · File Unique ID · MD5 · pHash · الذكاء الضبابي (Fuzzy)
  </p>
</div>
""")

# ---------- تسجيل الدخول ----------
if st.session_state.step == 'login':
    tab_phone, tab_session = st.tabs(["📱 رقم الهاتف", "🔑 Session String"])

    with tab_phone:
        with st.form("login_form"):
            st.caption("للاستخدام الأول أو عند انتهاء الجلسة")
            api_id   = st.text_input("API ID*", type="password")
            api_hash = st.text_input("API Hash*", type="password")
            phone    = st.text_input("رقم الهاتف*", placeholder="+963xxxxxxxxx")
            if st.form_submit_button("إرسال رمز التحقق", use_container_width=True, type="primary"):
                if not api_id or not api_hash or not phone:
                    st.error("جميع الحقول مطلوبة")
                else:
                    try:
                        client = run_sync(_make_client(api_id, api_hash))
                        if not run_sync(_is_authorized(client)):
                            sent = run_sync(_send_code(client, phone))
                            st.session_state.phone_code_hash = sent.phone_code_hash
                            st.session_state.session_string  = get_session_string(client)
                            st.session_state.api_id   = api_id
                            st.session_state.api_hash = api_hash
                            st.session_state.phone    = phone
                            st.session_state.client   = client
                            st.session_state.step     = 'verify_code'
                            st.rerun()
                        else:
                            st.session_state.session_string = get_session_string(client)
                            st.session_state.api_id   = api_id
                            st.session_state.api_hash = api_hash
                            st.session_state.client   = client
                            st.session_state.step     = 'channel'
                            st.rerun()
                    except Exception as e:
                        st.error(f"خطأ: {e}")

    with tab_session:
        with st.form("session_form"):
            st.caption("ادخل الـ Session String المحفوظة من جلسة سابقة — بدون حاجة لرمز SMS")
            s_api_id   = st.text_input("API ID*", type="password", key="s_api_id")
            s_api_hash = st.text_input("API Hash*", type="password", key="s_api_hash")
            s_session  = st.text_area("Session String*", placeholder="1BVtsOK...", height=100, key="s_session")
            if st.form_submit_button("دخول مباشر", use_container_width=True, type="primary"):
                if not s_api_id or not s_api_hash or not s_session:
                    st.error("جميع الحقول مطلوبة")
                else:
                    try:
                        client = run_sync(_make_client(s_api_id, s_api_hash, s_session.strip()))
                        if run_sync(_is_authorized(client)):
                            st.session_state.session_string = get_session_string(client)
                            st.session_state.api_id   = s_api_id
                            st.session_state.api_hash = s_api_hash
                            st.session_state.client   = client
                            st.session_state.step     = 'channel'
                            st.rerun()
                        else:
                            st.error("الجلسة منتهية أو غير صالحة، استخدم تبويب رقم الهاتف")
                    except Exception as e:
                        st.error(f"خطأ: {e}")

# ---------- إدخال رمز OTP ----------
elif st.session_state.step == 'verify_code':
    with st.form("verify_form"):
        st.subheader("📲 تأكيد الحساب")
        st.info("أدخل الرمز الذي وصلك على تيليجرام")
        code     = st.text_input("رمز OTP*")
        password = st.text_input("كلمة مرور 2FA (إن وجدت)", type="password")
        if st.form_submit_button("تأكيد", use_container_width=True):
            try:
                client = run_sync(_make_client(
                    st.session_state.api_id,
                    st.session_state.api_hash,
                    st.session_state.session_string
                ))
                run_sync(_sign_in(client, st.session_state.phone, code, st.session_state.phone_code_hash))
                st.session_state.session_string = get_session_string(client)
                st.session_state.client = client
                st.session_state.step   = 'channel'
                st.rerun()
            except SessionPasswordNeededError:
                if not password:
                    st.error("الحساب محمي بـ 2FA، أدخل كلمة المرور")
                else:
                    try:
                        client = run_sync(_make_client(
                            st.session_state.api_id,
                            st.session_state.api_hash,
                            st.session_state.session_string
                        ))
                        run_sync(_sign_in_password(client, password))
                        st.session_state.session_string = get_session_string(client)
                        st.session_state.client = client
                        st.session_state.step   = 'channel'
                        st.rerun()
                    except Exception as e:
                        st.error(f"كلمة مرور غير صحيحة: {e}")
            except Exception as e:
                st.error(f"رمز غير صحيح: {e}")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("🔄 إعادة إرسال الرمز", use_container_width=True):
            try:
                client = run_sync(_make_client(
                    st.session_state.api_id,
                    st.session_state.api_hash,
                    st.session_state.session_string
                ))
                sent = run_sync(_send_code(client, st.session_state.phone))
                st.session_state.phone_code_hash = sent.phone_code_hash
                st.session_state.session_string  = get_session_string(client)
                st.session_state.client = client
                st.success("✅ تم إعادة إرسال الرمز")
            except Exception as e:
                st.error(f"خطأ: {e}")

# ---------- اختيار القناة والإعدادات ----------
elif st.session_state.step == 'channel':
    st.html("<div style='padding:4px 0 16px;'><span style='color:#10b981; font-size:0.9rem; font-weight:600;'>✓ تم تسجيل الدخول بنجاح</span></div>")
    with st.form("channel_form"):
        st.subheader("📡 إعدادات القناة والمسح")
        channel_input = st.text_input("رابط القناة*", placeholder="@username أو https://t.me/+xxx")

        col1, col2 = st.columns(2)
        with col1:
            media_types   = st.multiselect("أنواع الملفات", ["photo", "video", "document"], default=["photo", "video"])
            keep_strategy = st.selectbox("استراتيجية الاحتفاظ", ["oldest (الأقدم)", "newest (الأحدث)", "largest (الأكبر)"])
            keep_strategy_map = {"oldest (الأقدم)": "oldest", "newest (الأحدث)": "newest", "largest (الأكبر)": "largest"}
        with col2:
            min_size_mb = st.number_input("الحد الأدنى للحجم (MB)", 0.0, 10000.0, 0.0, step=1.0)
            auto_mode   = st.toggle("الوضع الآلي", False, help="يفحص القناة كاملاً دفعة بعد دفعة بشكل تلقائي")

        st.markdown("---")
        st.subheader("🔬 طبقات اكتشاف التكرار")
        st.caption("كلما زادت الطبقات زادت الدقة لكن المسح يصبح أبطأ")

        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("✅ **File ID + File Unique ID** (أساسي، سريع جداً)")
            compute_md5 = st.checkbox("🔐 MD5 Hash – تطابق المحتوى", value=False,
                                      help="للملفات الصغيرة (<5MB). يضمن تطابقاً تاماً لكنه أبطأ.")
        with col_b:
            compute_phash = st.checkbox("🖼️ pHash – تشابه بصري للصور", value=_HAS_IMAGEHASH,
                                        disabled=not _HAS_IMAGEHASH,
                                        help="يكتشف الصور المتشابهة حتى لو اختلفت أبعادها. يستخدم الصور المصغرة.")

        st.markdown("---")
        st.subheader("🧠 الذكاء الضبابي (Fuzzy Detection)")
        use_fuzzy = st.checkbox("تفعيل الكشف الضبابي عن الفيديوهات المتشابهة", value=True,
                                help="يكتشف الفيديوهات المتشابهة حتى لو اختلفت دقتها أو حجمها قليلاً. (موصى به)")
        fuzzy_threshold = st.slider("عتبة التشابه الضبابي", 50, 95, 75, 5,
                                    help="كلما ارتفعت العتبة زادت الدقة وقلت النتائج. 75 قيمة موصى بها.")

        st.markdown("---")
        uploaded_db = st.file_uploader("📂 رفع قاعدة بيانات سابقة (اختياري)", type=['db'])
        if uploaded_db:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
            tmp.write(uploaded_db.getbuffer())
            st.session_state.db_path = tmp.name
            st.success("✅ تم تحميل قاعدة البيانات")

        if st.form_submit_button("🚀 بدء المسح", use_container_width=True):
            if not channel_input:
                st.error("أدخل رابط القناة")
            else:
                try:
                    entity = run_sync(_get_entity(st.session_state.client, channel_input.strip()))
                    st.session_state.channel = entity
                    st.session_state.scan_params = {
                        'media_types': media_types,
                        'keep_strategy': keep_strategy_map[keep_strategy],
                        'min_size_mb': min_size_mb,
                        'compute_md5': compute_md5,
                        'compute_phash': compute_phash,
                        'use_fuzzy': use_fuzzy,
                        'fuzzy_threshold': fuzzy_threshold,
                    }
                    st.session_state.auto_mode = auto_mode
                    if st.session_state.db_path is None:
                        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
                        st.session_state.db_path = tmp.name
                    db = Database(st.session_state.db_path)
                    _, total_scanned, files_saved = db.get_resume_state(entity.id)
                    st.session_state.total_scanned = total_scanned
                    st.session_state.files_saved   = files_saved
                    db.close()
                    st.session_state.auto_scan_running = auto_mode
                    st.session_state.step = 'scanning'
                    st.rerun()
                except Exception as e:
                    st.error(f"خطأ: {e}")

# ---------- المسح ----------
elif st.session_state.step == 'scanning':
    params  = st.session_state.scan_params
    channel = st.session_state.channel

    st.subheader(f"📡 مسح: {getattr(channel, 'title', str(channel.id))}")

    col1, col2, col3 = st.columns(3)
    with col1: st.metric("📊 تم فحص", st.session_state.total_scanned)
    with col2: st.metric("💾 تم حفظ", st.session_state.files_saved)
    with col3: st.metric("⚡ السرعة", f"{st.session_state.scan_speed:.1f} msg/s")

    should_scan = st.session_state.auto_scan_running

    col_btn1, col_btn2, col_btn3, col_btn4 = st.columns(4)
    with col_btn1:
        if not st.session_state.auto_scan_running:
            if st.button("▶️ فحص الدفعة التالية", type="primary", use_container_width=True):
                should_scan = True
        else:
            if st.button("⏹️ إيقاف الوضع الآلي", type="primary", use_container_width=True):
                st.session_state.auto_scan_running = False
                st.rerun()

    with col_btn2:
        if st.button("📋 عرض المكررات", use_container_width=True):
            st.session_state.step = 'results'
            st.rerun()

    with col_btn3:
        with open(st.session_state.db_path, "rb") as f:
            st.download_button("📥 تحميل DB", f, file_name=f"scan_{channel.id}.db", use_container_width=True)

    with col_btn4:
        if st.button("🔄 فحص من البداية", use_container_width=True, help="يمسح بيانات هذه القناة ويبدأ الفحص من أول رسالة"):
            st.session_state._confirm_rescan = True
            st.rerun()

    if st.session_state.get('_confirm_rescan'):
        st.warning("⚠️ سيتم مسح كل بيانات هذه القناة والبدء من الصفر. هل أنت متأكد؟")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("✅ نعم، ابدأ من الصفر", type="primary", use_container_width=True):
                db_reset = Database(st.session_state.db_path)
                db_reset.clear_channel(channel.id)
                db_reset.close()
                st.session_state.total_scanned    = 0
                st.session_state.files_saved      = 0
                st.session_state.scan_speed       = 0.0
                st.session_state.selected_ids     = set()
                st.session_state.auto_scan_running = False
                st.session_state._confirm_rescan  = False
                st.success("✅ تم مسح البيانات — الفحص سيبدأ من أول رسالة")
                st.rerun()
        with c2:
            if st.button("❌ إلغاء", use_container_width=True):
                st.session_state._confirm_rescan = False
                st.rerun()

    if should_scan:
        db = Database(st.session_state.db_path)
        last_id, _, _ = db.get_resume_state(channel.id)
        offset_id = 0 if last_id == 0 else last_id + 1
        client    = st.session_state.client
        progress  = st.progress(0, text="جاري الفحص...")
        start_time = time.time()
        try:
            messages = run_sync(_get_messages(client, channel, offset_id, BATCH_SCAN_SIZE))
            elapsed = time.time() - start_time
            if messages:
                st.session_state.scan_speed = len(messages) / elapsed if elapsed > 0 else 0

            if not messages:
                st.success("✅ تم الانتهاء من فحص كل الرسائل!")
                st.session_state.auto_scan_running = False
            else:
                scanned = saved = 0
                cur_last = offset_id
                for i, msg in enumerate(messages):
                    if not msg: continue
                    scanned  += 1
                    cur_last  = msg.id
                    progress.progress((i + 1) / max(len(messages), 1), text=f"فحص رسالة {msg.id}...")
                    if not msg.media: continue
                    info = run_sync(extract_file_info_async(
                        client, msg, params['compute_md5'], params['compute_phash']
                    ))
                    if not info: continue
                    if info['type'] not in params['media_types']: continue
                    if info['size'] < params['min_size_mb'] * 1024 * 1024: continue
                    saved += 1
                    db.buffer_insert((
                        channel.id, info['id'], info['file_id'], info['file_unique_id'], info['size'],
                        info['duration'], info['width'], info['height'],
                        info['md5'], info['phash'], info['date'],
                        info['type'], info['mime'], info['views'], info['name']
                    ))
                st.session_state.total_scanned += scanned
                st.session_state.files_saved   += saved
                db.save_progress(channel.id, cur_last, st.session_state.total_scanned, st.session_state.files_saved)
                progress.progress(1.0, text="✅ اكتملت الدفعة")
                st.info(f"دفعة: فحص {scanned} رسالة، حُفظ {saved} ملف")

                if st.session_state.auto_scan_running and scanned == BATCH_SCAN_SIZE:
                    time.sleep(0.5)
                    st.rerun()
                elif st.session_state.auto_scan_running:
                    st.success("✅ الوضع الآلي: تم الانتهاء من كل الرسائل!")
                    st.session_state.auto_scan_running = False
        except FloodWaitError as e:
            st.warning(f"⏳ انتظار {e.seconds} ثانية بسبب FloodWait")
            st.session_state.auto_scan_running = False
        except Exception as e:
            st.error(f"خطأ: {e}")
            st.session_state.auto_scan_running = False
        finally:
            db.close()

# ---------- النتائج ----------
elif st.session_state.step == 'results':
    params  = st.session_state.scan_params
    channel = st.session_state.channel
    db      = Database(st.session_state.db_path)

    duplicates = db.stream_duplicates(
        channel.id, params['keep_strategy'],
        int(params['min_size_mb'] * 1024 * 1024),
        use_md5=params.get('compute_md5', False),
        use_phash=params.get('compute_phash', False),
        use_fuzzy=params.get('use_fuzzy', True),
        fuzzy_threshold=params.get('fuzzy_threshold', 75)
    )

    st.html(f"<h3 style='margin:0 0 16px; color:#0f172a;'>📋 {getattr(channel, 'title', str(channel.id))}</h3>")

    if st.session_state.last_deleted_count > 0:
        st.success(f"✅ تم حذف {st.session_state.last_deleted_count} رسالة بنجاح")
        if st.session_state.last_deleted_failed > 0:
            st.warning(f"⚠️ فشل حذف {st.session_state.last_deleted_failed} رسالة")
        st.session_state.last_deleted_count = 0
        st.session_state.last_deleted_failed = 0

    if not duplicates:
        st.success("🎉 لا توجد مكررات!")
    else:
        st.html(f"""
        <div style="display:inline-flex; align-items:center; gap:8px; background:#fff7ed;
                    border:1px solid #fed7aa; border-radius:10px; padding:10px 16px; margin-bottom:16px;">
          <span style="font-size:1.1rem;">⚠️</span>
          <span style="color:#c2410c; font-weight:600; font-size:0.95rem;">
            {len(duplicates)} رسالة مكررة
          </span>
        </div>
        """)

        page = st.session_state.page
        total_pages = max(1, (len(duplicates) + PAGE_SIZE - 1) // PAGE_SIZE)
        page_dups = duplicates[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

        df = pd.DataFrame([
            {
                "معرف": d['id'],
                "النوع": d['type'],
                "الحجم": fmt_size(d['size']),
                "التاريخ": d['date'][:10],
                "اسم الملف": (d['name'][:25] + "…" if d['name'] and len(d['name']) > 25 else d['name']) or "—",
                "سبب التكرار": d.get('match_type', '—'),
                "تحديد": False
            }
            for d in page_dups
        ])
        edited = st.data_editor(
            df,
            column_config={"تحديد": st.column_config.CheckboxColumn("🗑️ حذف")},
            hide_index=True,
            use_container_width=True,
            height=400
        )
        for sid in edited[edited["تحديد"] == True]["معرف"].tolist():
            st.session_state.selected_ids.add(sid)

        if total_pages > 1:
            pcol1, pcol2, pcol3 = st.columns(3)
            with pcol1:
                if page > 0 and st.button("⬅️ السابقة", use_container_width=True):
                    st.session_state.page -= 1
                    st.rerun()
            with pcol2:
                st.markdown(f"<p style='text-align:center;'>صفحة {page + 1} من {total_pages}</p>", unsafe_allow_html=True)
            with pcol3:
                if page < total_pages - 1 and st.button("➡️ التالية", use_container_width=True):
                    st.session_state.page += 1
                    st.rerun()

        st.markdown("---")
        col_sel1, col_sel2, col_sel3 = st.columns(3)
        with col_sel1:
            if st.button("☑️ تحديد الكل في الصفحة", use_container_width=True):
                for d in page_dups:
                    st.session_state.selected_ids.add(d['id'])
                st.rerun()
        with col_sel2:
            if st.button("✖️ إلغاء تحديد الكل", use_container_width=True):
                st.session_state.selected_ids = set()
                st.rerun()
        with col_sel3:
            if st.button("📥 تحميل تقرير CSV", use_container_width=True):
                df_report = pd.DataFrame([
                    {"معرف": d['id'], "النوع": d['type'], "الحجم": fmt_size(d['size']), "التاريخ": d['date']}
                    for d in duplicates
                ])
                st.download_button("اضغط للتحميل", df_report.to_csv(index=False).encode('utf-8-sig'),
                                   "duplicates_report.csv", "text/csv", use_container_width=True)

        selected_count = len(st.session_state.selected_ids)
        if selected_count > 0:
            st.html(f"""
            <div style="background:#f0fdf4; border:1px solid #bbf7d0; border-radius:9px;
                        padding:10px 16px; margin:12px 0; color:#166534; font-weight:600; font-size:0.9rem;">
              📌 محدد للحذف: {selected_count} رسالة
            </div>
            """)

        if st.button(f"🗑️ حذف {selected_count} رسالة محددة", type="primary", disabled=selected_count == 0, use_container_width=True):
            if selected_count == 0:
                st.warning("لم تحدد أي رسائل")
            else:
                ids = list(st.session_state.selected_ids)
                prog = st.progress(0, text="جاري الحذف...")
                deleted = 0
                failed = 0
                for i in range(0, len(ids), BATCH_DELETE_SIZE):
                    batch = ids[i:i + BATCH_DELETE_SIZE]
                    try:
                        run_sync(_delete_messages(st.session_state.client, channel, batch))
                        db.delete_msg_records(channel.id, batch)
                        deleted += len(batch)
                    except FloodWaitError as e:
                        st.warning(f"⏳ انتظار {e.seconds}s")
                        time.sleep(e.seconds)
                        try:
                            run_sync(_delete_messages(st.session_state.client, channel, batch))
                            db.delete_msg_records(channel.id, batch)
                            deleted += len(batch)
                        except Exception:
                            failed += len(batch)
                    except Exception:
                        failed += len(batch)
                    prog.progress((i + len(batch)) / len(ids))

                st.session_state.last_deleted_count = deleted
                st.session_state.last_deleted_failed = failed
                st.session_state.selected_ids = set()
                db.close()
                st.rerun()

    db.close()

st.markdown("---")
st.markdown("<div class='footer-bar'>صُنع بعناية بواسطة <strong>F.ALSALEH</strong> · DupZap v4.0</div>", unsafe_allow_html=True)
