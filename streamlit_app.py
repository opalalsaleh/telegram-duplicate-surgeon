import streamlit as st
import asyncio
import gc
import hashlib
import io
import sqlite3
import tempfile
import time
import threading
from typing import Dict, List, Optional

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

st.html("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Arabic:wght@300;400;500;600;700&display=swap');
    html, body, [class*="css"], .stApp {
        font-family: 'IBM Plex Sans Arabic', 'Segoe UI', system-ui, sans-serif;
    }
    .stApp { background: #f8fafc; }
    .main .block-container { padding-top: 2rem; max-width: 960px; }

    [data-testid="stSidebar"] { background: #0f172a !important; }
    [data-testid="stSidebar"] > div:first-child { padding: 0; }
    [data-testid="stSidebar"] * { color: #cbd5e1 !important; }
    [data-testid="stSidebar"] hr { border-color: #1e293b !important; margin: 0.75rem 0 !important; }
    [data-testid="stSidebar"] .stButton > button {
        background: transparent !important; border: 1px solid #1e293b !important;
        color: #94a3b8 !important; border-radius: 8px; font-weight: 500; font-size: 0.85rem;
    }
    [data-testid="stSidebar"] .stButton > button:hover {
        background: #1e293b !important; border-color: #334155 !important; color: #e2e8f0 !important;
    }
    .sidebar-logo { text-align:center; padding:28px 16px 20px; border-bottom:1px solid #1e293b; margin-bottom:8px; }
    .sidebar-logo .logo-icon { font-size:2.6rem; line-height:1; display:block; margin-bottom:10px; }
    .sidebar-logo .logo-name {
        font-size:1.45rem; font-weight:700;
        background:linear-gradient(90deg,#38bdf8 0%,#818cf8 100%);
        -webkit-background-clip:text; -webkit-text-fill-color:transparent; display:block; margin-bottom:3px;
    }
    .sidebar-logo .logo-ver { font-size:0.72rem; color:#475569 !important; letter-spacing:0.04em; }

    h1 { font-size:2rem !important; font-weight:700 !important; color:#0f172a !important; letter-spacing:-0.02em; margin-bottom:0 !important; }
    h2, h3 { font-weight:600 !important; color:#1e293b !important; }

    .stButton > button {
        border-radius:9px; font-weight:600; font-size:0.88rem; min-height:42px;
        transition:all 0.18s cubic-bezier(.4,0,.2,1); border:1.5px solid #e2e8f0 !important;
        background:#ffffff !important; color:#374151 !important; box-shadow:0 1px 3px rgba(0,0,0,0.06);
    }
    .stButton > button:hover {
        border-color:#cbd5e1 !important; box-shadow:0 4px 12px rgba(0,0,0,0.09) !important;
        transform:translateY(-1px); color:#0f172a !important;
    }
    .stButton > button[kind="primary"] {
        background:linear-gradient(135deg,#0ea5e9 0%,#6366f1 100%) !important;
        color:#ffffff !important; border:none !important; box-shadow:0 3px 12px rgba(99,102,241,0.30) !important;
    }
    .stButton > button[kind="primary"]:hover {
        box-shadow:0 6px 20px rgba(99,102,241,0.40) !important; transform:translateY(-2px); color:#ffffff !important;
    }

    [data-testid="metric-container"] {
        background:#ffffff; border-radius:14px; padding:20px 16px;
        border:1px solid #e2e8f0; box-shadow:0 1px 4px rgba(0,0,0,0.05); transition:box-shadow 0.2s;
    }
    [data-testid="metric-container"]:hover { box-shadow:0 4px 16px rgba(0,0,0,0.08); }
    [data-testid="stMetricLabel"] { color:#64748b !important; font-size:0.82rem !important; }
    [data-testid="stMetricValue"] { color:#0f172a !important; font-weight:700 !important; }

    .stTextInput > div > div > input, .stNumberInput > div > div > input, .stSelectbox > div > div {
        border-radius:9px !important; border:1.5px solid #e2e8f0 !important;
        background:#ffffff !important; font-size:0.9rem;
    }
    hr { border-color:#f1f5f9 !important; margin:1.5rem 0 !important; }
    [data-testid="stForm"] {
        background:#ffffff; border-radius:16px; padding:28px !important;
        border:1px solid #e2e8f0; box-shadow:0 2px 8px rgba(0,0,0,0.05);
    }
    .stDataEditor { border-radius:12px !important; overflow:hidden; border:1px solid #e2e8f0 !important; }
    .stProgress > div > div > div > div {
        background:linear-gradient(90deg,#0ea5e9,#6366f1) !important; border-radius:99px;
    }
    [data-testid="stAlert"] { border-radius:10px !important; border-width:1px !important; }
    .footer-bar {
        text-align:center; padding:20px; color:#94a3b8; font-size:0.8rem;
        margin-top:40px; border-top:1px solid #f1f5f9;
    }
    .footer-bar strong { color:#64748b; font-weight:600; }
    ::-webkit-scrollbar { width:6px; height:6px; }
    ::-webkit-scrollbar-track { background:#f8fafc; }
    ::-webkit-scrollbar-thumb { background:#cbd5e1; border-radius:99px; }
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
        for s in media.photo.sizes:
            if isinstance(s, PhotoSize) and getattr(s, 'type', '') == 'm': return s
        sizes = [s for s in media.photo.sizes if hasattr(s, 'size')]
        return min(sizes, key=lambda s: s.size) if sizes else None
    if isinstance(media, MessageMediaDocument):
        doc = media.document
        if doc and doc.thumbs: return min(doc.thumbs, key=lambda t: getattr(t, 'size', 0))
    return None

# ================== Fuzzy Video Matching — Surgical Duplicate ==================

def is_surgical_duplicate(a: dict, b: dict, threshold: float = 0.85) -> bool:
    """
    مقارنة فيديوين بـ scoring مرجّح بدل hard thresholds.

    المدة  — وزن 60% (الأدق لأنها ثابتة حتى بعد إعادة الترميز)
    الحجم  — وزن 40% (يتغير بالضغط لكن يبقى قريباً)
    threshold افتراضي 0.85 → دقة عالية وfalse positives أقل
    """
    d1 = float(a.get("duration", 0))
    d2 = float(b.get("duration", 0))
    s1 = int(a.get("size", 0))
    s2 = int(b.get("size", 0))

    # رفض فيديوهات بدون metadata — لا مقارنة ممكنة
    if d1 == 0 or d2 == 0 or s1 == 0 or s2 == 0:
        return False

    # فلتر سريع: فرق المدة > 3 ثوان = مستحيل أن يكونا نفس الفيديو
    if abs(d1 - d2) > 3:
        return False

    # score المدة (60%)
    dur_diff = abs(d1 - d2)
    if dur_diff == 0:     dur_score = 1.0
    elif dur_diff <= 1:   dur_score = 0.7
    else:                 dur_score = 0.3   # فرق 2-3 ثوان — مريب

    # score الحجم (40%)
    size_ratio = abs(s1 - s2) / max(s1, s2)
    if size_ratio < 0.02:   size_score = 1.0   # تطابق شبه تام
    elif size_ratio < 0.08: size_score = 0.7   # ضغط طفيف
    elif size_ratio < 0.20: size_score = 0.4   # ضغط واضح
    else:                   size_score = 0.0   # مختلفان جداً → رفض

    score = dur_score * 0.6 + size_score * 0.4
    return score >= threshold


class _UnionFind:
    """
    Union-Find لمنع التعديم الزائف (Transitive Matching).
    بدونه: A≈B و B≈C → يحذف B وC حتى لو A وC مختلفان.
    معه: كل المتشابهين يُجمعون في مجموعة واحدة صحيحة.
    """
    def __init__(self, n):
        self.parent = list(range(n))
        self.rank   = [0] * n

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, x, y):
        rx, ry = self.find(x), self.find(y)
        if rx == ry: return
        if self.rank[rx] < self.rank[ry]: rx, ry = ry, rx
        self.parent[ry] = rx
        if self.rank[rx] == self.rank[ry]: self.rank[rx] += 1

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
        if hasattr(result, 'chat'): return result.chat
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

# ================== استخراج معلومات الملف ==================
async def extract_file_info_async(client, msg, compute_md5: bool, compute_phash: bool) -> Optional[Dict]:
    media = msg.media
    if not media: return None

    info = {
        "id": msg.id, "file_id": None,
        "size": 0, "duration": 0,
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
            if isinstance(attr, DocumentAttributeVideo): info["duration"] = attr.duration or 0
            if hasattr(attr, 'file_name'): info["name"] = attr.file_name

    elif isinstance(media, MessageMediaPhoto):
        photo = media.photo
        info["file_id"] = f"{photo.id}:{photo.dc_id}"
        info["type"]    = "photo"
        info["mime"]    = "image/jpeg"
        sizes = [s for s in getattr(photo, "sizes", []) if hasattr(s, "size") and s.size > 0]
        info["size"] = max(sizes, key=lambda s: s.size).size if sizes else 0
    else:
        return None

    if compute_md5 or compute_phash:
        thumb = get_thumb(media) if compute_phash else None
        data = None
        try:
            if thumb:
                data = await client.download_media(thumb, file=bytes)
            else:
                data = await client.download_media(msg, file=bytes,
                                                   size=PHASH_SIZE_LIMIT if compute_phash else None)
            if compute_md5 and info["size"] <= MD5_SIZE_LIMIT and data:
                info["md5"] = hashlib.md5(data).hexdigest()
            if compute_phash and _HAS_IMAGEHASH and info["type"] in ("photo", "image") and data:
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
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS seen_files (
                channel_id INTEGER, msg_id INTEGER, file_id TEXT,
                file_size INTEGER, duration INTEGER, md5_hash TEXT, phash TEXT,
                msg_date TEXT, file_type TEXT, mime_type TEXT, views INTEGER, file_name TEXT,
                PRIMARY KEY (channel_id, msg_id)
            ) WITHOUT ROWID
        """)
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
        self.conn.execute("INSERT OR REPLACE INTO seen_files VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", record)
        self.conn.commit()

    def delete_msg_records(self, channel_id, msg_ids):
        self.conn.executemany(
            "DELETE FROM seen_files WHERE channel_id=? AND msg_id=?",
            [(channel_id, mid) for mid in msg_ids]
        )
        self.conn.commit()

    def get_all_videos(self, channel_id, min_size=0) -> List[Dict]:
        """يجلب كل الفيديوهات للمقارنة الـ Fuzzy"""
        rows = self.conn.execute(
            "SELECT msg_id, file_id, file_size, duration, msg_date, file_name "
            "FROM seen_files WHERE channel_id=? AND file_type='video' AND file_size>=?",
            (channel_id, min_size)
        ).fetchall()
        return [
            {"id": r[0], "file_id": r[1], "size": r[2], "duration": r[3],
             "date": r[4], "name": r[5], "type": "video"}
            for r in rows
        ]

    def stream_duplicates(self, channel_id, keep_strategy, min_size=0,
                          use_md5=False, use_phash=False, use_fuzzy=False,
                          fuzzy_threshold=0.85):
        order = {"oldest": "msg_date ASC", "newest": "msg_date DESC", "largest": "file_size DESC"}[keep_strategy]
        duplicates = []
        seen_msg_ids: set = set()  # لتجنب إضافة نفس الرسالة مرتين

        def add_group(group, match_type):
            keeper = group[0]
            for dup in group[1:]:
                mid = dup[0]
                if mid not in seen_msg_ids:
                    seen_msg_ids.add(mid)
                    duplicates.append({
                        "id": mid, "size": dup[1], "date": dup[2], "file_id": dup[3],
                        "duration": dup[4], "phash": dup[5], "type": dup[6],
                        "mime": dup[7], "name": dup[8], "keeper_id": keeper[0],
                        "match_type": match_type
                    })

        # ── Layer 1: file_id متطابق (نفس الرسالة مُعاد توجيهها) ──
        cursor = self.conn.execute(
            "SELECT file_id FROM seen_files WHERE channel_id=? AND file_size>=? "
            "GROUP BY file_id HAVING COUNT(*)>1",
            (channel_id, min_size)
        )
        for row in cursor:
            group = self.conn.execute(
                f"SELECT msg_id, file_size, msg_date, file_id, duration, phash, file_type, mime_type, file_name "
                f"FROM seen_files WHERE channel_id=? AND file_id=? ORDER BY {order}",
                (channel_id, row[0])
            ).fetchall()
            add_group(group, "file_id")

        # ── Layer 2: MD5 ──
        if use_md5:
            cursor = self.conn.execute(
                "SELECT md5_hash FROM seen_files WHERE channel_id=? AND md5_hash IS NOT NULL AND file_size>=? "
                "GROUP BY md5_hash HAVING COUNT(DISTINCT file_id)>1",
                (channel_id, min_size)
            )
            for row in cursor:
                group = self.conn.execute(
                    f"SELECT msg_id, file_size, msg_date, file_id, duration, phash, file_type, mime_type, file_name "
                    f"FROM seen_files WHERE channel_id=? AND md5_hash=? ORDER BY {order}",
                    (channel_id, row[0])
                ).fetchall()
                add_group(group, "md5")

        # ── Layer 3: pHash بـ Hamming distance (يكتشف الصور المرفوعة من جديد) ──
        if use_phash and _HAS_IMAGEHASH:
            # نجيب كل الصور اللي عندها phash من DB — صفر تحميل إضافي
            rows = self.conn.execute(
                "SELECT msg_id, file_size, msg_date, file_id, duration, phash, file_type, mime_type, file_name "
                "FROM seen_files WHERE channel_id=? AND phash IS NOT NULL "
                "AND file_size>=? AND file_type IN ('photo','image')",
                (channel_id, min_size)
            ).fetchall()

            if rows:
                # Union-Find لمنع التعديم الزائف
                n   = len(rows)
                uf2 = _UnionFind(n)

                hashes = []
                for r in rows:
                    try:    hashes.append(imagehash.hex_to_hash(r[5]))
                    except: hashes.append(None)

                # O(n²) لكن على البيانات الموجودة فقط — بدون شبكة
                for i in range(n):
                    if hashes[i] is None or rows[i][0] in seen_msg_ids: continue
                    for j in range(i + 1, n):
                        if hashes[j] is None or rows[j][0] in seen_msg_ids: continue
                        if rows[i][3] == rows[j][3]: continue  # نفس file_id → Layer 1 يغطيه
                        # Hamming distance ≤ 6 من أصل 64 bit — صارم (تطابق شبه تام فقط)
                        if (hashes[i] - hashes[j]) <= 6:
                            uf2.union(i, j)

                # اجمع المجموعات
                groups2: Dict[int, List[int]] = {}
                for idx in range(n):
                    root = uf2.find(idx)
                    groups2.setdefault(root, []).append(idx)

                for root, members in groups2.items():
                    if len(members) < 2: continue
                    # رتّب حسب الاستراتيجية
                    if keep_strategy == "largest":
                        members.sort(key=lambda i: rows[i][1], reverse=True)
                    elif keep_strategy == "newest":
                        members.sort(key=lambda i: rows[i][2], reverse=True)
                    else:  # oldest
                        members.sort(key=lambda i: rows[i][2])
                    keeper = rows[members[0]]
                    for idx in members[1:]:
                        r = rows[idx]
                        if r[0] in seen_msg_ids: continue
                        seen_msg_ids.add(r[0])
                        duplicates.append({
                            "id": r[0], "size": r[1], "date": r[2], "file_id": r[3],
                            "duration": r[4], "phash": r[5], "type": r[6],
                            "mime": r[7], "name": r[8], "keeper_id": keeper[0],
                            "match_type": "phash"
                        })

        # ── Layer 4: Fuzzy Video Matching بـ Union-Find ──
        if use_fuzzy:
            videos = self.get_all_videos(channel_id, min_size)

            # رتّب حسب الاستراتيجية لتحديد الـ keeper (الأول في الترتيب)
            sort_key = {"oldest": lambda v: v["date"],
                        "newest": lambda v: v["date"],
                        "largest": lambda v: v["size"]}[keep_strategy]
            reverse = keep_strategy == "newest"
            videos.sort(key=sort_key, reverse=reverse)

            n = len(videos)
            uf = _UnionFind(n)

            # O(n²) للمقارنة لكن الـ Union-Find يمنع التعديم الزائف
            for i in range(n):
                if videos[i]["id"] in seen_msg_ids: continue
                for j in range(i + 1, n):
                    if videos[j]["id"] in seen_msg_ids: continue
                    if videos[i]["file_id"] == videos[j]["file_id"]: continue  # Layer 1 يغطيه
                    if is_surgical_duplicate(videos[i], videos[j], fuzzy_threshold):
                        uf.union(i, j)

            # اجمع المجموعات
            groups: Dict[int, List[int]] = {}
            for idx in range(n):
                root = uf.find(idx)
                groups.setdefault(root, []).append(idx)

            for root, members in groups.items():
                if len(members) < 2: continue
                # الأول في القائمة هو الـ keeper (مرتبون مسبقاً)
                keeper_idx  = members[0]
                keeper_id   = videos[keeper_idx]["id"]
                for idx in members[1:]:
                    dup = videos[idx]
                    if dup["id"] in seen_msg_ids: continue
                    seen_msg_ids.add(dup["id"])
                    row = self.conn.execute(
                        "SELECT msg_id, file_size, msg_date, file_id, duration, phash, "
                        "file_type, mime_type, file_name "
                        "FROM seen_files WHERE channel_id=? AND msg_id=?",
                        (channel_id, dup["id"])
                    ).fetchone()
                    if row:
                        duplicates.append({
                            "id": row[0], "size": row[1], "date": row[2], "file_id": row[3],
                            "duration": row[4], "phash": row[5], "type": row[6],
                            "mime": row[7], "name": row[8], "keeper_id": keeper_id,
                            "match_type": "fuzzy"
                        })

        return duplicates

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

# ================== الشريط الجانبي ==================
with st.sidebar:
    st.html("""
    <div class='sidebar-logo'>
        <div class='logo-icon'>✂️</div>
        <div class='logo-name'>DupZap</div>
        <div class='logo-ver'>v5.0 · Telegram</div>
    </div>
    """)
    st.divider()

    if st.session_state.client and st.session_state.step not in ['login', 'verify_code']:
        try:
            if not st.session_state.me:
                st.session_state.me = run_sync(st.session_state.client.get_me())
            me = st.session_state.me
            st.markdown(f"**👤 {me.first_name}**")
            if me.username: st.markdown(f"@{me.username}")
        except: pass

        if st.session_state.session_string:
            with st.expander("🔑 Session String"):
                st.caption("احفظها للدخول بدون SMS في المرة القادمة")
                st.code(st.session_state.session_string, language=None)

    if st.session_state.channel:
        st.markdown(f"**📢 {getattr(st.session_state.channel, 'title', 'قناة')}**")

    st.divider()

    current_step = st.session_state.step
    if current_step == 'verify_code':
        if st.button("⬅️ تسجيل الدخول", use_container_width=True):
            st.session_state.step = 'login'; st.rerun()
    elif current_step == 'scanning':
        if st.button("⬅️ تغيير القناة", use_container_width=True):
            st.session_state.step = 'channel'
            st.session_state.auto_scan_running = False; st.rerun()
    elif current_step == 'results':
        c1, c2 = st.columns(2)
        with c1:
            if st.button("⬅️ مسح", use_container_width=True):
                st.session_state.step = 'scanning'
                st.session_state.selected_ids = set(); st.rerun()
        with c2:
            if st.button("📋 قناة", use_container_width=True):
                st.session_state.step = 'channel'
                st.session_state.selected_ids = set(); st.rerun()

    st.divider()
    if current_step not in ['login', 'verify_code']:
        if st.button("🚪 تسجيل الخروج", use_container_width=True):
            for k in ['client', 'me', 'phone', 'api_id', 'api_hash', 'session_string']:
                st.session_state.pop(k, None)
            st.session_state.step = 'login'; st.rerun()

    st.markdown("<p style='text-align:center;font-size:0.8rem;color:#64748b;margin-top:8px;'>© F.ALSALEH</p>",
                unsafe_allow_html=True)

# ================== المحتوى الرئيسي ==================
st.html("""
<div style="padding:8px 0 24px;">
  <div style="display:flex;align-items:center;gap:12px;margin-bottom:4px;">
    <span style="font-size:2rem;">✂️</span>
    <span style="font-size:1.9rem;font-weight:700;color:#0f172a;letter-spacing:-0.02em;">DupZap</span>
    <span style="background:#f1f5f9;color:#64748b;font-size:0.72rem;font-weight:600;
                 padding:3px 10px;border-radius:99px;letter-spacing:0.04em;margin-top:4px;">v5.0</span>
  </div>
  <p style="color:#64748b;font-size:0.88rem;margin:0;">
    كشف المكررات عبر: File ID · MD5 · pHash · Fuzzy Video
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
            st.caption("ادخل الـ Session String المحفوظة — بدون حاجة لرمز SMS")
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
                            st.error("الجلسة منتهية، استخدم تبويب رقم الهاتف")
                    except Exception as e:
                        st.error(f"خطأ: {e}")

# ---------- OTP ----------
elif st.session_state.step == 'verify_code':
    with st.form("verify_form"):
        st.subheader("📲 تأكيد الحساب")
        st.info("أدخل الرمز الذي وصلك على تيليجرام")
        code     = st.text_input("رمز OTP*")
        password = st.text_input("كلمة مرور 2FA (إن وجدت)", type="password")
        if st.form_submit_button("تأكيد", use_container_width=True):
            try:
                client = run_sync(_make_client(st.session_state.api_id, st.session_state.api_hash,
                                               st.session_state.session_string))
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
                        client = run_sync(_make_client(st.session_state.api_id, st.session_state.api_hash,
                                                       st.session_state.session_string))
                        run_sync(_sign_in_password(client, password))
                        st.session_state.session_string = get_session_string(client)
                        st.session_state.client = client
                        st.session_state.step   = 'channel'
                        st.rerun()
                    except Exception as e:
                        st.error(f"كلمة مرور غير صحيحة: {e}")
            except Exception as e:
                st.error(f"رمز غير صحيح: {e}")

    st.markdown("---")
    if st.button("🔄 إعادة إرسال الرمز"):
        try:
            client = run_sync(_make_client(st.session_state.api_id, st.session_state.api_hash,
                                           st.session_state.session_string))
            sent = run_sync(_send_code(client, st.session_state.phone))
            st.session_state.phone_code_hash = sent.phone_code_hash
            st.session_state.session_string  = get_session_string(client)
            st.session_state.client = client
            st.success("✅ تم إعادة إرسال الرمز")
        except Exception as e:
            st.error(f"خطأ: {e}")

# ---------- إعدادات القناة ----------
elif st.session_state.step == 'channel':
    st.success("✅ تم تسجيل الدخول")
    with st.form("channel_form"):
        channel_input = st.text_input("رابط القناة / المجموعة*", placeholder="@username أو https://t.me/+xxx")

        col1, col2 = st.columns(2)
        with col1:
            media_types   = st.multiselect("أنواع الملفات", ["photo", "video", "document"],
                                           default=["photo", "video"])
            keep_strategy = st.selectbox("استراتيجية الاحتفاظ",
                                         ["oldest (الأقدم)", "newest (الأحدث)", "largest (الأكبر)"])
            keep_map = {"oldest (الأقدم)": "oldest", "newest (الأحدث)": "newest", "largest (الأكبر)": "largest"}
        with col2:
            min_size_mb = st.number_input("الحد الأدنى للحجم (MB)", 0.0, 10000.0, 0.0, step=1.0)
            auto_mode   = st.toggle("الوضع الآلي", False, help="يفحص القناة كاملاً دفعة بعد دفعة بشكل تلقائي")

        st.markdown("---")
        st.subheader("🔬 طبقات اكتشاف التكرار")
        st.caption("Layer 1 (File ID) دائماً مفعّل — فعّل طبقات إضافية حسب الحاجة")

        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("✅ **File ID** — تطابق مباشر (Forward)")
            compute_md5 = st.checkbox("🔐 MD5 — تطابق المحتوى البايتي",
                                      help="للملفات الصغيرة (<5MB). يضمن تطابقاً تاماً لكنه أبطأ.")
        with col_b:
            compute_phash = st.checkbox("🖼️ pHash — تشابه بصري للصور",
                                        value=_HAS_IMAGEHASH, disabled=not _HAS_IMAGEHASH,
                                        help="يكتشف الصور المتشابهة حتى لو اختلفت أبعادها.")

        st.markdown("---")
        st.subheader("🎬 Fuzzy Video Matching")
        st.caption("يكتشف الفيديوهات المكررة حتى لو أُعيد رفعها — بناءً على المدة والحجم")

        use_fuzzy = st.toggle("تفعيل Fuzzy Video Matching", value=False,
                              help="يكتشف الفيديوهات المرفوعة من جديد — scoring مرجّح: المدة 60% + الحجم 40%")

        fuzzy_threshold = 0.85
        if use_fuzzy:
            st.markdown("""
            <div style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:9px;
                        padding:10px 14px;margin-bottom:8px;font-size:0.84rem;color:#0369a1;">
            ⚙️ <b>كيف يعمل؟</b> يعطي كل زوج فيديوهات score من 0→1 &nbsp;|&nbsp;
            المدة (60%) + الحجم (40%)<br>
            فرق المدة &gt; 3 ثوان = رفض فوري · score ≥ الحد = مكرر
            </div>
            """, unsafe_allow_html=True)

            fuzzy_threshold = st.slider(
                "الحد الأدنى للـ Score (دقة الكشف)",
                min_value=0.70, max_value=0.99, value=0.85, step=0.01, format="%.2f",
                help="0.85 موصى به · ارفعه لتقليل false positives · اخفضه لكشف أكثر"
            )
            col_t1, col_t2, col_t3 = st.columns(3)
            with col_t1:
                bg = "#dcfce7" if fuzzy_threshold >= 0.85 else "#fef9c3"
                st.html(f"""<div style="background:{bg};border-radius:8px;padding:8px;
                            text-align:center;font-size:0.79rem;">
                <b>المدة → score</b><br>تطابق تام → 0.60<br>فرق 1ث → 0.42<br>فرق 2-3ث → 0.18</div>""")
            with col_t2:
                st.html("""<div style="background:#f1f5f9;border-radius:8px;padding:8px;
                           text-align:center;font-size:0.79rem;">
                <b>الحجم → score</b><br>&lt;2% فرق → 0.40<br>&lt;8% فرق → 0.28<br>&lt;20% فرق → 0.16</div>""")
            with col_t3:
                verdict = ("🟢 صارم جداً" if fuzzy_threshold >= 0.90
                           else "🟡 متوازن" if fuzzy_threshold >= 0.80
                           else "🔴 متساهل")
                st.html(f"""<div style="background:#f8fafc;border-radius:8px;padding:8px;
                            text-align:center;font-size:0.79rem;">
                <b>الحد الحالي</b><br>{fuzzy_threshold:.2f}<br>{verdict}</div>""")
            st.caption("مثال: فيديو مدته 120ث وحجمه يختلف 25% → score=0.60 → لا يُعتبر مكرراً عند 0.85")

        st.markdown("---")
        uploaded_db = st.file_uploader("📂 رفع قاعدة بيانات سابقة (اختياري)", type=['db'])
        if uploaded_db:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
            tmp.write(uploaded_db.getbuffer())
            st.session_state.db_path = tmp.name
            st.success("✅ تم تحميل قاعدة البيانات")

        if st.form_submit_button("🚀 بدء المسح", use_container_width=True, type="primary"):
            if not channel_input:
                st.error("أدخل رابط القناة")
            else:
                try:
                    entity = run_sync(_get_entity(st.session_state.client, channel_input.strip()))
                    st.session_state.channel     = entity
                    st.session_state.scan_params = {
                        'media_types': media_types,
                        'keep_strategy': keep_map[keep_strategy],
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
                st.session_state.auto_scan_running = False; st.rerun()
    with col_btn2:
        if st.button("📋 عرض المكررات", use_container_width=True):
            st.session_state.step = 'results'; st.rerun()
    with col_btn3:
        with open(st.session_state.db_path, "rb") as f:
            st.download_button("📥 تحميل DB", f, file_name=f"scan_{channel.id}.db", use_container_width=True)
    with col_btn4:
        if st.button("🔄 فحص من البداية", use_container_width=True):
            st.session_state._confirm_rescan = True; st.rerun()

    if st.session_state.get('_confirm_rescan'):
        st.warning("⚠️ سيتم مسح كل بيانات هذه القناة والبدء من الصفر. هل أنت متأكد؟")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("✅ نعم، ابدأ من الصفر", type="primary", use_container_width=True):
                db_reset = Database(st.session_state.db_path)
                db_reset.clear_channel(channel.id)
                db_reset.close()
                st.session_state.total_scanned     = 0
                st.session_state.files_saved        = 0
                st.session_state.scan_speed         = 0.0
                st.session_state.selected_ids       = set()
                st.session_state.auto_scan_running  = False
                st.session_state._confirm_rescan    = False
                st.rerun()
        with c2:
            if st.button("❌ إلغاء", use_container_width=True):
                st.session_state._confirm_rescan = False; st.rerun()

    if should_scan:
        db = Database(st.session_state.db_path)
        last_id, _, _ = db.get_resume_state(channel.id)
        offset_id  = 0 if last_id == 0 else last_id + 1
        client     = st.session_state.client
        progress   = st.progress(0, text="جاري الفحص...")
        start_time = time.time()
        try:
            messages = run_sync(_get_messages(client, channel, offset_id, BATCH_SCAN_SIZE))
            elapsed  = time.time() - start_time
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
                        channel.id, info['id'], info['file_id'], info['size'],
                        info['duration'], info['md5'], info['phash'], info['date'],
                        info['type'], info['mime'], info['views'], info['name']
                    ))
                st.session_state.total_scanned += scanned
                st.session_state.files_saved   += saved
                db.save_progress(channel.id, cur_last, st.session_state.total_scanned, st.session_state.files_saved)
                progress.progress(1.0, text="✅ اكتملت الدفعة")
                st.info(f"دفعة: فحص {scanned} رسالة، حُفظ {saved} ملف")

                if st.session_state.auto_scan_running and scanned == BATCH_SCAN_SIZE:
                    time.sleep(0.5); st.rerun()
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
        channel.id,
        params['keep_strategy'],
        int(params['min_size_mb'] * 1024 * 1024),
        use_md5=params.get('compute_md5', False),
        use_phash=params.get('compute_phash', False),
        use_fuzzy=params.get('use_fuzzy', False),
        fuzzy_threshold=params.get('fuzzy_threshold', 0.85),
    )

    st.html(f"<h3 style='margin:0 0 16px;color:#0f172a;'>📋 {getattr(channel, 'title', str(channel.id))}</h3>")

    if st.session_state.last_deleted_count > 0:
        st.success(f"✅ تم حذف {st.session_state.last_deleted_count} رسالة بنجاح من تيليجرام وقاعدة البيانات")
        if st.session_state.last_deleted_failed > 0:
            st.warning(f"⚠️ فشل حذف {st.session_state.last_deleted_failed} رسالة")
        st.session_state.last_deleted_count  = 0
        st.session_state.last_deleted_failed = 0

    if not duplicates:
        st.success("🎉 لا توجد مكررات!")
    else:
        # إحصاء حسب نوع الطبقة
        type_counts = {}
        for d in duplicates:
            mt = d.get('match_type', 'file_id')
            type_counts[mt] = type_counts.get(mt, 0) + 1

        badge_map = {
            "file_id": "🔗 File ID (Forward)",
            "md5":     "🔐 MD5",
            "phash":   "🖼️ pHash",
            "fuzzy":   "🎬 Fuzzy Video",
        }
        summary = " · ".join(f"{badge_map.get(k,k)}: {v}" for k, v in type_counts.items())

        st.html(f"""
        <div style="display:inline-flex;align-items:center;gap:8px;background:#fff7ed;
                    border:1px solid #fed7aa;border-radius:10px;padding:10px 16px;margin-bottom:16px;">
          <span style="font-size:1.1rem;">⚠️</span>
          <span style="color:#c2410c;font-weight:600;font-size:0.95rem;">
            {len(duplicates)} رسالة مكررة — {summary}
          </span>
        </div>
        """)

        page        = st.session_state.page
        total_pages = max(1, (len(duplicates) + PAGE_SIZE - 1) // PAGE_SIZE)
        page_dups   = duplicates[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

        badge_short = {"file_id": "🔗 Forward", "md5": "🔐 MD5", "phash": "🖼️ pHash", "fuzzy": "🎬 Fuzzy"}
        df = pd.DataFrame([
            {
                "معرف": d['id'],
                "النوع": d['type'],
                "الحجم": fmt_size(d['size']),
                "المدة (ث)": d['duration'] if d['duration'] else "—",
                "التاريخ": d['date'][:10],
                "سبب التكرار": badge_short.get(d.get('match_type', 'file_id'), '🔗'),
                "الأصل": d['keeper_id'],
                "تحديد": False
            }
            for d in page_dups
        ])
        edited = st.data_editor(
            df,
            column_config={"تحديد": st.column_config.CheckboxColumn("🗑️ حذف")},
            hide_index=True, use_container_width=True, height=400
        )
        for sid in edited[edited["تحديد"] == True]["معرف"].tolist():
            st.session_state.selected_ids.add(sid)

        if total_pages > 1:
            pcol1, pcol2, pcol3 = st.columns(3)
            with pcol1:
                if page > 0 and st.button("⬅️ السابقة", use_container_width=True):
                    st.session_state.page -= 1; st.rerun()
            with pcol2:
                st.markdown(f"<p style='text-align:center;'>صفحة {page+1} من {total_pages}</p>",
                            unsafe_allow_html=True)
            with pcol3:
                if page < total_pages - 1 and st.button("➡️ التالية", use_container_width=True):
                    st.session_state.page += 1; st.rerun()

        st.markdown("---")
        col_s1, col_s2, col_s3 = st.columns(3)
        with col_s1:
            if st.button("☑️ تحديد الكل في الصفحة", use_container_width=True):
                for d in page_dups: st.session_state.selected_ids.add(d['id'])
                st.rerun()
        with col_s2:
            if st.button("✖️ إلغاء تحديد الكل", use_container_width=True):
                st.session_state.selected_ids = set(); st.rerun()
        with col_s3:
            df_report = pd.DataFrame([
                {"معرف": d['id'], "النوع": d['type'], "الحجم": fmt_size(d['size']),
                 "التاريخ": d['date'], "سبب التكرار": d.get('match_type', '')}
                for d in duplicates
            ])
            st.download_button("📥 تقرير CSV", df_report.to_csv(index=False).encode('utf-8-sig'),
                               "duplicates_report.csv", "text/csv", use_container_width=True)

        selected_count = len(st.session_state.selected_ids)
        if selected_count > 0:
            st.html(f"""
            <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:9px;
                        padding:10px 16px;margin:12px 0;color:#166534;font-weight:600;font-size:0.9rem;">
              📌 محدد للحذف: {selected_count} رسالة
            </div>
            """)

        if st.button(f"🗑️ حذف {selected_count} رسالة محددة",
                     type="primary", disabled=selected_count == 0, use_container_width=True):
            ids     = list(st.session_state.selected_ids)
            prog    = st.progress(0, text="جاري الحذف...")
            deleted = 0
            failed  = 0
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
                    except Exception: failed += len(batch)
                except Exception: failed += len(batch)
                prog.progress((i + len(batch)) / len(ids))

            st.session_state.last_deleted_count  = deleted
            st.session_state.last_deleted_failed = failed
            st.session_state.selected_ids = set()
            db.close()
            st.rerun()

    db.close()

st.markdown("---")
st.markdown("<div class='footer-bar'>صُنع بعناية بواسطة <strong>F.ALSALEH</strong> · DupZap v5.0</div>",
            unsafe_allow_html=True)
