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
st.set_page_config(page_title="TeleSweep – مزيل المكررات", page_icon="🧹", layout="wide")

st.html("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Arabic:wght@300;400;500;600;700&display=swap');

/* ═══════════════════════════════════════════
   DARK BASE
═══════════════════════════════════════════ */
html, body, [class*="css"], .stApp {
    font-family: 'IBM Plex Sans Arabic', 'Segoe UI', system-ui, sans-serif;
    color-scheme: dark;
}
.stApp { background: #0f0e17 !important; }
.main .block-container {
    padding-top: 1.5rem !important;
    padding-bottom: 2rem !important;
    max-width: 1020px !important;
    background: transparent !important;
}

/* Override Streamlit white backgrounds */
section[data-testid="stSidebar"] + div { background: #0f0e17; }
.stApp > div { background: transparent; }

/* ═══════════════════════════════════════════
   SIDEBAR — Deep Indigo
═══════════════════════════════════════════ */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0d0b1e 0%, #1a1740 100%) !important;
    border-right: 1px solid #2d2a5e;
}
[data-testid="stSidebar"] > div:first-child { padding: 0; }
[data-testid="stSidebar"] * { color: #a5b4fc !important; }
[data-testid="stSidebar"] hr {
    border-color: #2d2a5e !important;
    margin: 0.6rem 0 !important;
}
[data-testid="stSidebar"] .stButton > button {
    background: rgba(99,102,241,0.08) !important;
    border: 1px solid rgba(99,102,241,0.2) !important;
    color: #a5b4fc !important;
    border-radius: 10px; font-weight: 500; font-size: 0.84rem;
    transition: all 0.15s;
}
[data-testid="stSidebar"] .stButton > button:hover {
    background: rgba(99,102,241,0.18) !important;
    border-color: rgba(165,180,252,0.5) !important;
    color: #e0e7ff !important;
    transform: none; box-shadow: none;
}

/* Sidebar Logo */
.sidebar-logo {
    text-align: center; padding: 26px 16px 18px;
    border-bottom: 1px solid rgba(99,102,241,0.15);
    margin-bottom: 6px;
}
.sidebar-logo .logo-icon {
    font-size: 2.4rem; line-height: 1;
    display: block; margin-bottom: 10px;
    filter: drop-shadow(0 0 16px rgba(129,140,248,0.8));
}
.sidebar-logo .logo-name {
    font-size: 1.5rem; font-weight: 700; letter-spacing: -0.02em;
    background: linear-gradient(90deg, #a5b4fc 0%, #818cf8 50%, #c084fc 100%);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    display: block; margin-bottom: 2px;
}
.sidebar-logo .logo-ver {
    font-size: 0.70rem; color: #4f46e5 !important;
    letter-spacing: 0.08em; text-transform: uppercase;
}

/* ═══════════════════════════════════════════
   MAIN HEADER
═══════════════════════════════════════════ */
.ts-header {
    background: linear-gradient(135deg, #3730a3 0%, #5b21b6 50%, #6d28d9 100%);
    border-radius: 18px; padding: 24px 28px; margin-bottom: 24px;
    box-shadow: 0 8px 40px rgba(79,70,229,0.4), 0 0 0 1px rgba(165,180,252,0.1);
    display: flex; align-items: center; gap: 16px;
    position: relative; overflow: hidden;
}
.ts-header::before {
    content: ''; position: absolute; top: -50%; right: -10%;
    width: 300px; height: 300px; border-radius: 50%;
    background: radial-gradient(circle, rgba(196,181,253,0.12) 0%, transparent 70%);
    pointer-events: none;
}
.ts-header-icon { font-size: 2.4rem; filter: drop-shadow(0 2px 12px rgba(0,0,0,0.3)); position: relative; }
.ts-header-title {
    font-size: 1.8rem; font-weight: 700; color: #ffffff !important;
    letter-spacing: -0.02em; margin: 0; line-height: 1.2; position: relative;
}
.ts-header-sub { font-size: 0.80rem; color: rgba(196,181,253,0.8); margin: 4px 0 0; position: relative; }
.ts-badge {
    background: rgba(255,255,255,0.12); color: #e0e7ff !important;
    font-size: 0.68rem; font-weight: 700; padding: 3px 10px;
    border-radius: 99px; letter-spacing: 0.08em;
    border: 1px solid rgba(255,255,255,0.18); white-space: nowrap; position: relative;
}

/* ═══════════════════════════════════════════
   CARDS / CONTAINERS — Dark
═══════════════════════════════════════════ */
[data-testid="metric-container"] {
    background: #1a1730 !important;
    border-radius: 16px; padding: 20px 18px !important;
    border: 1px solid #2d2a5e !important;
    box-shadow: 0 2px 12px rgba(0,0,0,0.3);
    transition: all 0.2s ease; position: relative; overflow: hidden;
}
[data-testid="metric-container"]::before {
    content: ''; position: absolute; top: 0; left: 0;
    width: 3px; height: 100%;
    background: linear-gradient(180deg, #818cf8, #c084fc);
}
[data-testid="metric-container"]:hover {
    box-shadow: 0 8px 28px rgba(99,102,241,0.25);
    transform: translateY(-2px); border-color: #4f46e5 !important;
}
[data-testid="stMetricLabel"] { color: #6366f1 !important; font-size: 0.80rem !important; font-weight: 500 !important; }
[data-testid="stMetricValue"] { color: #e0e7ff !important; font-weight: 700 !important; }

[data-testid="stForm"] {
    background: #151228 !important;
    border-radius: 18px !important; padding: 28px !important;
    border: 1px solid #2d2a5e !important;
    box-shadow: 0 4px 24px rgba(0,0,0,0.3) !important;
}

/* ═══════════════════════════════════════════
   BUTTONS
═══════════════════════════════════════════ */
.stButton > button {
    border-radius: 10px !important; font-weight: 600 !important;
    font-size: 0.87rem !important; min-height: 42px !important;
    transition: all 0.18s cubic-bezier(.4,0,.2,1) !important;
    border: 1px solid #2d2a5e !important;
    background: #1a1730 !important; color: #a5b4fc !important;
    box-shadow: 0 1px 4px rgba(0,0,0,0.2) !important;
}
.stButton > button:hover {
    border-color: #6366f1 !important;
    box-shadow: 0 4px 16px rgba(99,102,241,0.25) !important;
    transform: translateY(-1px) !important; color: #e0e7ff !important;
    background: #201c3a !important;
}
.stButton > button:active { transform: translateY(0) !important; }
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #4f46e5 0%, #7c3aed 100%) !important;
    color: #ffffff !important; border: none !important;
    box-shadow: 0 4px 18px rgba(79,70,229,0.45) !important;
}
.stButton > button[kind="primary"]:hover {
    box-shadow: 0 8px 28px rgba(79,70,229,0.55) !important;
    transform: translateY(-2px) !important; color: #ffffff !important;
}

/* ═══════════════════════════════════════════
   INPUTS — Dark
═══════════════════════════════════════════ */
.stTextInput > div > div > input,
.stNumberInput > div > div > input,
.stTextArea > div > textarea {
    border-radius: 10px !important;
    border: 1.5px solid #2d2a5e !important;
    background: #0d0b1e !important; color: #c7d2fe !important;
    font-size: 0.9rem !important;
    transition: border-color 0.15s, box-shadow 0.15s !important;
    padding: 10px 14px !important;
}
.stTextInput > div > div > input:focus,
.stNumberInput > div > div > input:focus,
.stTextArea > div > textarea:focus {
    border-color: #6366f1 !important;
    box-shadow: 0 0 0 3px rgba(99,102,241,0.2) !important;
}
.stTextInput > div > div > input::placeholder,
.stTextArea > div > textarea::placeholder { color: #4a4870 !important; }

.stSelectbox > div > div,
.stMultiSelect > div > div {
    border-radius: 10px !important;
    border: 1.5px solid #2d2a5e !important;
    background: #0d0b1e !important; color: #c7d2fe !important;
}
label, .stCheckbox label p, .stToggle label p, p, .stCaption p {
    color: #8b8fc7 !important; font-size: 0.87rem !important; font-weight: 400 !important;
}
h1,h2,h3,h4 { color: #e0e7ff !important; }
h2 { font-size: 1.1rem !important; }
h3 { font-size: 1rem !important; }
strong { color: #c7d2fe !important; }
.stMarkdown p { color: #8b8fc7 !important; }

/* ═══════════════════════════════════════════
   RESULT CARDS — Dark
═══════════════════════════════════════════ */
.dup-card {
    background: #151228;
    border: 1px solid #2d2a5e;
    border-radius: 14px; padding: 14px 18px; margin-bottom: 10px;
    transition: all 0.18s ease;
    display: flex; align-items: center; gap: 14px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.2);
}
.dup-card:hover {
    border-color: #6366f1;
    box-shadow: 0 6px 24px rgba(99,102,241,0.2);
    transform: translateX(-2px);
}
.dup-card-icon {
    font-size: 1.5rem; min-width: 38px; text-align: center;
    background: rgba(99,102,241,0.12); border-radius: 10px; padding: 8px;
}
.dup-card-body { flex: 1; min-width: 0; }
.dup-card-name { font-weight: 600; color: #c7d2fe; font-size: 0.9rem; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.dup-card-meta { font-size: 0.73rem; color: #4a4870; margin-top: 3px; }
.dup-badge { font-size: 0.67rem; font-weight: 700; padding: 3px 9px; border-radius: 99px; white-space: nowrap; }
.badge-fileid { background: rgba(22,163,74,0.15);  color: #4ade80; border: 1px solid rgba(74,222,128,0.25); }
.badge-md5    { background: rgba(59,130,246,0.15);  color: #60a5fa; border: 1px solid rgba(96,165,250,0.25); }
.badge-phash  { background: rgba(234,179,8,0.12);   color: #facc15; border: 1px solid rgba(250,204,21,0.25); }
.badge-exact  { background: rgba(139,92,246,0.15);  color: #a78bfa; border: 1px solid rgba(167,139,250,0.3); }

/* ═══════════════════════════════════════════
   ALERTS
═══════════════════════════════════════════ */
.ts-alert-warn {
    background: rgba(251,191,36,0.08);
    border: 1px solid rgba(251,191,36,0.25);
    border-radius: 12px; padding: 12px 18px; margin-bottom: 16px;
    display: flex; align-items: center; gap: 10px;
}
.ts-alert-ok {
    background: rgba(34,197,94,0.08);
    border: 1px solid rgba(34,197,94,0.25);
    border-radius: 12px; padding: 12px 18px; margin-bottom: 16px;
}
[data-testid="stAlert"] {
    border-radius: 12px !important;
    background: rgba(99,102,241,0.08) !important;
    border: 1px solid rgba(99,102,241,0.2) !important;
    color: #c7d2fe !important;
}

/* ═══════════════════════════════════════════
   TABS — Dark
═══════════════════════════════════════════ */
.stTabs [data-baseweb="tab-list"] {
    background: #1a1730 !important;
    border-radius: 12px !important; padding: 4px !important; gap: 4px !important;
    border: 1px solid #2d2a5e !important;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 9px !important; font-weight: 600 !important;
    font-size: 0.85rem !important; color: #4a4870 !important;
}
.stTabs [aria-selected="true"] {
    background: #2d2a5e !important; color: #a5b4fc !important;
    box-shadow: 0 1px 4px rgba(0,0,0,0.3) !important;
}

/* ═══════════════════════════════════════════
   EXPANDER — Dark
═══════════════════════════════════════════ */
[data-testid="stExpander"] {
    border: 1px solid #2d2a5e !important;
    border-radius: 12px !important; overflow: hidden !important;
    background: #151228 !important;
}
[data-testid="stExpander"] summary {
    font-weight: 600 !important; color: #a5b4fc !important;
    background: #1a1730 !important; padding: 12px 16px !important;
}

/* ═══════════════════════════════════════════
   PROGRESS
═══════════════════════════════════════════ */
.stProgress > div { background: #1a1730 !important; border-radius: 99px !important; }
.stProgress > div > div > div > div {
    background: linear-gradient(90deg, #4f46e5, #818cf8, #c084fc) !important;
    border-radius: 99px !important;
}

/* DATA TABLE */
.stDataEditor {
    border-radius: 14px !important; overflow: hidden !important;
    border: 1px solid #2d2a5e !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.2) !important;
}

/* MISC */
hr { border-color: #2d2a5e !important; margin: 1.2rem 0 !important; }
.footer-bar {
    text-align: center; padding: 20px; color: #4a4870;
    font-size: 0.78rem; margin-top: 40px;
    border-top: 1px solid #2d2a5e;
}
.footer-bar strong { color: #6366f1; }

::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-track { background: #0d0b1e; }
::-webkit-scrollbar-thumb { background: #2d2a5e; border-radius: 99px; }
::-webkit-scrollbar-thumb:hover { background: #4f46e5; }

/* Mobile */
@media (max-width: 768px) {
    .main .block-container { padding: 1rem 0.8rem !important; }
    .ts-header { padding: 16px 18px; gap: 12px; }
    .ts-header-title { font-size: 1.3rem; }
    [data-testid="metric-container"] { padding: 14px 12px !important; }
    .dup-card { padding: 10px 12px; gap: 10px; }
}
</style>

<link rel="manifest" href="data:application/json;base64,eyJuYW1lIjoiVGVsZVN3ZWVwIiwic2hvcnRfbmFtZSI6IlRlbGVTd2VlcCIsImRlc2NyaXB0aW9uIjoi2YXYstea2YQg2KfZhNmF2Lnal9in2K8g2KfZhNmF2Lnaq9ix2LHYqSIsInN0YXJ0X3VybCI6Ii8iLCJkaXNwbGF5Ijoic3RhbmRhbG9uZSIsImJhY2tncm91bmRfY29sb3IiOiIjMGYwZTE3IiwidGhlbWVfY29sb3IiOiIjNGY0NmU1IiwiaWNvbnMiOlt7InNyYyI6ImRhdGE6aW1hZ2Uvc3ZnK3htbDtiYXNlNjQsUEhOMlp5QjRiV3h1Y3owaWFIUjBjRG92TDNkM2R5NTNNeTV2Y21jdk1qQXdNQzl6ZG1jaUlIZHBaSFJvUFNJeU1EQWlJR2hsYVdkb2REMGlNakF3SWlCbWIyNTBMV1poYldsc2VUMGlibTl1WlNJK1BITndZVzRnWm1sc2JEMG5JalV3TXlJZ2MzUnlaV1Z1UFNJeE1TSmZQQzl6Y0dGdVBqd3ZjM1puUGc9PSIsInR5cGUiOiJpbWFnZS9zdmcreG1sIiwic2l6ZXMiOiIxOTJ4MTkyIn1dfQ==">
<script>
if ('serviceWorker' in navigator) {
    window.addEventListener('load', () => {
        navigator.serviceWorker.register('/sw.js').catch(() => {});
    });
}
// Add to home screen prompt
let deferredPrompt;
window.addEventListener('beforeinstallprompt', (e) => {
    e.preventDefault();
    deferredPrompt = e;
    // Show custom install button if needed
    const btn = document.getElementById('ts-install-btn');
    if (btn) btn.style.display = 'block';
});
</script>
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

# ================== Exact Video Matching ==================

def is_exact_video_duplicate(a: dict, b: dict,
                             duration_tolerance: float = 0.01,
                             size_tolerance_percent: float = 0.5) -> bool:
    """
    تطابق صارم جداً مع هامش ضئيل:
    - المدة: فرق <= duration_tolerance (افتراضي 0.01 ثانية)
    - الحجم: فرق <= size_tolerance_percent % (افتراضي 0.5%)
    - الأبعاد: يجب أن تتطابق تماماً (أو كلاهما غير معروف)
    """
    d1 = float(a.get("duration", 0))
    d2 = float(b.get("duration", 0))
    s1 = int(a.get("size", 0))
    s2 = int(b.get("size", 0))
    w1 = int(a.get("width", 0))
    h1 = int(a.get("height", 0))
    w2 = int(b.get("width", 0))
    h2 = int(b.get("height", 0))

    if d1 == 0 or d2 == 0 or s1 == 0 or s2 == 0:
        return False

    # المدة
    if abs(d1 - d2) > duration_tolerance:
        return False

    # الحجم (نسبة مئوية)
    size_diff = abs(s1 - s2)
    max_size = max(s1, s2)
    if max_size > 0 and (size_diff / max_size) * 100 > size_tolerance_percent:
        return False

    # الأبعاد: تطابق تام
    if (w1 != w2 or h1 != h2) and not (w1 == 0 and h1 == 0 and w2 == 0 and h2 == 0):
        return False

    return True


class _UnionFind:
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
            if hasattr(attr, 'file_name'): info["name"] = attr.file_name

    elif isinstance(media, MessageMediaPhoto):
        photo = media.photo
        info["file_id"] = f"{photo.id}:{photo.dc_id}"
        info["type"]    = "photo"
        info["mime"]    = "image/jpeg"
        sizes = [s for s in getattr(photo, "sizes", []) if hasattr(s, "size") and s.size > 0]
        if sizes:
            largest = max(sizes, key=lambda s: s.size)
            info["size"]   = largest.size
            info["width"]  = getattr(largest, 'w', 0) or 0
            info["height"] = getattr(largest, 'h', 0) or 0
        else:
            info["size"] = 0
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
                file_size INTEGER, duration INTEGER, width INTEGER DEFAULT 0, height INTEGER DEFAULT 0,
                md5_hash TEXT, phash TEXT,
                msg_date TEXT, file_type TEXT, mime_type TEXT, views INTEGER, file_name TEXT,
                PRIMARY KEY (channel_id, msg_id)
            ) WITHOUT ROWID
        """)
        try:
            self.conn.execute("ALTER TABLE seen_files ADD COLUMN width INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass
        try:
            self.conn.execute("ALTER TABLE seen_files ADD COLUMN height INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass

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
        self.conn.execute("INSERT OR REPLACE INTO seen_files VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", record)
        self.conn.commit()

    def delete_msg_records(self, channel_id, msg_ids):
        self.conn.executemany(
            "DELETE FROM seen_files WHERE channel_id=? AND msg_id=?",
            [(channel_id, mid) for mid in msg_ids]
        )
        self.conn.commit()

    def get_all_videos(self, channel_id, min_size=0) -> List[Dict]:
        rows = self.conn.execute(
            "SELECT msg_id, file_id, file_size, duration, width, height, msg_date, file_name "
            "FROM seen_files WHERE channel_id=? AND file_type='video' AND file_size>=?",
            (channel_id, min_size)
        ).fetchall()
        return [
            {"id": r[0], "file_id": r[1], "size": r[2], "duration": r[3],
             "width": r[4], "height": r[5], "date": r[6], "name": r[7], "type": "video"}
            for r in rows
        ]

    def stream_duplicates(self, channel_id, keep_strategy, min_size=0,
                          use_md5=False, use_phash=False, use_exact_video=False,
                          duration_tolerance=0.01, size_tolerance_percent=0.5):
        order = {"oldest": "msg_date ASC", "newest": "msg_date DESC", "largest": "file_size DESC"}[keep_strategy]
        duplicates = []
        seen_msg_ids: set = set()

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

        # Layer 1: file_id
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

        # Layer 2: MD5
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

        # Layer 3: pHash
        if use_phash and _HAS_IMAGEHASH:
            rows = self.conn.execute(
                "SELECT msg_id, file_size, msg_date, file_id, duration, phash, file_type, mime_type, file_name "
                "FROM seen_files WHERE channel_id=? AND phash IS NOT NULL "
                "AND file_size>=? AND file_type IN ('photo','image')",
                (channel_id, min_size)
            ).fetchall()

            if rows:
                n   = len(rows)
                uf2 = _UnionFind(n)
                hashes = []
                for r in rows:
                    try:    hashes.append(imagehash.hex_to_hash(r[5]))
                    except: hashes.append(None)

                for i in range(n):
                    if hashes[i] is None or rows[i][0] in seen_msg_ids: continue
                    for j in range(i + 1, n):
                        if hashes[j] is None or rows[j][0] in seen_msg_ids: continue
                        if rows[i][3] == rows[j][3]: continue
                        if (hashes[i] - hashes[j]) <= 6:
                            uf2.union(i, j)

                groups2: Dict[int, List[int]] = {}
                for idx in range(n):
                    root = uf2.find(idx)
                    groups2.setdefault(root, []).append(idx)

                for root, members in groups2.items():
                    if len(members) < 2: continue
                    if keep_strategy == "largest":
                        members.sort(key=lambda i: rows[i][1], reverse=True)
                    elif keep_strategy == "newest":
                        members.sort(key=lambda i: rows[i][2], reverse=True)
                    else:
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

        # Layer 4: Exact Video
        if use_exact_video:
            videos = self.get_all_videos(channel_id, min_size)

            sort_key = {"oldest": lambda v: v["date"],
                        "newest": lambda v: v["date"],
                        "largest": lambda v: v["size"]}[keep_strategy]
            reverse = keep_strategy == "newest"
            videos.sort(key=sort_key, reverse=reverse)

            n = len(videos)
            uf = _UnionFind(n)

            for i in range(n):
                if videos[i]["id"] in seen_msg_ids: continue
                for j in range(i + 1, n):
                    if videos[j]["id"] in seen_msg_ids: continue
                    if videos[i]["file_id"] == videos[j]["file_id"]: continue
                    if is_exact_video_duplicate(videos[i], videos[j],
                                                duration_tolerance, size_tolerance_percent):
                        uf.union(i, j)

            groups: Dict[int, List[int]] = {}
            for idx in range(n):
                root = uf.find(idx)
                groups.setdefault(root, []).append(idx)

            for root, members in groups.items():
                if len(members) < 2: continue
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
                            "match_type": "exact_video"
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
    'me': None, '_confirm_rescan': False, 'my_channels': None,
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ================== الشريط الجانبي ==================
with st.sidebar:
    st.html("""
    <div class='sidebar-logo'>
        <div class='logo-icon'>🧹</div>
        <div class='logo-name'>TeleSweep</div>
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
<div class="ts-header">
  <div class="ts-header-icon">🧹</div>
  <div style="flex:1;">
    <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
      <span class="ts-header-title">TeleSweep</span>
      <span class="ts-badge">v5.0</span>
    </div>
    <p class="ts-header-sub">كشف وإزالة المكررات · File ID · MD5 · pHash · Exact Video Match</p>
  </div>
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

# ---------- إعدادات القناة (مع جلب القنوات بشكل آمن) ----------
elif st.session_state.step == 'channel':
    st.success("✅ تم تسجيل الدخول")
    
    # --- جلب قائمة القنوات ---
    if st.button("📋 جلب قنواتي ومجموعاتي", use_container_width=True):
        client = st.session_state.get('client')
        if client is None:
            st.error("❌ لم يتم العثور على جلسة تيليجرام. الرجاء إعادة تسجيل الدخول.")
        else:
            with st.spinner("جاري جلب القنوات والمجموعات..."):
                try:
                    async def fetch_dialogs():
                        if not client.is_connected():
                            await client.connect()
                        dialogs = await client.get_dialogs()
                        channels = []
                        for d in dialogs:
                            if d.is_channel or d.is_group:
                                channels.append({
                                    "name": d.name,
                                    "id": d.id,
                                    "entity": d.entity
                                })
                        return channels
                    st.session_state.my_channels = run_sync(fetch_dialogs())
                    if st.session_state.my_channels:
                        st.success(f"✅ تم جلب {len(st.session_state.my_channels)} قناة/مجموعة")
                    else:
                        st.info("ℹ️ لم يتم العثور على أي قنوات أو مجموعات.")
                except Exception as e:
                    st.error(f"❌ خطأ في جلب القنوات: {e}")

    channel_input = None
    if st.session_state.my_channels:
        options = {f"{c['name']} (ID: {c['id']})": c for c in st.session_state.my_channels}
        selected = st.selectbox("اختر قناة أو مجموعة:", list(options.keys()))
        channel_input = options[selected]["entity"]
        st.caption("أو يمكنك إدخال رابط يدويًا أدناه (سيتم تجاهل الاختيار أعلاه)")
    
    manual_input = st.text_input("أو أدخل رابط القناة / المجموعة يدويًا", placeholder="@username أو https://t.me/+xxx")
    if manual_input:
        channel_input = manual_input

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
                                    value=False,  # ❗ غير مفعل افتراضياً
                                    disabled=not _HAS_IMAGEHASH,
                                    help="يكتشف الصور المتشابهة حتى لو اختلفت أبعادها.")

    st.markdown("---")
    st.subheader("🎬 Exact Video Matching (دقة عالية جداً)")

    use_exact_video = st.toggle("تفعيل Exact Video Matching", value=False,
                                help="تطابق صارم مع هامش صغير جداً للمدة والحجم (الأبعاد يجب أن تتطابق تماماً).")

    duration_tolerance = 0.01
    size_tolerance_percent = 0.5

    if use_exact_video:
        st.markdown("""
        <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:9px;
                    padding:10px 14px;margin-bottom:12px;font-size:0.84rem;color:#166534;">
        ✅ <b>دقة متناهية:</b> الإعدادات الافتراضية (0.01 ثانية و 0.5% حجم) تمنع تقريباً كل الإيجابيات الكاذبة.
        </div>
        """, unsafe_allow_html=True)

        st.markdown("**📏 هامش المدة (بالثواني)**")
        duration_tolerance = st.radio(
            "اختر مستوى الدقة للمدة:",
            options=[0.001, 0.01, 0.05, 0.1],
            index=1,  # افتراضي 0.01
            format_func=lambda x: (
                f"{x} ثانية - دقة قصوى (شبه تام)" if x == 0.001 else
                f"{x} ثانية - دقة عالية (موصى به)" if x == 0.01 else
                f"{x} ثانية - دقة متوسطة" if x == 0.05 else
                f"{x} ثانية - دقة منخفضة (قد يلتقط مكررات أكثر)"
            ),
            help="الفرق المسموح به في المدة بين الفيديوهين ليتم اعتبارهما مكررين."
        )

        st.markdown("**📦 هامش الحجم (%)**")
        size_tolerance_percent = st.radio(
            "اختر النسبة المئوية للفرق المسموح به في حجم الملف:",
            options=[0.0, 0.01, 0.1, 0.25, 0.5, 1.0],
            index=4,  # افتراضي 0.5
            format_func=lambda x: (
                f"{x}% - تطابق تام (byte-perfect)" if x == 0.0 else
                f"{x}% - شبه تام (metadata فقط)" if x == 0.01 else
                f"{x}% - شبه تام" if x == 0.1 else
                f"{x}% - دقيق جداً" if x == 0.25 else
                f"{x}% - دقة عالية (موصى به)" if x == 0.5 else
                f"{x}% - دقة متوسطة (قد يلتقط مكررات أكثر)"
            ),
            help="القيمة 0.01% تكتشف نفس الملف حتى مع اختلافات metadata الضئيلة جداً."
        )

    st.markdown("---")
    uploaded_db = st.file_uploader("📂 رفع قاعدة بيانات سابقة (اختياري)", type=['db'])
    if uploaded_db:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        tmp.write(uploaded_db.getbuffer())
        st.session_state.db_path = tmp.name
        st.success("✅ تم تحميل قاعدة البيانات")

    if st.button("🚀 بدء المسح", use_container_width=True, type="primary"):
        if not channel_input:
            st.error("الرجاء اختيار قناة أو إدخال رابط")
        else:
            try:
                if isinstance(channel_input, str):
                    entity = run_sync(_get_entity(st.session_state.client, channel_input.strip()))
                else:
                    entity = channel_input
                st.session_state.channel     = entity
                st.session_state.scan_params = {
                    'media_types': media_types,
                    'keep_strategy': keep_map[keep_strategy],
                    'min_size_mb': min_size_mb,
                    'compute_md5': compute_md5,
                    'compute_phash': compute_phash,
                    'use_exact_video': use_exact_video,
                    'duration_tolerance': duration_tolerance,
                    'size_tolerance_percent': size_tolerance_percent,
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

    channel_title = getattr(channel, 'title', str(channel.id))
    st.html(f"""
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:16px;">
      <span style="font-size:1.4rem;">📡</span>
      <div>
        <div style="font-size:1.1rem;font-weight:700;color:#1e1b4b;">مسح القناة</div>
        <div style="font-size:0.82rem;color:#6366f1;font-weight:600;">{channel_title}</div>
      </div>
    </div>
    """)

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
        use_exact_video=params.get('use_exact_video', False),
        duration_tolerance=params.get('duration_tolerance', 0.01),
        size_tolerance_percent=params.get('size_tolerance_percent', 0.5),
    )

    channel_title = getattr(channel, 'title', str(channel.id))
    st.html(f"""
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:20px;">
      <span style="font-size:1.4rem;">📋</span>
      <div>
        <div style="font-size:1.1rem;font-weight:700;color:#1e1b4b;">نتائج الفحص</div>
        <div style="font-size:0.82rem;color:#6366f1;font-weight:600;">{channel_title}</div>
      </div>
    </div>
    """)

    if st.session_state.last_deleted_count > 0:
        st.success(f"✅ تم حذف {st.session_state.last_deleted_count} رسالة بنجاح من تيليجرام وقاعدة البيانات")
        if st.session_state.last_deleted_failed > 0:
            st.warning(f"⚠️ فشل حذف {st.session_state.last_deleted_failed} رسالة")
        st.session_state.last_deleted_count  = 0
        st.session_state.last_deleted_failed = 0

    if not duplicates:
        st.success("🎉 لا توجد مكررات!")
    else:
        type_counts = {}
        for d in duplicates:
            mt = d.get('match_type', 'file_id')
            type_counts[mt] = type_counts.get(mt, 0) + 1

        badge_map = {
            "file_id":     "🔗 File ID",
            "md5":         "🔐 MD5",
            "phash":       "🖼️ pHash",
            "exact_video": "🎬 Exact Video",
        }
        summary_parts = " · ".join(
            f"<span style='color:#4338ca;font-weight:700;'>{badge_map.get(k,k)}</span>: {v}"
            for k, v in type_counts.items()
        )

        st.html(f"""
        <div class="ts-alert-warn">
          <span style="font-size:1.4rem;">⚠️</span>
          <div>
            <div style="font-weight:700;color:#92400e;font-size:0.95rem;">
              {len(duplicates)} ملف مكرر بحاجة للمراجعة
            </div>
            <div style="font-size:0.78rem;color:#b45309;margin-top:2px;">{summary_parts}</div>
          </div>
        </div>
        """)

        page        = st.session_state.page
        total_pages = max(1, (len(duplicates) + PAGE_SIZE - 1) // PAGE_SIZE)
        page_dups   = duplicates[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

        # ── Card view للمكررات ──
        type_icon = {"video": "🎬", "photo": "🖼️", "image": "🖼️", "document": "📄"}
        badge_cls = {"file_id": "badge-fileid", "md5": "badge-md5", "phash": "badge-phash", "exact_video": "badge-exact"}
        badge_lbl = {"file_id": "🔗 Forward", "md5": "🔐 MD5", "phash": "🖼️ pHash", "exact_video": "🎬 Exact"}

        cards_html = ""
        for d in page_dups:
            icon  = type_icon.get(d['type'], "📁")
            bc    = badge_cls.get(d.get('match_type','file_id'), 'badge-fileid')
            bl    = badge_lbl.get(d.get('match_type','file_id'), '🔗')
            name  = (d.get('name') or f"رسالة #{d['id']}")
            if len(name) > 35: name = name[:35] + "…"
            dur   = f" · {d['duration']}ث" if d.get('duration') else ""
            cards_html += f"""
            <div class="dup-card" id="card-{d['id']}">
              <div class="dup-card-icon">{icon}</div>
              <div class="dup-card-body">
                <div class="dup-card-name">{name}</div>
                <div class="dup-card-meta">{fmt_size(d['size'])}{dur} · {d['date'][:10]} · أصل: #{d['keeper_id']}</div>
              </div>
              <span class="dup-badge {bc}">{bl}</span>
            </div>"""
        st.html(cards_html)

        # ── Editable table للتحديد ──
        badge_short = {"file_id": "🔗 Forward", "md5": "🔐 MD5", "phash": "🖼️ pHash", "exact_video": "🎬 Exact"}
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

        editor_key = f"editor_{channel.id}_{st.session_state.page}"
        edited = st.data_editor(
            df,
            column_config={"تحديد": st.column_config.CheckboxColumn("🗑️ حذف")},
            hide_index=True,
            use_container_width=True,
            height=400,
            key=editor_key
        )

        if edited is not None and not edited.empty and "تحديد" in edited.columns:
            selected_rows = edited[edited["تحديد"] == True]
            if not selected_rows.empty:
                for sid in selected_rows["معرف"].tolist():
                    st.session_state.selected_ids.add(sid)

        # ── مقارنة الـ Exact Video ──
        exact_video_dups = [d for d in page_dups if d.get('match_type') == 'exact_video']
        if exact_video_dups:
            raw_id = getattr(channel, 'id', None)
            ch_username = getattr(channel, 'username', None)

            def tg_link(msg_id):
                if ch_username:
                    return f"https://t.me/{ch_username}/{msg_id}"
                return f"https://t.me/c/{raw_id}/{msg_id}"

            with st.expander(f"🔍 مقارنة الـ Exact Video ({len(exact_video_dups)} فيديو) — اضغط للمراجعة قبل الحذف"):
                st.caption("الروابط تفتح الفيديو مباشرة في تيليجرام — لا يوجد تحميل")
                for d in exact_video_dups:
                    keeper_row = db.conn.execute(
                        "SELECT msg_id, file_size, duration, width, height, msg_date FROM seen_files "
                        "WHERE channel_id=? AND msg_id=?",
                        (channel.id, d['keeper_id'])
                    ).fetchone()

                    st.markdown("---")
                    c1, c2 = st.columns(2)

                    with c1:
                        st.markdown("**✅ الأصل (يُحتفظ به)**")
                        if keeper_row:
                            st.markdown(f"""
| | |
|---|---|
| المعرف | `{keeper_row[0]}` |
| الحجم | {fmt_size(keeper_row[1])} |
| المدة | {keeper_row[2]} ث |
| الأبعاد | {keeper_row[3]}×{keeper_row[4]} |
| التاريخ | {keeper_row[5][:10]} |
""")
                        st.link_button("▶️ فتح في تيليجرام",
                                      tg_link(d['keeper_id']),
                                      use_container_width=True)

                    with c2:
                        st.markdown("**🗑️ المكرر (سيُحذف)**")
                        st.markdown(f"""
| | |
|---|---|
| المعرف | `{d['id']}` |
| الحجم | {fmt_size(d['size'])} |
| المدة | {d['duration']} ث |
| التاريخ | {d['date'][:10]} |
""")
                        st.link_button("▶️ فتح في تيليجرام",
                                      tg_link(d['id']),
                                      use_container_width=True)

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

        # ── أزرار التحديد الذكي حسب نوع الطبقة ──
        types_present = {}
        for d in duplicates:
            mt = d.get('match_type', 'file_id')
            types_present.setdefault(mt, []).append(d['id'])

        type_labels = {
            "file_id": ("🔗 كل Forward",  "آمن 100% — تطابق تام",        "#f0fdf4", "#166534"),
            "md5":     ("🔐 كل MD5",      "آمن — تطابق بايتي كامل",       "#f0fdf4", "#166534"),
            "phash":   ("🖼️ كل pHash",    "تشابه بصري — راجع قبل الحذف", "#fefce8", "#854d0e"),
            "exact_video": ("🎬 كل Exact Video", "تطابق دقيق جداً — آمن", "#f0fdf4", "#166534"),
        }

        present_keys = [k for k in ["file_id", "md5", "phash", "exact_video"] if k in types_present]
        if present_keys:
            btn_cols = st.columns(len(present_keys) + 1)
            for i, mt in enumerate(present_keys):
                label, tip, bg, color = type_labels[mt]
                count = len(types_present[mt])
                with btn_cols[i]:
                    st.html(f"""<div style="font-size:0.72rem;color:{color};background:{bg};
                                border-radius:6px;padding:3px 6px;text-align:center;margin-bottom:4px;">
                                {tip}</div>""")
                    if st.button(f"{label} ({count})", use_container_width=True, key=f"sel_{mt}"):
                        for mid in types_present[mt]:
                            st.session_state.selected_ids.add(mid)
                        st.rerun()
            with btn_cols[-1]:
                st.html("""<div style="font-size:0.72rem;color:#64748b;background:#f1f5f9;
                           border-radius:6px;padding:3px 6px;text-align:center;margin-bottom:4px;">
                           إلغاء كل التحديد</div>""")
                if st.button("✖️ إلغاء الكل", use_container_width=True, key="desel_all"):
                    st.session_state.selected_ids = set(); st.rerun()

        # زر CSV
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
st.markdown("<div class='footer-bar'>صُنع بعناية بواسطة <strong>F.ALSALEH</strong> · TeleSweep v5.0</div>",
            unsafe_allow_html=True)
