import streamlit as st
import asyncio
import gc
import hashlib
import io
import sqlite3
import tempfile
import time
import threading
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError, SessionPasswordNeededError
from telethon.tl.types import (
    MessageMediaDocument, MessageMediaPhoto, DocumentAttributeVideo, PhotoSize
)
from PIL import Image

# محاولة جلب مكتبة الهوية البصرية
try:
    import imagehash
    _HAS_IMAGEHASH = True
except ImportError:
    _HAS_IMAGEHASH = False

# ================== تهيئة الصفحة والجماليات ==================
st.set_page_config(page_title="DupZap v4.0 PRO", page_icon="✂️", layout="wide")

st.html("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Arabic:wght@300;400;500;600;700&display=swap');
    html, body, [class*="css"], .stApp { font-family: 'IBM Plex Sans Arabic', sans-serif; background: #f8fafc; }
    [data-testid="stSidebar"] { background: #0f172a !important; }
    [data-testid="stSidebar"] * { color: #cbd5e1 !important; }
    .sidebar-logo { text-align: center; padding: 25px; border-bottom: 1px solid #1e293b; margin-bottom: 10px; }
    .sidebar-logo .logo-name { font-size: 1.6rem; font-weight: 700; background: linear-gradient(90deg, #38bdf8 0%, #818cf8 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
    .stButton > button { border-radius: 9px; font-weight: 600; min-height: 45px; transition: all 0.2s; border: 1.5px solid #e2e8f0 !important; }
    .stButton > button[kind="primary"] { background: linear-gradient(135deg, #0ea5e9 0%, #6366f1 100%) !important; color: white !important; border: none !important; box-shadow: 0 4px 12px rgba(99,102,241,0.3); }
    [data-testid="metric-container"] { background: white; border-radius: 15px; padding: 20px; border: 1px solid #e2e8f0; box-shadow: 0 1px 4px rgba(0,0,0,0.05); }
    .footer-bar { text-align: center; padding: 20px; color: #94a3b8; font-size: 0.8rem; margin-top: 40px; border-top: 1px solid #f1f5f9; }
</style>
""")

# ================== نظام إدارة الجلسة (Session) ==================
def get_db_path():
    return os.path.join(tempfile.gettempdir(), "dupzap_pro_v4.db")

if 'step' not in st.session_state:
    st.session_state.update({
        'step': 'login',
        'client': None,
        'db_path': get_db_path(),
        'channel': None,
        'total_scanned': 0,
        'files_saved': 0,
        'api_id': '',
        'api_hash': '',
        'phone': '',
        'session_string': None,
        'scan_params': {},
        'me': None
    })

# ================== محرك Async ثابت ==================
if '_bg_loop' not in st.session_state:
    loop = asyncio.new_event_loop()
    t = threading.Thread(target=loop.run_forever, daemon=True)
    t.start()
    st.session_state._bg_loop = loop

def run_sync(coro):
    future = asyncio.run_coroutine_threadsafe(coro, st.session_state._bg_loop)
    return future.result(timeout=180)

# ================== قاعدة البيانات الاحترافية ==================
class Database:
    def __init__(self, path):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS seen_files (
                channel_id INTEGER, msg_id INTEGER, fuid TEXT,
                size INTEGER, duration INTEGER, md5 TEXT, phash TEXT,
                date TEXT, type TEXT, name TEXT, PRIMARY KEY (channel_id, msg_id)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                channel_id INTEGER PRIMARY KEY, last_id INTEGER, total INTEGER, saved INTEGER
            )
        """)
        self.conn.commit()

    def save_progress(self, cid, last_id, total, saved):
        self.conn.execute("INSERT OR REPLACE INTO meta VALUES (?,?,?,?)", (cid, last_id, total, saved))
        self.conn.commit()

    def get_progress(self, cid):
        row = self.conn.execute("SELECT last_id, total, saved FROM meta WHERE channel_id=?", (cid,)).fetchone()
        return row if row else (0, 0, 0)

    def insert_file(self, cid, info):
        self.conn.execute("INSERT OR REPLACE INTO seen_files VALUES (?,?,?,?,?,?,?,?,?,?)",
                          (cid, info['id'], info['fuid'], info['size'], info['duration'], 
                           info['md5'], info['phash'], info['date'], info['type'], info['name']))
        self.conn.commit()

    def get_duplicates(self, cid, strategy):
        order = "date ASC" if strategy == "الأقدم" else "date DESC"
        cursor = self.conn.execute(f"SELECT fuid FROM seen_files WHERE channel_id=? GROUP BY fuid HAVING COUNT(*)>1", (cid,))
        dupes = []
        for row in cursor:
            group = self.conn.execute(f"SELECT msg_id, size, date, type, name FROM seen_files WHERE channel_id=? AND fuid=? ORDER BY {order}", (cid, row[0])).fetchall()
            for dup in group[1:]:
                dupes.append({"معرف": dup[0], "الحجم": dup[1], "التاريخ": dup[2], "النوع": dup[3], "الاسم": dup[4]})
        return dupes

    def clear(self, cid):
        self.conn.execute("DELETE FROM seen_files WHERE channel_id=?", (cid,))
        self.conn.execute("DELETE FROM meta WHERE channel_id=?", (cid,))
        self.conn.commit()

# ================== استخراج معلومات الملف (جراحي) ==================
async def extract_info(client, msg):
    media = msg.media
    if not media: return None
    
    info = {"id": msg.id, "fuid": None, "size": 0, "duration": 0, "mime": "", "type": "", "date": msg.date.isoformat(), "md5": None, "phash": None, "name": "ملف غير مسمى"}
    
    if isinstance(media, MessageMediaDocument):
        doc = media.document
        info["size"] = doc.size or 0
        info["mime"] = doc.mime_type or ""
        info["type"] = "video" if info["mime"].startswith("video/") else "document"
        for attr in doc.attributes:
            if isinstance(attr, DocumentAttributeVideo): info["duration"] = attr.duration or 0
            if hasattr(attr, 'file_name'): info["name"] = attr.file_name
        # بصمة فريدة (حجم + مدة) - لا تحتاج لبوت
        info["fuid"] = f"v_{info['size']}_{int(info['duration'])}"

    elif isinstance(media, MessageMediaPhoto):
        photo = media.photo
        info["type"] = "photo"
        sizes = [s for s in getattr(photo, "sizes", []) if hasattr(s, "size")]
        info["size"] = max(sizes, key=lambda s: s.size).size if sizes else 0
        info["fuid"] = f"p_{photo.id}"
        info["name"] = f"صورة_{photo.id}.jpg"
            
    return info

# ================== واجهة المستخدم ==================
with st.sidebar:
    st.html("<div class='sidebar-logo'><div class='logo-name'>DupZap PRO</div></div>")
    if st.session_state.client:
        try:
            if not st.session_state.me:
                st.session_state.me = run_sync(st.session_state.client.get_me())
            st.markdown(f"👤 **{st.session_state.me.first_name}**")
        except: pass
    
    st.divider()
    if st.button("🚪 تسجيل الخروج / إعادة تعيين"):
        st.session_state.step = 'login'
        st.rerun()

# --- 1. تسجيل الدخول ---
if st.session_state.step == 'login':
    st.subheader("🔐 الدخول للحساب")
    tab1, tab2 = st.tabs(["📱 رقم الهاتف", "🔑 Session String"])
    
    with tab1:
        with st.form("f1"):
            aid = st.text_input("API ID")
            ahash = st.text_input("API Hash")
            phone = st.text_input("رقم الهاتف")
            if st.form_submit_button("إرسال الكود", use_container_width=True, type="primary"):
                c = TelegramClient(StringSession(), int(aid), ahash)
                run_sync(c.connect())
                st.session_state.auth = run_sync(c.send_code_request(phone))
                st.session_state.client, st.session_state.api_id, st.session_state.api_hash, st.session_state.phone = c, aid, ahash, phone
                st.session_state.step = 'verify'
                st.rerun()
    with tab2:
        with st.form("f2"):
            aid = st.text_input("API ID", key="sa")
            ahash = st.text_input("API Hash", key="sh")
            ss = st.text_area("Session String")
            if st.form_submit_button("دخول مباشر", use_container_width=True, type="primary"):
                c = TelegramClient(StringSession(ss), int(aid), ahash)
                run_sync(c.connect())
                if run_sync(c.is_user_authorized()):
                    st.session_state.client, st.session_state.step = c, 'setup'
                    st.rerun()

elif st.session_state.step == 'verify':
    code = st.text_input("أدخل الكود")
    if st.button("تأكيد", type="primary"):
        run_sync(st.session_state.client.sign_in(st.session_state.phone, code, phone_code_hash=st.session_state.auth.phone_code_hash))
        st.session_state.step = 'setup'
        st.rerun()

# --- 2. الإعدادات ---
elif st.session_state.step == 'setup':
    st.subheader("📡 إعدادات القناة")
    with st.form("f3"):
        url = st.text_input("رابط القناة (عامة أو خاصة)")
        strat = st.selectbox("الاحتفاظ بـ", ["الأقدم", "الأحدث"])
        types = st.multiselect("الأنواع", ["video", "photo", "document"], default=["video", "photo"])
        if st.form_submit_button("🚀 بدء الفحص", type="primary", use_container_width=True):
            entity = run_sync(st.session_state.client.get_entity(url))
            st.session_state.channel = entity
            st.session_state.scan_params = {'strat': strat, 'types': types}
            st.session_state.step = 'scanning'
            st.rerun()

# --- 3. المسح الجاري ---
elif st.session_state.step == 'scanning':
    db = Database(st.session_state.db_path)
    cid = st.session_state.channel.id
    last_id, total, saved = db.get_progress(cid)
    
    st.subheader(f"📡 فحص: {getattr(st.session_state.channel, 'title', 'قناة خاصة')}")
    c1, c2 = st.columns(2)
    c1.metric("📊 تم فحص", total)
    c2.metric("💾 تم حفظ", saved)
    
    if st.button("⏹️ إيقاف ومعاينة النتائج", use_container_width=True, type="primary"):
        st.session_state.step = 'results'
        st.rerun()

    # تنفيذ المسح (Batch)
    async def scan():
        msgs = await st.session_state.client.get_messages(st.session_state.channel, limit=50, offset_id=last_id)
        s, v, li = 0, 0, last_id
        for m in msgs:
            s += 1
            li = m.id
            info = await extract_info(st.session_state.client, m)
            if info and info['type'] in st.session_state.scan_params['types']:
                db.insert_file(cid, info)
                v += 1
        return li, s, v

    res_last, inc_s, inc_v = run_sync(scan())
    db.save_progress(cid, res_last, total + inc_s, saved + inc_v)
    
    if inc_s == 0:
        st.success("✅ اكتمل الفحص!")
        st.session_state.step = 'results'
    time.sleep(1)
    st.rerun()

# --- 4. النتائج والحذف ---
elif st.session_state.step == 'results':
    db = Database(st.session_state.db_path)
    dupes = db.get_duplicates(st.session_state.channel.id, st.session_state.scan_params['strat'])
    
    st.subheader(f"📋 المكررات ({len(dupes)})")
    if not dupes:
        st.success("🎉 القناة نظيفة!")
        if st.button("🔄 قناة أخرى"): st.session_state.step = 'setup'; st.rerun()
    else:
        df = pd.DataFrame(dupes)
        df['حذف'] = False
        edit = st.data_editor(df, use_container_width=True)
        
        ids = edit[edit['حذف'] == True]['معرف'].tolist()
        if st.button(f"🗑️ حذف {len(ids)} ملف", type="primary", use_container_width=True):
            run_sync(st.session_state.client.delete_messages(st.session_state.channel, ids))
            st.success("✅ تم الحذف!"); st.rerun()
            
        if st.button("🧹 تصفير بيانات القناة"):
            db.clear(st.session_state.channel.id)
            st.session_state.step = 'setup'; st.rerun()

st.html("<div class='footer-bar'>صُنع بواسطة <strong>F.ALSALEH</strong> · DupZap v4.0 PRO</div>")
