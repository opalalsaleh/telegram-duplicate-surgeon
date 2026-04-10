import streamlit as st
import asyncio
import gc
import hashlib
import io
import sqlite3
import tempfile
import time
import threading
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
try:
    import imagehash
    _HAS_IMAGEHASH = True
except ImportError:
    _HAS_IMAGEHASH = False

# ================== تهيئة الصفحة ==================
st.set_page_config(page_title="DupZap v4.0 – الحساب الشخصي", page_icon="✂️", layout="wide")

# ================== التنسيق العام (CSS الاحترافي) ==================
st.html("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Arabic:wght@300;400;500;600;700&display=swap');
    html, body, [class*="css"], .stApp { font-family: 'IBM Plex Sans Arabic', sans-serif; }
    .stApp { background: #f8fafc; }
    [data-testid="stSidebar"] { background: #0f172a !important; }
    [data-testid="stSidebar"] * { color: #cbd5e1 !important; }
    .sidebar-logo { text-align: center; padding: 20px; border-bottom: 1px solid #1e293b; margin-bottom: 10px; }
    .sidebar-logo .logo-name { font-size: 1.45rem; font-weight: 700; background: linear-gradient(90deg, #38bdf8 0%, #818cf8 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
    .stButton > button { border-radius: 9px; font-weight: 600; min-height: 42px; transition: all 0.2s; border: 1.5px solid #e2e8f0 !important; }
    .stButton > button[kind="primary"] { background: linear-gradient(135deg, #0ea5e9 0%, #6366f1 100%) !important; color: white !important; border: none !important; }
    [data-testid="metric-container"] { background: white; border-radius: 14px; padding: 15px; border: 1px solid #e2e8f0; box-shadow: 0 1px 4px rgba(0,0,0,0.05); }
    .footer-bar { text-align: center; padding: 20px; color: #94a3b8; font-size: 0.8rem; margin-top: 40px; border-top: 1px solid #f1f5f9; }
</style>
""")

# ================== إدارة الـ Async Loop ==================
if '_bg_loop' not in st.session_state:
    loop = asyncio.new_event_loop()
    t = threading.Thread(target=loop.run_forever, daemon=True)
    t.start()
    st.session_state._bg_loop = loop

def run_sync(coro):
    future = asyncio.run_coroutine_threadsafe(coro, st.session_state._bg_loop)
    return future.result(timeout=180)

# ================== الثوابت ==================
BATCH_SCAN_SIZE   = 50
BATCH_DELETE_SIZE = 25
PAGE_SIZE         = 50

# ================== دوال مساعدة ==================
def fmt_size(n: int) -> str:
    if n == 0: return "0 B"
    for u in ("B", "KB", "MB", "GB"):
        if n < 1024: return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} GB"

async def _get_entity(client, channel_input):
    # دعم الروابط الخاصة والعامة بالحساب الشخصي
    return await client.get_entity(channel_input)

# ================== استخراج معلومات الملف (المحرك الجديد) ==================
async def extract_file_info_async(client, msg, compute_md5: bool, compute_phash: bool) -> Optional[Dict]:
    media = msg.media
    if not media: return None
    
    info = {
        "id": msg.id, "fuid": None, "size": 0, "duration": 0,
        "mime": "", "type": "", "date": msg.date.isoformat(),
        "md5": None, "phash": None, "name": None
    }
    
    if isinstance(media, MessageMediaDocument):
        doc = media.document
        info["size"] = doc.size or 0
        info["mime"] = doc.mime_type or ""
        info["type"] = "video" if info["mime"].startswith("video/") else "document"
        for attr in doc.attributes:
            if isinstance(attr, DocumentAttributeVideo): info["duration"] = attr.duration or 0
            if hasattr(attr, 'file_name'): info["name"] = attr.file_name
        
        # صناعة بصمة فريدة (حجم + مدة) - لا تتأثر بـ Bot API
        if info["duration"] > 0:
            info["fuid"] = f"v_{info['size']}_{int(info['duration'])}"
        else:
            info["fuid"] = f"d_{doc.id}"

    elif isinstance(media, MessageMediaPhoto):
        photo = media.photo
        info["type"] = "photo"
        sizes = [s for s in getattr(photo, "sizes", []) if hasattr(s, "size")]
        info["size"] = max(sizes, key=lambda s: s.size).size if sizes else 0
        info["fuid"] = f"p_{photo.id}"

    # حساب الهاش عند الطلب للملفات الصغيرة فقط
    if (compute_md5 or compute_phash) and info["size"] < 10 * 1024 * 1024:
        try:
            data = await client.download_media(msg, file=bytes, thumb=-1 if compute_phash else None)
            if data:
                if compute_md5: info["md5"] = hashlib.md5(data).hexdigest()
                if compute_phash and _HAS_IMAGEHASH and info["type"] in ("photo", "image"):
                    with io.BytesIO(data) as bio:
                        img = Image.open(bio)
                        info["phash"] = str(imagehash.phash(img))
        except: pass
    
    return info

# ================== قاعدة البيانات المحسنة ==================
class Database:
    def __init__(self, path):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init_tables()

    def _init_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS seen_files (
                channel_id INTEGER, msg_id INTEGER, fuid TEXT,
                size INTEGER, duration INTEGER, md5 TEXT, phash TEXT,
                date TEXT, type TEXT, name TEXT, PRIMARY KEY (channel_id, msg_id)
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS resume_meta (
                channel_id INTEGER PRIMARY KEY, last_id INTEGER, total INTEGER, saved INTEGER
            )
        """)
        self.conn.commit()

    def save_progress(self, cid, last_id, total, saved):
        self.conn.execute("INSERT OR REPLACE INTO resume_meta VALUES (?,?,?,?)", (cid, last_id, total, saved))
        self.conn.commit()

    def get_resume(self, cid):
        row = self.conn.execute("SELECT last_id, total, saved FROM resume_meta WHERE channel_id=?", (cid,)).fetchone()
        return row if row else (0, 0, 0)

    def insert_file(self, cid, info):
        self.conn.execute("INSERT OR REPLACE INTO seen_files VALUES (?,?,?,?,?,?,?,?,?,?)",
                          (cid, info['id'], info['fuid'], info['size'], info['duration'], 
                           info['md5'], info['phash'], info['date'], info['type'], info['name']))
        self.conn.commit()

    def get_duplicates(self, cid, strategy):
        order = {"الأقدم": "date ASC", "الأحدث": "date DESC"}[strategy]
        # تجميع حسب البصمة الفريدة (FUID)
        cursor = self.conn.execute(f"SELECT fuid FROM seen_files WHERE channel_id=? GROUP BY fuid HAVING COUNT(*)>1", (cid,))
        duplicates = []
        for row in cursor:
            group = self.conn.execute(f"SELECT msg_id, size, date, type, name FROM seen_files WHERE channel_id=? AND fuid=? ORDER BY {order}", (cid, row[0])).fetchall()
            keeper = group[0]
            for dup in group[1:]:
                duplicates.append({"id": dup[0], "size": dup[1], "date": dup[2], "type": dup[3], "name": dup[4], "keeper_id": keeper[0]})
        return duplicates

    def clear_channel(self, cid):
        self.conn.execute("DELETE FROM seen_files WHERE channel_id=?", (cid,))
        self.conn.execute("DELETE FROM resume_meta WHERE channel_id=?", (cid,))
        self.conn.commit()

# ================== حالة الجلسة ==================
defaults = {
    'client': None, 'step': 'login', 'db_path': 'dupzap_v4.db', 'channel': None,
    'scan_params': {}, 'page': 0, 'selected_ids': set(), 'total_scanned': 0, 
    'files_saved': 0, 'api_id': '', 'api_hash': '', 'session_string': None, 'auto_scan': False
}
for k, v in defaults.items():
    if k not in st.session_state: st.session_state[k] = v

# ================== الشريط الجانبي ==================
with st.sidebar:
    st.html("<div class='sidebar-logo'><div class='logo-name'>DupZap v4.0</div><div style='color:#64748b; font-size:0.7rem;'>PRO ACCOUNT EDITION</div></div>")
    
    if st.session_state.client:
        if st.button("🚪 تسجيل الخروج", use_container_width=True):
            st.session_state.step = 'login'
            st.rerun()
            
    if st.session_state.channel:
        st.info(f"📢 القناة: {getattr(st.session_state.channel, 'title', 'خاصة')}")

# ================== الصفحة الرئيسية ==================
st.title("✂️ DupZap: مزيل المكررات الجراحي")

# --- الخطوة 1: تسجيل الدخول (الهاتف أو الـ Session) ---
if st.session_state.step == 'login':
    tab1, tab2 = st.tabs(["📱 رقم الهاتف", "🔑 Session String"])
    
    with tab1:
        with st.form("login_phone"):
            api_id = st.text_input("API ID")
            api_hash = st.text_input("API Hash")
            phone = st.text_input("رقم الهاتف")
            if st.form_submit_button("إرسال الكود", use_container_width=True, type="primary"):
                client = TelegramClient(StringSession(), int(api_id), api_hash)
                run_sync(client.connect())
                st.session_state.auth = run_sync(client.send_code_request(phone))
                st.session_state.client = client
                st.session_state.api_id = api_id
                st.session_state.api_hash = api_hash
                st.session_state.phone = phone
                st.session_state.step = 'verify'
                st.rerun()

    with tab2:
        with st.form("login_session"):
            api_id = st.text_input("API ID")
            api_hash = st.text_input("API Hash")
            sess_str = st.text_area("Session String")
            if st.form_submit_button("دخول مباشر", use_container_width=True, type="primary"):
                client = TelegramClient(StringSession(sess_str), int(api_id), api_hash)
                run_sync(client.connect())
                if run_sync(client.is_user_authorized()):
                    st.session_state.client = client
                    st.session_state.step = 'setup'
                    st.rerun()
                else: st.error("الجلسة غير صالحة")

elif st.session_state.step == 'verify':
    code = st.text_input("أدخل الكود")
    if st.button("تأكيد", type="primary"):
        run_sync(st.session_state.client.sign_in(st.session_state.phone, code, phone_code_hash=st.session_state.auth.phone_code_hash))
        st.session_state.session_string = st.session_state.client.session.save()
        st.session_state.step = 'setup'
        st.rerun()

# --- الخطوة 2: إعدادات القناة ---
elif st.session_state.step == 'setup':
    with st.form("setup_form"):
        url = st.text_input("رابط القناة (عامة أو خاصة t.me/+...)")
        strategy = st.selectbox("الاحتفاظ بـ", ["الأقدم", "الأحدث"])
        types = st.multiselect("الأنواع", ["video", "photo", "document"], default=["video", "photo"])
        st.markdown("---")
        c_md5 = st.checkbox("استخدام MD5 (أكثر دقة للملفات الصغيرة)")
        c_ph = st.checkbox("استخدام pHash (كشف تشابه الصور البصري)")
        
        if st.form_submit_button("🚀 بدء المسح", type="primary", use_container_width=True):
            entity = run_sync(_get_entity(st.session_state.client, url))
            st.session_state.channel = entity
            st.session_state.scan_params = {'strategy': strategy, 'types': types, 'md5': c_md5, 'phash': c_ph}
            st.session_state.step = 'scanning'
            st.rerun()

# --- الخطوة 3: المسح الجاري ---
elif st.session_state.step == 'scanning':
    cid = st.session_state.channel.id
    db = Database(st.session_state.db_path)
    last_id, total, saved = db.get_resume(cid)
    
    st.subheader("📡 جاري فحص القناة...")
    col1, col2 = st.columns(2)
    col1.metric("تم فحص", total)
    col2.metric("تم حفظ", saved)
    
    prog = st.progress(0)
    
    if st.button("⏹️ إيقاف ومعاينة النتائج", type="primary", use_container_width=True):
        st.session_state.step = 'results'
        st.rerun()

    # محرك المسح
    client = st.session_state.client
    params = st.session_state.scan_params
    
    async def scan_batch():
        scanned_count = 0
        saved_count = 0
        curr_last = last_id
        async for msg in client.iter_messages(st.session_state.channel, offset_id=last_id, limit=BATCH_SCAN_SIZE, reverse=False):
            scanned_count += 1
            curr_last = msg.id
            info = await extract_file_info_async(client, msg, params['md5'], params['phash'])
            if info and info['type'] in params['types']:
                db.insert_file(cid, info)
                saved_count += 1
        return curr_last, scanned_count, saved_count

    res_last, s_inc, v_inc = run_sync(scan_batch())
    db.save_progress(cid, res_last, total + s_inc, saved + v_inc)
    
    if s_inc == 0:
        st.success("✅ اكتمل مسح القناة بالكامل!")
        if st.button("📋 عرض المكررات"):
            st.session_state.step = 'results'
            st.rerun()
    else:
        time.sleep(1)
        st.rerun()

# --- الخطوة 4: النتائج والحذف ---
elif st.session_state.step == 'results':
    db = Database(st.session_state.db_path)
    dupes = db.get_duplicates(st.session_state.channel.id, st.session_state.scan_params['strategy'])
    
    st.subheader(f"📋 المكررات المكتشفة ({len(dupes)})")
    
    if not dupes:
        st.success("🎉 القناة نظيفة، لا يوجد مكررات!")
        if st.button("🔄 فحص قناة أخرى"):
            st.session_state.step = 'setup'
            st.rerun()
    else:
        df = pd.DataFrame(dupes)
        df['حجم'] = df['size'].apply(fmt_size)
        df['حذف'] = False
        
        edited_df = st.data_editor(df[['id', 'name', 'حجم', 'date', 'type', 'حذف']], use_container_width=True)
        
        to_delete = edited_df[edited_df['حذف'] == True]['id'].tolist()
        
        col1, col2 = st.columns(2)
        if col1.button(f"🗑️ حذف {len(to_delete)} ملف محدد", type="primary", use_container_width=True):
            run_sync(st.session_state.client.delete_messages(st.session_state.channel, to_delete))
            st.success("✅ تم الحذف بنجاح!")
            st.rerun()
            
        if col2.button("🧹 مسح بيانات هذه القناة والبدء من الصفر", use_container_width=True):
            db.clear_channel(st.session_state.channel.id)
            st.session_state.step = 'setup'
            st.rerun()

st.html("<div class='footer-bar'>صُنع بواسطة <strong>F.ALSALEH</strong> · DupZap v4.0 PRO</div>")
