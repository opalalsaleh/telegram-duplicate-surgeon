import streamlit as st
import asyncio
import gc
import hashlib
import io
import sqlite3
import tempfile
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
from telethon import TelegramClient
from telethon.errors import FloodWaitError, SessionPasswordNeededError
from telethon.tl.types import (
    MessageMediaDocument, MessageMediaPhoto, DocumentAttributeVideo,
    Message, Channel, Chat, PhotoSize
)
from PIL import Image
try:
    import imagehash
    _HAS_IMAGEHASH = True
except ImportError:
    _HAS_IMAGEHASH = False

# ================== إدارة Event Loop آمنة ==================
def run_async(coro):
    """تشغيل coroutine بأمان في Streamlit بإنشاء event loop جديد كل مرة."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

# ================== تهيئة الصفحة ==================
st.set_page_config(page_title="Telegram Duplicate Surgeon", page_icon="🦖", layout="wide")

st.html("""
<style>
    .stApp { background: linear-gradient(135deg, #f5f7fa 0%, #e9ecf2 100%); }
    .stButton > button { border-radius: 12px; font-weight: 600; min-height: 48px; }
    .stButton > button[kind="primary"] { background: linear-gradient(135deg, #10b981 0%, #059669 100%) !important; color: white !important; }
    .footer { width: 100%; text-align: center; padding: 16px; color: #64748b; margin-top: 30px; }
    [data-testid="metric-container"] { background-color: #ffffff; border-radius: 16px; padding: 16px; box-shadow: 0 4px 12px rgba(0,0,0,0.08); }
</style>
""")

# ================== الثوابت ==================
BATCH_SCAN_SIZE = 50
BATCH_DELETE_SIZE = 25
PHASH_SIZE_LIMIT = 5 * 1024 * 1024
MD5_SIZE_LIMIT = 5 * 1024 * 1024
PAGE_SIZE = 50

# ================== دوال مساعدة ==================
def fmt_size(size_bytes: int) -> str:
    if size_bytes == 0: return "0 B"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size_bytes < 1024: return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"

def get_thumb(media):
    if isinstance(media, MessageMediaPhoto):
        if not media.photo: return None
        for size in media.photo.sizes:
            if isinstance(size, PhotoSize) and hasattr(size, 'type') and size.type == 'm': return size
        sizes = [s for s in media.photo.sizes if hasattr(s, 'size')]
        return min(sizes, key=lambda s: s.size) if sizes else None
    elif isinstance(media, MessageMediaDocument):
        doc = media.document
        if doc and doc.thumbs: return min(doc.thumbs, key=lambda t: getattr(t, 'size', 0))
    return None

async def compute_hashes_async(client, msg, info, compute_md5, compute_phash):
    md5 = phash = None
    if not (compute_md5 or compute_phash): return md5, phash
    media = msg.media
    thumb = get_thumb(media) if compute_phash else None
    data = None
    try:
        if thumb: data = await client.download_media(thumb, file=bytes)
        else: data = await client.download_media(msg, file=bytes, size=PHASH_SIZE_LIMIT if compute_phash else None)
        if compute_md5 and info["size"] <= MD5_SIZE_LIMIT: md5 = hashlib.md5(data).hexdigest()
        if compute_phash and _HAS_IMAGEHASH:
            try:
                with io.BytesIO(data) as bio:
                    with Image.open(bio) as img: phash = str(imagehash.phash(img))
            except: pass
    except: pass
    finally:
        del data
        gc.collect()
    return md5, phash

async def extract_file_info_async(client, msg, compute_md5, compute_phash):
    media = msg.media
    if not media: return None
    info = {"id": msg.id, "file_id": None, "size": 0, "duration": 0, "mime": "", "type": "", "date": msg.date.isoformat(), "md5": None, "phash": None, "views": msg.views or 0, "name": None}
    if isinstance(media, MessageMediaDocument):
        doc = media.document
        info["file_id"] = f"{doc.id}:{doc.dc_id}"
        info["size"] = doc.size or 0
        info["mime"] = doc.mime_type or ""
        info["type"] = "video" if info["mime"].startswith("video/") else "image" if info["mime"].startswith("image/") else "document"
        for attr in doc.attributes:
            if isinstance(attr, DocumentAttributeVideo): info["duration"] = attr.duration or 0
            if hasattr(attr, 'file_name'): info["name"] = attr.file_name
    elif isinstance(media, MessageMediaPhoto):
        photo = media.photo
        info["file_id"] = f"{photo.id}:{photo.dc_id}"
        info["type"] = "photo"
        info["mime"] = "image/jpeg"
        sizes = getattr(photo, "sizes", [])
        candidates = [s for s in sizes if hasattr(s, "size") and s.size > 0]
        info["size"] = max(candidates, key=lambda s: s.size).size if candidates else 0
    else: return None
    info["md5"], info["phash"] = await compute_hashes_async(client, msg, info, compute_md5, compute_phash)
    return info

async def get_messages_async(client, channel, offset_id, limit):
    return await client.get_messages(channel, limit=limit, offset_id=offset_id)

async def delete_messages_async(client, channel, batch_ids):
    return await client.delete_messages(channel, batch_ids)

# ================== قاعدة البيانات ==================
class Database:
    def __init__(self, path):
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init_tables()
    def _init_tables(self):
        self.conn.execute("CREATE TABLE IF NOT EXISTS seen_files (channel_id INTEGER, msg_id INTEGER, file_id TEXT, file_size INTEGER, duration INTEGER, md5_hash TEXT, phash TEXT, msg_date TEXT, file_type TEXT, mime_type TEXT, views INTEGER, file_name TEXT, PRIMARY KEY (channel_id, msg_id)) WITHOUT ROWID")
        self.conn.execute("CREATE TABLE IF NOT EXISTS resume_meta (channel_id INTEGER PRIMARY KEY, last_scanned_id INTEGER DEFAULT 0, total_scanned INTEGER DEFAULT 0, files_saved INTEGER DEFAULT 0)")
        self.conn.commit()
    def get_resume_state(self, channel_id):
        row = self.conn.execute("SELECT last_scanned_id, total_scanned, files_saved FROM resume_meta WHERE channel_id=?", (channel_id,)).fetchone()
        return (row[0], row[1], row[2]) if row else (0, 0, 0)
    def save_progress(self, channel_id, last_id, total_scanned, files_saved):
        self.conn.execute("INSERT OR REPLACE INTO resume_meta VALUES (?,?,?,?)", (channel_id, last_id, total_scanned, files_saved))
        self.conn.commit()
    def buffer_insert(self, record):
        self.conn.execute("INSERT OR REPLACE INTO seen_files VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", record)
        self.conn.commit()
    def stream_duplicates(self, channel_id, keep_strategy, min_size=0, limit=None, offset=0):
        order = {"oldest": "msg_date ASC", "newest": "msg_date DESC", "largest": "file_size DESC"}[keep_strategy]
        query = f"SELECT file_id FROM seen_files WHERE channel_id=? AND file_size>=? GROUP BY file_id HAVING COUNT(*)>1 LIMIT {limit} OFFSET {offset}" if limit else f"SELECT file_id FROM seen_files WHERE channel_id=? AND file_size>=? GROUP BY file_id HAVING COUNT(*)>1"
        cursor = self.conn.execute(query, (channel_id, min_size))
        duplicates = []
        for row in cursor:
            group = self.conn.execute(f"SELECT msg_id, file_size, msg_date, file_id, duration, phash, file_type, mime_type, file_name FROM seen_files WHERE channel_id=? AND file_id=? ORDER BY {order}", (channel_id, row[0])).fetchall()
            keeper = group[0]
            for dup in group[1:]:
                duplicates.append({"id": dup[0], "size": dup[1], "date": dup[2], "file_id": dup[3], "duration": dup[4], "phash": dup[5], "type": dup[6], "mime": dup[7], "name": dup[8], "keeper_id": keeper[0]})
        return duplicates
    def clear_channel(self, channel_id):
        self.conn.execute("DELETE FROM seen_files WHERE channel_id=?", (channel_id,))
        self.conn.execute("DELETE FROM resume_meta WHERE channel_id=?", (channel_id,))
        self.conn.commit()
    def close(self): self.conn.close()

# ================== حالة الجلسة ==================
if 'client' not in st.session_state: st.session_state.client = None
if 'step' not in st.session_state: st.session_state.step = 'login'
if 'db_path' not in st.session_state: st.session_state.db_path = None
if 'channel' not in st.session_state: st.session_state.channel = None
if 'scan_params' not in st.session_state: st.session_state.scan_params = {}
if 'page' not in st.session_state: st.session_state.page = 0
if 'selected_ids' not in st.session_state: st.session_state.selected_ids = set()
if 'auto_mode' not in st.session_state: st.session_state.auto_mode = False
if 'total_scanned' not in st.session_state: st.session_state.total_scanned = 0
if 'files_saved' not in st.session_state: st.session_state.files_saved = 0

# ================== دوال تسجيل الدخول ==================
async def async_login(api_id, api_hash, phone):
    client = TelegramClient("streamlit_session", api_id, api_hash)
    await client.connect()
    if not await client.is_user_authorized():
        await client.send_code_request(phone)
    return client

async def async_verify(client, phone, code, password):
    try:
        await client.sign_in(phone, code)
        return True, None
    except SessionPasswordNeededError:
        if password:
            await client.sign_in(password=password)
            return True, None
        return False, "2FA"
    except Exception as e:
        return False, str(e)

# ================== واجهة المستخدم ==================
st.title("🦖 Telegram Duplicate Surgeon")

if st.session_state.step == 'login':
    with st.form("login_form"):
        st.subheader("🔐 تسجيل الدخول إلى تيليجرام")
        api_id = st.text_input("API ID*", type="password")
        api_hash = st.text_input("API Hash*", type="password")
        phone = st.text_input("رقم الهاتف*", placeholder="+963xxxxxxxxx")
        if st.form_submit_button("إرسال رمز التحقق"):
            if not api_id or not api_hash or not phone:
                st.error("جميع الحقول مطلوبة")
            else:
                try:
                    client = run_async(async_login(int(api_id), api_hash, phone))
                    st.session_state.client = client
                    st.session_state.phone = phone
                    st.session_state.step = 'verify_code'
                    st.rerun()
                except Exception as e:
                    st.error(f"خطأ: {e}")

elif st.session_state.step == 'verify_code':
    with st.form("verify_form"):
        st.subheader("📲 تأكيد الحساب")
        code = st.text_input("رمز OTP*")
        password = st.text_input("كلمة مرور 2FA (إن وجدت)", type="password")
        if st.form_submit_button("تأكيد"):
            client = st.session_state.client
            success, error = run_async(async_verify(client, st.session_state.phone, code, password))
            if success:
                st.session_state.step = 'channel'
                st.rerun()
            elif error == "2FA":
                st.error("الحساب محمي بكلمة مرور، الرجاء إدخالها")
            else:
                st.error(f"رمز غير صحيح: {error}")

elif st.session_state.step == 'channel':
    st.success("✅ تم تسجيل الدخول")
    with st.form("channel_form"):
        channel_input = st.text_input("رابط القناة*", placeholder="@username")
        media_types = st.multiselect("أنواع الملفات", ["photo", "video", "document"], default=["photo", "video"])
        keep_strategy = st.selectbox("استراتيجية الاحتفاظ", ["oldest", "newest", "largest"])
        dry_run = st.checkbox("وضع المعاينة", True)
        min_size_mb = st.number_input("الحد الأدنى للحجم (MB)", 0.0, 10000.0, 0.0)
        auto_mode = st.toggle("الوضع الآلي", False)
        uploaded_db = st.file_uploader("رفع قاعدة بيانات سابقة", type=['db'])
        if uploaded_db:
            temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
            temp_db.write(uploaded_db.getbuffer())
            st.session_state.db_path = temp_db.name
            st.success("✅ تم التحميل")
        if st.form_submit_button("🚀 بدء المسح"):
            if not channel_input: st.error("أدخل رابط القناة")
            else:
                try:
                    client = st.session_state.client
                    entity = run_async(client.get_entity(channel_input))
                    st.session_state.channel = entity
                    st.session_state.scan_params = {'media_types': media_types, 'keep_strategy': keep_strategy, 'dry_run': dry_run, 'min_size_mb': min_size_mb}
                    st.session_state.auto_mode = auto_mode
                    if st.session_state.db_path is None:
                        temp_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
                        st.session_state.db_path = temp_file.name
                    db = Database(st.session_state.db_path)
                    last_id, total_scanned, files_saved = db.get_resume_state(entity.id)
                    st.session_state.total_scanned = total_scanned
                    st.session_state.files_saved = files_saved
                    db.close()
                    st.session_state.step = 'scanning'
                    st.rerun()
                except Exception as e:
                    st.error(f"خطأ: {e}")

elif st.session_state.step == 'scanning':
    params = st.session_state.scan_params
    channel = st.session_state.channel
    db = Database(st.session_state.db_path)
    last_id, _, _ = db.get_resume_state(channel.id)
    offset_id = 0 if last_id == 0 else last_id + 1
    
    col1, col2 = st.columns(2)
    with col1: st.metric("📊 تم فحص", st.session_state.total_scanned)
    with col2: st.metric("💾 تم حفظ", st.session_state.files_saved)
    
    if st.button("فحص الدفعة التالية" if not st.session_state.auto_mode else "▶️ استمرار آلي", type="primary"):
        client = st.session_state.client
        progress = st.progress(0)
        try:
            messages = run_async(get_messages_async(client, channel, offset_id, BATCH_SCAN_SIZE))
            scanned = saved = 0
            last_id = offset_id
            for i, msg in enumerate(messages):
                if not msg: continue
                scanned += 1
                last_id = msg.id
                progress.progress((i+1)/len(messages))
                if not msg.media: continue
                info = run_async(extract_file_info_async(client, msg, False, False))
                if not info or info['type'] not in params['media_types'] or info['size'] < params['min_size_mb']*1024*1024: continue
                saved += 1
                db.buffer_insert((channel.id, info['id'], info['file_id'], info['size'], info['duration'], info['md5'], info['phash'], info['date'], info['type'], info['mime'], info['views'], info['name']))
            st.session_state.total_scanned += scanned
            st.session_state.files_saved += saved
            db.save_progress(channel.id, last_id, st.session_state.total_scanned, st.session_state.files_saved)
            progress.progress(1.0)
            if st.session_state.auto_mode and scanned == BATCH_SCAN_SIZE:
                time.sleep(1)
                st.rerun()
        except FloodWaitError as e:
            st.warning(f"انتظار {e.seconds} ثانية")
        except Exception as e:
            st.error(f"خطأ: {e}")
        finally:
            db.close()
    
    if st.button("📋 عرض المكررات"):
        st.session_state.step = 'results'
        st.rerun()
    
    with open(st.session_state.db_path, "rb") as f:
        st.download_button("📥 تحميل قاعدة البيانات", f, file_name=f"scan_{channel.id}.db")

elif st.session_state.step == 'results':
    params = st.session_state.scan_params
    channel = st.session_state.channel
    db = Database(st.session_state.db_path)
    duplicates = db.stream_duplicates(channel.id, params['keep_strategy'], int(params['min_size_mb']*1024*1024))
    
    if not duplicates:
        st.success("🎉 لا توجد مكررات!")
    else:
        st.warning(f"🔔 {len(duplicates)} مكرر")
        page = st.session_state.page
        page_duplicates = duplicates[page*PAGE_SIZE:(page+1)*PAGE_SIZE]
        df = pd.DataFrame([{"معرف": d['id'], "النوع": d['type'], "الحجم": fmt_size(d['size']), "تحديد": False} for d in page_duplicates])
        edited = st.data_editor(df, column_config={"تحديد": st.column_config.CheckboxColumn("🗑️")}, hide_index=True)
        for sid in edited[edited["تحديد"] == True]["معرف"].tolist(): st.session_state.selected_ids.add(sid)
        
        if st.button("🗑️ حذف المحدد", type="primary"):
            if params['dry_run']:
                st.info(f"معاينة: حذف {len(st.session_state.selected_ids)}")
            else:
                ids = list(st.session_state.selected_ids)
                prog = st.progress(0)
                deleted = 0
                for i in range(0, len(ids), BATCH_DELETE_SIZE):
                    try:
                        run_async(delete_messages_async(st.session_state.client, channel, ids[i:i+BATCH_DELETE_SIZE]))
                        deleted += len(ids[i:i+BATCH_DELETE_SIZE])
                    except FloodWaitError as e:
                        st.warning(f"انتظار {e.seconds}s")
                        time.sleep(e.seconds)
                    prog.progress((i+len(ids[i:i+BATCH_DELETE_SIZE]))/len(ids))
                st.success(f"✅ تم حذف {deleted} رسالة")
                st.session_state.selected_ids.clear()
    db.close()

st.markdown("<div class='footer'>تم التطوير بواسطة <strong>F.ALSALEH</strong></div>", unsafe_allow_html=True)
