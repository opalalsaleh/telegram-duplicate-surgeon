import streamlit as st
import gc
import hashlib
import io
import sqlite3
import tempfile
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
from telethon.sync import TelegramClient
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

# ================== تهيئة الصفحة ==================
st.set_page_config(
    page_title="Telegram Duplicate Surgeon",
    page_icon="🦖",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
    .stApp { background-color: #f8fafc; }
    .stProgress > div > div > div > div { background-color: #10b981; border-radius: 10px; }
    .stDataFrame tbody tr:hover { background-color: #e2e8f0; }
    .stButton > button {
        border-radius: 10px;
        font-weight: 600;
        transition: all 0.2s;
        border: none;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        min-height: 48px;
    }
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.15);
    }
    .footer {
        width: 100%;
        background-color: #ffffff;
        text-align: center;
        padding: 12px;
        font-size: 14px;
        color: #64748b;
        border-top: 1px solid #e2e8f0;
        margin-top: 20px;
    }
    @media (max-width: 768px) {
        .stButton > button {
            min-height: 52px;
            font-size: 16px;
        }
        .stDataFrame {
            font-size: 12px;
        }
        .main .block-container {
            padding-bottom: 80px;
        }
    }
    h1, h2, h3 { color: #0f172a; }
    .stAlert { border-radius: 12px; }
</style>
""", unsafe_allow_html=True)

# ================== الثوابت والإعدادات ==================
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

def get_file_unique_id(msg):
    """استخراج file_unique_id من الرسالة (ثابت للملف حتى لو أعيد رفعه)"""
    media = msg.media
    if not media: return None
    if isinstance(media, MessageMediaDocument):
        return str(media.document.id)
    elif isinstance(media, MessageMediaPhoto):
        return str(media.photo.id)
    return None

def compute_hashes(client: TelegramClient, msg: Message, info: Dict,
                   compute_md5: bool, compute_phash: bool) -> Tuple[Optional[str], Optional[str]]:
    md5 = None
    phash = None
    if not (compute_md5 or compute_phash):
        return md5, phash

    media = msg.media
    thumb = get_thumb(media) if compute_phash else None
    data = None
    try:
        if thumb:
            data = client.download_media(thumb, file=bytes)
        else:
            limit = PHASH_SIZE_LIMIT if compute_phash else None
            data = client.download_media(msg, file=bytes, size=limit)

        if compute_md5 and info["size"] <= MD5_SIZE_LIMIT:
            md5 = hashlib.md5(data).hexdigest()

        if compute_phash and _HAS_IMAGEHASH:
            try:
                with io.BytesIO(data) as bio:
                    with Image.open(bio) as img:
                        phash = str(imagehash.phash(img))
            except Exception:
                pass
    except Exception:
        pass
    finally:
        del data
        gc.collect()
    return md5, phash

def extract_file_info(client: TelegramClient, msg: Message,
                      compute_md5: bool, compute_phash: bool) -> Optional[Dict]:
    media = msg.media
    if not media:
        return None
    info = {
        "id": msg.id, "file_id": None, "size": 0, "duration": 0,
        "mime": "", "type": "", "date": msg.date.isoformat(),
        "md5": None, "phash": None, "views": msg.views or 0, "name": None,
        "file_unique_id": None  # 🆕
    }
    info["file_unique_id"] = get_file_unique_id(msg)  # 🆕

    if isinstance(media, MessageMediaDocument):
        doc = media.document
        info["file_id"] = f"{doc.id}:{doc.dc_id}"
        info["size"] = doc.size or 0
        info["mime"] = doc.mime_type or ""
        if info["mime"].startswith("video/"):
            info["type"] = "video"
        elif info["mime"].startswith("image/"):
            info["type"] = "image"
        else:
            info["type"] = "document"
        for attr in doc.attributes:
            if isinstance(attr, DocumentAttributeVideo):
                info["duration"] = attr.duration or 0
            if hasattr(attr, 'file_name') and attr.file_name:
                info["name"] = attr.file_name
    elif isinstance(media, MessageMediaPhoto):
        photo = media.photo
        info["file_id"] = f"{photo.id}:{photo.dc_id}"
        info["type"] = "photo"
        info["mime"] = "image/jpeg"
        sizes = getattr(photo, "sizes", [])
        candidates = [s for s in sizes if hasattr(s, "size") and s.size > 0]
        info["size"] = max(candidates, key=lambda s: s.size).size if candidates else 0
    else:
        return None

    info["md5"], info["phash"] = compute_hashes(client, msg, info, compute_md5, compute_phash)
    return info

# ================== قاعدة البيانات (مُحدّثة بدعم file_unique_id) ==================
class Database:
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA cache_size=-64000")
        self._init_tables()

    def _init_tables(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS seen_files (
                channel_id INTEGER NOT NULL,
                msg_id INTEGER NOT NULL,
                file_id TEXT,
                file_unique_id TEXT,
                file_size INTEGER DEFAULT 0,
                duration INTEGER DEFAULT 0,
                md5_hash TEXT,
                phash TEXT,
                msg_date TEXT,
                file_type TEXT,
                mime_type TEXT,
                views INTEGER DEFAULT 0,
                file_name TEXT,
                PRIMARY KEY (channel_id, msg_id)
            ) WITHOUT ROWID
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS resume_meta (
                channel_id INTEGER PRIMARY KEY,
                last_scanned_id INTEGER DEFAULT 0,
                total_scanned INTEGER DEFAULT 0,
                files_saved INTEGER DEFAULT 0,
                updated_at TEXT
            )
        """)
        # إضافة العمود إذا لم يكن موجودًا (لترقية قاعدة بيانات قديمة)
        self._add_column_if_not_exists("seen_files", "file_unique_id", "TEXT")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_file_id ON seen_files(channel_id, file_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_file_unique_id ON seen_files(channel_id, file_unique_id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_phash ON seen_files(channel_id, phash)")
        self.conn.commit()

    def _add_column_if_not_exists(self, table: str, column: str, definition: str):
        cur = self.conn.execute(f"PRAGMA table_info({table})")
        columns = [row[1] for row in cur.fetchall()]
        if column not in columns:
            self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def get_resume_state(self, channel_id: int) -> Tuple[int, int, int]:
        row = self.conn.execute(
            "SELECT last_scanned_id, total_scanned, files_saved FROM resume_meta WHERE channel_id=?",
            (channel_id,)
        ).fetchone()
        return (row[0], row[1], row[2]) if row else (0, 0, 0)

    def save_progress(self, channel_id: int, last_id: int, total_scanned: int, files_saved: int):
        self.conn.execute(
            """INSERT OR REPLACE INTO resume_meta 
               (channel_id, last_scanned_id, total_scanned, files_saved, updated_at)
               VALUES (?, ?, ?, ?, ?)""",
            (channel_id, last_id, total_scanned, files_saved, datetime.now().isoformat())
        )
        self.conn.commit()

    def buffer_insert(self, record: Tuple):
        with self.conn:
            self.conn.execute(
                """INSERT OR REPLACE INTO seen_files
                   (channel_id, msg_id, file_id, file_unique_id, file_size, duration, md5_hash, phash, msg_date, file_type, mime_type, views, file_name)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                record
            )

    def delete_msg_records(self, channel_id: int, msg_ids: List[int]):
        self.conn.executemany(
            "DELETE FROM seen_files WHERE channel_id=? AND msg_id=?",
            [(channel_id, mid) for mid in msg_ids]
        )
        self.conn.commit()

    def stream_duplicates(self, channel_id: int, keep_strategy: str, min_size: int = 0,
                          min_date: Optional[str] = None, use_md5: bool = False, use_phash: bool = False) -> List[Dict]:
        order_map = {
            "oldest": "msg_date ASC",
            "newest": "msg_date DESC",
            "largest": "file_size DESC, msg_date ASC"
        }
        order_clause = order_map.get(keep_strategy, "msg_date ASC")
        conditions = ["channel_id = ?", "file_id IS NOT NULL"]
        params = [channel_id]
        if min_size > 0:
            conditions.append("file_size >= ?")
            params.append(min_size)
        if min_date:
            conditions.append("msg_date >= ?")
            params.append(min_date)
        where_clause = " AND ".join(conditions)

        duplicates = []
        
        # 🆕 الطبقة 0: file_unique_id (الأدق والأسرع)
        cursor = self.conn.execute(
            f"SELECT file_unique_id FROM seen_files WHERE {where_clause} AND file_unique_id IS NOT NULL "
            "GROUP BY file_unique_id HAVING COUNT(DISTINCT file_id) > 1",
            params
        )
        for row in cursor:
            group = self.conn.execute(
                f"SELECT msg_id, file_size, msg_date, file_id, duration, phash, file_type, mime_type, file_name "
                f"FROM seen_files WHERE channel_id=? AND file_unique_id=? ORDER BY {order_clause}",
                (channel_id, row[0])
            ).fetchall()
            keeper = group[0]
            seen = set()
            for dup in group[1:]:
                if dup[3] not in seen:
                    seen.add(dup[3])
                    duplicates.append({
                        "id": dup[0], "size": dup[1], "date": dup[2], "file_id": dup[3],
                        "duration": dup[4], "phash": dup[5], "type": dup[6], "mime": dup[7], "name": dup[8],
                        "keeper_id": keeper[0]
                    })

        # الطبقة 1: file_id (النسخ المعاد توجيهها)
        cursor = self.conn.execute(
            f"SELECT file_id FROM seen_files WHERE {where_clause} GROUP BY file_id HAVING COUNT(*) > 1",
            params
        )
        for row in cursor:
            group = self.conn.execute(
                f"SELECT msg_id, file_size, msg_date, file_id, duration, phash, file_type, mime_type, file_name "
                f"FROM seen_files WHERE channel_id=? AND file_id=? ORDER BY {order_clause}",
                (channel_id, row[0])
            ).fetchall()
            keeper = group[0]
            for dup in group[1:]:
                duplicates.append({
                    "id": dup[0], "size": dup[1], "date": dup[2], "file_id": dup[3],
                    "duration": dup[4], "phash": dup[5], "type": dup[6], "mime": dup[7], "name": dup[8],
                    "keeper_id": keeper[0]
                })

        # الطبقة 2: MD5 (اختياري)
        if use_md5:
            cursor = self.conn.execute(
                f"SELECT md5_hash FROM seen_files WHERE {where_clause} AND md5_hash IS NOT NULL "
                "GROUP BY md5_hash HAVING COUNT(DISTINCT file_id) > 1",
                params
            )
            for row in cursor:
                group = self.conn.execute(
                    f"SELECT msg_id, file_size, msg_date, file_id, duration, phash, file_type, mime_type, file_name "
                    f"FROM seen_files WHERE channel_id=? AND md5_hash=? ORDER BY {order_clause}",
                    (channel_id, row[0])
                ).fetchall()
                keeper = group[0]
                seen = set()
                for dup in group[1:]:
                    if dup[3] not in seen:
                        seen.add(dup[3])
                        duplicates.append({
                            "id": dup[0], "size": dup[1], "date": dup[2], "file_id": dup[3],
                            "duration": dup[4], "phash": dup[5], "type": dup[6], "mime": dup[7], "name": dup[8],
                            "keeper_id": keeper[0]
                        })

        # الطبقة 3: pHash (اختياري)
        if use_phash and _HAS_IMAGEHASH:
            cursor = self.conn.execute(
                f"SELECT phash FROM seen_files WHERE {where_clause} AND phash IS NOT NULL "
                "GROUP BY phash HAVING COUNT(DISTINCT file_id) > 1",
                params
            )
            for row in cursor:
                group = self.conn.execute(
                    f"SELECT msg_id, file_size, msg_date, file_id, duration, phash, file_type, mime_type, file_name "
                    f"FROM seen_files WHERE channel_id=? AND phash=? ORDER BY {order_clause}",
                    (channel_id, row[0])
                ).fetchall()
                keeper = group[0]
                seen = set()
                for dup in group[1:]:
                    if dup[3] not in seen:
                        seen.add(dup[3])
                        duplicates.append({
                            "id": dup[0], "size": dup[1], "date": dup[2], "file_id": dup[3],
                            "duration": dup[4], "phash": dup[5], "type": dup[6], "mime": dup[7], "name": dup[8],
                            "keeper_id": keeper[0]
                        })

        # إزالة التكرارات المحتملة
        unique_dups = {}
        for d in duplicates:
            key = (d['id'], d['file_id'])
            if key not in unique_dups or d['size'] > unique_dups[key]['size']:
                unique_dups[key] = d
        return list(unique_dups.values())

    def clear_channel(self, channel_id: int):
        self.conn.execute("DELETE FROM seen_files WHERE channel_id=?", (channel_id,))
        self.conn.execute("DELETE FROM resume_meta WHERE channel_id=?", (channel_id,))
        self.conn.commit()

    def close(self):
        self.conn.close()

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
if 'last_deleted_count' not in st.session_state: st.session_state.last_deleted_count = 0
if 'last_deleted_failed' not in st.session_state: st.session_state.last_deleted_failed = 0

# ================== واجهة المستخدم ==================
st.title("🦖 Telegram Duplicate Surgeon Pro")
st.caption("الأداة الجراحية – مع دعم معرف الملف الثابت (file_unique_id)")

# الخطوة 1: تسجيل الدخول
if st.session_state.step == 'login':
    with st.form("login_form"):
        st.subheader("🔐 تسجيل الدخول إلى تيليجرام")
        api_id = st.text_input("API ID*", type="password")
        api_hash = st.text_input("API Hash*", type="password")
        phone = st.text_input("رقم الهاتف*", placeholder="+963xxxxxxxxx")
        if st.form_submit_button("إرسال رمز التحقق", use_container_width=True):
            if not api_id or not api_hash or not phone:
                st.error("جميع الحقول مطلوبة")
            else:
                try:
                    client = TelegramClient("streamlit_session", int(api_id), api_hash)
                    client.connect()
                    if not client.is_user_authorized():
                        client.send_code_request(phone)
                        st.session_state.client = client
                        st.session_state.phone = phone
                        st.session_state.step = 'verify_code'
                        st.rerun()
                    else:
                        st.session_state.client = client
                        st.session_state.step = 'channel'
                        st.rerun()
                except Exception as e:
                    st.error(f"خطأ في الاتصال: {e}")

# الخطوة 2: رمز التحقق
elif st.session_state.step == 'verify_code':
    with st.form("verify_form"):
        st.subheader("📲 تأكيد الحساب")
        code = st.text_input("رمز OTP*")
        password = st.text_input("كلمة مرور التحقق بخطوتين (إن وجدت)", type="password")
        if st.form_submit_button("تأكيد", use_container_width=True):
            client = st.session_state.client
            try:
                client.sign_in(st.session_state.phone, code)
                st.session_state.step = 'channel'
                st.rerun()
            except SessionPasswordNeededError:
                if not password:
                    st.error("الحساب محمي بكلمة مرور، الرجاء إدخالها")
                else:
                    try:
                        client.sign_in(password=password)
                        st.session_state.step = 'channel'
                        st.rerun()
                    except Exception as e:
                        st.error(f"كلمة مرور غير صحيحة: {e}")
            except Exception as e:
                st.error(f"رمز التحقق غير صحيح: {e}")

# الخطوة 3: اختيار القناة والإعدادات
elif st.session_state.step == 'channel':
    st.success("✅ تم تسجيل الدخول بنجاح")
    with st.form("channel_form"):
        st.subheader("📡 إعدادات القناة والمسح")
        channel_input = st.text_input("رابط القناة أو المعرف*", placeholder="@username أو https://t.me/...")
        
        media_types = st.multiselect("أنواع الملفات", ["photo", "video", "document"], default=["photo", "video"])
        keep_strategy = st.selectbox("استراتيجية الاحتفاظ", ["oldest (الأقدم)", "newest (الأحدث)", "largest (الأكبر حجمًا)"])
        keep_strategy_map = {"oldest (الأقدم)": "oldest", "newest (الأحدث)": "newest", "largest (الأكبر حجمًا)": "largest"}
        
        col1, col2 = st.columns(2)
        with col1:
            dry_run = st.checkbox("🔍 وضع المعاينة (بدون حذف)", value=True)
            min_size_mb = st.number_input("الحد الأدنى للحجم (MB)", 0.0, 10000.0, 0.0, step=1.0)
        with col2:
            reset = st.checkbox("🔄 مسح التقدم السابق")
            auto_mode = st.toggle("🤖 الوضع الآلي المستمر", value=st.session_state.auto_mode)
            st.session_state.auto_mode = auto_mode

        st.markdown("---")
        st.subheader("🔬 طبقات اكتشاف التكرار")
        st.caption("اختر الطبقات الإضافية – كلما زادت الطبقات زادت الدقة لكن المسح يصبح أبطأ")
        
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("✅ **File Unique ID** (أساسي، دقيق، لا يستهلك موارد)")
            compute_md5 = st.checkbox("🔐 MD5 Hash – تطابق المحتوى", value=False)
        with col_b:
            compute_phash = st.checkbox("🖼️ pHash – تشابه بصري للصور", value=_HAS_IMAGEHASH, disabled=not _HAS_IMAGEHASH)

        st.markdown("---")
        st.subheader("💾 استئناف المسح السابق")
        uploaded_db = st.file_uploader("ارفع ملف قاعدة البيانات (.db)", type=['db'])
        if uploaded_db:
            temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
            temp_db.write(uploaded_db.getbuffer())
            st.session_state.db_path = temp_db.name
            st.success("✅ تم تحميل قاعدة البيانات بنجاح.")

        if st.form_submit_button("🚀 بدء المسح", use_container_width=True):
            if not channel_input:
                st.error("الرجاء إدخال رابط القناة")
            else:
                try:
                    client = st.session_state.client
                    entity = client.get_entity(channel_input)
                    st.session_state.channel = entity
                    st.session_state.scan_params = {
                        'media_types': media_types,
                        'keep_strategy': keep_strategy_map[keep_strategy],
                        'dry_run': dry_run,
                        'compute_md5': compute_md5,
                        'compute_phash': compute_phash,
                        'min_size_mb': min_size_mb,
                        'reset': reset
                    }
                    if st.session_state.db_path is None:
                        temp_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
                        st.session_state.db_path = temp_file.name

                    db = Database(st.session_state.db_path)
                    if reset:
                        db.clear_channel(entity.id)
                    else:
                        _, total_scanned, files_saved = db.get_resume_state(entity.id)
                        st.session_state.total_scanned = total_scanned
                        st.session_state.files_saved = files_saved
                    db.close()

                    st.session_state.step = 'scanning'
                    st.rerun()
                except Exception as e:
                    st.error(f"خطأ في الوصول للقناة: {e}")

# الخطوة 4: المسح
elif st.session_state.step == 'scanning':
    mode_text = " (الوضع الآلي مفعّل)" if st.session_state.auto_mode else ""
    st.info(f"🔍 وضع المسح التفاعلي{mode_text}")
    params = st.session_state.scan_params
    channel = st.session_state.channel

    db = Database(st.session_state.db_path)
    last_id, _, _ = db.get_resume_state(channel.id)
    offset_id = 0 if last_id == 0 else last_id + 1

    col1, col2 = st.columns(2)
    with col1:
        st.metric("📊 تم فحص", st.session_state.total_scanned)
    with col2:
        st.metric("💾 تم حفظ", st.session_state.files_saved)

    button_label = "فحص الدفعة التالية" if not st.session_state.auto_mode else "▶️ بدء / استمرار المسح الآلي"
    if st.button(button_label, type="primary", use_container_width=True):
        client = st.session_state.client
        scanned = st.session_state.total_scanned
        saved = st.session_state.files_saved
        last_processed_id = last_id

        progress_bar = st.progress(0, text="جاري فحص الدفعة...")
        try:
            messages = list(client.iter_messages(channel, offset_id=offset_id, limit=BATCH_SCAN_SIZE, reverse=False))
            for i, msg in enumerate(messages):
                scanned += 1
                last_processed_id = msg.id
                progress_bar.progress(
                    (i + 1) / len(messages),
                    text=f"فحص {i+1}/{len(messages)} | المحفوظ: {saved}"
                )

                if not msg.media:
                    continue

                info = extract_file_info(client, msg, params['compute_md5'], params['compute_phash'])
                if not info: continue
                if info['type'] not in params['media_types']: continue
                if info['size'] < params['min_size_mb'] * 1024 * 1024: continue

                saved += 1
                record = (
                    channel.id, info['id'], info['file_id'], info['file_unique_id'],
                    info['size'], info['duration'], info['md5'], info['phash'],
                    info['date'], info['type'], info['mime'], info['views'], info['name']
                )
                db.buffer_insert(record)
                time.sleep(0.01)

            st.session_state.total_scanned = scanned
            st.session_state.files_saved = saved
            db.save_progress(channel.id, last_processed_id, scanned, saved)
            progress_bar.progress(1.0, text="✅ تم الانتهاء من الدفعة!")

            if st.session_state.auto_mode and len(messages) == BATCH_SCAN_SIZE:
                time.sleep(1)
                st.rerun()

        except FloodWaitError as e:
            st.warning(f"توقف مؤقت: انتظر {e.seconds} ثانية")
        except Exception as e:
            st.error(f"خطأ: {e}")
        finally:
            db.close()

    if st.button("📋 عرض المكررات المكتشفة حتى الآن", use_container_width=True):
        st.session_state.step = 'results'
        st.rerun()

    with open(st.session_state.db_path, "rb") as f:
        st.download_button("📥 تحميل قاعدة البيانات (لحفظ التقدم)", f, file_name=f"scan_{channel.id}.db")

# الخطوة 5: عرض النتائج والحذف
elif st.session_state.step == 'results':
    params = st.session_state.scan_params
    channel = st.session_state.channel

    db = Database(st.session_state.db_path)
    all_duplicates = db.stream_duplicates(
        channel.id, params['keep_strategy'],
        int(params['min_size_mb'] * 1024 * 1024),
        use_md5=params.get('compute_md5', False),
        use_phash=params.get('compute_phash', False)
    )

    if st.button("⬅️ العودة للمسح", use_container_width=True):
        st.session_state.step = 'scanning'
        st.rerun()

    if st.session_state.last_deleted_count > 0:
        st.success(f"✅ تم حذف {st.session_state.last_deleted_count} رسالة بنجاح")
        st.session_state.last_deleted_count = 0

    if not all_duplicates:
        st.balloons()
        st.success("🎉 لا توجد مكررات! القناة نظيفة.")
    else:
        st.warning(f"🔔 تم العثور على {len(all_duplicates)} مكرر")

        total_pages = (len(all_duplicates) + PAGE_SIZE - 1) // PAGE_SIZE
        page = st.session_state.page
        page_duplicates = all_duplicates[page * PAGE_SIZE : (page + 1) * PAGE_SIZE]

        df = pd.DataFrame([{
            "معرف": d['id'],
            "النوع": d['type'],
            "الحجم": fmt_size(d['size']),
            "التاريخ": d['date'][:10],
            "اسم": d['name'][:20] + "..." if d['name'] and len(d['name']) > 20 else d['name'],
            "تحديد": False
        } for d in page_duplicates])

        edited_df = st.data_editor(df, column_config={"تحديد": st.column_config.CheckboxColumn("🗑️")},
                                   hide_index=True, use_container_width=True, height=350)

        selected = edited_df[edited_df["تحديد"] == True]["معرف"].tolist()
        for sid in selected:
            st.session_state.selected_ids.add(sid)

        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("🗑️ حذف المحدد", type="primary", use_container_width=True):
                if not st.session_state.selected_ids:
                    st.warning("حدد رسائل")
                elif params['dry_run']:
                    st.info(f"معاينة: حذف {len(st.session_state.selected_ids)} رسالة")
                else:
                    ids = list(st.session_state.selected_ids)
                    prog = st.progress(0)
                    deleted = 0
                    for i in range(0, len(ids), BATCH_DELETE_SIZE):
                        batch = ids[i:i + BATCH_DELETE_SIZE]
                        try:
                            st.session_state.client.delete_messages(channel, batch)
                            db.delete_msg_records(channel.id, batch)
                            deleted += len(batch)
                        except FloodWaitError as e:
                            st.warning(f"انتظار {e.seconds}s")
                            time.sleep(e.seconds)
                        except Exception as e:
                            st.error(f"فشل: {e}")
                        prog.progress((i + len(batch)) / len(ids))
                    st.success(f"✅ تم حذف {deleted} رسالة")
                    st.session_state.last_deleted_count = deleted
                    st.session_state.selected_ids.clear()
                    st.rerun()
        with c2:
            if st.button("✅ تحديد الكل", use_container_width=True):
                for d in page_duplicates:
                    st.session_state.selected_ids.add(d['id'])
                st.rerun()
        with c3:
            if st.button("📥 CSV", use_container_width=True):
                df_report = pd.DataFrame([{
                    "معرف": d['id'], "حجم": fmt_size(d['size']), "تاريخ": d['date']
                } for d in all_duplicates])
                st.download_button("تحميل", df_report.to_csv(index=False).encode('utf-8-sig'),
                                   "report.csv", "text/csv")

        # Pagination
        col1, col2, col3, col4, col5 = st.columns([1, 1, 2, 1, 1])
        with col1:
            if st.button("⏪") and page > 0:
                st.session_state.page = 0
                st.rerun()
        with col2:
            if st.button("◀") and page > 0:
                st.session_state.page -= 1
                st.rerun()
        with col3:
            st.markdown(f"<div style='text-align:center'>صفحة {page+1} من {total_pages}</div>", unsafe_allow_html=True)
        with col4:
            if st.button("▶") and page < total_pages - 1:
                st.session_state.page += 1
                st.rerun()
        with col5:
            if st.button("⏩") and page < total_pages - 1:
                st.session_state.page = total_pages - 1
                st.rerun()

    db.close()

# تذييل
st.markdown("---")
st.markdown("<div class='footer'>تم التطوير بواسطة <strong>F.ALSALEH</strong> | Telegram Duplicate Surgeon Pro v2.4</div>", unsafe_allow_html=True)
