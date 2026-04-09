import streamlit as st
import asyncio
import gc
import hashlib
import io
import sqlite3
import tempfile
import time
import threading

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

# ================== دوال مساعدة ==================
def fmt_size(n: int) -> str:
    if n == 0: return "0 B"
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024: return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} PB"

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

# ================== استخراج معلومات الملف ==================
def extract_file_info(msg):
    media = msg.media
    if not media: return None
    info = {
        "id": msg.id, "file_id": None, "size": 0, "duration": 0,
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
        """يحذف سجلات الرسائل المحذوفة من قاعدة البيانات"""
        self.conn.executemany(
            "DELETE FROM seen_files WHERE channel_id=? AND msg_id=?",
            [(channel_id, mid) for mid in msg_ids]
        )
        self.conn.commit()

    def stream_duplicates(self, channel_id, keep_strategy, min_size=0):
        order = {"oldest": "msg_date ASC", "newest": "msg_date DESC", "largest": "file_size DESC"}[keep_strategy]
        cursor = self.conn.execute(
            "SELECT file_id FROM seen_files WHERE channel_id=? AND file_size>=? GROUP BY file_id HAVING COUNT(*)>1",
            (channel_id, min_size)
        )
        duplicates = []
        for row in cursor:
            group = self.conn.execute(
                f"SELECT msg_id, file_size, msg_date, file_id, duration, phash, file_type, mime_type, file_name "
                f"FROM seen_files WHERE channel_id=? AND file_id=? ORDER BY {order}",
                (channel_id, row[0])
            ).fetchall()
            keeper = group[0]
            for dup in group[1:]:
                duplicates.append({
                    "id": dup[0], "size": dup[1], "date": dup[2], "file_id": dup[3],
                    "duration": dup[4], "phash": dup[5], "type": dup[6],
                    "mime": dup[7], "name": dup[8], "keeper_id": keeper[0]
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
    'auto_scan_running': False,
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ================== واجهة المستخدم ==================
st.title("🦖 Telegram Duplicate Surgeon")

# ---------- تسجيل الدخول ----------
if st.session_state.step == 'login':
    with st.form("login_form"):
        st.subheader("🔐 تسجيل الدخول إلى تيليجرام")
        api_id   = st.text_input("API ID*", type="password")
        api_hash = st.text_input("API Hash*", type="password")
        phone    = st.text_input("رقم الهاتف*", placeholder="+963xxxxxxxxx")
        if st.form_submit_button("إرسال رمز التحقق"):
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

# ---------- إدخال رمز OTP ----------
elif st.session_state.step == 'verify_code':
    with st.form("verify_form"):
        st.subheader("📲 تأكيد الحساب")
        st.info("أدخل الرمز الذي وصلك على تيليجرام")
        code     = st.text_input("رمز OTP*")
        password = st.text_input("كلمة مرور 2FA (إن وجدت)", type="password")
        if st.form_submit_button("تأكيد"):
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

    st.markdown("---")
    if st.button("🔄 إعادة إرسال الرمز"):
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

# ---------- اختيار القناة ----------
elif st.session_state.step == 'channel':
    st.success("✅ تم تسجيل الدخول")
    with st.form("channel_form"):
        channel_input = st.text_input("رابط القناة*", placeholder="@username أو https://t.me/+xxx")
        media_types   = st.multiselect("أنواع الملفات", ["photo", "video", "document"], default=["photo", "video"])
        keep_strategy = st.selectbox("استراتيجية الاحتفاظ", ["oldest", "newest", "largest"])
        dry_run       = st.checkbox("وضع المعاينة (بدون حذف فعلي)", True)
        min_size_mb   = st.number_input("الحد الأدنى للحجم (MB)", 0.0, 10000.0, 0.0)
        auto_mode     = st.toggle("الوضع الآلي (مسح مستمر بدون ضغط)", False)
        uploaded_db   = st.file_uploader("رفع قاعدة بيانات سابقة", type=['db'])
        if uploaded_db:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
            tmp.write(uploaded_db.getbuffer())
            st.session_state.db_path = tmp.name
            st.success("✅ تم تحميل قاعدة البيانات")
        if st.form_submit_button("🚀 بدء المسح"):
            if not channel_input:
                st.error("أدخل رابط القناة")
            else:
                try:
                    entity = run_sync(_get_entity(st.session_state.client, channel_input.strip()))
                    st.session_state.channel     = entity
                    st.session_state.scan_params = {
                        'media_types': media_types, 'keep_strategy': keep_strategy,
                        'dry_run': dry_run, 'min_size_mb': min_size_mb
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

    col1, col2 = st.columns(2)
    with col1: st.metric("📊 تم فحص", st.session_state.total_scanned)
    with col2: st.metric("💾 تم حفظ", st.session_state.files_saved)

    # ✅ إصلاح: الوضع الآلي يشتغل تلقائياً بدون ضغط
    should_scan = st.session_state.auto_scan_running

    col_btn1, col_btn2, col_btn3 = st.columns(3)
    with col_btn1:
        if not st.session_state.auto_scan_running:
            if st.button("▶️ فحص الدفعة التالية", type="primary"):
                should_scan = True
        else:
            if st.button("⏹️ إيقاف الوضع الآلي", type="primary"):
                st.session_state.auto_scan_running = False
                st.rerun()

    with col_btn2:
        # ✅ إصلاح: زر رجوع لعرض المكررات
        if st.button("📋 عرض المكررات"):
            st.session_state.step = 'results'
            st.rerun()

    with col_btn3:
        with open(st.session_state.db_path, "rb") as f:
            st.download_button("📥 تحميل DB", f, file_name=f"scan_{channel.id}.db")

    if should_scan:
        db = Database(st.session_state.db_path)
        last_id, _, _ = db.get_resume_state(channel.id)
        offset_id = 0 if last_id == 0 else last_id + 1
        client    = st.session_state.client
        progress  = st.progress(0, text="جاري الفحص...")
        try:
            messages = run_sync(_get_messages(client, channel, offset_id, BATCH_SCAN_SIZE))
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
                    info = extract_file_info(msg)
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
                # ✅ إصلاح: الوضع الآلي يعيد التشغيل تلقائياً
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
    params     = st.session_state.scan_params
    channel    = st.session_state.channel
    db         = Database(st.session_state.db_path)
    duplicates = db.stream_duplicates(
        channel.id, params['keep_strategy'],
        int(params['min_size_mb'] * 1024 * 1024)
    )

    st.subheader(f"📋 المكررات في: {getattr(channel, 'title', str(channel.id))}")

    # ✅ إصلاح: زر رجوع للمسح
    if st.button("⬅️ رجوع للمسح"):
        st.session_state.step = 'scanning'
        st.session_state.selected_ids = set()
        st.rerun()

    # ✅ إظهار نتيجة آخر عملية حذف
    if st.session_state.last_deleted_count > 0:
        st.success(f"✅ تم حذف {st.session_state.last_deleted_count} رسالة بنجاح من تيليجرام وقاعدة البيانات")
        if st.session_state.last_deleted_failed > 0:
            st.warning(f"⚠️ فشل حذف {st.session_state.last_deleted_failed} رسالة (ربما لا صلاحية أو محذوفة مسبقاً)")
        st.session_state.last_deleted_count  = 0
        st.session_state.last_deleted_failed = 0

    if not duplicates:
        st.success("🎉 لا توجد مكررات!")
    else:
        st.warning(f"🔔 {len(duplicates)} رسالة مكررة")

        if params['dry_run']:
            st.info("🔍 وضع المعاينة مفعّل — لن يُحذف شيء فعلاً. غيّر الإعداد من صفحة القناة للحذف الفعلي.")

        page      = st.session_state.page
        total_pages = max(1, (len(duplicates) + PAGE_SIZE - 1) // PAGE_SIZE)
        page_dups = duplicates[page * PAGE_SIZE:(page + 1) * PAGE_SIZE]

        df = pd.DataFrame([
            {
                "معرف": d['id'],
                "النوع": d['type'],
                "الحجم": fmt_size(d['size']),
                "التاريخ": d['date'][:10],
                "الأصل (يُحتفظ به)": d['keeper_id'],
                "تحديد للحذف": False
            }
            for d in page_dups
        ])
        edited = st.data_editor(
            df,
            column_config={"تحديد للحذف": st.column_config.CheckboxColumn("🗑️ حذف")},
            hide_index=True,
            use_container_width=True
        )
        for sid in edited[edited["تحديد للحذف"] == True]["معرف"].tolist():
            st.session_state.selected_ids.add(sid)

        # Pagination
        if total_pages > 1:
            pcol1, pcol2, pcol3 = st.columns(3)
            with pcol1:
                if page > 0 and st.button("⬅️ الصفحة السابقة"):
                    st.session_state.page -= 1
                    st.rerun()
            with pcol2:
                st.write(f"صفحة {page + 1} من {total_pages}")
            with pcol3:
                if page < total_pages - 1 and st.button("➡️ الصفحة التالية"):
                    st.session_state.page += 1
                    st.rerun()

        st.markdown("---")
        col_sel1, col_sel2 = st.columns(2)
        with col_sel1:
            if st.button("☑️ تحديد الكل في هذه الصفحة"):
                for d in page_dups:
                    st.session_state.selected_ids.add(d['id'])
                st.rerun()
        with col_sel2:
            if st.button("✖️ إلغاء تحديد الكل"):
                st.session_state.selected_ids = set()
                st.rerun()

        selected_count = len(st.session_state.selected_ids)
        if selected_count > 0:
            st.info(f"📌 محدد: {selected_count} رسالة")

        if st.button(f"🗑️ حذف {selected_count} رسالة محددة", type="primary", disabled=selected_count == 0):
            if selected_count == 0:
                st.warning("لم تحدد أي رسائل")
            elif params['dry_run']:
                st.info(f"🔍 معاينة: سيتم حذف {selected_count} رسالة — وضع المعاينة مفعّل، لا حذف فعلي")
            else:
                ids      = list(st.session_state.selected_ids)
                prog     = st.progress(0, text="جاري الحذف...")
                deleted  = 0
                failed   = 0
                status   = st.empty()
                for i in range(0, len(ids), BATCH_DELETE_SIZE):
                    batch = ids[i:i + BATCH_DELETE_SIZE]
                    try:
                        run_sync(_delete_messages(st.session_state.client, channel, batch))
                        # ✅ إصلاح: نحذف السجلات من قاعدة البيانات فوراً
                        db.delete_msg_records(channel.id, batch)
                        deleted += len(batch)
                        status.write(f"⏳ تم حذف {deleted} من {len(ids)}...")
                    except FloodWaitError as e:
                        st.warning(f"⏳ انتظار {e.seconds}s")
                        time.sleep(e.seconds)
                        # أعد المحاولة
                        try:
                            run_sync(_delete_messages(st.session_state.client, channel, batch))
                            db.delete_msg_records(channel.id, batch)
                            deleted += len(batch)
                        except Exception:
                            failed += len(batch)
                    except Exception as ex:
                        failed += len(batch)
                        st.warning(f"⚠️ فشل حذف دفعة: {ex}")
                    prog.progress((i + len(batch)) / len(ids))

                st.session_state.last_deleted_count  = deleted
                st.session_state.last_deleted_failed = failed
                st.session_state.selected_ids = set()
                db.close()
                st.rerun()  # ✅ يعيد تحميل الصفحة ليظهر تأكيد الحذف والقائمة المحدّثة

    db.close()

st.markdown("<div class='footer'>تم التطوير بواسطة <strong>F.ALSALEH</strong></div>", unsafe_allow_html=True)
