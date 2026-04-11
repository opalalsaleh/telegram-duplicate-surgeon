<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>TeleSweep – مزيل المكررات</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Arabic:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
:root {
  --bg: #0c0b14;
  --bg2: #13111f;
  --bg3: #1a1829;
  --bg4: #201e32;
  --accent: #6366f1;
  --accent2: #818cf8;
  --accent3: #c084fc;
  --border: rgba(99,102,241,0.15);
  --border2: rgba(99,102,241,0.3);
  --text: #e2e0f0;
  --text2: #8b87b8;
  --text3: #4d4a6b;
  --success: #34d399;
  --warning: #fbbf24;
  --danger: #f87171;
  --mono: 'JetBrains Mono', monospace;
  --sans: 'IBM Plex Sans Arabic', sans-serif;
  --r: 12px;
  --r2: 16px;
}

*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

html, body {
  height: 100%;
  background: var(--bg);
  color: var(--text);
  font-family: var(--sans);
  font-size: 14px;
  line-height: 1.6;
}

/* ─── LAYOUT ─────────────────────────────── */
.app { display: flex; height: 100vh; overflow: hidden; }

/* ─── SIDEBAR ─────────────────────────────── */
.sidebar {
  width: 220px;
  flex-shrink: 0;
  background: var(--bg2);
  border-left: 1px solid var(--border);
  display: flex;
  flex-direction: column;
  padding: 0;
  transition: width 0.3s;
}

.logo {
  padding: 24px 20px 20px;
  border-bottom: 1px solid var(--border);
}
.logo-icon {
  width: 38px; height: 38px;
  background: linear-gradient(135deg, var(--accent), var(--accent3));
  border-radius: 10px;
  display: flex; align-items: center; justify-content: center;
  font-size: 18px; margin-bottom: 12px;
}
.logo-name { font-size: 17px; font-weight: 600; color: var(--text); letter-spacing: -0.02em; }
.logo-ver { font-size: 10px; color: var(--text3); font-family: var(--mono); margin-top: 1px; }

.nav { flex: 1; padding: 12px 10px; display: flex; flex-direction: column; gap: 2px; }

.nav-item {
  display: flex; align-items: center; gap: 10px;
  padding: 9px 12px; border-radius: var(--r);
  color: var(--text2); cursor: pointer;
  transition: all 0.15s; font-size: 13px;
  border: 1px solid transparent;
  user-select: none;
}
.nav-item:hover { color: var(--text); background: var(--bg3); }
.nav-item.active {
  color: var(--accent2);
  background: rgba(99,102,241,0.1);
  border-color: var(--border);
}
.nav-item .icon { font-size: 15px; flex-shrink: 0; }

.sidebar-footer {
  padding: 16px;
  border-top: 1px solid var(--border);
}

.user-chip {
  display: flex; align-items: center; gap: 10px;
  padding: 10px 12px;
  background: var(--bg3);
  border-radius: var(--r);
  border: 1px solid var(--border);
}
.avatar {
  width: 30px; height: 30px;
  border-radius: 50%;
  background: linear-gradient(135deg, var(--accent), var(--accent3));
  display: flex; align-items: center; justify-content: center;
  font-size: 12px; font-weight: 600; color: #fff; flex-shrink: 0;
}
.user-info { flex: 1; min-width: 0; }
.user-name { font-size: 12px; font-weight: 500; color: var(--text); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.user-sub { font-size: 10px; color: var(--text3); }

.logout-btn {
  width: 100%; margin-top: 8px;
  padding: 8px; border-radius: var(--r);
  background: transparent; border: 1px solid var(--border);
  color: var(--text2); cursor: pointer; font-family: var(--sans);
  font-size: 12px; transition: all 0.15s;
}
.logout-btn:hover { border-color: var(--danger); color: var(--danger); }

/* ─── MAIN ─────────────────────────────── */
.main {
  flex: 1;
  display: flex;
  flex-direction: column;
  overflow: hidden;
}

.topbar {
  padding: 16px 24px;
  border-bottom: 1px solid var(--border);
  display: flex; align-items: center; justify-content: space-between;
  background: var(--bg2);
  flex-shrink: 0;
}
.topbar-title { font-size: 15px; font-weight: 600; color: var(--text); }
.topbar-sub { font-size: 12px; color: var(--text2); margin-top: 1px; }

.status-dot {
  width: 8px; height: 8px; border-radius: 50%;
  background: var(--success);
  box-shadow: 0 0 8px var(--success);
  animation: pulse 2s infinite;
}
@keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }

.content { flex: 1; overflow-y: auto; padding: 24px; }
.content::-webkit-scrollbar { width: 4px; }
.content::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 99px; }

/* ─── SCREENS ─────────────────────────────── */
.screen { display: none; animation: fadein 0.2s ease; }
.screen.active { display: block; }
@keyframes fadein { from{opacity:0;transform:translateY(6px)} to{opacity:1;transform:none} }

/* ─── CARDS ─────────────────────────────── */
.card {
  background: var(--bg2);
  border: 1px solid var(--border);
  border-radius: var(--r2);
  padding: 20px;
  margin-bottom: 16px;
}
.card-title {
  font-size: 13px; font-weight: 600; color: var(--text2);
  letter-spacing: 0.05em; text-transform: uppercase;
  margin-bottom: 14px;
}

/* ─── FORM ELEMENTS ─────────────────────────────── */
.field { margin-bottom: 14px; }
.label {
  font-size: 11px; font-weight: 500; color: var(--text2);
  display: block; margin-bottom: 5px; letter-spacing: 0.03em;
}
.input-wrap { position: relative; }

input, textarea, select {
  width: 100%;
  padding: 10px 14px;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: var(--r);
  color: var(--text);
  font-family: var(--sans);
  font-size: 13px;
  outline: none;
  transition: border-color 0.15s, box-shadow 0.15s;
}
input:focus, textarea:focus, select:focus {
  border-color: var(--accent);
  box-shadow: 0 0 0 3px rgba(99,102,241,0.15);
}
input::placeholder, textarea::placeholder { color: var(--text3); }
input[type="password"] { font-family: var(--mono); letter-spacing: 0.1em; }

textarea { resize: vertical; min-height: 72px; font-family: var(--mono); font-size: 12px; }
select { appearance: none; cursor: pointer; }

.eye-btn {
  position: absolute; left: 10px; top: 50%;
  transform: translateY(-50%);
  background: none; border: none; cursor: pointer;
  color: var(--text3); font-size: 14px; padding: 2px;
  transition: color 0.15s; line-height: 1;
}
.eye-btn:hover { color: var(--text2); }
input.has-eye { padding-left: 36px; }

.hint { font-size: 11px; color: var(--text3); margin-top: 4px; }

/* ─── TABS ─────────────────────────────── */
.tabs-bar {
  display: flex; gap: 4px;
  background: var(--bg);
  border-radius: var(--r); padding: 4px;
  border: 1px solid var(--border);
  margin-bottom: 20px;
}
.tab-btn {
  flex: 1; padding: 8px 10px;
  background: transparent; border: none;
  border-radius: 8px;
  color: var(--text3); cursor: pointer;
  font-family: var(--sans); font-size: 12px; font-weight: 500;
  transition: all 0.15s;
}
.tab-btn.active { background: var(--bg4); color: var(--accent2); }
.tab-pane { display: none; }
.tab-pane.active { display: block; }

/* ─── BUTTONS ─────────────────────────────── */
.btn {
  display: inline-flex; align-items: center; justify-content: center; gap: 6px;
  padding: 10px 16px; border-radius: var(--r);
  font-family: var(--sans); font-size: 13px; font-weight: 500;
  cursor: pointer; border: 1px solid; transition: all 0.15s;
  white-space: nowrap;
}
.btn-primary {
  background: var(--accent); border-color: var(--accent); color: #fff;
  box-shadow: 0 4px 16px rgba(99,102,241,0.35);
}
.btn-primary:hover { background: var(--accent2); border-color: var(--accent2); box-shadow: 0 6px 20px rgba(99,102,241,0.45); }
.btn-secondary { background: var(--bg3); border-color: var(--border); color: var(--text2); }
.btn-secondary:hover { border-color: var(--border2); color: var(--text); }
.btn-danger { background: transparent; border-color: rgba(248,113,113,0.3); color: var(--danger); }
.btn-danger:hover { background: rgba(248,113,113,0.1); }
.btn-full { width: 100%; }
.btn:disabled { opacity: 0.4; cursor: not-allowed; }

/* ─── METRICS ─────────────────────────────── */
.metrics { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-bottom: 16px; }
.metric {
  background: var(--bg2); border: 1px solid var(--border);
  border-radius: var(--r2); padding: 16px;
  position: relative; overflow: hidden;
}
.metric::before {
  content: ''; position: absolute; top: 0; right: 0;
  width: 3px; height: 100%;
  background: linear-gradient(180deg, var(--accent), var(--accent3));
}
.metric-label { font-size: 10px; color: var(--text3); text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 6px; }
.metric-value { font-size: 22px; font-weight: 600; color: var(--text); font-family: var(--mono); }
.metric-sub { font-size: 10px; color: var(--text3); margin-top: 3px; }

/* ─── PROGRESS ─────────────────────────────── */
.progress-bar { height: 4px; background: var(--bg4); border-radius: 99px; overflow: hidden; margin: 12px 0; }
.progress-fill {
  height: 100%;
  background: linear-gradient(90deg, var(--accent), var(--accent3));
  border-radius: 99px;
  transition: width 0.3s;
}

/* ─── GRID OPTIONS ─────────────────────────────── */
.grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
.grid-3 { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; }

/* ─── TOGGLE ─────────────────────────────── */
.toggle-row {
  display: flex; align-items: center; justify-content: space-between;
  padding: 10px 0;
  border-bottom: 1px solid var(--border);
}
.toggle-row:last-child { border-bottom: none; }
.toggle-info .toggle-title { font-size: 13px; color: var(--text); font-weight: 500; }
.toggle-info .toggle-sub { font-size: 11px; color: var(--text3); margin-top: 2px; }

.toggle {
  width: 36px; height: 20px; flex-shrink: 0;
  background: var(--bg4); border-radius: 99px;
  position: relative; cursor: pointer;
  border: 1px solid var(--border);
  transition: all 0.2s;
}
.toggle.on { background: var(--accent); border-color: var(--accent); }
.toggle::after {
  content: '';
  position: absolute; top: 2px; right: 2px;
  width: 14px; height: 14px;
  border-radius: 50%; background: #fff;
  transition: transform 0.2s;
}
.toggle.on::after { transform: translateX(-16px); }

/* ─── BADGE ─────────────────────────────── */
.badge {
  display: inline-flex; align-items: center; gap: 4px;
  padding: 2px 8px; border-radius: 99px;
  font-size: 10px; font-weight: 600; font-family: var(--mono);
  border: 1px solid;
}
.badge-blue { background: rgba(99,102,241,0.12); color: var(--accent2); border-color: rgba(99,102,241,0.3); }
.badge-green { background: rgba(52,211,153,0.1); color: var(--success); border-color: rgba(52,211,153,0.3); }
.badge-amber { background: rgba(251,191,36,0.1); color: var(--warning); border-color: rgba(251,191,36,0.3); }
.badge-purple { background: rgba(192,132,252,0.1); color: var(--accent3); border-color: rgba(192,132,252,0.3); }
.badge-red { background: rgba(248,113,113,0.1); color: var(--danger); border-color: rgba(248,113,113,0.3); }

/* ─── DUP CARD ─────────────────────────────── */
.dup-list { display: flex; flex-direction: column; gap: 8px; }
.dup-card {
  display: flex; align-items: center; gap: 12px;
  padding: 12px 14px;
  background: var(--bg2); border: 1px solid var(--border);
  border-radius: var(--r); transition: all 0.15s;
  cursor: pointer;
}
.dup-card:hover { border-color: var(--border2); background: var(--bg3); }
.dup-card.selected { border-color: var(--accent); background: rgba(99,102,241,0.05); }
.dup-icon {
  width: 36px; height: 36px; border-radius: 9px;
  background: var(--bg3); border: 1px solid var(--border);
  display: flex; align-items: center; justify-content: center;
  font-size: 16px; flex-shrink: 0;
}
.dup-body { flex: 1; min-width: 0; }
.dup-name { font-size: 13px; font-weight: 500; color: var(--text); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.dup-meta { font-size: 11px; color: var(--text3); margin-top: 2px; }
.dup-check {
  width: 18px; height: 18px; border-radius: 5px;
  border: 1.5px solid var(--border2); flex-shrink: 0;
  display: flex; align-items: center; justify-content: center;
  transition: all 0.15s;
}
.dup-card.selected .dup-check {
  background: var(--accent); border-color: var(--accent);
  color: #fff; font-size: 10px;
}

/* ─── HELP CARDS ─────────────────────────────── */
.help-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-top: 14px; }
.help-card {
  padding: 12px 14px;
  background: var(--bg3); border: 1px solid var(--border);
  border-radius: var(--r); cursor: pointer; transition: all 0.15s;
}
.help-card:hover { border-color: var(--border2); }
.help-card-icon { font-size: 18px; margin-bottom: 6px; }
.help-card-title { font-size: 12px; font-weight: 500; color: var(--text); }
.help-card-sub { font-size: 10px; color: var(--text3); margin-top: 2px; }

/* ─── CHANNEL CHIP ─────────────────────────────── */
.channel-chip {
  display: flex; align-items: center; gap: 10px;
  padding: 10px 14px;
  background: rgba(99,102,241,0.08); border: 1px solid var(--border);
  border-radius: var(--r); margin-bottom: 14px;
}
.channel-avatar {
  width: 32px; height: 32px; border-radius: 8px;
  background: linear-gradient(135deg, var(--accent), var(--accent3));
  display: flex; align-items: center; justify-content: center;
  font-size: 14px; flex-shrink: 0;
}

/* ─── SELECT CHANNEL ─────────────────────────────── */
.channel-list { display: flex; flex-direction: column; gap: 6px; }
.channel-item {
  display: flex; align-items: center; gap: 10px;
  padding: 10px 12px; border-radius: var(--r);
  border: 1px solid var(--border); cursor: pointer;
  transition: all 0.15s; background: var(--bg3);
}
.channel-item:hover { border-color: var(--border2); }
.channel-item.selected { border-color: var(--accent); background: rgba(99,102,241,0.08); }
.channel-item-icon { width: 30px; height: 30px; border-radius: 7px; background: var(--bg4); display: flex; align-items: center; justify-content: center; font-size: 14px; flex-shrink: 0; }
.channel-item-name { font-size: 13px; font-weight: 500; color: var(--text); }
.channel-item-type { font-size: 10px; color: var(--text3); }

/* ─── ALERT ─────────────────────────────── */
.alert {
  display: flex; align-items: flex-start; gap: 10px;
  padding: 12px 14px; border-radius: var(--r);
  margin-bottom: 14px; border: 1px solid;
}
.alert-success { background: rgba(52,211,153,0.08); border-color: rgba(52,211,153,0.2); color: var(--success); }
.alert-warn { background: rgba(251,191,36,0.08); border-color: rgba(251,191,36,0.2); color: var(--warning); }
.alert-info { background: rgba(99,102,241,0.08); border-color: var(--border); color: var(--accent2); }
.alert-icon { font-size: 15px; flex-shrink: 0; margin-top: 1px; }
.alert-text { font-size: 12px; line-height: 1.5; }

/* ─── ACTION BAR ─────────────────────────────── */
.action-bar {
  display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
  margin-bottom: 16px;
}

/* ─── SECTION DIVIDER ─────────────────────────────── */
.section-head {
  display: flex; align-items: center; gap: 8px;
  margin-bottom: 14px;
}
.section-head h3 { font-size: 13px; font-weight: 600; color: var(--text2); }
.section-line { flex: 1; height: 1px; background: var(--border); }

/* ─── OPTION PILLS ─────────────────────────────── */
.pill-group { display: flex; gap: 6px; flex-wrap: wrap; }
.pill {
  padding: 5px 12px; border-radius: 99px;
  border: 1px solid var(--border);
  font-size: 12px; color: var(--text2); cursor: pointer;
  transition: all 0.15s; background: var(--bg3);
}
.pill.active { border-color: var(--accent); color: var(--accent2); background: rgba(99,102,241,0.1); }

/* ─── SCROLL STYLED ─────────────────────────────── */
.scroll-inner { max-height: 260px; overflow-y: auto; padding-right: 4px; }
.scroll-inner::-webkit-scrollbar { width: 3px; }
.scroll-inner::-webkit-scrollbar-thumb { background: var(--border2); border-radius: 99px; }

/* ─── FOOTER ─────────────────────────────── */
.footer { text-align: center; padding: 20px 0 0; color: var(--text3); font-size: 11px; }

/* ─── MOBILE ─────────────────────────────── */
@media (max-width: 640px) {
  .sidebar { display: none; }
  .metrics { grid-template-columns: 1fr 1fr; }
  .grid-2 { grid-template-columns: 1fr; }
  .help-grid { grid-template-columns: 1fr; }
  .content { padding: 16px; }
  .topbar { padding: 12px 16px; }
}

/* ─── SCAN ANIMATION ─────────────────────────────── */
.scan-pulse {
  display: inline-block;
  width: 8px; height: 8px; border-radius: 50%;
  background: var(--accent);
  animation: scanpulse 1.2s ease-in-out infinite;
}
@keyframes scanpulse {
  0%,100%{transform:scale(1);opacity:1}
  50%{transform:scale(1.5);opacity:0.5}
}
</style>
</head>
<body>

<div class="app">

  <!-- ─── SIDEBAR ─────────────────────────────── -->
  <aside class="sidebar" id="sidebar">
    <div class="logo">
      <div class="logo-icon">🧹</div>
      <div class="logo-name">TeleSweep</div>
      <div class="logo-ver">v5.0 · Telegram</div>
    </div>

    <nav class="nav">
      <div class="nav-item active" onclick="goTo('login')" id="nav-login">
        <span class="icon">🔐</span> تسجيل الدخول
      </div>
      <div class="nav-item" onclick="goTo('channel')" id="nav-channel">
        <span class="icon">📢</span> اختيار القناة
      </div>
      <div class="nav-item" onclick="goTo('scan')" id="nav-scan">
        <span class="icon">🔍</span> المسح
      </div>
      <div class="nav-item" onclick="goTo('results')" id="nav-results">
        <span class="icon">📋</span> النتائج
      </div>
    </nav>

    <div class="sidebar-footer">
      <div class="user-chip" id="user-chip" style="display:none">
        <div class="avatar" id="user-avatar">؟</div>
        <div class="user-info">
          <div class="user-name" id="user-name">—</div>
          <div class="user-sub">متصل</div>
        </div>
        <div class="status-dot"></div>
      </div>
      <button class="logout-btn" onclick="goTo('login')">🚪 تسجيل الخروج</button>
    </div>
  </aside>

  <!-- ─── MAIN ─────────────────────────────── -->
  <div class="main">

    <!-- TOPBAR -->
    <div class="topbar">
      <div>
        <div class="topbar-title" id="topbar-title">تسجيل الدخول</div>
        <div class="topbar-sub" id="topbar-sub">أدخل بيانات حسابك للمتابعة</div>
      </div>
      <div style="display:flex;align-items:center;gap:10px;">
        <span class="badge badge-blue">v5.0</span>
      </div>
    </div>

    <!-- CONTENT -->
    <div class="content">

      <!-- ═══════════════════════════════════ LOGIN ═══ -->
      <div class="screen active" id="screen-login">

        <div class="card" style="max-width:460px;margin:0 auto;">
          <div class="tabs-bar">
            <button class="tab-btn active" onclick="switchTab('phone')">📱 رقم الهاتف</button>
            <button class="tab-btn" onclick="switchTab('session')">🔑 Session String</button>
          </div>

          <!-- Phone -->
          <div class="tab-pane active" id="pane-phone">
            <div class="alert alert-info">
              <span class="alert-icon">ℹ️</span>
              <span class="alert-text">احصل على API ID وAPI Hash من <strong>my.telegram.org</strong> → API Development Tools</span>
            </div>
            <div class="field">
              <label class="label">API ID</label>
              <div class="input-wrap">
                <input type="password" class="has-eye" id="p-api-id" placeholder="مثال: 12345678">
                <button class="eye-btn" onclick="toggleEye('p-api-id',this)">👁</button>
              </div>
            </div>
            <div class="field">
              <label class="label">API Hash</label>
              <div class="input-wrap">
                <input type="password" class="has-eye" id="p-api-hash" placeholder="32 حرف هيكس">
                <button class="eye-btn" onclick="toggleEye('p-api-hash',this)">👁</button>
              </div>
            </div>
            <div class="field">
              <label class="label">رقم الهاتف</label>
              <input type="tel" id="p-phone" placeholder="+963xxxxxxxxx" dir="ltr">
              <div class="hint">بصيغة دولية مع رمز البلد</div>
            </div>
            <button class="btn btn-primary btn-full" onclick="doLogin()">إرسال رمز التحقق ←</button>
          </div>

          <!-- Session -->
          <div class="tab-pane" id="pane-session">
            <div class="alert alert-info">
              <span class="alert-icon">💡</span>
              <span class="alert-text">ادخل Session String محفوظة مسبقاً للدخول مباشرة بدون رمز SMS</span>
            </div>
            <div class="field">
              <label class="label">API ID</label>
              <div class="input-wrap">
                <input type="password" class="has-eye" id="s-api-id" placeholder="مثال: 12345678">
                <button class="eye-btn" onclick="toggleEye('s-api-id',this)">👁</button>
              </div>
            </div>
            <div class="field">
              <label class="label">API Hash</label>
              <div class="input-wrap">
                <input type="password" class="has-eye" id="s-api-hash" placeholder="32 حرف هيكس">
                <button class="eye-btn" onclick="toggleEye('s-api-hash',this)">👁</button>
              </div>
            </div>
            <div class="field">
              <label class="label">Session String</label>
              <textarea id="s-session" placeholder="1BVtsOKABCDEFGH..."></textarea>
            </div>
            <button class="btn btn-primary btn-full" onclick="doSessionLogin()">دخول مباشر ←</button>
          </div>

          <div class="help-grid">
            <div class="help-card">
              <div class="help-card-icon">🔧</div>
              <div class="help-card-title">كيف أحصل على API؟</div>
              <div class="help-card-sub">my.telegram.org</div>
            </div>
            <div class="help-card">
              <div class="help-card-icon">💾</div>
              <div class="help-card-title">ما هو Session String؟</div>
              <div class="help-card-sub">دخول آمن بدون SMS</div>
            </div>
          </div>
        </div>

        <div class="footer">TeleSweep v5.0 · صُنع بعناية بواسطة <strong style="color:var(--accent2)">F.ALSALEH</strong></div>
      </div>

      <!-- ═══════════════════════════════════ OTP ═══ -->
      <div class="screen" id="screen-otp">
        <div class="card" style="max-width:420px;margin:0 auto;">
          <div style="text-align:center;margin-bottom:20px;">
            <div style="font-size:36px;margin-bottom:10px;">📲</div>
            <div style="font-size:16px;font-weight:600;color:var(--text)">تأكيد الحساب</div>
            <div style="font-size:12px;color:var(--text2);margin-top:4px;">أدخل الرمز الذي وصلك على تيليجرام</div>
          </div>
          <div class="field">
            <label class="label">رمز OTP</label>
            <input type="text" id="otp-code" placeholder="12345" style="font-family:var(--mono);font-size:20px;letter-spacing:0.2em;text-align:center;" maxlength="6">
          </div>
          <div class="field">
            <label class="label">كلمة مرور 2FA (إن وجدت)</label>
            <div class="input-wrap">
              <input type="password" class="has-eye" id="otp-pass" placeholder="اختياري">
              <button class="eye-btn" onclick="toggleEye('otp-pass',this)">👁</button>
            </div>
          </div>
          <button class="btn btn-primary btn-full" onclick="doVerify()">تأكيد ←</button>
          <button class="btn btn-secondary btn-full" style="margin-top:8px;" onclick="goTo('login')">⬅ إعادة الإرسال</button>
        </div>
      </div>

      <!-- ═══════════════════════════════════ CHANNEL ═══ -->
      <div class="screen" id="screen-channel">
        <div class="grid-2">

          <!-- Left: Channel select -->
          <div>
            <div class="card">
              <div class="card-title">اختيار القناة</div>
              <button class="btn btn-secondary" style="margin-bottom:12px;width:100%;" onclick="fetchChannels()">📋 جلب قنواتي ومجموعاتي</button>

              <div class="scroll-inner" id="channel-list">
                <!-- Demo channels -->
                <div class="channel-list">
                  <div class="channel-item selected" onclick="selectChannel(this)">
                    <div class="channel-item-icon">📢</div>
                    <div>
                      <div class="channel-item-name">قناة الملفات</div>
                      <div class="channel-item-type">قناة · 1,240 عضو</div>
                    </div>
                  </div>
                  <div class="channel-item" onclick="selectChannel(this)">
                    <div class="channel-item-icon">👥</div>
                    <div>
                      <div class="channel-item-name">مجموعة المشاريع</div>
                      <div class="channel-item-type">مجموعة · 87 عضو</div>
                    </div>
                  </div>
                  <div class="channel-item" onclick="selectChannel(this)">
                    <div class="channel-item-icon">📁</div>
                    <div>
                      <div class="channel-item-name">أرشيف الوسائط</div>
                      <div class="channel-item-type">قناة · 540 عضو</div>
                    </div>
                  </div>
                </div>
              </div>

              <div style="margin-top:12px;">
                <label class="label">أو أدخل رابطاً يدوياً</label>
                <input type="text" placeholder="@username أو https://t.me/+xxx">
              </div>
            </div>
          </div>

          <!-- Right: Scan settings -->
          <div>
            <div class="card">
              <div class="card-title">إعدادات الفحص</div>

              <div class="field">
                <label class="label">أنواع الملفات</label>
                <div class="pill-group">
                  <div class="pill active" onclick="this.classList.toggle('active')">🖼️ صور</div>
                  <div class="pill active" onclick="this.classList.toggle('active')">🎬 فيديو</div>
                  <div class="pill" onclick="this.classList.toggle('active')">📄 مستندات</div>
                </div>
              </div>

              <div class="field">
                <label class="label">الاحتفاظ بـ</label>
                <select>
                  <option>الأقدم (Oldest)</option>
                  <option>الأحدث (Newest)</option>
                  <option>الأكبر (Largest)</option>
                </select>
              </div>

              <div class="field">
                <label class="label">الحد الأدنى للحجم (MB)</label>
                <input type="number" value="0" min="0" placeholder="0">
              </div>
            </div>

            <div class="card">
              <div class="card-title">طبقات الاكتشاف</div>

              <div class="toggle-row">
                <div class="toggle-info">
                  <div class="toggle-title">🔗 File ID <span class="badge badge-green" style="font-size:9px;">دائماً</span></div>
                  <div class="toggle-sub">تطابق مباشر — Forward آمن 100%</div>
                </div>
                <div class="toggle on" style="opacity:0.5;cursor:not-allowed;"></div>
              </div>

              <div class="toggle-row">
                <div class="toggle-info">
                  <div class="toggle-title">🔐 MD5</div>
                  <div class="toggle-sub">تطابق بايتي كامل للملفات الصغيرة</div>
                </div>
                <div class="toggle" onclick="this.classList.toggle('on')"></div>
              </div>

              <div class="toggle-row">
                <div class="toggle-info">
                  <div class="toggle-title">🖼️ pHash</div>
                  <div class="toggle-sub">تشابه بصري للصور</div>
                </div>
                <div class="toggle" onclick="this.classList.toggle('on')"></div>
              </div>

              <div class="toggle-row">
                <div class="toggle-info">
                  <div class="toggle-title">🎬 Exact Video</div>
                  <div class="toggle-sub">تطابق دقيق بالمدة والحجم والأبعاد</div>
                </div>
                <div class="toggle" onclick="this.classList.toggle('on')"></div>
              </div>
            </div>

            <button class="btn btn-primary btn-full" onclick="goTo('scan')">🚀 بدء المسح ←</button>
          </div>
        </div>
      </div>

      <!-- ═══════════════════════════════════ SCAN ═══ -->
      <div class="screen" id="screen-scan">

        <div class="channel-chip">
          <div class="channel-avatar">📢</div>
          <div>
            <div style="font-size:13px;font-weight:500;color:var(--text);">قناة الملفات</div>
            <div style="font-size:11px;color:var(--text3);">يجري الفحص...</div>
          </div>
          <div style="margin-right:auto;display:flex;align-items:center;gap:6px;">
            <div class="scan-pulse"></div>
            <span style="font-size:11px;color:var(--accent2);">نشط</span>
          </div>
        </div>

        <div class="metrics">
          <div class="metric">
            <div class="metric-label">تم فحص</div>
            <div class="metric-value" id="m-scanned">2,450</div>
            <div class="metric-sub">رسالة</div>
          </div>
          <div class="metric">
            <div class="metric-label">تم حفظ</div>
            <div class="metric-value" id="m-saved">847</div>
            <div class="metric-sub">ملف</div>
          </div>
          <div class="metric">
            <div class="metric-label">السرعة</div>
            <div class="metric-value" id="m-speed">12.3</div>
            <div class="metric-sub">msg/s</div>
          </div>
        </div>

        <div class="card">
          <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;">
            <span style="font-size:12px;color:var(--text2);">تقدم الفحص</span>
            <span style="font-size:12px;font-family:var(--mono);color:var(--accent2);" id="scan-pct">48%</span>
          </div>
          <div class="progress-bar"><div class="progress-fill" id="scan-bar" style="width:48%"></div></div>
          <div style="font-size:11px;color:var(--text3);margin-top:6px;" id="scan-status">فحص رسالة #8,340 ...</div>
        </div>

        <div class="action-bar">
          <button class="btn btn-primary" id="scan-btn" onclick="toggleScan()">⏸ إيقاف الوضع الآلي</button>
          <button class="btn btn-secondary" onclick="goTo('results')">📋 عرض النتائج</button>
          <button class="btn btn-secondary">📥 تحميل DB</button>
          <button class="btn btn-danger" onclick="confirmReset()">🔄 من البداية</button>
        </div>

        <div class="alert alert-warn" id="reset-confirm" style="display:none;">
          <span class="alert-icon">⚠️</span>
          <div class="alert-text">
            سيتم مسح كل بيانات هذه القناة والبدء من الصفر. هل أنت متأكد؟
            <div style="margin-top:8px;display:flex;gap:8px;">
              <button class="btn btn-danger" style="padding:6px 14px;font-size:12px;">✅ نعم، ابدأ</button>
              <button class="btn btn-secondary" style="padding:6px 14px;font-size:12px;" onclick="document.getElementById('reset-confirm').style.display='none'">❌ إلغاء</button>
            </div>
          </div>
        </div>

        <div class="card">
          <div class="card-title">سجل الفحص</div>
          <div style="display:flex;flex-direction:column;gap:6px;font-size:12px;font-family:var(--mono);color:var(--text2);">
            <div>[12:04:18] ✅ دفعة 50 رسالة · حُفظ 18 ملف</div>
            <div>[12:04:06] ✅ دفعة 50 رسالة · حُفظ 21 ملف</div>
            <div>[12:03:54] ✅ دفعة 50 رسالة · حُفظ 15 ملف</div>
            <div>[12:03:42] ✅ دفعة 50 رسالة · حُفظ 24 ملف</div>
            <div style="color:var(--text3)">[12:03:30] ▶ بدء الفحص من رسالة #0</div>
          </div>
        </div>
      </div>

      <!-- ═══════════════════════════════════ RESULTS ═══ -->
      <div class="screen" id="screen-results">

        <div class="alert alert-warn">
          <span class="alert-icon">⚠️</span>
          <div class="alert-text">
            <strong>127 ملف مكرر</strong> بحاجة للمراجعة ·
            <span class="badge badge-green">🔗 Forward: 84</span>
            <span class="badge badge-blue" style="margin:0 4px;">🔐 MD5: 31</span>
            <span class="badge badge-purple">🎬 Video: 12</span>
          </div>
        </div>

        <div class="grid-2">
          <div>
            <div class="action-bar" style="margin-bottom:12px;">
              <button class="btn btn-secondary" style="font-size:12px;" onclick="selectAll()">✅ تحديد الكل</button>
              <button class="btn btn-secondary" style="font-size:12px;" onclick="deselectAll()">✖ إلغاء الكل</button>
              <button class="btn btn-secondary" style="font-size:12px;">📥 CSV</button>
            </div>

            <div class="section-head">
              <h3>🔗 File ID Match</h3><div class="section-line"></div>
              <span class="badge badge-green">84</span>
            </div>

            <div class="dup-list" id="dup-list">
              <!-- Dup cards injected by JS -->
            </div>

            <div style="margin-top:12px;display:flex;gap:6px;align-items:center;justify-content:center;">
              <button class="btn btn-secondary" style="padding:6px 12px;font-size:12px;">⬅ السابقة</button>
              <span style="font-size:12px;color:var(--text2);">صفحة 1 من 3</span>
              <button class="btn btn-secondary" style="padding:6px 12px;font-size:12px;">التالية ➡</button>
            </div>
          </div>

          <div>
            <div class="card" style="position:sticky;top:0;">
              <div class="card-title">ملخص الحذف</div>
              <div class="metrics" style="grid-template-columns:1fr 1fr;">
                <div class="metric">
                  <div class="metric-label">محدد</div>
                  <div class="metric-value" id="sel-count">0</div>
                </div>
                <div class="metric">
                  <div class="metric-label">حجم التوفير</div>
                  <div class="metric-value" id="sel-size">0 MB</div>
                </div>
              </div>

              <div style="margin-top:8px;">
                <button class="btn btn-danger btn-full" id="del-btn" onclick="deleteSelected()" disabled>
                  🗑️ حذف المحددات
                </button>
              </div>

              <div style="margin-top:12px;padding-top:12px;border-top:1px solid var(--border);">
                <div class="card-title">توزيع أنواع التكرار</div>
                <div style="display:flex;flex-direction:column;gap:8px;font-size:12px;">
                  <div style="display:flex;justify-content:space-between;align-items:center;">
                    <span style="color:var(--text2)">🔗 File ID</span>
                    <div style="display:flex;align-items:center;gap:6px;">
                      <div style="width:80px;height:4px;background:var(--bg4);border-radius:99px;overflow:hidden;">
                        <div style="width:66%;height:100%;background:var(--success);border-radius:99px;"></div>
                      </div>
                      <span class="badge badge-green">84</span>
                    </div>
                  </div>
                  <div style="display:flex;justify-content:space-between;align-items:center;">
                    <span style="color:var(--text2)">🔐 MD5</span>
                    <div style="display:flex;align-items:center;gap:6px;">
                      <div style="width:80px;height:4px;background:var(--bg4);border-radius:99px;overflow:hidden;">
                        <div style="width:24%;height:100%;background:var(--accent2);border-radius:99px;"></div>
                      </div>
                      <span class="badge badge-blue">31</span>
                    </div>
                  </div>
                  <div style="display:flex;justify-content:space-between;align-items:center;">
                    <span style="color:var(--text2)">🎬 Exact Video</span>
                    <div style="display:flex;align-items:center;gap:6px;">
                      <div style="width:80px;height:4px;background:var(--bg4);border-radius:99px;overflow:hidden;">
                        <div style="width:10%;height:100%;background:var(--accent3);border-radius:99px;"></div>
                      </div>
                      <span class="badge badge-purple">12</span>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>

    </div><!-- /content -->
  </div><!-- /main -->
</div><!-- /app -->

<script>
// ─── NAV ───────────────────────────────────────────
const screens = ['login','otp','channel','scan','results'];
const topbarInfo = {
  login:   ['تسجيل الدخول','أدخل بيانات حسابك للمتابعة'],
  otp:     ['تأكيد الهوية','أدخل رمز التحقق من تيليجرام'],
  channel: ['اختيار القناة','حدد القناة وإعدادات الفحص'],
  scan:    ['فحص القناة','يجري البحث عن الملفات المكررة'],
  results: ['نتائج الفحص','راجع المكررات واحذف ما تريد'],
};

function goTo(page) {
  screens.forEach(s => {
    document.getElementById('screen-'+s)?.classList.remove('active');
    document.getElementById('nav-'+s)?.classList.remove('active');
  });
  const el = document.getElementById('screen-'+page);
  if (!el) return;
  el.classList.add('active');
  const nav = document.getElementById('nav-'+page);
  if (nav) nav.classList.add('active');

  const info = topbarInfo[page] || ['TeleSweep',''];
  document.getElementById('topbar-title').textContent = info[0];
  document.getElementById('topbar-sub').textContent = info[1];
}

// ─── LOGIN ──────────────────────────────────────────
function switchTab(t) {
  document.querySelectorAll('.tab-btn').forEach((b,i) => {
    b.classList.toggle('active', (t==='phone'&&i===0)||(t==='session'&&i===1));
  });
  document.getElementById('pane-phone').classList.toggle('active', t==='phone');
  document.getElementById('pane-session').classList.toggle('active', t==='session');
}

function toggleEye(id, btn) {
  const el = document.getElementById(id);
  el.type = el.type === 'password' ? 'text' : 'password';
  btn.textContent = el.type === 'password' ? '👁' : '🙈';
}

function doLogin() {
  const id = document.getElementById('p-api-id').value.trim();
  const hash = document.getElementById('p-api-hash').value.trim();
  const phone = document.getElementById('p-phone').value.trim();
  if (!id || !hash || !phone) { alert('الرجاء تعبئة جميع الحقول'); return; }
  goTo('otp');
}

function doSessionLogin() {
  const id = document.getElementById('s-api-id').value.trim();
  const hash = document.getElementById('s-api-hash').value.trim();
  const sess = document.getElementById('s-session').value.trim();
  if (!id || !hash || !sess) { alert('الرجاء تعبئة جميع الحقول'); return; }
  document.getElementById('user-chip').style.display = 'flex';
  document.getElementById('user-name').textContent = 'مستخدم تيليجرام';
  document.getElementById('user-avatar').textContent = 'م';
  goTo('channel');
}

function doVerify() {
  const code = document.getElementById('otp-code').value.trim();
  if (!code) { alert('أدخل رمز OTP'); return; }
  document.getElementById('user-chip').style.display = 'flex';
  document.getElementById('user-name').textContent = 'مستخدم تيليجرام';
  document.getElementById('user-avatar').textContent = 'م';
  goTo('channel');
}

// ─── CHANNEL ──────────────────────────────────────────
function selectChannel(el) {
  document.querySelectorAll('.channel-item').forEach(c => c.classList.remove('selected'));
  el.classList.add('selected');
}

function fetchChannels() {
  // demo
}

// ─── SCAN ──────────────────────────────────────────
let scanning = true;
let scanPct = 48;
let scanInterval;

function toggleScan() {
  scanning = !scanning;
  const btn = document.getElementById('scan-btn');
  if (scanning) {
    btn.textContent = '⏸ إيقاف الوضع الآلي';
    startScanAnimation();
  } else {
    btn.textContent = '▶ متابعة الفحص';
    clearInterval(scanInterval);
  }
}

function startScanAnimation() {
  clearInterval(scanInterval);
  scanInterval = setInterval(() => {
    if (!scanning) return;
    scanPct = Math.min(100, scanPct + Math.random() * 2);
    document.getElementById('scan-bar').style.width = scanPct + '%';
    document.getElementById('scan-pct').textContent = Math.round(scanPct) + '%';
    const msgId = 8000 + Math.round(scanPct * 50);
    document.getElementById('scan-status').textContent = `فحص رسالة #${msgId.toLocaleString('ar')} ...`;
    document.getElementById('m-scanned').textContent = (2000 + Math.round(scanPct * 10)).toLocaleString('ar');
    if (scanPct >= 100) { clearInterval(scanInterval); document.getElementById('scan-status').textContent = '✅ اكتمل الفحص!'; }
  }, 400);
}

function confirmReset() {
  document.getElementById('reset-confirm').style.display = 'flex';
}

// ─── RESULTS ──────────────────────────────────────────
const dummyDups = [
  { name: 'project-demo.mp4', size: '124 MB', dur: '3:42', type: '🎬', tag: 'file_id', tagLabel: '🔗 Forward', tagClass: 'badge-green' },
  { name: 'DSC_0042.jpg', size: '8.4 MB', dur: '', type: '🖼️', tag: 'md5', tagLabel: '🔐 MD5', tagClass: 'badge-blue' },
  { name: 'tutorial_final.mp4', size: '340 MB', dur: '18:22', type: '🎬', tag: 'exact_video', tagLabel: '🎬 Exact', tagClass: 'badge-purple' },
  { name: 'report-q3.pdf', size: '2.1 MB', dur: '', type: '📄', tag: 'file_id', tagLabel: '🔗 Forward', tagClass: 'badge-green' },
  { name: 'photo_2024.jpg', size: '5.7 MB', dur: '', type: '🖼️', tag: 'phash', tagLabel: '🖼️ pHash', tagClass: 'badge-amber' },
  { name: 'backup_2024.zip', size: '890 MB', dur: '', type: '📦', tag: 'md5', tagLabel: '🔐 MD5', tagClass: 'badge-blue' },
];

let selected = new Set();

function buildDupList() {
  const container = document.getElementById('dup-list');
  container.innerHTML = dummyDups.map((d, i) => `
    <div class="dup-card" id="dup-${i}" onclick="toggleDup(${i})">
      <div class="dup-icon">${d.type}</div>
      <div class="dup-body">
        <div class="dup-name">${d.name}</div>
        <div class="dup-meta">${d.size}${d.dur ? ' · ' + d.dur : ''} · أصل: #${1000+i}</div>
      </div>
      <span class="badge ${d.tagClass}">${d.tagLabel}</span>
      <div class="dup-check" id="check-${i}"></div>
    </div>
  `).join('');
}

function toggleDup(i) {
  const card = document.getElementById('dup-'+i);
  const check = document.getElementById('check-'+i);
  if (selected.has(i)) {
    selected.delete(i);
    card.classList.remove('selected');
    check.textContent = '';
  } else {
    selected.add(i);
    card.classList.add('selected');
    check.textContent = '✓';
  }
  updateSelCount();
}

function selectAll() {
  dummyDups.forEach((_, i) => {
    selected.add(i);
    document.getElementById('dup-'+i)?.classList.add('selected');
    const ch = document.getElementById('check-'+i);
    if (ch) ch.textContent = '✓';
  });
  updateSelCount();
}

function deselectAll() {
  selected.clear();
  dummyDups.forEach((_, i) => {
    document.getElementById('dup-'+i)?.classList.remove('selected');
    const ch = document.getElementById('check-'+i);
    if (ch) ch.textContent = '';
  });
  updateSelCount();
}

function updateSelCount() {
  document.getElementById('sel-count').textContent = selected.size;
  const totalMb = [...selected].reduce((s,i) => s + parseFloat(dummyDups[i].size) || 0, 0);
  document.getElementById('sel-size').textContent = totalMb.toFixed(0) + ' MB';
  document.getElementById('del-btn').disabled = selected.size === 0;
}

function deleteSelected() {
  if (!selected.size) return;
  if (!confirm(`حذف ${selected.size} ملف؟ لا يمكن التراجع.`)) return;
  selected.forEach(i => document.getElementById('dup-'+i)?.remove());
  selected.clear();
  updateSelCount();
}

// ─── INIT ──────────────────────────────────────────
buildDupList();
startScanAnimation();
</script>
</body>
</html>
