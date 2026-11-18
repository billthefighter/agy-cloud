import { escapeHtml } from "./markdown.js";

export const MODELS = [
  { id: "gemini-4-hydrogen", label: "Hydrogen" },
  { id: "gemini-4-carbon", label: "Carbon" },
  { id: "gemini-4-argon", label: "Argon" },
  { id: "gemini-3.8-flash", label: "Flash" },
];

export const PUSH_CATEGORIES = [
  { key: "coding", label: "Coding Jobs" },
  { key: "chat", label: "Direct Chat Replies" },
  { key: "sentry", label: "Sentry Triage" },
  { key: "ci", label: "CI Healer" },
  { key: "k8s", label: "K8s / Argo CD Alerts" },
  { key: "billing", label: "$100/mo Billing Alerts" },
  { key: "deps", label: "Dependency Upgrades" },
  { key: "food", label: "Food Digest & Logs" },
  { key: "ops", label: "Test & Ops Pings" },
];

function readMeta(name, fallback = "") {
  return document.querySelector(`meta[name="${name}"]`)?.getAttribute("content") ?? fallback;
}

export const appMode = readMeta("agy-app-mode", "chat");
export const chatBase = readMeta("agy-chat-base", "/agy").replace(/\/$/, "");
export const healthBase = readMeta("agy-health-base", "/health").replace(/\/$/, "");

export const state = {
  authenticated: false,
  caAvailable: false,
  slackEnabled: true,
  vapidPublicKey: "",
  wsStatus: "offline",
  tab: appMode === "health" ? "food" : "inbox",
  folder: "inbox",
  filter: "all",
  kind: "all",
  query: "",
  threads: [],
  counts: { unread_threads: 0, unread_total: 0, needs_review: 0, running: 0, inbox: 0, archived: 0 },
  defaultRepo: "lodbrok-app/rattlsnek",
  defaultModel: "gemini-4-hydrogen",
  activeThreadId: null,
  activeThread: null,
  activeMessages: [],
  activeIssue: null,
  replyModel: "gemini-4-hydrogen",
  replyText: "",
  replyFiles: [],
  newModalOpen: false,
  newMode: "chat",
  newPrompt: "",
  newRepo: "lodbrok-app/rattlsnek",
  newModel: "gemini-4-hydrogen",
  newFiles: [],
  newIncludeHealth: false,
  patchModalDiff: null,
  foodDate: "",
  foodToday: null,
  foodWeek: null,
  foodTrends: null,
  foodShowWeek: false,
  foodShowTargets: false,
  foodInput: "",
  foodModel: "gemini-4-hydrogen",
  foodFiles: [],
  foodStatus: "",
  foodPendingTs: "",
  foodError: "",
  foodNotice: "",
  foodSuggestResult: "",
  foodPhotoModal: null,
  opsOverview: null,
  pushSub: null,
  pushPrefs: Object.fromEntries(PUSH_CATEGORIES.map((c) => [c.key, true])),
  swReg: null,
  submitting: false,
  booting: true,
  bootStep: "Initializing…",
  bootDiag: null,
};

let ws = null;
let wsReconnectDelay = 1000;
let wsTimer = null;

let modeAdapter = {
  initialTab: "inbox",
  focusInputIds: [],
  onAuthenticated: async () => {},
  onVisible: () => {},
  onWsEvent: () => {},
  onSwMessage: () => {},
  renderMain: () => "",
  renderModals: () => "",
  renderTabBar: () => "",
  wireModeEvents: () => {},
};

export function registerAppMode(adapter) {
  modeAdapter = { ...modeAdapter, ...adapter };
  if (adapter.initialTab) {
    state.tab = adapter.initialTab;
  }
}

export function syncViewportHeight() {
  const vv = window.visualViewport;
  if (!vv) return;
  const inset = Math.max(0, Math.round(window.innerHeight - vv.height - vv.offsetTop));
  document.documentElement.style.setProperty("--kb-inset", `${inset}px`);
}
window.visualViewport?.addEventListener("resize", syncViewportHeight);
window.addEventListener("resize", syncViewportHeight);
syncViewportHeight();

export function getSavedToken() {
  try {
    return localStorage.getItem("agy_pwa_token") || "";
  } catch {
    return "";
  }
}

export function setSavedToken(tok) {
  try {
    if (tok) localStorage.setItem("agy_pwa_token", tok);
    else localStorage.removeItem("agy_pwa_token");
  } catch {
    /* ignore */
  }
}

export function toast(msg) {
  const root = document.getElementById("toast-root");
  if (!root) return;
  const el = document.createElement("div");
  el.className = "toast";
  el.textContent = msg;
  root.appendChild(el);
  setTimeout(() => el.remove(), 3400);
}

export async function apiFetch(path, options = {}) {
  const tok = getSavedToken();
  const headers = new Headers(options.headers || {});
  headers.set("X-AGY-Client", "pwa");
  if (tok && !headers.has("Authorization")) {
    headers.set("Authorization", `Bearer ${tok}`);
  }
  let resp;
  try {
    resp = await fetch(`./${path.replace(/^\.?\//, "")}`, {
      ...options,
      headers,
      credentials: "same-origin",
    });
  } catch (err) {
    throw new Error(`Network error: ${err?.message || "request failed"}`);
  }
  if (resp.status === 401) {
    state.authenticated = false;
    render();
    throw new Error("Session expired — please sign in.");
  }
  if (!resp.ok) {
    let detail = `HTTP ${resp.status}`;
    try {
      const rawText = await resp.text();
      if (rawText) {
        try {
          const errJson = JSON.parse(rawText);
          detail = errJson.detail || detail;
        } catch {
          const snippet = rawText.replace(/<[^>]+>/g, " ").replace(/\s+/g, " ").trim().slice(0, 200);
          if (snippet) detail = `${detail}: ${snippet}`;
        }
      }
    } catch {
      /* ignore */
    }
    throw new Error(detail);
  }
  return resp.json();
}

export function modelShortLabel(modelId) {
  const found = MODELS.find((m) => m.id === modelId);
  return found ? found.label : modelId || "Hydrogen";
}

export function cycleModel(currentId) {
  const idx = MODELS.findIndex((m) => m.id === currentId);
  return MODELS[(idx + 1) % MODELS.length].id;
}

function urlBase64ToUint8Array(base64String) {
  const padding = "=".repeat((4 - (base64String.length % 4)) % 4);
  const base64 = (base64String + padding).replace(/-/g, "+").replace(/_/g, "/");
  const rawData = atob(base64);
  const outputArray = new Uint8Array(rawData.length);
  for (let i = 0; i < rawData.length; ++i) {
    outputArray[i] = rawData.charCodeAt(i);
  }
  return outputArray;
}

export function isStandaloneMode() {
  return (
    window.matchMedia("(display-mode: standalone)").matches ||
    window.navigator.standalone === true
  );
}

export function updateAppBadge() {
  if (appMode === "health") return;
  if (navigator.setAppBadge) {
    const count = Number(state.counts?.unread_total || 0);
    if (count > 0) navigator.setAppBadge(count).catch(() => {});
    else if (navigator.clearAppBadge) navigator.clearAppBadge().catch(() => {});
  }
}

export async function registerServiceWorker() {
  if (!("serviceWorker" in navigator)) return;
  try {
    const reg = await navigator.serviceWorker.register("./sw.js", { scope: "./" });
    state.swReg = reg;
    reg.update?.().catch(() => {});
    if (reg.pushManager) {
      state.pushSub = await reg.pushManager.getSubscription();
    }
    navigator.serviceWorker.addEventListener("message", (event) => {
      modeAdapter.onSwMessage(event.data);
    });
  } catch (err) {
    console.warn("SW registration skipped:", err);
  }
}

export function connectWebSocket() {
  if (!state.authenticated) return;
  if (ws && (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING)) return;

  const proto = location.protocol === "https:" ? "wss:" : "ws:";
  const baseDir = location.pathname.endsWith("/")
    ? location.pathname
    : `${location.pathname.replace(/\/[^/]*$/, "")}/`;
  const tok = getSavedToken();
  const q = tok ? `?token=${encodeURIComponent(tok)}` : "";
  const url = `${proto}//${location.host}${baseDir}api/ws${q}`;

  state.wsStatus = "reconnecting";
  updateWsDot();
  ws = new WebSocket(url);

  ws.onopen = () => {
    state.wsStatus = "online";
    wsReconnectDelay = 1000;
    updateWsDot();
    sendWsViewState();
  };

  ws.onmessage = (event) => {
    try {
      const pkt = JSON.parse(event.data);
      const { type, data } = pkt || {};
      if (type === "hello" && data?.counts) {
        state.counts = data.counts;
        updateAppBadge();
        render();
        return;
      }
      modeAdapter.onWsEvent(pkt);
    } catch {
      /* ignore */
    }
  };

  ws.onclose = () => {
    state.wsStatus = "offline";
    updateWsDot();
    ws = null;
    if (state.authenticated) {
      clearTimeout(wsTimer);
      wsTimer = setTimeout(connectWebSocket, wsReconnectDelay);
      wsReconnectDelay = Math.min(15000, wsReconnectDelay * 1.6);
    }
  };
}

export function sendWsViewState() {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  ws.send(
    JSON.stringify({
      type: "view_thread",
      thread_id: state.activeThreadId,
      visible: document.visibilityState === "visible",
    })
  );
}

document.addEventListener("visibilitychange", () => {
  if (document.visibilityState === "visible" && state.authenticated) {
    connectWebSocket();
    sendWsViewState();
    modeAdapter.onVisible();
  } else {
    sendWsViewState();
  }
});

export function updateWsDot() {
  const dot = document.querySelector(".ws-dot");
  if (dot) {
    dot.className = `ws-dot ${state.wsStatus}`;
  }
}

export async function handleLogin(tokenStr, { rethrow = false } = {}) {
  const clean = (tokenStr || "").trim();
  if (!clean) return toast("Enter your PWA access token.");
  try {
    const res = await fetchWithTimeout(
      "./api/auth/login",
      {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-AGY-Client": "pwa" },
        credentials: "same-origin",
        body: JSON.stringify({ token: clean }),
      },
      8000
    );
    if (!res.ok) {
      throw new Error(res.status === 401 ? "Invalid access token (HTTP 401)." : `HTTP ${res.status}`);
    }
    const data = await res.json();
    setSavedToken(clean);
    state.authenticated = true;
    state.bootDiag = null;
    state.vapidPublicKey = data.vapid_public_key || state.vapidPublicKey;
    connectWebSocket();
    await modeAdapter.onAuthenticated(new URLSearchParams(location.hash.replace(/^#/, "")));
    toast("Connected to makesense.");
  } catch (err) {
    if (rethrow) throw err;
    toast(`Sign-in failed: ${err.message}`);
  }
}

export async function enableWebPush() {
  if (!("serviceWorker" in navigator) || !("PushManager" in window)) {
    return toast(
      isStandaloneMode()
        ? "Push API unavailable in this browser."
        : "First tap Share → Add to Home Screen in Safari, then open from Home Screen."
    );
  }
  try {
    const perm = await Notification.requestPermission();
    if (perm !== "granted") {
      return toast("Notification permission denied.");
    }
    const reg = state.swReg || (await navigator.serviceWorker.ready);
    state.swReg = reg;
    const keyRes = await apiFetch("api/push/vapid-public-key");
    const pubKey = keyRes.public_key || state.vapidPublicKey;
    if (!pubKey) throw new Error("Missing VAPID public key.");

    let sub = await reg.pushManager.getSubscription();
    if (!sub) {
      sub = await reg.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey: urlBase64ToUint8Array(pubKey),
      });
    }
    const subJson = sub.toJSON();
    await apiFetch("api/push/subscribe", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        endpoint: subJson.endpoint,
        keys: subJson.keys,
        prefs: state.pushPrefs,
      }),
    });
    state.pushSub = sub;
    render();
    toast("iOS Web Push enabled!");
  } catch (err) {
    toast(`Push setup failed: ${err.message}`);
  }
}

export async function sendTestPush() {
  try {
    const res = await apiFetch("api/push/test", { method: "POST" });
    toast(`Sent to ${res.sent} device(s) (pruned ${res.pruned}).`);
  } catch (err) {
    toast(err.message);
  }
}

export async function savePushPrefs() {
  if (!state.pushSub) return;
  try {
    const subJson = state.pushSub.toJSON();
    await apiFetch("api/push/preferences", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ endpoint: subJson.endpoint, prefs: state.pushPrefs }),
    });
    toast("Notification preferences saved.");
  } catch (err) {
    toast(err.message);
  }
}

function renderDiagLayerRow(label, statusText, tone) {
  const colors = {
    ok: "var(--green, #22c55e)",
    warn: "var(--amber, #f59e0b)",
    bad: "var(--red, #ef4444)",
    muted: "var(--text-muted)",
  };
  const color = colors[tone] || colors.muted;
  return `
    <div style="display:flex;justify-content:space-between;align-items:center;gap:8px;padding:4px 0;border-bottom:1px solid rgba(255,255,255,0.06);font-size:12px;">
      <span style="color:var(--text-muted);">${escapeHtml(label)}</span>
      <strong style="color:${color};font-weight:600;">${escapeHtml(statusText)}</strong>
    </div>
  `;
}

function renderBootDiagCard() {
  const d = state.bootDiag;
  if (!d) return "";

  const effectiveHttpStatus = d.httpStatus || d.healthz?.status || null;
  const gatewayReached = Boolean(effectiveHttpStatus);
  const serviceDownStatus = new Set([404, 500, 502, 503, 504]);
  const isInternetDown = !d.online || d.internetReachable === false;
  const isNetworkOrTlsAbort =
    !gatewayReached &&
    !d.timedOut &&
    /load failed|failed to fetch|networkerror/i.test(d.errorMessage || "");

  let badge = "NETWORK/TLS";
  let headline = "Request failed before reaching the server.";
  let diagnosis = "Check your connection and retry.";

  if (isInternetDown) {
    badge = "NO INTERNET";
    headline = "No internet connection on this device";
    diagnosis =
      "Public internet check failed. Connect to Wi-Fi or cellular data, then tap Retry.";
  } else if (effectiveHttpStatus && serviceDownStatus.has(effectiveHttpStatus)) {
    badge = `SERVICE DOWN (${effectiveHttpStatus})`;
    headline = `agent-tasks service is down or restarting (HTTP ${effectiveHttpStatus})`;
    diagnosis =
      effectiveHttpStatus === 502 || effectiveHttpStatus === 503 || effectiveHttpStatus === 504
        ? "NetBird VPN and TLS are working — Traefik reached the cluster, but agent-tasks-controller is down, restarting, or not ready yet."
        : `NetBird VPN and TLS are working, but the service returned HTTP ${effectiveHttpStatus} during ${d.step}.`;
  } else if (effectiveHttpStatus) {
    badge = `HTTP ${effectiveHttpStatus}`;
    headline = `Service returned HTTP ${effectiveHttpStatus}`;
    diagnosis = `NetBird VPN and TLS are working, but ${d.step} failed with HTTP ${effectiveHttpStatus}${d.healthz?.ok ? " (/healthz is OK)" : ""}.`;
  } else if (d.timedOut) {
    badge = "TIMEOUT";
    headline = `Timed out after ${(d.elapsedMs / 1000).toFixed(1)}s`;
    diagnosis =
      "Your device has internet, but makesense-k3s.netbird.cloud did not respond in time. Either NetBird VPN is disconnected/asleep, or the controller pod is hung/restarting.";
  } else if (isNetworkOrTlsAbort) {
    badge = "NETBIRD OR TLS";
    headline = "NetBird VPN disconnected or TLS CA not trusted";
    diagnosis =
      "Your device has internet, but the connection to makesense-k3s.netbird.cloud aborted immediately. " +
      "1) Open NetBird and verify it is Connected (this domain only resolves on NetBird). " +
      "2) On iOS, verify Settings → General → About → Certificate Trust Settings → Homelab Internal CA is enabled.";
  }

  const internetRow = renderDiagLayerRow(
    "Device Internet",
    isInternetDown ? "Offline" : d.internetReachable ? "Online" : "Assumed Online",
    isInternetDown ? "bad" : "ok"
  );
  const netbirdRow = renderDiagLayerRow(
    "NetBird VPN / DNS",
    gatewayReached
      ? "Connected (Traefik reached)"
      : isInternetDown
        ? "Waiting for internet"
        : d.timedOut
          ? "Timed out / asleep?"
          : "Unreachable or TLS blocked",
    gatewayReached ? "ok" : isInternetDown ? "muted" : "warn"
  );
  const tlsRow = renderDiagLayerRow(
    "TLS (Homelab CA)",
    gatewayReached ? "Trusted" : isNetworkOrTlsAbort ? "Unverified (check iOS Trust)" : "Not reached",
    gatewayReached ? "ok" : isNetworkOrTlsAbort ? "warn" : "muted"
  );
  const serviceLabel = appMode === "health" ? `AGY Health (${healthBase || "/health"})` : `AGY Service (${chatBase || "/agy"})`;
  const serviceRow = renderDiagLayerRow(
    serviceLabel,
    d.healthz?.ok
      ? `Healthy (uptime ${d.healthz.uptime_s ?? "?"}s)`
      : effectiveHttpStatus
        ? `Down / Error (HTTP ${effectiveHttpStatus})`
        : "Not reached",
    d.healthz?.ok ? "ok" : effectiveHttpStatus ? "bad" : "muted"
  );

  const healthzLine = d.healthz
    ? `healthz=${d.healthz.ok ? `200 (uptime ${d.healthz.uptime_s}s)` : d.healthz.status ? `HTTP ${d.healthz.status}` : d.healthz.error || "failed"}`
    : "healthz=not-run";

  const detailsText = [
    `step=${d.step} ms=${d.elapsedMs} status=${d.httpStatus ?? "none"}`,
    `err=${d.errorName}: ${d.errorMessage}`,
    `internet=${d.internetReachable} online=${d.online} ${healthzLine}`,
    `mode=${d.standalone ? "standalone-pwa" : "safari-tab"} token=${d.hasToken ? "saved" : "none"} sw=${d.swState}`,
    `url=${d.url}`,
  ].join("\n");

  return `
    <div class="card" style="padding:16px;border-color:rgba(239,68,68,0.55);background:rgba(239,68,68,0.08);">
      <div style="display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:6px;">
        <strong style="color:var(--red);font-size:15px;">Connection Check Failed</strong>
        <span class="status-badge failed">${escapeHtml(badge)}</span>
      </div>
      <div style="font-size:13px;font-weight:600;color:var(--text);margin:0 0 4px;">${escapeHtml(headline)}</div>
      <p style="margin:0 0 10px;font-size:12.5px;line-height:1.45;color:var(--text-muted);">${escapeHtml(diagnosis)}</p>
      <div style="margin:0 0 10px;padding:6px 10px;border-radius:8px;background:rgba(0,0,0,0.22);">
        ${internetRow}
        ${netbirdRow}
        ${tlsRow}
        ${serviceRow}
      </div>
      <pre style="margin:0 0 10px;padding:8px 10px;border-radius:8px;background:rgba(0,0,0,0.28);font-size:11px;line-height:1.4;white-space:pre-wrap;word-break:break-word;color:var(--text-muted);">${escapeHtml(detailsText)}</pre>
      <div style="display:flex;gap:8px;flex-wrap:wrap;">
        <button id="btn-retry-boot" class="full-btn secondary" style="flex:1;min-width:140px;">↻ Retry Connection</button>
        <a href="./healthz" target="_blank" rel="noopener" class="full-btn secondary" style="flex:1;min-width:140px;text-align:center;text-decoration:none;display:inline-flex;align-items:center;justify-content:center;">Test /healthz in Safari ↗</a>
      </div>
    </div>
  `;
}

function renderLoginScreen() {
  const caHint = state.caAvailable
    ? `<p style="font-size:13px;color:var(--text-muted);line-height:1.45;margin:0 0 10px;">
        <strong>First-time iPhone setup:</strong><br/>
        1. <a href="./ca.crt" style="color:var(--accent);">Download homelab-ca.crt</a> → Settings → Profile Downloaded → Install.<br/>
        2. Settings → General → About → Certificate Trust Settings → enable <strong>Homelab Internal CA</strong>.<br/>
        3. Safari → Share → <strong>Add to Home Screen</strong> for push notifications.
      </p>`
    : "";

  const appTitle = appMode === "health" ? "AGY Health" : "AGY Tasks";
  return `
    <div class="main-scroll" style="display:flex;flex-direction:column;justify-content:center;gap:14px;">
      ${renderBootDiagCard()}
      <div class="card" style="padding:20px;">
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:12px;">
          <img src="./icons/icon-180.png" width="48" height="48" style="border-radius:12px;" alt="${escapeHtml(appTitle)}" />
          <div>
            <div style="font-size:20px;font-weight:750;">${escapeHtml(appTitle)}</div>
            <div style="font-size:13px;color:var(--text-muted);">makesense k3s mobile gateway</div>
          </div>
        </div>
        ${caHint}
        <label class="field-label" for="login-token">PWA Access Token</label>
        <input id="login-token" class="text-field" type="password" placeholder="Paste PWA_API_TOKEN from .env.makesense" autocomplete="current-password" />
        <button id="login-submit" class="full-btn" style="margin-top:12px;">Sign In</button>
      </div>
    </div>
  `;
}

export function renderSettingsView() {
  const standalone = isStandaloneMode();
  const pushSupported = "serviceWorker" in navigator && "PushManager" in window;
  const perm = typeof Notification !== "undefined" ? Notification.permission : "unsupported";
  const subActive = Boolean(state.pushSub);
  const categories =
    appMode === "health"
      ? PUSH_CATEGORIES.filter((c) => c.key === "food" || c.key === "ops")
      : PUSH_CATEGORIES;

  const switchHref = appMode === "health" ? `${chatBase}/` : `${healthBase}/`;
  const switchLabel = appMode === "health" ? "Open AGY Tasks (Chat & Ops) ↗" : "Open AGY Health (Food & Garmin) ↗";

  return `
    <div class="top-bar">
      <div class="top-bar-title"><span class="ws-dot ${state.wsStatus}"></span><span>Settings & Push</span></div>
    </div>
    <div class="main-scroll">
      <div class="card">
        <strong>iOS Web Push Notifications</strong>
        <p style="font-size:13px;color:var(--text-muted);line-height:1.45;">
          Standalone Mode: <strong>${standalone ? "Yes (Home Screen)" : "No (Safari Tab)"}</strong><br/>
          Push API: <strong>${pushSupported ? perm : "Unavailable — add to Home Screen first"}</strong><br/>
          Subscription: <strong>${subActive ? "Active on this device" : "Not subscribed yet"}</strong>
        </p>
        <div style="display:flex;flex-direction:column;gap:8px;margin-top:10px;">
          <button id="btn-enable-push" class="full-btn">${subActive ? "Re-sync Push Subscription" : "Enable Push Notifications"}</button>
          <button id="btn-test-push" class="full-btn secondary">Send Test Push</button>
        </div>
      </div>

      <div class="card">
        <strong style="display:block;margin-bottom:6px;">Notification Categories</strong>
        ${categories
          .map(
            (cat) => `
          <label class="toggle-row">
            <span>${escapeHtml(cat.label)}</span>
            <input type="checkbox" data-push-pref="${cat.key}" ${state.pushPrefs[cat.key] !== false ? "checked" : ""} />
          </label>`
          )
          .join("")}
        ${subActive ? '<button id="btn-save-prefs" class="full-btn secondary" style="margin-top:10px;">Save Preferences</button>' : ""}
      </div>

      <div class="card">
        <a href="${escapeHtml(switchHref)}" class="full-btn secondary" style="display:block;text-align:center;text-decoration:none;margin-bottom:8px;">${escapeHtml(switchLabel)}</a>
        ${
          state.caAvailable
            ? `<a href="./ca.crt" class="full-btn secondary" style="display:block;text-align:center;text-decoration:none;margin-bottom:8px;">Download homelab-ca.crt</a>`
            : ""
        }
        <button id="btn-logout" class="full-btn secondary" style="color:var(--red);">Sign Out</button>
      </div>
    </div>
  `;
}

export function render() {
  const root = document.getElementById("app");
  if (!root) return;

  const activeId = document.activeElement?.id;

  let bodyHtml = "";
  if (state.booting && !state.authenticated) {
    bodyHtml = `
      <div class="boot-loader">
        <div class="spinner"></div>
        <p>${escapeHtml(state.bootStep || "Connecting to makesense…")}</p>
      </div>
    `;
  } else if (!state.authenticated) {
    bodyHtml = renderLoginScreen();
  } else if (state.tab === "settings") {
    bodyHtml = renderSettingsView();
  } else {
    bodyHtml = modeAdapter.renderMain();
  }

  const tabBarHtml = state.authenticated && !state.activeThreadId ? modeAdapter.renderTabBar() : "";
  const modalsHtml = state.authenticated ? modeAdapter.renderModals() : "";
  root.innerHTML = `${bodyHtml}${tabBarHtml}${modalsHtml}`;
  wireCommonEvents();
  modeAdapter.wireModeEvents();

  if (activeId && modeAdapter.focusInputIds.includes(activeId)) {
    const el = document.getElementById(activeId);
    if (el) {
      el.focus();
      if (typeof el.setSelectionRange === "function" && el.value) {
        const end = el.value.length;
        el.setSelectionRange(end, end);
      }
    }
  }
}

function wireCommonEvents() {
  document.getElementById("btn-retry-boot")?.addEventListener("click", () => {
    boot();
  });
  document.getElementById("login-submit")?.addEventListener("click", () => {
    handleLogin(document.getElementById("login-token")?.value || "");
  });
  document.getElementById("login-token")?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") handleLogin(e.target.value);
  });

  document.getElementById("btn-enable-push")?.addEventListener("click", enableWebPush);
  document.getElementById("btn-test-push")?.addEventListener("click", sendTestPush);
  document.querySelectorAll("[data-push-pref]").forEach((chk) => {
    chk.addEventListener("change", () => {
      state.pushPrefs[chk.dataset.pushPref] = chk.checked;
    });
  });
  document.getElementById("btn-save-prefs")?.addEventListener("click", savePushPrefs);
  document.getElementById("btn-logout")?.addEventListener("click", async () => {
    try {
      await fetch("./api/auth/logout", {
        method: "POST",
        headers: { "X-AGY-Client": "pwa" },
        credentials: "same-origin",
      });
    } catch {
      /* ignore */
    }
    setSavedToken("");
    state.authenticated = false;
    ws?.close();
    render();
  });
}

async function fetchWithTimeout(path, options = {}, timeoutMs = 8000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error(`Timed out after ${timeoutMs / 1000}s`)), timeoutMs);
  try {
    return await fetch(path, {
      cache: "no-store",
      ...options,
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timer);
  }
}

async function probeHealthz() {
  try {
    const resp = await fetchWithTimeout("./healthz", { credentials: "same-origin" }, 3500);
    let uptime = null;
    let ok = resp.ok;
    try {
      const data = await resp.json();
      ok = Boolean(resp.ok && data?.ok);
      if (typeof data?.uptime_s === "number") uptime = data.uptime_s;
    } catch {
      /* non-JSON 502/503 HTML from Traefik */
    }
    return { ok, status: resp.status, uptime_s: uptime, error: null };
  } catch (err) {
    return {
      ok: false,
      status: null,
      uptime_s: null,
      error: err?.name === "AbortError" ? "timeout" : err?.message || String(err),
    };
  }
}

async function probePublicInternet() {
  if (!navigator.onLine) return false;
  try {
    await fetchWithTimeout("https://1.1.1.1/cdn-cgi/trace", { mode: "no-cors" }, 2500);
    return true;
  } catch {
    return false;
  }
}

export async function boot() {
  state.booting = true;
  state.bootDiag = null;
  state.bootStep = "Registering offline worker…";
  render();

  await Promise.race([
    registerServiceWorker(),
    new Promise((resolve) => setTimeout(resolve, 2000)),
  ]);

  const hashParams = new URLSearchParams(location.hash.replace(/^#/, ""));
  const hashToken = hashParams.get("token");
  if (hashToken) {
    setSavedToken(hashToken);
    history.replaceState(null, "", location.pathname + location.search);
  }

  let step = "GET ./api/auth/session";
  let httpStatus = null;
  const startedAt = performance.now();
  state.bootStep = "Checking makesense gateway…";
  render();

  try {
    const resp = await fetchWithTimeout("./api/auth/session", { credentials: "same-origin" }, 8000);
    httpStatus = resp.status;
    if (!resp.ok) {
      throw new Error(`HTTP ${resp.status} ${resp.statusText || ""}`.trim());
    }
    const sess = await resp.json();
    state.caAvailable = Boolean(sess.ca_available);
    state.slackEnabled = Boolean(sess.slack_enabled);
    state.vapidPublicKey = sess.vapid_public_key || "";
    let loggedInViaHelper = false;
    if (sess.authenticated) {
      state.authenticated = true;
    } else if (hashToken || getSavedToken()) {
      step = "POST ./api/auth/login";
      state.bootStep = "Signing in with saved token…";
      render();
      await handleLogin(hashToken || getSavedToken(), { rethrow: true });
      loggedInViaHelper = true;
    }
    if (state.authenticated && !loggedInViaHelper) {
      connectWebSocket();
      await modeAdapter.onAuthenticated(hashParams);
    }
  } catch (err) {
    const elapsedMs = Math.round(performance.now() - startedAt);
    const errName = err?.name || "Error";
    const errMessage = err?.message || String(err);
    const timedOut = errName === "AbortError" || /timed out/i.test(errMessage);
    const swState = !("serviceWorker" in navigator)
      ? "unsupported"
      : navigator.serviceWorker.controller
        ? "controlling"
        : state.swReg?.active
          ? "active"
          : "none";
    state.bootStep = "Diagnosing connection failure…";
    render();
    const [healthz, internetReachable] = await Promise.all([
      probeHealthz(),
      probePublicInternet(),
    ]);
    state.bootDiag = {
      step,
      elapsedMs,
      httpStatus,
      timedOut,
      errorName: errName,
      errorMessage: errMessage,
      online: navigator.onLine,
      internetReachable,
      healthz,
      standalone: isStandaloneMode(),
      hasToken: Boolean(hashToken || getSavedToken()),
      swState,
      url: location.href,
    };
    toast(`Connection check failed: ${errMessage}`);
  } finally {
    state.booting = false;
  }

  if (!state.authenticated) {
    render();
  }
}
