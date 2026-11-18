import { escapeHtml, relativeTime, renderDiff, renderMarkdown } from "./markdown.js";
import {
  apiFetch,
  cycleModel,
  healthBase,
  modelShortLabel,
  registerAppMode,
  render,
  sendWsViewState,
  state,
  toast,
  updateAppBadge,
} from "./core.js";

async function loadThreads() {
  try {
    const params = new URLSearchParams({
      folder: state.filter === "archived" ? "archived" : state.folder,
      filter: state.filter,
      kind: state.kind === "all" ? "" : state.kind,
      exclude_kinds: "food",
      q: state.query,
    });
    const res = await apiFetch(`api/threads?${params.toString()}`);
    state.threads = res.threads || [];
    state.counts = res.counts || state.counts;
    if (res.default_repo) {
      state.defaultRepo = res.default_repo;
      if (!state.newRepo) state.newRepo = res.default_repo;
    }
    if (res.default_model) {
      state.defaultModel = res.default_model;
    }
    updateAppBadge();
    render();
  } catch (err) {
    toast(err.message);
  }
}

async function openThread(threadId) {
  state.tab = "inbox";
  state.activeThreadId = threadId;
  sendWsViewState();
  try {
    const res = await apiFetch(`api/threads/${encodeURIComponent(threadId)}`);
    state.activeThread = res.thread;
    state.activeMessages = res.messages || [];
    state.activeIssue = res.issue || null;
    state.replyModel = res.thread?.model || state.defaultModel;
    if (res.counts) state.counts = res.counts;
    updateAppBadge();
    render();
    scrollMessagesBottom();
  } catch (err) {
    toast(err.message);
  }
}

function closeThread() {
  state.activeThreadId = null;
  state.activeThread = null;
  state.activeMessages = [];
  state.activeIssue = null;
  sendWsViewState();
  render();
  loadThreads();
}

function scrollMessagesBottom() {
  requestAnimationFrame(() => {
    const scroller = document.getElementById("thread-scroll");
    if (scroller) scroller.scrollTop = scroller.scrollHeight;
  });
}

async function loadOpsOverview() {
  try {
    state.opsOverview = await apiFetch("api/ops/overview");
    render();
  } catch (err) {
    toast(err.message);
  }
}

async function toggleThreadOrg(threadId, patch) {
  try {
    const res = await apiFetch(`api/threads/${encodeURIComponent(threadId)}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(patch),
    });
    if (res.counts) state.counts = res.counts;
    if (state.activeThreadId === threadId && res.thread) {
      state.activeThread = res.thread;
    }
    await loadThreads();
  } catch (err) {
    toast(err.message);
  }
}

async function submitNewTaskOrChat() {
  if (state.submitting) return;
  const prompt = state.newPrompt.trim();
  if (!prompt && state.newFiles.length === 0) {
    return toast("Enter a prompt or attach a photo.");
  }
  state.submitting = true;
  render();
  try {
    if (state.newMode === "chat") {
      const fd = new FormData();
      fd.append("prompt", prompt);
      fd.append("model", state.newModel);
      fd.append("include_health_context", state.newIncludeHealth ? "true" : "false");
      for (const f of state.newFiles) fd.append("files", f);
      const res = await apiFetch("api/chats", { method: "POST", body: fd });
      state.newModalOpen = false;
      state.newPrompt = "";
      state.newFiles = [];
      state.submitting = false;
      if (res.thread?.thread_id) {
        await openThread(res.thread.thread_id);
      }
    } else {
      const res = await apiFetch("api/coding", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          prompt,
          repo: state.newRepo.trim() || state.defaultRepo,
          model: state.newModel,
        }),
      });
      state.newModalOpen = false;
      state.newPrompt = "";
      state.newFiles = [];
      state.submitting = false;
      toast("Coding job queued.");
      if (res.thread?.thread_id) {
        await openThread(res.thread.thread_id);
      }
    }
  } catch (err) {
    state.submitting = false;
    render();
    toast(err.message);
  }
}

async function submitThreadReply() {
  if (!state.activeThreadId || state.submitting) return;
  const txt = state.replyText.trim();
  if (!txt && state.replyFiles.length === 0) return;

  state.submitting = true;
  render();
  try {
    const fd = new FormData();
    fd.append("text", txt);
    fd.append("model", state.replyModel);
    for (const f of state.replyFiles) fd.append("files", f);
    const tid = state.activeThreadId;
    state.replyText = "";
    state.replyFiles = [];
    await apiFetch(`api/threads/${encodeURIComponent(tid)}/reply`, {
      method: "POST",
      body: fd,
    });
    state.submitting = false;
    await openThread(tid);
  } catch (err) {
    state.submitting = false;
    render();
    toast(err.message);
  }
}

async function runThreadAction(actionId) {
  if (!state.activeThreadId) return;
  try {
    toast(`Running ${actionId.replace(/_/g, " ")}…`);
    const res = await apiFetch(
      `api/threads/${encodeURIComponent(state.activeThreadId)}/actions`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: actionId }),
      }
    );
    state.activeThread = res.thread;
    state.activeMessages = res.messages || state.activeMessages;
    render();
    scrollMessagesBottom();
  } catch (err) {
    toast(err.message);
  }
}

async function viewFullPatch(threadId) {
  try {
    const res = await apiFetch(`api/threads/${encodeURIComponent(threadId)}/patch`);
    state.patchModalDiff = res.diff || "No patch diff available.";
    render();
  } catch (err) {
    toast(err.message);
  }
}

function renderInboxView() {
  const c = state.counts || {};
  const filters = [
    { id: "all", label: "Inbox", count: null },
    { id: "needs_review", label: "Review", count: c.needs_review || 0 },
    { id: "running", label: "Active", count: c.running || 0 },
    { id: "chats", label: "Chats", count: null },
    { id: "archived", label: "Archived", count: null },
  ];
  const kinds = [
    ["all", "All"],
    ["chat", "Chat"],
    ["coding", "Coding"],
    ["sentry", "Sentry"],
    ["ci", "CI"],
    ["k8s", "K8s"],
    ["billing", "Billing"],
  ];

  const listHtml = state.threads.length
    ? state.threads
        .map((t) => {
          const unreadCls = t.unread_count > 0 ? " unread" : "";
          const pinLabel = t.pinned ? "📌 " : "";
          const archLabel = t.folder === "archived" ? "Unarchive" : "Archive";
          const nextFolder = t.folder === "archived" ? "inbox" : "archived";
          return `
            <div class="thread-card${unreadCls}" data-open-thread="${escapeHtml(t.thread_id)}">
              <div class="thread-card-top">
                <div class="thread-badges">
                  ${t.unread_count > 0 ? '<span class="unread-dot"></span>' : ""}
                  <span class="kind-badge ${escapeHtml(t.kind)}">${escapeHtml(t.kind)}</span>
                  <span class="status-badge ${escapeHtml(t.status)}">${escapeHtml(t.status.replace(/_/g, " "))}</span>
                </div>
                <span style="font-size:11px;color:var(--text-dim);">${escapeHtml(relativeTime(t.updated_at))}</span>
              </div>
              <div class="thread-title">${pinLabel}${escapeHtml(t.title)}</div>
              ${t.last_message_preview ? `<div class="thread-preview">${escapeHtml(t.last_message_preview)}</div>` : ""}
              <div class="thread-meta-row">
                <span>${escapeHtml(t.subtitle || modelShortLabel(t.model))}</span>
                <div class="mini-actions">
                  <button class="mini-btn" data-pin-thread="${escapeHtml(t.thread_id)}" data-pinned="${t.pinned ? "1" : "0"}">${t.pinned ? "Unpin" : "Pin"}</button>
                  <button class="mini-btn" data-folder-thread="${escapeHtml(t.thread_id)}" data-next-folder="${nextFolder}">${archLabel}</button>
                </div>
              </div>
            </div>
          `;
        })
        .join("")
    : `<div class="empty-state">No threads match this filter.<br/>Tap <strong>+ New</strong> to start a fast chat or repo coding job.</div>`;

  return `
    <div class="top-bar">
      <div class="top-bar-title">
        <span class="ws-dot ${state.wsStatus}"></span>
        <span class="truncate">Inbox</span>
      </div>
      <div class="top-actions">
        ${healthBase ? `<a href="${escapeHtml(healthBase)}/" class="icon-btn" title="Open AGY Health" style="text-decoration:none;">🍽️</a>` : ""}
        <button id="btn-refresh" class="icon-btn" title="Refresh">↻</button>
        <button id="btn-new-modal" class="icon-btn primary">+ New</button>
      </div>
    </div>
    <div class="main-scroll">
      <div class="seg-control">
        ${filters
          .map(
            (f) => `
          <button class="seg-btn${state.filter === f.id ? " active" : ""}" data-filter="${f.id}">
            ${f.label}${f.count ? `<span class="pill-count">${f.count}</span>` : ""}
          </button>`
          )
          .join("")}
      </div>
      <div class="chip-row">
        ${kinds
          .map(
            ([k, lbl]) =>
              `<button class="chip${state.kind === k ? " active" : ""}" data-kind="${k}">${lbl}</button>`
          )
          .join("")}
      </div>
      <input id="inbox-search" class="search-input" type="search" placeholder="Search threads, repos, errors…" value="${escapeHtml(state.query)}" />
      ${listHtml}
    </div>
  `;
}

function renderThreadDetailView() {
  const t = state.activeThread;
  if (!t) return renderInboxView();

  const msgsHtml = state.activeMessages
    .map((m) => {
      const p = m.payload || {};
      const mediaHtml = Array.isArray(p.media) && p.media.length
        ? `<div class="msg-photos">${p.media
            .map((rel) => `<img class="msg-photo" src="./api/media/${escapeHtml(rel)}" alt="attachment" />`)
            .join("")}</div>`
        : "";

      const report = p.report;
      const reportMeta = report
        ? `<div style="display:flex;flex-wrap:wrap;gap:6px;margin-top:8px;font-size:11px;">
            <span class="status-badge">${escapeHtml(report.confidence || "medium")} conf</span>
            <span class="status-badge ${report.tests_passed ? "resolved" : "failed"}">${
            report.tests_passed ? "Tests PASSED" : "Needs Check"
          }</span>
            ${p.cost_line ? `<span class="kind-badge">${escapeHtml(p.cost_line)}</span>` : ""}
          </div>`
        : p.cost_line
          ? `<div style="margin-top:6px;font-size:11px;color:var(--text-dim);">${escapeHtml(p.cost_line)}</div>`
          : "";

      const diffHtml = p.diff_preview
        ? `<div class="diff-box">
            <div class="diff-summary-bar">
              <span>Proposed Patch (${p.patch_len || p.diff_preview.length} chars)</span>
              ${
                p.has_full_patch
                  ? `<button class="mini-btn" data-full-patch="${escapeHtml(t.thread_id)}">Full Patch</button>`
                  : ""
              }
            </div>
            <pre class="diff-pre">${renderDiff(p.diff_preview)}</pre>
          </div>`
        : "";

      const actionsHtml =
        Array.isArray(p.actions) && p.actions.length && t.issue_id
          ? `<div class="action-strip">${p.actions
              .map(
                (a) =>
                  `<button class="act-btn ${escapeHtml(a.style || "")}" data-thread-action="${escapeHtml(a.id)}">${escapeHtml(a.label)}</button>`
              )
              .join("")}</div>`
          : "";

      return `
        <div class="msg-bubble ${escapeHtml(m.sender)}">
          <div class="msg-header">
            <span>${escapeHtml(m.sender.toUpperCase())}${p.model ? ` • ${escapeHtml(modelShortLabel(p.model))}` : ""}</span>
            <span>${escapeHtml(relativeTime(m.created_at))}</span>
          </div>
          <div class="md-body">${renderMarkdown(m.body_md)}</div>
          ${mediaHtml}
          ${reportMeta}
          ${diffHtml}
          ${actionsHtml}
        </div>
      `;
    })
    .join("");

  const prBanner = state.activeIssue?.pr_url
    ? `<div class="card" style="padding:10px 12px;border-color:rgba(34,197,94,0.4);">
        <strong>PR Linked:</strong> <a href="${escapeHtml(state.activeIssue.pr_url)}" target="_blank" rel="noopener noreferrer" style="color:var(--accent);">${escapeHtml(state.activeIssue.pr_url)}</a>
      </div>`
    : "";

  const attachLabel = state.replyFiles.length
    ? `<span style="font-size:11px;color:var(--accent);">${state.replyFiles.length} photo(s) attached</span>`
    : `<span style="font-size:11px;color:var(--text-dim);">${escapeHtml(t.subtitle || t.kind)}</span>`;

  return `
    <div class="top-bar">
      <button id="btn-back-inbox" class="icon-btn">‹ Back</button>
      <div class="top-bar-title" style="flex:1;">
        <span class="ws-dot ${state.wsStatus}"></span>
        <span class="truncate">${escapeHtml(t.title)}</span>
      </div>
      <div class="top-actions">
        <button class="icon-btn" data-pin-thread="${escapeHtml(t.thread_id)}" data-pinned="${t.pinned ? "1" : "0"}">${t.pinned ? "📌" : "Pin"}</button>
        <button class="icon-btn" data-folder-thread="${escapeHtml(t.thread_id)}" data-next-folder="${t.folder === "archived" ? "inbox" : "archived"}">${t.folder === "archived" ? "Unarch" : "Arch"}</button>
      </div>
    </div>
    <div id="thread-scroll" class="main-scroll">
      ${prBanner}
      <div class="msg-list">
        ${msgsHtml || '<div class="empty-state">No messages yet.</div>'}
      </div>
    </div>
    <div class="composer-dock">
      <div class="composer-tools">
        ${attachLabel}
        <div style="display:flex;gap:6px;align-items:center;">
          <label class="mini-btn" style="cursor:pointer;">
            📷
            <input id="reply-file-input" type="file" accept="image/*" multiple style="display:none;" />
          </label>
          <button id="btn-reply-model" class="model-pill">${escapeHtml(modelShortLabel(state.replyModel))}</button>
        </div>
      </div>
      <div class="composer-row">
        <textarea id="reply-input" class="composer-input" rows="1" placeholder="${
          t.kind === "chat" ? "Message AGY…" : "Reply with feedback or [carbon]…"
        }">${escapeHtml(state.replyText)}</textarea>
        <button id="btn-send-reply" class="composer-send"${state.submitting ? " disabled" : ""}>${
    state.submitting ? "…" : "Send"
  }</button>
      </div>
    </div>
  `;
}

function renderOpsView() {
  const ops = state.opsOverview;
  const b = ops?.billing;
  const pct = b ? Math.min(100, Math.round(b.pct)) : 0;
  const fillCls = pct >= 90 ? "red" : pct >= 70 ? "amber" : "green";

  return `
    <div class="top-bar">
      <div class="top-bar-title"><span class="ws-dot ${state.wsStatus}"></span><span>Ops & Billing</span></div>
      <div class="top-actions">
        <button id="btn-ops-refresh" class="icon-btn">↻</button>
      </div>
    </div>
    <div class="main-scroll">
      ${
        b
          ? `<div class="card">
              <div style="display:flex;justify-content:space-between;align-items:center;">
                <strong>$${b.spent_usd.toFixed(2)} / $${b.credit_usd.toFixed(2)} (${b.pct.toFixed(1)}%)</strong>
                <span class="status-badge ${b.paused ? "failed" : "resolved"}">${b.paused ? "PAUSED" : "ACTIVE"}</span>
              </div>
              <div class="progress-track" style="height:10px;"><div class="progress-fill ${fillCls}" style="width:${pct}%"></div></div>
              <div style="font-size:12px;color:var(--text-muted);margin-top:8px;">
                Cycle <code>${escapeHtml(b.cycle_key)}</code> · ${b.remaining_days}d left · ${b.job_count} turns
                ${b.projected_usd != null ? `<br/>Projected: <strong>$${b.projected_usd.toFixed(2)}</strong> ($${(b.daily_burn_usd || 0).toFixed(2)}/day)` : ""}
              </div>
              <div class="action-strip">
                <button class="act-btn ${b.paused ? "primary" : "danger"}" data-billing-cmd="${b.paused ? "resume" : "pause"}">${
              b.paused ? "Resume Automations" : "Pause Background"
            }</button>
                <button id="btn-billing-sync" class="act-btn">Sync Spend</button>
                <button class="act-btn" data-billing-cmd="test-alert">Test Alert</button>
              </div>
            </div>`
          : '<div class="card">Loading billing ledger…</div>'
      }

      <div class="card">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
          <strong>Dependency Audit</strong>
          <div style="display:flex;gap:6px;">
            <button class="mini-btn" data-deps-focus="python">Python</button>
            <button class="mini-btn" data-deps-focus="web">Web</button>
            <button class="mini-btn" data-deps-focus="">All</button>
          </div>
        </div>
        <div style="font-size:12px;color:var(--text-muted);">
          Node: <code>${escapeHtml(ops?.node_name || "-")}</code> · Model: <code>${escapeHtml(ops?.worker_model || "-")}</code> · Slack: ${ops?.slack_enabled ? "On" : "Off"}
        </div>
      </div>

      ${
        ops?.reports?.k8s
          ? `<div class="card"><strong style="display:block;margin-bottom:6px;">K8s & Argo CD</strong><div class="md-body">${renderMarkdown(ops.reports.k8s)}</div></div>`
          : ""
      }
      ${
        ops?.reports?.ci
          ? `<div class="card"><strong style="display:block;margin-bottom:6px;">CI Healer</strong><div class="md-body">${renderMarkdown(ops.reports.ci)}</div></div>`
          : ""
      }
      ${
        ops?.reports?.controller
          ? `<div class="card"><strong style="display:block;margin-bottom:6px;">Controller Status</strong><div class="md-body">${renderMarkdown(ops.reports.controller)}</div></div>`
          : ""
      }
    </div>
  `;
}

function renderNewModal() {
  if (!state.newModalOpen) return "";
  return `
    <div class="sheet-backdrop" id="new-modal-backdrop">
      <div class="sheet-panel">
        <div style="display:flex;justify-content:space-between;align-items:center;">
          <strong style="font-size:17px;">Start New</strong>
          <button id="btn-close-new" class="icon-btn">✕</button>
        </div>
        <div class="seg-control" style="margin:0;">
          <button class="seg-btn${state.newMode === "chat" ? " active" : ""}" data-new-mode="chat">⚡ Fast AI Chat</button>
          <button class="seg-btn${state.newMode === "coding" ? " active" : ""}" data-new-mode="coding">🛠 Repo Coding Job</button>
        </div>
        ${
          state.newMode === "coding"
            ? `<div>
                <label class="field-label">GitHub Repository</label>
                <input id="new-repo-input" class="text-field" value="${escapeHtml(state.newRepo)}" placeholder="lodbrok-app/rattlsnek" />
              </div>`
            : ""
        }
        <div>
          <label class="field-label">Prompt</label>
          <textarea id="new-prompt-input" class="text-field" rows="4" placeholder="${
            state.newMode === "chat"
              ? "Ask anything (runs instantly in-controller)…"
              : "Describe the code change or bug fix to run in an isolated K8s Job…"
          }">${escapeHtml(state.newPrompt)}</textarea>
        </div>
        <div style="display:flex;justify-content:space-between;align-items:center;">
          ${
            state.newMode === "chat"
              ? `<label class="mini-btn" style="cursor:pointer;">
                  📷 ${state.newFiles.length ? `${state.newFiles.length} photo(s)` : "Attach Photo"}
                  <input id="new-file-input" type="file" accept="image/*" multiple style="display:none;" />
                </label>`
              : `<span style="font-size:12px;color:var(--text-dim);">Runs isolated worker pod</span>`
          }
          <button id="btn-new-model" class="model-pill">Model: ${escapeHtml(modelShortLabel(state.newModel))}</button>
        </div>
        ${
          state.newMode === "chat"
            ? `<label class="coach-toggle">
                <input id="chk-include-health" type="checkbox"${state.newIncludeHealth ? " checked" : ""} />
                <span>Include 7-day Garmin, Sleep, HRV & Nutrition context</span>
              </label>`
            : ""
        }
        <button id="btn-submit-new" class="full-btn"${state.submitting ? " disabled" : ""}>${
    state.submitting ? "Starting…" : state.newMode === "chat" ? "Send Chat" : "Queue Coding Job"
  }</button>
      </div>
    </div>
  `;
}

function renderPatchModal() {
  if (state.patchModalDiff === null) return "";
  return `
    <div class="sheet-backdrop" id="patch-modal-backdrop">
      <div class="sheet-panel" style="max-height:92dvh;">
        <div style="display:flex;justify-content:space-between;align-items:center;">
          <strong>Full Patch Diff</strong>
          <button id="btn-close-patch" class="icon-btn">✕</button>
        </div>
        <div class="diff-box" style="margin-top:0;">
          <pre class="diff-pre" style="max-height:70dvh;">${renderDiff(state.patchModalDiff)}</pre>
        </div>
      </div>
    </div>
  `;
}

function renderChatTabBar() {
  const unreadBadge =
    (state.counts?.unread_threads || 0) > 0
      ? `<span class="tab-badge">${state.counts.unread_threads}</span>`
      : "";
  const tabs = [
    ["inbox", "💬", "Inbox", unreadBadge],
    ["ops", "⚡", "Ops", ""],
    ["settings", "⚙️", "Push", ""],
  ];
  return `
    <nav class="tab-bar">
      ${tabs
        .map(
          ([id, icon, lbl, badge]) => `
        <button class="tab-btn${state.tab === id ? " active" : ""}" data-tab="${id}">
          ${badge}
          <span class="tab-icon">${icon}</span>
          <span>${lbl}</span>
        </button>`
        )
        .join("")}
    </nav>
  `;
}

function wireChatEvents() {
  document.querySelectorAll("[data-tab]").forEach((btn) => {
    btn.addEventListener("click", () => {
      state.tab = btn.dataset.tab;
      state.activeThreadId = null;
      render();
      if (state.tab === "inbox") loadThreads();
      else if (state.tab === "ops") loadOpsOverview();
    });
  });

  document.querySelectorAll("[data-filter]").forEach((btn) => {
    btn.addEventListener("click", () => {
      state.filter = btn.dataset.filter;
      state.folder = state.filter === "archived" ? "archived" : "inbox";
      loadThreads();
    });
  });

  document.querySelectorAll("[data-kind]").forEach((btn) => {
    btn.addEventListener("click", () => {
      state.kind = btn.dataset.kind;
      loadThreads();
    });
  });

  let searchDebounce = null;
  document.getElementById("inbox-search")?.addEventListener("input", (e) => {
    state.query = e.target.value;
    clearTimeout(searchDebounce);
    searchDebounce = setTimeout(loadThreads, 220);
  });

  document.getElementById("btn-refresh")?.addEventListener("click", loadThreads);
  document.getElementById("btn-new-modal")?.addEventListener("click", () => {
    state.newModalOpen = true;
    render();
  });
  document.getElementById("btn-close-new")?.addEventListener("click", () => {
    state.newModalOpen = false;
    render();
  });
  document.getElementById("new-modal-backdrop")?.addEventListener("click", (e) => {
    if (e.target.id === "new-modal-backdrop") {
      state.newModalOpen = false;
      render();
    }
  });
  document.querySelectorAll("[data-new-mode]").forEach((btn) => {
    btn.addEventListener("click", () => {
      state.newMode = btn.dataset.newMode;
      render();
    });
  });
  document.getElementById("new-repo-input")?.addEventListener("input", (e) => {
    state.newRepo = e.target.value;
  });
  document.getElementById("new-prompt-input")?.addEventListener("input", (e) => {
    state.newPrompt = e.target.value;
  });
  document.getElementById("new-file-input")?.addEventListener("change", (e) => {
    state.newFiles = Array.from(e.target.files || []);
    render();
  });
  document.getElementById("btn-new-model")?.addEventListener("click", () => {
    state.newModel = cycleModel(state.newModel);
    render();
  });
  document.getElementById("chk-include-health")?.addEventListener("change", (e) => {
    state.newIncludeHealth = Boolean(e.target.checked);
  });
  document.getElementById("btn-submit-new")?.addEventListener("click", submitNewTaskOrChat);

  document.querySelectorAll("[data-open-thread]").forEach((card) => {
    card.addEventListener("click", (e) => {
      if (e.target.closest("button")) return;
      openThread(card.dataset.openThread);
    });
  });

  document.querySelectorAll("[data-pin-thread]").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      toggleThreadOrg(btn.dataset.pinThread, { pinned: btn.dataset.pinned !== "1" });
    });
  });

  document.querySelectorAll("[data-folder-thread]").forEach((btn) => {
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      toggleThreadOrg(btn.dataset.folderThread, { folder: btn.dataset.nextFolder });
    });
  });

  document.getElementById("btn-back-inbox")?.addEventListener("click", closeThread);
  document.getElementById("btn-reply-model")?.addEventListener("click", () => {
    state.replyModel = cycleModel(state.replyModel);
    render();
  });
  document.getElementById("reply-input")?.addEventListener("input", (e) => {
    state.replyText = e.target.value;
  });
  document.getElementById("reply-file-input")?.addEventListener("change", (e) => {
    state.replyFiles = Array.from(e.target.files || []);
    render();
  });
  document.getElementById("btn-send-reply")?.addEventListener("click", submitThreadReply);

  document.querySelectorAll("[data-thread-action]").forEach((btn) => {
    btn.addEventListener("click", () => runThreadAction(btn.dataset.threadAction));
  });
  document.querySelectorAll("[data-full-patch]").forEach((btn) => {
    btn.addEventListener("click", () => viewFullPatch(btn.dataset.fullPatch));
  });
  document.getElementById("btn-close-patch")?.addEventListener("click", () => {
    state.patchModalDiff = null;
    render();
  });
  document.getElementById("patch-modal-backdrop")?.addEventListener("click", (e) => {
    if (e.target.id === "patch-modal-backdrop") {
      state.patchModalDiff = null;
      render();
    }
  });

  document.getElementById("btn-ops-refresh")?.addEventListener("click", loadOpsOverview);
  document.querySelectorAll("[data-billing-cmd]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      try {
        const res = await apiFetch("api/ops/billing", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ command: btn.dataset.billingCmd }),
        });
        await loadOpsOverview();
        toast(res.reply?.split("\n")[0] || "Updated.");
      } catch (err) {
        toast(err.message);
      }
    });
  });
  document.getElementById("btn-billing-sync")?.addEventListener("click", async () => {
    const raw = window.prompt("Actual USD spent this cycle (from AI Ultra console):", "");
    if (!raw) return;
    try {
      await apiFetch("api/ops/billing", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ command: `sync ${raw}` }),
      });
      await loadOpsOverview();
      toast("Ledger synced.");
    } catch (err) {
      toast(err.message);
    }
  });
  document.querySelectorAll("[data-deps-focus]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      try {
        const res = await apiFetch("api/ops/deps", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ focus: btn.dataset.depsFocus }),
        });
        toast(res.ack || "Dependency audit queued.");
      } catch (err) {
        toast(err.message);
      }
    });
  });
}

export function initChatMode() {
  registerAppMode({
    initialTab: "inbox",
    focusInputIds: ["inbox-search", "reply-input"],
    onAuthenticated: async (hashParams) => {
      await loadThreads();
      const hashThread = hashParams?.get("thread");
      const newHealthChat = hashParams?.get("new_health_chat");
      if (hashThread) {
        await openThread(hashThread);
      } else if (newHealthChat) {
        state.newModalOpen = true;
        state.newMode = "chat";
        state.newIncludeHealth = true;
        render();
      }
    },
    onVisible: () => {
      loadThreads();
    },
    onSwMessage: (msg) => {
      if (msg?.type === "OPEN_THREAD" && msg.thread_id) {
        openThread(msg.thread_id);
      }
    },
    onWsEvent: (pkt) => {
      const { type, data } = pkt || {};
      if (type === "thread.updated" && data?.thread) {
        if (data.counts) state.counts = data.counts;
        if (state.activeThreadId === data.thread.thread_id) {
          state.activeThread = data.thread;
        }
        loadThreads();
        updateAppBadge();
        return;
      }
      if ((type === "message.created" || type === "message.updated") && data?.message) {
        if (data.thread && state.activeThreadId === data.thread.thread_id) {
          state.activeThread = data.thread;
          const idx = state.activeMessages.findIndex((m) => m.id === data.message.id);
          if (idx >= 0) state.activeMessages[idx] = data.message;
          else state.activeMessages.push(data.message);
          render();
          scrollMessagesBottom();
        }
        loadThreads();
      }
    },
    renderMain: () => {
      if (state.activeThreadId) return renderThreadDetailView();
      if (state.tab === "ops") return renderOpsView();
      return renderInboxView();
    },
    renderModals: () => `${renderNewModal()}${renderPatchModal()}`,
    renderTabBar: renderChatTabBar,
    wireModeEvents: wireChatEvents,
  });
}
