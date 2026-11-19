let apiFetch;
let chatBase;
let cycleModel;
let escapeHtml;
let modelShortLabel;
let registerAppMode;
let relativeTime;
let render;
let renderDiff;
let renderMarkdown;
let state;
let toast;

const hState = {
  handoffs: [],
  filter: "all",
  query: "",
  activeId: null,
  activeDetail: null,
  loading: false,
  loadingDetail: false,
  feedbackText: "",
  feedbackModel: "gemini-4-hydrogen",
  sendingFeedback: false,
  showFullDiff: false,
};

const ACTIVE_STATUSES = new Set(["queued", "running"]);
const REVIEW_STATUSES = new Set(["awaiting_review"]);
const DONE_STATUSES = new Set(["pr_created", "resolved", "pushed", "completed"]);
const FAILED_STATUSES = new Set(["failed", "error", "timed_out", "cancelled"]);

function matchesFilter(h) {
  const st = String(h.status || "").toLowerCase();
  if (hState.filter === "active" && !ACTIVE_STATUSES.has(st)) return false;
  if (hState.filter === "review" && !REVIEW_STATUSES.has(st)) return false;
  if (hState.filter === "done" && !DONE_STATUSES.has(st)) return false;
  if (hState.filter === "failed" && !FAILED_STATUSES.has(st)) return false;
  if (hState.query.trim()) {
    const q = hState.query.trim().toLowerCase();
    const hay = `${h.short_id || ""} ${h.title || ""} ${h.github_repo || ""} ${h.branch || ""} ${h.base_sha || ""}`.toLowerCase();
    if (!hay.includes(q)) return false;
  }
  return true;
}

async function loadHandoffs({ silent = false } = {}) {
  if (!silent) {
    hState.loading = true;
    render();
  }
  try {
    const res = await apiFetch("api/handoff?limit=40");
    hState.handoffs = Array.isArray(res.handoffs) ? res.handoffs : [];
  } catch (err) {
    toast(err.message);
  } finally {
    hState.loading = false;
    render();
  }
}

async function openHandoff(handoffId, { silent = false } = {}) {
  hState.activeId = handoffId;
  hState.showFullDiff = false;
  if (!silent) {
    hState.loadingDetail = true;
    render();
  }
  try {
    const detail = await apiFetch(`api/handoff/${encodeURIComponent(handoffId)}?include_diff=true`);
    hState.activeDetail = detail;
    if (detail?.model) {
      hState.feedbackModel = detail.model;
    }
  } catch (err) {
    toast(err.message);
  } finally {
    hState.loadingDetail = false;
    render();
  }
}

function closeHandoff() {
  hState.activeId = null;
  hState.activeDetail = null;
  hState.feedbackText = "";
  render();
  loadHandoffs({ silent: true });
}

async function copyPullCommand(shortId) {
  const cmd = `/handoff pull ${shortId}`;
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(cmd);
      toast(`Copied: ${cmd}`);
      return;
    }
  } catch {
    /* fallback below */
  }
  window.prompt("Copy handoff pull command:", cmd);
}

async function sendHandoffFeedback() {
  if (!hState.activeDetail || hState.sendingFeedback) return;
  const text = hState.feedbackText.trim();
  if (!text) return toast("Enter follow-up instructions first.");
  hState.sendingFeedback = true;
  render();
  try {
    await apiFetch(`api/handoff/${encodeURIComponent(hState.activeDetail.short_id)}/feedback`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        feedback: text,
        model: hState.feedbackModel,
      }),
    });
    hState.feedbackText = "";
    toast("Follow-up turn queued on k3s worker.");
    await openHandoff(hState.activeDetail.short_id, { silent: true });
  } catch (err) {
    toast(err.message);
  } finally {
    hState.sendingFeedback = false;
    render();
  }
}

async function runHandoffThreadAction(actionId) {
  const threadId = hState.activeDetail?.thread_id;
  if (!threadId) return;
  try {
    toast(`Running ${actionId.replace(/_/g, " ")}…`);
    await apiFetch(`api/threads/${encodeURIComponent(threadId)}/actions`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: actionId }),
    });
    await openHandoff(hState.activeDetail.short_id, { silent: true });
  } catch (err) {
    toast(err.message);
  }
}

function renderListView() {
  const all = hState.handoffs || [];
  const activeCount = all.filter((h) => ACTIVE_STATUSES.has(String(h.status || "").toLowerCase())).length;
  const reviewCount = all.filter((h) => REVIEW_STATUSES.has(String(h.status || "").toLowerCase())).length;
  const filters = [
    { id: "all", label: "All", count: all.length || null },
    { id: "active", label: "Active", count: activeCount || null },
    { id: "review", label: "Review", count: reviewCount || null },
    { id: "done", label: "Done", count: null },
    { id: "failed", label: "Failed", count: null },
  ];

  const visible = all.filter(matchesFilter);

  const cardsHtml = visible.length
    ? visible
        .map((h) => {
          const shortSha = h.base_sha ? h.base_sha.slice(0, 7) : "";
          const report = h.report || null;
          const testBadge =
            report && typeof report.tests_passed === "boolean"
              ? `<span class="status-badge ${report.tests_passed ? "resolved" : "failed"}">${
                  report.tests_passed ? "Tests ✓" : "Tests ✗"
                }</span>`
              : "";
          const patchBadge = h.has_patch
            ? `<span class="kind-badge">${escapeHtml(h.patch_stats || "patch ready")}</span>`
            : "";
          return `
            <div class="thread-card" data-open-handoff="${escapeHtml(h.short_id)}">
              <div class="thread-card-top">
                <div class="thread-badges">
                  <span class="kind-badge coding">${escapeHtml(h.short_id)}</span>
                  <span class="status-badge ${escapeHtml(h.status)}">${escapeHtml(String(h.status || "").replace(/_/g, " "))}</span>
                  ${testBadge}
                </div>
                <span style="font-size:11px;color:var(--text-dim);">${escapeHtml(relativeTime(h.updated_at))}</span>
              </div>
              <div class="thread-title">${escapeHtml(h.title || h.short_id)}</div>
              ${
                h.last_error
                  ? `<div class="thread-preview" style="color:var(--red);">${escapeHtml(h.last_error)}</div>`
                  : report?.fix_summary
                    ? `<div class="thread-preview">${escapeHtml(report.fix_summary)}</div>`
                    : ""
              }
              <div class="thread-meta-row">
                <span style="font-family:ui-monospace,SFMono-Regular,monospace;font-size:11px;">
                  ${escapeHtml(h.github_repo || "")}${h.branch ? ` @ ${escapeHtml(h.branch)}` : ""}${shortSha ? ` (${escapeHtml(shortSha)})` : ""}
                </span>
                <div class="mini-actions">
                  ${patchBadge}
                  <span class="kind-badge">T${Number(h.turn || 1)} · ${escapeHtml(modelShortLabel(h.model))}</span>
                </div>
              </div>
            </div>
          `;
        })
        .join("")
    : `<div class="empty-state">${
        hState.loading
          ? "Loading handoff sessions…"
          : 'No handoff sessions match this filter.<br/>Run <code>/handoff</code> from your laptop to dispatch a session to k3s.'
      }</div>`;

  return `
    <div class="top-bar">
      <div class="top-bar-title">
        <span class="ws-dot ${state.wsStatus}"></span>
        <span class="truncate">Handoffs</span>
      </div>
      <div class="top-actions">
        <a href="${escapeHtml(chatBase)}/" class="icon-btn" title="Open AGY Tasks" style="text-decoration:none;">💬</a>
        <button id="btn-handoff-refresh" class="icon-btn" title="Refresh">↻</button>
      </div>
    </div>
    <div class="main-scroll">
      <div class="seg-control">
        ${filters
          .map(
            (f) => `
          <button class="seg-btn${hState.filter === f.id ? " active" : ""}" data-h-filter="${f.id}">
            ${f.label}${f.count ? `<span class="pill-count">${f.count}</span>` : ""}
          </button>`
          )
          .join("")}
      </div>
      <input id="handoff-search" class="search-input" type="search" placeholder="Filter by ID, repo, branch, SHA…" value="${escapeHtml(hState.query)}" />
      ${cardsHtml}
    </div>
  `;
}

function renderDetailView() {
  const h = hState.activeDetail;
  if (!h) {
    return `
      <div class="top-bar">
        <div class="top-bar-title">
          <button id="btn-handoff-back" class="icon-btn">←</button>
          <span class="truncate">${escapeHtml(hState.activeId || "Handoff")}</span>
        </div>
      </div>
      <div class="main-scroll"><div class="empty-state">Loading handoff details…</div></div>
    `;
  }

  const shortSha = h.base_sha ? h.base_sha.slice(0, 10) : "HEAD";
  const report = h.report || null;
  const usage = h.usage || null;
  const costStr =
    usage && typeof usage.estimated_usd === "number" ? `$${usage.estimated_usd.toFixed(4)}` : "";
  const files = Array.isArray(h.affected_files) ? h.affected_files : [];
  const diffLines = (h.diff || "").split("\n");
  const isLongDiff = diffLines.length > 160 && !hState.showFullDiff;
  const visibleDiff = isLongDiff ? diffLines.slice(0, 160).join("\n") : h.diff || "";

  return `
    <div class="top-bar">
      <div class="top-bar-title">
        <button id="btn-handoff-back" class="icon-btn">←</button>
        <span class="truncate">${escapeHtml(h.short_id)}</span>
      </div>
      <div class="top-actions">
        <button id="btn-handoff-reload-detail" class="icon-btn" title="Refresh">↻</button>
        <a href="${escapeHtml(chatBase)}/#thread=${encodeURIComponent(h.thread_id || h.issue_id)}" class="icon-btn" title="Open Chat Thread" style="text-decoration:none;">💬</a>
      </div>
    </div>
    <div class="main-scroll">
      <div class="card">
        <div style="display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:6px;flex-wrap:wrap;">
          <div class="thread-badges">
            <span class="status-badge ${escapeHtml(h.status)}">${escapeHtml(String(h.status || "").replace(/_/g, " "))}</span>
            <span class="kind-badge">Turn ${Number(h.turn || 1)}</span>
            <span class="kind-badge">${escapeHtml(modelShortLabel(h.model))}</span>
            ${costStr ? `<span class="kind-badge">${escapeHtml(costStr)}</span>` : ""}
          </div>
          <span style="font-size:11px;color:var(--text-dim);">${escapeHtml(relativeTime(h.updated_at))}</span>
        </div>
        <div style="font-size:15px;font-weight:650;margin-bottom:8px;">${escapeHtml(h.title || h.short_id)}</div>
        <div style="font-size:12px;color:var(--text-muted);line-height:1.6;font-family:ui-monospace,SFMono-Regular,monospace;">
          Repo: <strong>${escapeHtml(h.github_repo || "—")}</strong><br/>
          Branch: <strong>${escapeHtml(h.branch || "—")}</strong> · Base: <strong>${escapeHtml(shortSha)}</strong>${
            h.had_base_patch ? " (+ local diff)" : ""
          }
        </div>
        <div style="display:flex;gap:8px;margin-top:10px;flex-wrap:wrap;">
          <button id="btn-copy-pull" class="mini-btn" style="flex:1;">📋 Copy /handoff pull ${escapeHtml(h.short_id)}</button>
          ${
            h.pr_url
              ? `<a href="${escapeHtml(h.pr_url)}" target="_blank" rel="noopener" class="mini-btn" style="text-decoration:none;">Open PR ↗</a>`
              : ""
          }
        </div>
        ${
          h.status === "awaiting_review"
            ? `<div style="display:flex;gap:8px;margin-top:8px;">
                <button class="full-btn" data-h-action="approve_pr" style="flex:1;">Approve &amp; Open PR</button>
                <button class="full-btn secondary" data-h-action="push_branch" style="flex:1;">Push Branch</button>
              </div>`
            : ""
        }
      </div>

      ${
        h.last_error
          ? `<div class="card" style="border-color:rgba(239,68,68,0.5);background:rgba(239,68,68,0.08);">
              <strong style="color:var(--red);font-size:13px;">Worker Error</strong>
              <pre style="margin:6px 0 0;font-size:11.5px;white-space:pre-wrap;word-break:break-word;color:var(--text-muted);">${escapeHtml(h.last_error)}</pre>
            </div>`
          : ""
      }

      ${
        report
          ? `<div class="card">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;gap:8px;flex-wrap:wrap;">
                <strong>Worker Verification Report</strong>
                <div class="thread-badges">
                  ${report.confidence ? `<span class="status-badge">${escapeHtml(report.confidence)} conf</span>` : ""}
                  <span class="status-badge ${report.tests_passed ? "resolved" : "failed"}">${
                    report.tests_passed ? "Tests PASSED" : "Needs Check"
                  }</span>
                </div>
              </div>
              ${report.fix_summary ? `<div class="md-body">${renderMarkdown(report.fix_summary)}</div>` : ""}
              ${
                report.verification_summary
                  ? `<div class="md-body" style="margin-top:8px;padding-top:8px;border-top:1px solid rgba(255,255,255,0.08);font-size:12.5px;">${renderMarkdown(report.verification_summary)}</div>`
                  : ""
              }
              ${
                files.length
                  ? `<div style="display:flex;flex-wrap:wrap;gap:6px;margin-top:8px;">
                      ${files.map((f) => `<span class="kind-badge" style="font-family:ui-monospace,monospace;">${escapeHtml(f)}</span>`).join("")}
                    </div>`
                  : ""
              }
            </div>`
          : ""
      }

      ${
        h.has_patch
          ? `<div class="card">
              <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
                <strong>Patch Diff ${h.patch_stats ? `<span style="font-weight:400;color:var(--text-muted);">(${escapeHtml(h.patch_stats)})</span>` : ""}</strong>
                ${
                  isLongDiff
                    ? `<button id="btn-expand-diff" class="mini-btn">Show all ${diffLines.length} lines</button>`
                    : ""
                }
              </div>
              <div class="diff-box" style="margin-top:0;">
                <pre class="diff-pre" style="max-height:55dvh;">${renderDiff(visibleDiff)}</pre>
              </div>
            </div>`
          : ""
      }

      <div class="card">
        <strong style="display:block;margin-bottom:6px;">Send Follow-up Turn</strong>
        <textarea id="handoff-feedback-input" class="text-field" rows="3" placeholder="Ask the k3s worker to refine the patch, fix a test, or adjust scope…">${escapeHtml(hState.feedbackText)}</textarea>
        <div style="display:flex;justify-content:space-between;align-items:center;gap:8px;margin-top:8px;">
          <button id="btn-handoff-model" class="model-pill">Model: ${escapeHtml(modelShortLabel(hState.feedbackModel))}</button>
          <button id="btn-send-handoff-feedback" class="full-btn" style="width:auto;padding:8px 16px;"${hState.sendingFeedback ? " disabled" : ""}>${
            hState.sendingFeedback ? "Queuing…" : "Send Feedback"
          }</button>
        </div>
      </div>
    </div>
  `;
}

function renderHandoffTabBar() {
  const tabs = [
    ["handoffs", "🚀", "Handoffs"],
    ["settings", "⚙️", "Push"],
  ];
  return `
    <nav class="tab-bar">
      ${tabs
        .map(
          ([id, icon, lbl]) => `
        <button class="tab-btn${state.tab === id ? " active" : ""}" data-h-tab="${id}">
          <span class="tab-icon">${icon}</span>
          <span>${lbl}</span>
        </button>`
        )
        .join("")}
    </nav>
  `;
}

function wireHandoffEvents() {
  document.querySelectorAll("[data-h-tab]").forEach((btn) => {
    btn.addEventListener("click", () => {
      state.tab = btn.dataset.hTab;
      if (state.tab === "handoffs") {
        hState.activeId = null;
        hState.activeDetail = null;
        loadHandoffs({ silent: true });
      } else {
        render();
      }
    });
  });

  document.getElementById("btn-handoff-refresh")?.addEventListener("click", () => loadHandoffs());
  document.querySelectorAll("[data-h-filter]").forEach((btn) => {
    btn.addEventListener("click", () => {
      hState.filter = btn.dataset.hFilter;
      render();
    });
  });
  document.getElementById("handoff-search")?.addEventListener("input", (e) => {
    hState.query = e.target.value;
    render();
  });
  document.querySelectorAll("[data-open-handoff]").forEach((card) => {
    card.addEventListener("click", () => openHandoff(card.dataset.openHandoff));
  });

  document.getElementById("btn-handoff-back")?.addEventListener("click", closeHandoff);
  document.getElementById("btn-handoff-reload-detail")?.addEventListener("click", () => {
    if (hState.activeId) openHandoff(hState.activeId);
  });
  document.getElementById("btn-copy-pull")?.addEventListener("click", () => {
    if (hState.activeDetail?.short_id) copyPullCommand(hState.activeDetail.short_id);
  });
  document.getElementById("btn-expand-diff")?.addEventListener("click", () => {
    hState.showFullDiff = true;
    render();
  });
  document.querySelectorAll("[data-h-action]").forEach((btn) => {
    btn.addEventListener("click", () => runHandoffThreadAction(btn.dataset.hAction));
  });
  document.getElementById("handoff-feedback-input")?.addEventListener("input", (e) => {
    hState.feedbackText = e.target.value;
  });
  document.getElementById("btn-handoff-model")?.addEventListener("click", () => {
    hState.feedbackModel = cycleModel(hState.feedbackModel);
    render();
  });
  document.getElementById("btn-send-handoff-feedback")?.addEventListener("click", sendHandoffFeedback);
}

export function init(core) {
  ({
    apiFetch,
    chatBase,
    cycleModel,
    escapeHtml,
    modelShortLabel,
    registerAppMode,
    relativeTime,
    render,
    renderDiff,
    renderMarkdown,
    state,
    toast,
  } = core);

  hState.feedbackModel = state.defaultModel || "gemini-4-hydrogen";

  registerAppMode({
    initialTab: "handoffs",
    focusInputIds: ["handoff-search", "handoff-feedback-input"],
    onAuthenticated: async (hashParams) => {
      await loadHandoffs();
      const targetId = hashParams?.get("id") || hashParams?.get("handoff");
      if (targetId) {
        await openHandoff(targetId);
      }
    },
    onVisible: () => {
      if (hState.activeId) openHandoff(hState.activeId, { silent: true });
      else loadHandoffs({ silent: true });
    },
    onWsEvent: (pkt) => {
      const { type } = pkt || {};
      if (type === "thread.updated" || type === "message.created" || type === "message.updated") {
        if (hState.activeId) openHandoff(hState.activeId, { silent: true });
        else loadHandoffs({ silent: true });
      }
    },
    renderMain: () => (hState.activeId ? renderDetailView() : renderListView()),
    renderModals: () => "",
    renderTabBar: renderHandoffTabBar,
    wireModeEvents: wireHandoffEvents,
  });
}
