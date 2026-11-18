import { escapeHtml, renderMarkdown } from "./markdown.js";
import {
  apiFetch,
  chatBase,
  cycleModel,
  modelShortLabel,
  registerAppMode,
  render,
  state,
  toast,
} from "./core.js";

async function loadFoodToday(dateOverride = state.foodDate) {
  try {
    const q = dateOverride ? `?date=${encodeURIComponent(dateOverride)}` : "";
    const data = await apiFetch(`api/food/today${q}`);
    state.foodToday = data;
    state.foodDate = data.local_date;
    render();
  } catch (err) {
    toast(err.message);
  }
}

async function loadFoodWeek() {
  try {
    const [week, trends] = await Promise.all([
      apiFetch("api/food/week"),
      apiFetch("api/food/trends?days=14").catch(() => null),
    ]);
    state.foodWeek = week;
    state.foodTrends = trends;
    render();
  } catch (err) {
    toast(err.message);
  }
}

async function updateGarminSettings(patch) {
  try {
    state.foodToday = await apiFetch("api/food/garmin/settings", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(patch),
    });
    if (state.foodShowWeek) {
      loadFoodWeek();
    } else {
      render();
    }
    toast("Updated target settings");
  } catch (err) {
    toast(err.message);
  }
}

async function logFoodMeal({ stapleSlug = "", multiplier = 1.0 } = {}) {
  if (state.submitting) return;
  if (!stapleSlug) {
    const domInput = document.getElementById("food-text-input");
    if (domInput) state.foodInput = domInput.value;
  }
  if (!stapleSlug && !state.foodInput.trim() && state.foodFiles.length === 0) {
    return toast("Snap a photo or describe a meal.");
  }
  const statusMsg = stapleSlug
    ? `Logging ${stapleSlug}…`
    : `Submitted — analyzing meal with ${modelShortLabel(state.foodModel)}…`;
  state.submitting = true;
  state.foodPendingTs = "";
  state.foodError = "";
  state.foodNotice = "";
  state.foodStatus = statusMsg;
  render();
  toast(statusMsg);
  try {
    const fd = new FormData();
    fd.append("text", stapleSlug ? "" : state.foodInput.trim());
    fd.append("model", state.foodModel);
    fd.append("staple_slug", stapleSlug);
    fd.append("multiplier", String(multiplier));
    for (const f of state.foodFiles) fd.append("files", f);
    const data = await apiFetch("api/food/log", { method: "POST", body: fd });
    state.foodToday = data;
    state.foodDate = data.local_date;
    if (!stapleSlug) {
      state.foodInput = "";
      state.foodFiles = [];
    }
    if (data.queued) {
      state.foodPendingTs = data.thread_ts || "pending";
      state.foodStatus = "Still analyzing meal in background… (updates automatically)";
      toast("Analyzing meal in background…");
    } else {
      const logged = data.logged_meal;
      const doneMsg = logged
        ? `Logged: ${logged.title} (${Math.round(logged.total_calories_kcal)} kcal · ${Math.round(logged.total_protein_g)}g P)`
        : data.notice || (stapleSlug ? `Logged ${stapleSlug}.` : "Meal logged.");
      state.foodNotice = doneMsg;
      toast(doneMsg);
    }
  } catch (err) {
    state.foodPendingTs = "";
    state.foodError = err.message || "Failed to log meal.";
    toast(`Log failed: ${state.foodError}`);
  } finally {
    state.submitting = false;
    if (!state.foodPendingTs) state.foodStatus = "";
    render();
  }
}

function renderMacroBar(label, cur, target, unit, colorClass) {
  const curR = Math.round(cur);
  const targetR = Math.round(target);
  const pct = target > 0 ? Math.min(100, Math.round((cur / target) * 100)) : 0;
  const rem = targetR - curR;
  const remLabel = rem >= 0 ? `${rem}${unit} left` : `${Math.abs(rem)}${unit} over`;
  return `
    <div class="macro-cell">
      <div style="display:flex;justify-content:space-between;">
        <strong>${label}</strong>
        <span style="color:var(--text-muted);">${curR} / ${targetR}${unit}</span>
      </div>
      <div class="progress-track"><div class="progress-fill ${colorClass}" style="width:${pct}%"></div></div>
      <div style="font-size:11px;color:var(--text-dim);margin-top:4px;">${remLabel}</div>
    </div>
  `;
}

function renderMacroPieChart(proteinG, carbsG, fatG) {
  const pG = Math.max(0, Number(proteinG) || 0);
  const cG = Math.max(0, Number(carbsG) || 0);
  const fG = Math.max(0, Number(fatG) || 0);
  const pKcal = pG * 4;
  const cKcal = cG * 4;
  const fKcal = fG * 9;
  const totalKcal = pKcal + cKcal + fKcal;

  const slices = [
    { label: "Protein", g: pG, kcal: pKcal, color: "var(--green)" },
    { label: "Carbs", g: cG, kcal: cKcal, color: "var(--accent)" },
    { label: "Fat", g: fG, kcal: fKcal, color: "var(--amber)" },
  ];

  const r = 32;
  const circ = 2 * Math.PI * r;
  let offset = 0;
  const arcsHtml =
    totalKcal > 0
      ? slices
          .map((s) => {
            const len = (s.kcal / totalKcal) * circ;
            const arc = `<circle cx="44" cy="44" r="${r}" fill="none" stroke="${s.color}" stroke-width="14" stroke-dasharray="${len.toFixed(2)} ${circ.toFixed(2)}" stroke-dashoffset="${(-offset).toFixed(2)}" />`;
            offset += len;
            return arc;
          })
          .join("")
      : `<circle cx="44" cy="44" r="${r}" fill="none" stroke="var(--border-strong)" stroke-width="14" stroke-dasharray="4 4" />`;

  const legendHtml = slices
    .map((s) => {
      const pct = totalKcal > 0 ? Math.round((s.kcal / totalKcal) * 100) : 0;
      return `
        <div class="macro-pie-row">
          <span class="macro-pie-label"><span class="macro-pie-dot" style="background:${s.color};"></span>${s.label}</span>
          <span class="macro-pie-val"><strong>${pct}%</strong> · ${Math.round(s.g)}g · ${Math.round(s.kcal)} kcal</span>
        </div>
      `;
    })
    .join("");

  return `
    <div class="macro-pie-wrap">
      <div class="macro-pie-svg-box">
        <svg viewBox="0 0 88 88" width="88" height="88" style="transform:rotate(-90deg);display:block;">
          ${arcsHtml}
        </svg>
        <div class="macro-pie-center">
          <strong>${Math.round(totalKcal)}</strong>
          <span>kcal</span>
        </div>
      </div>
      <div class="macro-pie-legend">
        ${legendHtml}
      </div>
    </div>
  `;
}

function renderFoodTrendsHtml(trends) {
  if (!trends || !Array.isArray(trends.series) || trends.series.length === 0) return "";
  const series = trends.series;
  const w = 320;
  const h = 110;
  const maxKcal = Math.max(
    2000,
    ...series.map((s) => Math.max(s.intake_kcal || 0, s.garmin_burn_kcal || 0, s.target_kcal || 0))
  );
  const bw = w / series.length;
  const bars = series
    .map((s, idx) => {
      const x = idx * bw;
      const burnH = s.garmin_burn_kcal ? Math.max(2, (s.garmin_burn_kcal / maxKcal) * (h - 20)) : 0;
      const intakeH = s.intake_kcal ? Math.max(2, (s.intake_kcal / maxKcal) * (h - 20)) : 0;
      return `
        ${burnH ? `<rect x="${(x + 2).toFixed(1)}" y="${(h - 14 - burnH).toFixed(1)}" width="${Math.max(3, bw * 0.36).toFixed(1)}" height="${burnH.toFixed(1)}" rx="2" fill="rgba(129,140,248,0.65)" />` : ""}
        ${intakeH ? `<rect x="${(x + 2 + bw * 0.4).toFixed(1)}" y="${(h - 14 - intakeH).toFixed(1)}" width="${Math.max(3, bw * 0.36).toFixed(1)}" height="${intakeH.toFixed(1)}" rx="2" fill="rgba(34,197,94,0.85)" />` : ""}
      `;
    })
    .join("");

  const weights = series
    .map((s, idx) => ({ idx, v: s.ema_weight_lbs ?? s.weight_lbs }))
    .filter((p) => p.v != null);
  let weightLine = "";
  if (weights.length >= 2) {
    const minW = Math.min(...weights.map((p) => p.v));
    const maxW = Math.max(...weights.map((p) => p.v));
    const spanW = Math.max(0.5, maxW - minW);
    const pts = weights
      .map((p) => {
        const px = (p.idx + 0.5) * bw;
        const py = h - 20 - ((p.v - minW) / spanW) * (h - 32);
        return `${px.toFixed(1)},${py.toFixed(1)}`;
      })
      .join(" ");
    weightLine = `<polyline fill="none" stroke="#f59e0b" stroke-width="2" points="${pts}" />`;
  }

  const cal = trends.calibration;
  const bankLine =
    trends.weekly_net_kcal != null
      ? `<div style="font-size:12px;color:var(--text-muted);margin-top:6px;">
          7d Net Banked: <strong>${trends.weekly_net_kcal >= 0 ? `+${Math.round(trends.weekly_net_kcal)}` : Math.round(trends.weekly_net_kcal)} kcal</strong>
          (${trends.weekly_projected_lbs >= 0 ? `+${trends.weekly_projected_lbs}` : trends.weekly_projected_lbs} lbs/wk pace · ${trends.weekly_net_days}d)
        </div>`
      : "";
  const zoneLine = `<div style="font-size:12px;color:var(--text-muted);margin-top:4px;">⚡ 7d Zone Minutes: <strong>${trends.weekly_intensity_min || 0} / ${trends.intensity_goal_min || 150} min</strong></div>`;
  const calLine = cal
    ? `<div style="font-size:12px;color:var(--accent);margin-top:6px;display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap;">
        <span>Watch vs Scale TDEE: ${cal.bias_pct >= 0 ? `+${cal.bias_pct}` : cal.bias_pct}%</span>
        ${cal.recommended_active_factor != null ? `<button id="btn-apply-cal" class="mini-btn" data-cal-factor="${cal.recommended_active_factor}">Apply ×${cal.recommended_active_factor}</button>` : ""}
      </div>`
    : "";

  return `
    <div class="chart-wrap">
      <div style="display:flex;justify-content:space-between;font-size:11px;color:var(--text-dim);margin-bottom:4px;">
        <span>14d Burn vs Intake & Weight</span>
        <span><span style="color:#818cf8;">■ Burn</span> <span style="color:#22c55e;">■ Food</span> <span style="color:#f59e0b;">━ EMA</span></span>
      </div>
      <svg viewBox="0 0 ${w} ${h}" style="width:100%;height:110px;display:block;">
        ${bars}
        ${weightLine}
        <text x="2" y="${h - 2}" fill="#64748b" font-size="9">${escapeHtml(series[0].local_date.slice(5))}</text>
        <text x="${w - 28}" y="${h - 2}" fill="#64748b" font-size="9">${escapeHtml(series[series.length - 1].local_date.slice(5))}</text>
      </svg>
      ${bankLine}
      ${zoneLine}
      ${calLine}
    </div>
  `;
}

function renderFoodView() {
  const d = state.foodToday;
  const t = d?.target || { calories_kcal: 2200, protein_g: 170, carbs_g: 220, fat_g: 70, fiber_g: 30 };
  const kcalPct = t.calories_kcal > 0 ? Math.min(100, Math.round(((d?.total_calories_kcal || 0) / t.calories_kcal) * 100)) : 0;

  const staplesHtml = (d?.saved_foods || []).length
    ? `<div class="chip-row">${d.saved_foods
        .map(
          (sf) =>
            `<button class="chip" data-log-staple="${escapeHtml(sf.slug)}">⚡ ${escapeHtml(sf.name)} (${Math.round(sf.calories_kcal)})</button>`
        )
        .join("")}</div>`
    : "";

  const mealsHtml = (d?.meals || []).length
    ? d.meals
        .map((m) => {
          const scales = [0.5, 0.75, 1.0, 1.25, 1.5];
          const reactions = ["🤮", "🤤", "💪"];
          const itemsSummary = (m.items || [])
            .map((it) => `${it.name} (${Math.round(it.calories_kcal * m.scale_factor)} kcal)`)
            .join(" · ");
          return `
            <div class="card meal-card">
              <div class="meal-header">
                ${m.photo_url ? `<img class="meal-thumb" src="${escapeHtml(m.photo_url)}" alt="${escapeHtml(m.title)}" role="button" tabindex="0" data-photo-url="${escapeHtml(m.photo_url)}" data-photo-title="${escapeHtml(m.title)}" data-photo-kcal="${Math.round(m.total_calories_kcal)}" />` : ""}
                <div style="flex:1;min-width:0;">
                  <div style="display:flex;justify-content:space-between;gap:8px;">
                    <strong style="font-size:14px;">${escapeHtml(m.title)}${m.reaction_emoji ? ` <span class="meal-reaction-badge">${escapeHtml(m.reaction_emoji)}</span>` : ""}</strong>
                    <span style="color:var(--accent);font-weight:700;">${Math.round(m.total_calories_kcal)} kcal</span>
                  </div>
                  <div style="font-size:12px;color:var(--text-muted);margin-top:2px;">
                    ${Math.round(m.total_protein_g)}g P · ${Math.round(m.total_carbs_g)}g C · ${Math.round(m.total_fat_g)}g F · ${escapeHtml(m.meal_slot)}
                  </div>
                  ${itemsSummary ? `<div style="font-size:11px;color:var(--text-dim);margin-top:4px;">${escapeHtml(itemsSummary)}</div>` : ""}
                </div>
              </div>
              <div class="scale-row">
                ${scales
                  .map(
                    (s) =>
                      `<button class="scale-btn${Math.abs(m.scale_factor - s) < 0.01 ? " active" : ""}" data-scale-meal="${m.id}" data-mult="${s}">${s}×</button>`
                  )
                  .join("")}
                ${reactions
                  .map(
                    (em) =>
                      `<button class="scale-btn reaction-btn${m.reaction_emoji === em ? " active" : ""}" data-react-meal="${m.id}" data-emoji="${em}" title="React ${em}">${em}</button>`
                  )
                  .join("")}
                <button class="scale-btn" data-revise-meal="food-${m.id}" title="Discuss in Chat">💬</button>
                <button class="scale-btn" data-staple-meal="${m.id}" title="Save as Staple">⭐</button>
                <button class="scale-btn" data-del-meal="${m.id}" style="color:var(--red);" title="Delete">🗑️</button>
              </div>
            </div>
          `;
        })
        .join("")
    : `<div class="empty-state">No meals logged for ${escapeHtml(state.foodDate || "today")}.</div>`;

  const weekHtml =
    state.foodShowWeek && state.foodWeek
      ? `<div class="card">
          <div style="font-weight:700;margin-bottom:6px;">7-Day Rolling & Trends (${state.foodWeek.days_logged}d logged)</div>
          <div style="font-size:13px;color:var(--text-muted);">
            Avg: <strong>${Math.round(state.foodWeek.avg_calories_kcal)} kcal</strong> ·
            <strong>${Math.round(state.foodWeek.avg_protein_g)}g P</strong> ·
            Protein hits: ${state.foodWeek.protein_hit_days}/${state.foodWeek.days_logged}d
          </div>
          ${
            state.foodWeek.tdee
              ? `<div style="font-size:12px;color:var(--accent);margin-top:4px;">Adaptive TDEE: ~${Math.round(state.foodWeek.tdee.estimated_tdee_kcal)} kcal/day</div>`
              : ""
          }
          ${
            state.foodWeek.garmin_days_synced > 0 && state.foodWeek.avg_garmin_burn_kcal != null
              ? `<div style="font-size:12px;color:var(--text-muted);margin-top:4px;">⌚ Garmin Avg Burn (${state.foodWeek.garmin_days_synced}d): ~${Math.round(state.foodWeek.avg_garmin_burn_kcal)} kcal/day (${Math.round(state.foodWeek.avg_garmin_bmr_kcal || 0)} BMR + ${Math.round(state.foodWeek.avg_garmin_active_kcal || 0)} active)</div>`
              : ""
          }
          ${renderFoodTrendsHtml(state.foodTrends)}
        </div>`
      : "";

  const g = d?.garmin;
  const gs = d?.garmin_settings || {};
  const readinessPills = [];
  if (g) {
    if (g.sleep_seconds) {
      const h = (g.sleep_seconds / 3600).toFixed(1);
      const sc = g.sleep_score != null ? ` · ${g.sleep_score}` : "";
      readinessPills.push(`<span class="garmin-pill${g.sleep_seconds < 6.5 * 3600 ? " warn" : ""}">😴 <strong>${h}h</strong>${sc}</span>`);
    }
    if (g.hrv_last_night_avg != null) {
      const warn = ["LOW", "POOR", "UNBALANCED"].includes(String(g.hrv_status || "").toUpperCase());
      readinessPills.push(`<span class="garmin-pill${warn ? " warn" : ""}">💓 HRV <strong>${g.hrv_last_night_avg}ms</strong> (${escapeHtml((g.hrv_status || "").toLowerCase() || "ok")})</span>`);
    }
    if (g.body_battery_current != null) {
      readinessPills.push(`<span class="garmin-pill">🔋 BB <strong>${g.body_battery_current}</strong></span>`);
    }
    if (g.resting_hr != null) {
      const r7 = g.resting_hr_7d_avg != null ? `/${g.resting_hr_7d_avg}` : "";
      readinessPills.push(`<span class="garmin-pill">❤️ RHR <strong>${g.resting_hr}${r7}</strong></span>`);
    }
    readinessPills.push(`<span class="garmin-pill">👟 <strong>${Number(g.steps || 0).toLocaleString()}</strong>/${Number(g.step_goal || 10000).toLocaleString()}</span>`);
  }
  const workoutsHtml = (g?.activities || []).length
    ? `<div class="garmin-workouts">${g.activities
        .map((a) => {
          const mins = Math.max(1, Math.round((a.duration_seconds || 0) / 60));
          const hr = a.average_hr ? ` · ${Math.round(a.average_hr)} bpm` : "";
          return `<div class="workout-chip"><span>🏃 <strong>${escapeHtml(a.name)}</strong> (${mins}m${hr})</span><strong>+${Math.round(a.calories_kcal)} kcal</strong></div>`;
        })
        .join("")}</div>`
    : "";
  const curDelta = Number(gs.goal_delta_kcal ?? d?.dynamic_target?.goal_delta_kcal ?? 0);
  const curMode = gs.dynamic_mode || "rolling_baseline";
  const dynEnabled = gs.dynamic_enabled !== false;
  const targetsPanelHtml = state.foodShowTargets
    ? `<div class="garmin-controls">
        <div style="display:flex;justify-content:space-between;align-items:center;">
          <strong>Dynamic Garmin Target Settings</strong>
          <button id="btn-garmin-toggle-dyn" class="mini-btn">${dynEnabled ? "Dynamic: ON" : "Dynamic: OFF"}</button>
        </div>
        <div style="display:flex;gap:5px;flex-wrap:wrap;">
          ${[
            [-500, "-500 Cut"],
            [-250, "-250 Mild"],
            [0, "0 Maint"],
            [250, "+250 Lean"],
          ]
            .map(
              ([val, lbl]) =>
                `<button class="scale-btn${curDelta === val ? " active" : ""}" data-garmin-delta="${val}">${lbl}</button>`
            )
            .join("")}
        </div>
        <div style="display:flex;gap:6px;align-items:center;justify-content:space-between;flex-wrap:wrap;">
          <span>Mode:</span>
          <div style="display:flex;gap:5px;">
            <button class="scale-btn${curMode === "rolling_baseline" ? " active" : ""}" data-garmin-mode="rolling_baseline">7d Rolling</button>
            <button class="scale-btn${curMode === "daily_burn" ? " active" : ""}" data-garmin-mode="daily_burn">Daily Burn</button>
          </div>
        </div>
        <div style="font-size:11px;color:var(--text-dim);">Active factor: ×${Number(gs.active_factor ?? 0.8).toFixed(2)}</div>
      </div>`
    : "";

  return `
    <div class="top-bar">
      <div class="top-bar-title"><span class="ws-dot ${state.wsStatus}"></span><span>Food & Macros</span></div>
      <div class="top-actions">
        <button id="btn-food-coach" class="icon-btn" title="Ask AI Coach with 7d Garmin + Food data">🧠</button>
        <button id="btn-food-prev" class="icon-btn">‹</button>
        <span style="font-size:12px;font-weight:600;">${escapeHtml(state.foodDate || "Today")}</span>
        <button id="btn-food-next" class="icon-btn">›</button>
        <button id="btn-food-week" class="icon-btn">7d</button>
      </div>
    </div>
    <div class="main-scroll">
      <div class="card">
        <div style="display:flex;justify-content:space-between;align-items:baseline;">
          <strong style="font-size:16px;">${Math.round(d?.total_calories_kcal || 0)} / ${Math.round(t.calories_kcal || 0)} kcal</strong>
          <span style="font-size:12px;color:var(--text-muted);">${(() => {
            const remKcal = Math.round(t.calories_kcal || 0) - Math.round(d?.total_calories_kcal || 0);
            return remKcal >= 0 ? `${remKcal} left` : `${Math.abs(remKcal)} over`;
          })()}</span>
        </div>
        <div class="progress-track" style="height:10px;">
          <div class="progress-fill ${kcalPct > 105 ? "red" : "green"}" style="width:${kcalPct}%"></div>
        </div>
        <div class="macro-grid">
          ${renderMacroBar("Protein", d?.total_protein_g || 0, t.protein_g, "g", "green")}
          ${renderMacroBar("Carbs", d?.total_carbs_g || 0, t.carbs_g, "g", "")}
          ${renderMacroBar("Fat", d?.total_fat_g || 0, t.fat_g, "g", "amber")}
          ${renderMacroBar("Fiber", d?.total_fiber_g || 0, t.fiber_g, "g", "")}
        </div>
        ${renderMacroPieChart(d?.total_protein_g || 0, d?.total_carbs_g || 0, d?.total_fat_g || 0)}
        ${
          d?.garmin || d?.dynamic_target || d?.garmin_configured
            ? `<div style="margin-top:10px;padding-top:8px;border-top:1px solid var(--border);font-size:12px;color:var(--text-muted);display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap;">
                <span>${
                  d?.garmin
                    ? `⌚ ${Math.round(d.garmin.bmr_full_day_kcal)} BMR + ${Math.round(d.garmin.active_kcal)} active = <strong>${Math.round(d.garmin.total_burn_kcal)} kcal</strong>`
                    : "⌚ Garmin ready"
                }${
                  d?.dynamic_target
                    ? (() => {
                        const dt = d.dynamic_target;
                        const deltaStr = dt.goal_delta_kcal >= 0 ? `+${dt.goal_delta_kcal}` : `${dt.goal_delta_kcal}`;
                        const parts = [deltaStr];
                        if (dt.active_factor != null && Math.abs(dt.active_factor - 1) > 0.001) {
                          parts.push(`×${dt.active_factor} active`);
                        }
                        if (dt.mode === "rolling_baseline") {
                          const sur = dt.workout_surplus_kcal > 0 ? `+${Math.round(dt.workout_surplus_kcal)}` : "";
                          parts.push(`7d avg ${Math.round(dt.baseline_active_kcal)}${sur}`);
                        }
                        return ` · 🎯 ${dt.effective_calories_kcal} kcal (${parts.join(" · ")})`;
                      })()
                    : ""
                }</span>
                <div style="display:flex;gap:5px;">
                  <button id="btn-toggle-targets" class="mini-btn">⚙️</button>
                  ${d?.garmin_configured ? `<button id="btn-sync-garmin" class="mini-btn">⌚ Sync</button>` : ""}
                </div>
              </div>`
            : ""
        }
        ${readinessPills.length ? `<div class="garmin-pills">${readinessPills.join("")}</div>` : ""}
        ${workoutsHtml}
        ${
          d?.readiness_warning
            ? `<div class="food-banner" style="background:rgba(245,158,11,0.12);border:1px solid rgba(245,158,11,0.35);color:var(--amber);">⚠️ ${escapeHtml(d.readiness_warning)}</div>`
            : ""
        }
        ${targetsPanelHtml}
        ${
          d?.latest_weight
            ? `<div style="margin-top:10px;font-size:12px;color:var(--text-muted);display:flex;justify-content:space-between;">
                <span>⚖️ ${d.latest_weight.weight_lbs} lbs (EMA ${d.latest_weight.ema_weight_lbs})</span>
                <button id="btn-log-weight" class="mini-btn">+ Weight</button>
              </div>`
            : `<div style="margin-top:10px;text-align:right;"><button id="btn-log-weight" class="mini-btn">+ Log Weight</button></div>`
        }
      </div>

      ${weekHtml}

      <div class="card">
        <div style="display:flex;gap:8px;align-items:center;margin-bottom:8px;">
          <label class="icon-btn primary" style="cursor:pointer;">
            📷 Photo
            <input id="food-file-input" type="file" accept="image/*" multiple style="display:none;" />
          </label>
          <input id="food-text-input" class="text-field" placeholder="Describe meal or upload photo…" value="${escapeHtml(state.foodInput)}" />
          <button id="btn-food-log" class="composer-send"${state.submitting ? " disabled" : ""}>${
    state.submitting ? "Logging…" : "Log"
  }</button>
        </div>
        <div style="display:flex;justify-content:space-between;align-items:center;font-size:12px;">
          <span>${state.foodFiles.length ? `${state.foodFiles.length} photo(s) ready` : "Quick tap a staple or snap a plate"}</span>
          <div style="display:flex;gap:6px;">
            <button id="btn-food-suggest" class="mini-btn">✨ Suggest</button>
            <button id="btn-food-model" class="model-pill">${escapeHtml(modelShortLabel(state.foodModel))}</button>
          </div>
        </div>
        ${
          state.foodStatus
            ? `<div class="food-banner pending"><span class="spinner" style="width:15px;height:15px;border-width:2px;flex-shrink:0;"></span><span>${escapeHtml(state.foodStatus)}</span></div>`
            : state.foodError
              ? `<div class="food-banner error"><strong>Failed:</strong> ${escapeHtml(state.foodError)}</div>`
              : state.foodNotice
                ? `<div class="food-banner ok">${escapeHtml(state.foodNotice)}</div>`
                : ""
        }
        ${
          state.foodSuggestResult
            ? `<div class="md-body" style="margin-top:10px;padding-top:10px;border-top:1px solid var(--border);">${renderMarkdown(state.foodSuggestResult)}</div>`
            : ""
        }
      </div>

      ${staplesHtml}
      ${mealsHtml}
    </div>
  `;
}

function renderHealthTabBar() {
  const tabs = [
    ["food", "🍽️", "Health"],
    ["settings", "⚙️", "Push"],
  ];
  return `
    <nav class="tab-bar">
      ${tabs
        .map(
          ([id, icon, lbl]) => `
        <button class="tab-btn${state.tab === id ? " active" : ""}" data-tab="${id}">
          <span class="tab-icon">${icon}</span>
          <span>${lbl}</span>
        </button>`
        )
        .join("")}
    </nav>
  `;
}

function wireHealthEvents() {
  document.querySelectorAll("[data-tab]").forEach((btn) => {
    btn.addEventListener("click", () => {
      state.tab = btn.dataset.tab;
      render();
      if (state.tab === "food") loadFoodToday();
    });
  });

  document.getElementById("btn-food-prev")?.addEventListener("click", () => {
    if (!state.foodDate) return;
    const d = new Date(`${state.foodDate}T12:00:00Z`);
    d.setUTCDate(d.getUTCDate() - 1);
    loadFoodToday(d.toISOString().slice(0, 10));
  });
  document.getElementById("btn-food-next")?.addEventListener("click", () => {
    if (!state.foodDate) return;
    const d = new Date(`${state.foodDate}T12:00:00Z`);
    d.setUTCDate(d.getUTCDate() + 1);
    loadFoodToday(d.toISOString().slice(0, 10));
  });
  document.getElementById("btn-food-week")?.addEventListener("click", () => {
    state.foodShowWeek = !state.foodShowWeek;
    if (state.foodShowWeek) loadFoodWeek();
    else render();
  });
  document.getElementById("food-text-input")?.addEventListener("input", (e) => {
    state.foodInput = e.target.value;
  });
  document.getElementById("food-text-input")?.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.isComposing) {
      e.preventDefault();
      logFoodMeal();
    }
  });
  document.getElementById("food-file-input")?.addEventListener("change", (e) => {
    state.foodFiles = Array.from(e.target.files || []);
    render();
  });
  document.getElementById("btn-food-log")?.addEventListener("click", () => logFoodMeal());
  document.getElementById("btn-food-model")?.addEventListener("click", () => {
    state.foodModel = cycleModel(state.foodModel);
    render();
  });
  document.getElementById("btn-food-suggest")?.addEventListener("click", async () => {
    const craving = window.prompt("Any craving or constraint? (optional)", "") ?? null;
    if (craving === null) return;
    try {
      toast("Generating suggestions…");
      const res = await apiFetch("api/food/suggest", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ craving, model: state.foodModel }),
      });
      state.foodSuggestResult = res.suggestion || "";
      render();
    } catch (err) {
      toast(err.message);
    }
  });
  document.getElementById("btn-log-weight")?.addEventListener("click", async () => {
    const raw = window.prompt("Weight (lbs):", state.foodToday?.latest_weight?.weight_lbs || "");
    if (!raw) return;
    const val = Number.parseFloat(raw);
    if (!Number.isFinite(val)) return toast("Invalid weight.");
    try {
      const res = await apiFetch("api/food/weight", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ weight_lbs: val }),
      });
      state.foodToday = res.today;
      render();
      toast(`Logged ${val.toFixed(1)} lbs`);
    } catch (err) {
      toast(err.message);
    }
  });
  document.getElementById("btn-sync-garmin")?.addEventListener("click", async () => {
    try {
      const res = await apiFetch("api/food/garmin/sync", { method: "POST" });
      state.foodToday = res.today;
      render();
      toast(`Synced ${res.days_synced} Garmin day(s)`);
    } catch (err) {
      toast(err.message);
    }
  });
  document.getElementById("btn-food-coach")?.addEventListener("click", () => {
    window.location.href = `${chatBase}/#new_health_chat=1`;
  });
  document.getElementById("btn-toggle-targets")?.addEventListener("click", () => {
    state.foodShowTargets = !state.foodShowTargets;
    render();
  });
  document.getElementById("btn-garmin-toggle-dyn")?.addEventListener("click", () => {
    const cur = state.foodToday?.garmin_settings?.dynamic_enabled !== false;
    updateGarminSettings({ dynamic_enabled: !cur });
  });
  document.querySelectorAll("[data-garmin-delta]").forEach((btn) => {
    btn.addEventListener("click", () => {
      updateGarminSettings({ goal_delta_kcal: Number(btn.dataset.garminDelta) });
    });
  });
  document.querySelectorAll("[data-garmin-mode]").forEach((btn) => {
    btn.addEventListener("click", () => {
      updateGarminSettings({ dynamic_mode: btn.dataset.garminMode });
    });
  });
  document.getElementById("btn-apply-cal")?.addEventListener("click", (e) => {
    const factor = Number(e.currentTarget.dataset.calFactor);
    if (Number.isFinite(factor)) updateGarminSettings({ active_factor: factor });
  });
  document.querySelectorAll("[data-log-staple]").forEach((btn) => {
    btn.addEventListener("click", () => logFoodMeal({ stapleSlug: btn.dataset.logStaple }));
  });
  document.querySelectorAll("[data-scale-meal]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      try {
        state.foodToday = await apiFetch(`api/food/entries/${btn.dataset.scaleMeal}/scale`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ multiplier: Number(btn.dataset.mult) }),
        });
        render();
      } catch (err) {
        toast(err.message);
      }
    });
  });
  document.querySelectorAll("[data-staple-meal]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      try {
        state.foodToday = await apiFetch(`api/food/entries/${btn.dataset.stapleMeal}/save-staple`, {
          method: "POST",
        });
        render();
        toast("Saved as staple.");
      } catch (err) {
        toast(err.message);
      }
    });
  });
  document.querySelectorAll("[data-del-meal]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      if (!window.confirm("Delete this meal entry?")) return;
      try {
        state.foodToday = await apiFetch(`api/food/entries/${btn.dataset.delMeal}`, {
          method: "DELETE",
        });
        render();
      } catch (err) {
        toast(err.message);
      }
    });
  });
  document.querySelectorAll("[data-revise-meal]").forEach((btn) => {
    btn.addEventListener("click", () => {
      window.location.href = `${chatBase}/#thread=${encodeURIComponent(btn.dataset.reviseMeal)}`;
    });
  });
  document.querySelectorAll("[data-react-meal]").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const nextEmoji = btn.classList.contains("active") ? null : btn.dataset.emoji;
      try {
        state.foodToday = await apiFetch(`api/food/entries/${btn.dataset.reactMeal}/reaction`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ emoji: nextEmoji }),
        });
        render();
      } catch (err) {
        toast(err.message);
      }
    });
  });
  document.querySelectorAll("[data-photo-url]").forEach((img) => {
    const openPhoto = () => {
      state.foodPhotoModal = {
        url: img.dataset.photoUrl,
        title: img.dataset.photoTitle || "",
        kcal: img.dataset.photoKcal || "",
      };
      render();
    };
    img.addEventListener("click", openPhoto);
    img.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        openPhoto();
      }
    });
  });
  document.getElementById("photo-lightbox-backdrop")?.addEventListener("click", (e) => {
    if (e.target.id === "photo-lightbox-backdrop") {
      state.foodPhotoModal = null;
      render();
    }
  });
  document.getElementById("btn-close-photo-modal")?.addEventListener("click", () => {
    state.foodPhotoModal = null;
    render();
  });
}

function renderHealthModals() {
  const m = state.foodPhotoModal;
  if (!m) return "";
  const caption = m.kcal ? `${m.title} · ${m.kcal} kcal` : m.title;
  return `
    <div id="photo-lightbox-backdrop" class="photo-lightbox-backdrop" role="dialog" aria-modal="true">
      <div class="photo-lightbox-card">
        <button id="btn-close-photo-modal" class="photo-lightbox-close" aria-label="Close photo">✕</button>
        <img class="photo-lightbox-img" src="${escapeHtml(m.url)}" alt="${escapeHtml(m.title)}" />
        ${caption ? `<div class="photo-lightbox-caption">${escapeHtml(caption)}</div>` : ""}
      </div>
    </div>
  `;
}

export function initHealthMode() {
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && state.foodPhotoModal) {
      state.foodPhotoModal = null;
      render();
    }
  });
  registerAppMode({
    initialTab: "food",
    focusInputIds: ["food-text-input"],
    onAuthenticated: async () => {
      await loadFoodToday();
    },
    onVisible: () => {
      if (state.tab === "food") loadFoodToday();
    },
    onWsEvent: (pkt) => {
      const { type, data } = pkt || {};
      if (type === "food.updated") {
        if (state.foodPendingTs && (!data?.thread_ts || data.thread_ts === state.foodPendingTs)) {
          state.foodPendingTs = "";
          state.foodStatus = "";
          if (data?.error) {
            state.foodError = data.error;
            toast(`Log failed: ${data.error}`);
          } else if (data?.meal_title) {
            state.foodNotice = `Logged: ${data.meal_title} (${Math.round(data.calories_kcal || 0)} kcal · ${Math.round(data.protein_g || 0)}g P)`;
            toast(state.foodNotice);
          } else if (data?.notice) {
            state.foodNotice = data.notice;
            toast(data.notice);
          }
        }
        if (state.tab === "food") {
          loadFoodToday();
        } else {
          render();
        }
      }
    },
    renderMain: renderFoodView,
    renderModals: renderHealthModals,
    renderTabBar: renderHealthTabBar,
    wireModeEvents: wireHealthEvents,
  });
}
