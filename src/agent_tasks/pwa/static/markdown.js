export function escapeHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function renderInline(escapedText) {
  let out = escapedText;
  // Slack-style <url|label> links or raw https:// URLs
  out = out.replace(
    /&lt;(https?:\/\/[^|&]+)\|([^&]+)&gt;/g,
    '<a href="$1" target="_blank" rel="noopener noreferrer">$2</a>'
  );
  out = out.replace(/`([^`]+)`/g, "<code>$1</code>");
  out = out.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
  out = out.replace(/(^|[\s(])\*([^*\n]+)\*/g, "$1<strong>$2</strong>");
  out = out.replace(/(^|[\s(])_([^_\n]+)_/g, "$1<em>$2</em>");
  return out;
}

export function renderMarkdown(rawText) {
  const src = String(rawText ?? "");
  const parts = src.split(/```([a-zA-Z0-9_-]*)\n?([\s\S]*?)```/g);
  let html = "";

  for (let i = 0; i < parts.length; i += 3) {
    const prose = parts[i] || "";
    const lang = parts[i + 1];
    const code = parts[i + 2];

    if (prose.trim()) {
      const blocks = prose.trim().split(/\n{2,}/);
      for (const blk of blocks) {
        const lines = blk.split("\n");
        const isBulletList = lines.every((l) => /^\s*(?:•|-|\*|\d+\.)\s+/.test(l));
        if (isBulletList) {
          html += "<ul>";
          for (const l of lines) {
            const clean = l.replace(/^\s*(?:•|-|\*|\d+\.)\s+/, "");
            html += `<li>${renderInline(escapeHtml(clean))}</li>`;
          }
          html += "</ul>";
        } else {
          html += `<p>${lines.map((l) => renderInline(escapeHtml(l))).join("<br />")}</p>`;
        }
      }
    }

    if (code !== undefined) {
      const cls = lang ? ` class="lang-${escapeHtml(lang)}"` : "";
      html += `<pre><code${cls}>${escapeHtml(code.replace(/\n$/, ""))}</code></pre>`;
    }
  }
  return html || "<p></p>";
}

export function renderDiff(diffText) {
  const lines = String(diffText ?? "").split("\n");
  return lines
    .map((line) => {
      let cls = "diff-line";
      if (line.startsWith("+") && !line.startsWith("+++")) cls += " diff-add";
      else if (line.startsWith("-") && !line.startsWith("---")) cls += " diff-del";
      else if (line.startsWith("@@") || line.startsWith("diff --git")) cls += " diff-hunk";
      return `<span class="${cls}">${escapeHtml(line)}</span>`;
    })
    .join("");
}

export function relativeTime(isoStr) {
  if (!isoStr) return "";
  const dt = new Date(isoStr);
  if (Number.isNaN(dt.getTime())) return "";
  const diffSec = Math.max(0, Math.floor((Date.now() - dt.getTime()) / 1000));
  if (diffSec < 60) return "now";
  if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m`;
  if (diffSec < 86400) return `${Math.floor(diffSec / 3600)}h`;
  return `${Math.floor(diffSec / 86400)}d`;
}
