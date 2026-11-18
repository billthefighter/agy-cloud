from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

_ENTRY_LINE_RE = re.compile(r"^- \[(mem-[0-9a-f]{6})\]\s*(.*)$")
_REPO_SLUG_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
_CONTROL_OR_TAG_RE = re.compile(
    r"</?\s*(?:system|Persistent_memory|untrusted_)[^>]*>", re.IGNORECASE
)

DEFAULT_MAX_MEMORY_CHARS = 8000


@dataclass(slots=True)
class MemoryEntry:
    entry_id: str
    scope: str  # "instructions" | "suppressed" | "repo:<owner/name>"
    text: str


def _sanitize_memory_text(raw: str, max_chars: int = 500) -> str:
    collapsed = " ".join((raw or "").split()).strip()
    scrubbed = _CONTROL_OR_TAG_RE.sub("", collapsed)
    if len(scrubbed) > max_chars:
        scrubbed = scrubbed[: max_chars - 1].rstrip() + "…"
    return scrubbed


def _repo_filename(github_repo: str) -> str:
    clean = github_repo.strip()
    if not _REPO_SLUG_RE.fullmatch(clean) or ".." in clean.split("/"):
        raise ValueError(f"Invalid repository slug `{github_repo}`.")
    return f"{clean.replace('/', '__')}.md"


def _repo_from_filename(filename: str) -> str:
    stem = filename.removesuffix(".md")
    return stem.replace("__", "/", 1)


class MemoryStore:
    """Durable, human-governed markdown memory store on `/state/memory/`.

    Mounted read-only (`/memory`) into ephemeral worker pods so untrusted Sentry/CI/K8s
    payloads cannot poison cross-run instructions, while keeping token layout deterministic
    for Gemini prompt caching.
    """

    def __init__(self, root_dir: Path) -> None:
        self.root_dir = root_dir
        self.repos_dir = root_dir / "repos"
        self.instructions_path = root_dir / "user_instructions.md"
        self.suppressed_path = root_dir / "suppressed_patterns.md"

    def ensure_dirs(self) -> Path:
        self.root_dir.mkdir(parents=True, exist_ok=True)
        self.repos_dir.mkdir(parents=True, exist_ok=True)
        return self.root_dir

    def repo_file(self, github_repo: str) -> Path:
        self.ensure_dirs()
        return self.repos_dir / _repo_filename(github_repo)

    @staticmethod
    def _parse_entries(path: Path, scope: str) -> list[MemoryEntry]:
        if not path.exists():
            return []
        entries: list[MemoryEntry] = []
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            m = _ENTRY_LINE_RE.match(raw_line.strip())
            if m:
                entries.append(MemoryEntry(entry_id=m.group(1), scope=scope, text=m.group(2)))
        return entries

    @staticmethod
    def _write_entries(path: Path, heading: str, entries: list[MemoryEntry]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not entries:
            if path.exists():
                path.unlink()
            return
        lines = [f"# {heading}", ""]
        for entry in entries:
            lines.append(f"- [{entry.entry_id}] {entry.text}")
        lines.append("")
        path.write_text("\n".join(lines), encoding="utf-8")

    @staticmethod
    def _make_entry_id(scope: str, clean_text: str) -> str:
        digest = hashlib.sha1(f"{scope}:{clean_text.lower()}".encode()).hexdigest()[:6]
        return f"mem-{digest}"

    def _append_unique(
        self,
        *,
        path: Path,
        heading: str,
        scope: str,
        raw_text: str,
        author: str = "",
        source_ref: str = "",
    ) -> MemoryEntry:
        self.ensure_dirs()
        clean = _sanitize_memory_text(raw_text)
        if not clean:
            raise ValueError("Memory note cannot be empty.")

        meta_parts: list[str] = []
        if source_ref.strip():
            meta_parts.append(source_ref.strip())
        if author.strip():
            meta_parts.append(f"by {author.strip()}")
        if meta_parts:
            today = datetime.now(UTC).strftime("%Y-%m-%d")
            annotated = f"{clean} _({', '.join(meta_parts)}, {today})_"
        else:
            annotated = clean

        entry_id = self._make_entry_id(scope, clean)
        existing = self._parse_entries(path, scope)
        for item in existing:
            if item.entry_id == entry_id:
                return item

        created = MemoryEntry(entry_id=entry_id, scope=scope, text=annotated)
        existing.append(created)
        self._write_entries(path, heading, existing)
        return created

    def add_instruction(self, text: str, *, author: str = "") -> MemoryEntry:
        return self._append_unique(
            path=self.instructions_path,
            heading="Standing User Instructions",
            scope="instructions",
            raw_text=text,
            author=author,
        )

    def add_suppressed_pattern(self, text: str, *, author: str = "") -> MemoryEntry:
        return self._append_unique(
            path=self.suppressed_path,
            heading="Known Transient / Non-Actionable Patterns",
            scope="suppressed",
            raw_text=text,
            author=author,
        )

    def add_repo_note(
        self,
        github_repo: str,
        text: str,
        *,
        author: str = "",
        source_ref: str = "",
    ) -> MemoryEntry:
        path = self.repo_file(github_repo)
        return self._append_unique(
            path=path,
            heading=f"Repository Notes ({github_repo})",
            scope=f"repo:{github_repo}",
            raw_text=text,
            author=author,
            source_ref=source_ref,
        )

    def remove_entry(self, entry_id: str) -> MemoryEntry | None:
        target_id = entry_id.strip().lower()
        if not target_id.startswith("mem-"):
            target_id = f"mem-{target_id}"

        candidates: list[tuple[Path, str, str]] = [
            (self.instructions_path, "Standing User Instructions", "instructions"),
            (self.suppressed_path, "Known Transient / Non-Actionable Patterns", "suppressed"),
        ]
        if self.repos_dir.exists():
            for repo_md in sorted(self.repos_dir.glob("*.md")):
                repo_slug = _repo_from_filename(repo_md.name)
                candidates.append((repo_md, f"Repository Notes ({repo_slug})", f"repo:{repo_slug}"))

        for path, heading, scope in candidates:
            entries = self._parse_entries(path, scope)
            kept: list[MemoryEntry] = []
            removed: MemoryEntry | None = None
            for item in entries:
                if item.entry_id == target_id and removed is None:
                    removed = item
                else:
                    kept.append(item)
            if removed is not None:
                self._write_entries(path, heading, kept)
                return removed
        return None

    def list_all(self, *, github_repo: str | None = None) -> list[MemoryEntry]:
        results: list[MemoryEntry] = []
        results.extend(self._parse_entries(self.instructions_path, "instructions"))
        if github_repo:
            try:
                rp = self.repo_file(github_repo)
                results.extend(self._parse_entries(rp, f"repo:{github_repo}"))
            except ValueError:
                pass
        elif self.repos_dir.exists():
            for repo_md in sorted(self.repos_dir.glob("*.md")):
                repo_slug = _repo_from_filename(repo_md.name)
                results.extend(self._parse_entries(repo_md, f"repo:{repo_slug}"))
        results.extend(self._parse_entries(self.suppressed_path, "suppressed"))
        return results

    def format_slack_summary(self) -> str:
        entries = self.list_all()
        if not entries:
            return (
                "*`agent-tasks` Persistent Memory* _(empty)_\n"
                "• Add standing rule: `!remember <instruction>`\n"
                "• Add repo rule: `!remember repo:lodbrok-app/rattlsnek <note>`\n"
                "• Add noise filter: `!remember suppress <pattern>`\n"
                "• Remove entry: `!forget <mem-xxxxxx>`"
            )

        by_scope: dict[str, list[MemoryEntry]] = {}
        for entry in entries:
            by_scope.setdefault(entry.scope, []).append(entry)

        lines = [f"*`agent-tasks` Persistent Memory ({len(entries)} entries)*"]
        if "instructions" in by_scope:
            lines.append("\n*Standing User Instructions:*")
            for e in by_scope.pop("instructions"):
                lines.append(f"• `{e.entry_id}` — {e.text}")

        for scope in sorted(k for k in by_scope if k.startswith("repo:")):
            repo_name = scope.split(":", 1)[1]
            lines.append(f"\n*Repo `{repo_name}`:*")
            for e in by_scope.pop(scope):
                lines.append(f"• `{e.entry_id}` — {e.text}")

        if "suppressed" in by_scope:
            lines.append("\n*Known Transient / Non-Actionable Patterns:*")
            for e in by_scope.pop("suppressed"):
                lines.append(f"• `{e.entry_id}` — {e.text}")

        lines.append(
            "\n_Manage with `!remember [repo:<slug>|suppress] <note>` or `!forget <mem-id>`._"
        )
        return "\n".join(lines)


def load_worker_memory_context(
    memory_dir: Path,
    github_repo: str = "",
    *,
    max_chars: int = DEFAULT_MAX_MEMORY_CHARS,
) -> str:
    """Load bounded, deterministically ordered persistent memory for `worker_main.py`.

    Placed in `system_instructions` so repeated runs share identical prefix tokens
    and benefit from Gemini prompt caching.
    """
    if not memory_dir.exists():
        return ""

    store = MemoryStore(memory_dir)
    sections: list[str] = []

    instr = store._parse_entries(store.instructions_path, "instructions")
    if instr:
        sections.append(
            "### Standing User Instructions\n" + "\n".join(f"- {e.text}" for e in instr)
        )

    if github_repo.strip():
        try:
            repo_path = store.repos_dir / _repo_filename(github_repo)
            repo_entries = store._parse_entries(repo_path, f"repo:{github_repo}")
            if repo_entries:
                sections.append(
                    f"### Repository Learnings (`{github_repo}`)\n"
                    + "\n".join(f"- {e.text}" for e in repo_entries)
                )
        except ValueError:
            pass

    suppressed = store._parse_entries(store.suppressed_path, "suppressed")
    if suppressed:
        sections.append(
            "### Known Transient / Non-Actionable Patterns\n"
            + "\n".join(f"- {e.text}" for e in suppressed)
        )

    if not sections:
        return ""

    combined = "\n\n".join(sections).strip()
    if len(combined) > max_chars:
        combined = combined[: max_chars - 20].rstrip() + "\n…(truncated)…"

    return f"<persistent_memory>\n{combined}\n</persistent_memory>"
