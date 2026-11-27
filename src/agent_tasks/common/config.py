from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Literal

import pydantic
import yaml


def resolve_gemini_auth(env: Mapping[str, str]) -> tuple[str | None, bool]:
    """Return `(api_key, use_vertex)` for `LocalAgentConfig`.

    Google AI Studio issues both `AIza...` and `AQ....` keys for the Gemini
    Developer API (`generativelanguage.googleapis.com`, `vertex=False`). Set
    `GEMINI_VERTEX=true` explicitly when routing against Vertex AI / Agent Platform
    (`aiplatform.googleapis.com`).
    """
    api_key = (env.get("GEMINI_API_KEY") or env.get("GOOGLE_API_KEY") or "").strip() or None
    explicit = env.get("GEMINI_VERTEX", "").strip().lower()
    use_vertex = explicit in {"1", "true", "yes"}
    return api_key, use_vertex


WorkerBackend = Literal["agy_cli", "sdk"]

CLI_MODEL_DEFAULTS: dict[str, str] = {
    "gemini-4-hydrogen": "gemini-4-hydrogen-medium",
    "gemini-4-neon": "gemini-4-neon-medium",
    "gemini-4-carbon": "gemini-4-carbon-medium",
    "gemini-4-argon": "gemini-4-argon-medium",
    "gemini-3.8-flash": "gemini-3.8-flash-medium",
}


_EFFORT_TIERS = ("low", "medium", "high")


def resolve_worker_model_for_cli(model: str) -> str:
    """Map an SDK-style or shorthand model ID to an `agy` CLI model ID (preserving explicit IDs)."""
    clean = (model or "").strip() or DEFAULT_WORKER_MODEL
    lower = clean.lower()
    if lower in MODEL_ALIASES:
        clean = MODEL_ALIASES[lower]
    else:
        for tier in _EFFORT_TIERS:
            suffix = f"-{tier}"
            if lower.endswith(suffix):
                stem = lower[: -len(suffix)]
                if stem in MODEL_ALIASES:
                    return f"{MODEL_ALIASES[stem]}{suffix}"
    return CLI_MODEL_DEFAULTS.get(clean, clean)


def resolve_worker_model_for_sdk(model: str) -> str:
    """Normalize a model ID for the `google-antigravity` SDK (stripping CLI effort suffixes)."""
    clean = (model or "").strip() or DEFAULT_WORKER_MODEL
    lower = clean.lower()
    if lower in MODEL_ALIASES:
        return MODEL_ALIASES[lower]
    for tier in _EFFORT_TIERS:
        suffix = f"-{tier}"
        if lower.endswith(suffix):
            stem = lower[: -len(suffix)]
            if stem in MODEL_ALIASES:
                return MODEL_ALIASES[stem]
    return clean


def normalize_worker_backend(value: str | None) -> WorkerBackend | None:
    """Normalize a backend name (`agy_cli` / `agy-cli` / `account` or `sdk` / `api_key`)."""
    raw = (value or "").strip().lower().replace("-", "_")
    if raw in {"agy_cli", "agy", "cli", "account"}:
        return "agy_cli"
    if raw in {"sdk", "api_key", "api"}:
        return "sdk"
    return None


def resolve_worker_backend(
    targets: TargetsConfig,
    *,
    task_kind: str,
    is_handoff: bool = False,
    override: str | None = None,
) -> WorkerBackend:
    """Resolve whether a worker pod runs via `agy_cli` (Google account) or `sdk` (API key)."""
    explicit = normalize_worker_backend(override)
    if explicit is not None:
        return explicit
    if is_handoff:
        return targets.slack_coding.handoff_worker_backend
    if task_kind == "sentry":
        return targets.sentry.worker_backend
    if task_kind == "slack_coding":
        return targets.slack_coding.worker_backend
    if task_kind == "ci_healer":
        return targets.ci_healer.worker_backend
    if task_kind == "k8s_watchdog":
        return targets.k8s_watchdog.worker_backend
    if task_kind == "dep_upgrader":
        return targets.dep_upgrader.worker_backend
    return "sdk"


DEFAULT_WORKER_MODEL = "gemini-4-hydrogen"

MODEL_ALIASES: dict[str, str] = {
    "hydrogen": "gemini-4-hydrogen",
    "gemini-4-hydrogen": "gemini-4-hydrogen",
    "neon": "gemini-4-neon",
    "gemini-4-neon": "gemini-4-neon",
    "carbon": "gemini-4-carbon",
    "gemini-4-carbon": "gemini-4-carbon",
    "argon": "gemini-4-argon",
    "gemini-4-argon": "gemini-4-argon",
    "flash": "gemini-3.8-flash",
    "gemini-3.8-flash": "gemini-3.8-flash",
}

_BRACKET_MODEL_RE = re.compile(
    r"\[(" + "|".join(re.escape(k) for k in MODEL_ALIASES) + r")\]",
    re.IGNORECASE,
)
_PHRASE_MODEL_RE = re.compile(
    r"(?:^|\s)(?:with|use|using)\s+(hydrogen|neon|carbon|argon|flash)\b(?=[.!?\s,:-]*$)",
    re.IGNORECASE,
)
_LEADING_MODEL_RE = re.compile(
    r"^(?:with|use|using)\s+(hydrogen|neon|carbon|argon|flash)\b[:,\s-]+",
    re.IGNORECASE,
)


def parse_model_override(text: str) -> tuple[str | None, str]:
    """Extract an explicit model override tag/phrase and return `(model_id, cleaned_text)`.

    Recognizes bracketed tags (`[carbon]`, `[hydrogen]`, `[argon]`, `[flash]`) anywhere
    or explicit leading/trailing `with <model>` / `use <model>` instructions.
    """
    selected: str | None = None
    cleaned = text

    bracket_matches = list(_BRACKET_MODEL_RE.finditer(cleaned))
    if bracket_matches:
        key = bracket_matches[-1].group(1).lower()
        selected = MODEL_ALIASES.get(key)
        cleaned = _BRACKET_MODEL_RE.sub(" ", cleaned)

    if selected is None:
        m_lead = _LEADING_MODEL_RE.match(cleaned.strip())
        if m_lead:
            selected = MODEL_ALIASES.get(m_lead.group(1).lower())
            cleaned = cleaned.strip()[m_lead.end() :]
        else:
            m_tail = _PHRASE_MODEL_RE.search(cleaned.rstrip())
            if m_tail:
                selected = MODEL_ALIASES.get(m_tail.group(1).lower())
                cleaned = cleaned[: m_tail.start()]

    normalized = " ".join(cleaned.split()).strip()
    return selected, normalized


class SentryGlobalConfig(pydantic.BaseModel):
    organization_slug: str = "lodbrok"
    base_url: str = "https://us.sentry.io"
    poll_interval_seconds: int = 180
    lookback_query: str = "is:unresolved firstSeen:-24h"
    max_issues_per_poll: int = 5
    settle_seconds: int = 180
    min_events_bypass_settle: int = 2
    regression_cooldown_seconds: int = 3600
    worker_backend: WorkerBackend = "sdk"


class ProjectTargetConfig(pydantic.BaseModel):
    github_repo: str
    default_branch: str = "main"
    focus_subdir: str | None = None
    verify_hint: str = ""
    enabled: bool = True


class SlackCodingConfig(pydantic.BaseModel):
    enabled: bool = True
    github_repo: str = "lodbrok-app/rattlsnek"
    default_branch: str = "main"
    focus_subdir: str | None = ""
    verify_hint: str = (
        "cd rattlsnek && uv sync --package edge --package edge_utils && "
        "uv run ruff check . && uv run ty check"
    )
    worker_backend: WorkerBackend = "sdk"
    handoff_worker_backend: WorkerBackend = "agy_cli"


class CiRepoConfig(pydantic.BaseModel):
    github_repo: str = "lodbrok-app/rattlsnek"
    default_branch: str = "main"
    verify_hint: str = "cd rattlsnek && uv run ruff check . && uv run ty check"


class CiHealerConfig(pydantic.BaseModel):
    enabled: bool = True
    poll_interval_seconds: int = 180
    lookback_hours: int = 6
    settle_seconds: int = 90
    cooldown_seconds: int = 3600
    worker_backend: WorkerBackend = "sdk"
    repos: list[CiRepoConfig] = pydantic.Field(default_factory=lambda: [CiRepoConfig()])


class K8sWatchdogConfig(pydantic.BaseModel):
    enabled: bool = True
    poll_interval_seconds: int = 120
    settle_polls: int = 2
    argo_out_of_sync_polls: int = 15
    recovery_polls: int = 3
    reopen_cooldown_seconds: int = 21600
    watched_namespaces: list[str] = pydantic.Field(
        default_factory=lambda: ["rattlsnek", "monitoring", "argocd", "agent-tasks"]
    )
    ignored_argo_apps: list[str] = pydantic.Field(default_factory=lambda: ["root", "agent-tasks"])
    github_repo: str = "lodbrok-app/rattlsnek"
    default_branch: str = "main"
    verify_hint: str = "cd rattlsnek && uv run ruff check ."
    worker_backend: WorkerBackend = "sdk"


class DepUpgraderConfig(pydantic.BaseModel):
    enabled: bool = True
    interval_seconds: int = 604800
    github_repo: str = "lodbrok-app/rattlsnek"
    default_branch: str = "main"
    verify_hint: str = (
        "cd rattlsnek && uv sync --package edge --package edge_utils && "
        "uv run ruff check edge edge_utils && uv run pytest edge edge_utils"
    )
    worker_backend: WorkerBackend = "sdk"


class ChannelRoutingConfig(pydantic.BaseModel):
    """Optional static Slack channel IDs per automation role (overridden/extended via `!bind`)."""

    sentry: str = ""
    ci: str = ""
    deps: str = ""
    k8s: str = ""
    coding: str = ""
    billing: str = ""
    food: str = ""


class FoodTrackerConfig(pydantic.BaseModel):
    """Configuration for the `#agy-food` multimodal nutrition tracker."""

    enabled: bool = True
    timezone: str = "America/Los_Angeles"
    day_rollover_hour: int = 3
    default_calories_kcal: int = 2200
    default_protein_g: float = 170.0
    default_carbs_g: float = 220.0
    default_fat_g: float = 70.0
    default_fiber_g: float = 30.0
    evening_digest_enabled: bool = True
    evening_digest_hour: int = 20
    evening_digest_minute: int = 30
    daily_backup_enabled: bool = True
    backup_keep_days: int = 14
    garmin_enabled: bool = True
    garmin_base_url: str = "https://personal.connectapi.garmin.com"
    garmin_sync_interval_seconds: int = 900
    garmin_lookback_days: int = 7
    garmin_sync_weight: bool = True
    garmin_dynamic_targets_enabled: bool = True
    garmin_dynamic_mode: Literal["rolling_baseline", "daily_burn"] = "rolling_baseline"
    garmin_active_factor: float = 0.80
    garmin_burn_offset_kcal: int = 0
    garmin_goal_delta_kcal: int = 0
    garmin_min_calories_kcal: int = 1600
    garmin_max_calories_kcal: int = 4000


class ModelRateConfig(pydantic.BaseModel):
    """Google Cloud list-pricing USD per 1M tokens for credit ledger estimation."""

    input_usd_per_1m: float
    cached_input_usd_per_1m: float
    output_usd_per_1m: float


def default_model_rates() -> dict[str, ModelRateConfig]:
    return {
        "gemini-4-hydrogen": ModelRateConfig(
            input_usd_per_1m=1.50,
            cached_input_usd_per_1m=0.15,
            output_usd_per_1m=7.50,
        ),
        "gemini-4-carbon": ModelRateConfig(
            input_usd_per_1m=4.00,
            cached_input_usd_per_1m=0.40,
            output_usd_per_1m=20.00,
        ),
        "gemini-4-argon": ModelRateConfig(
            input_usd_per_1m=4.00,
            cached_input_usd_per_1m=0.40,
            output_usd_per_1m=20.00,
        ),
        "gemini-3.8-flash": ModelRateConfig(
            input_usd_per_1m=1.50,
            cached_input_usd_per_1m=0.15,
            output_usd_per_1m=7.50,
        ),
        "gemini-3.5-flash-lite": ModelRateConfig(
            input_usd_per_1m=0.30,
            cached_input_usd_per_1m=0.03,
            output_usd_per_1m=2.50,
        ),
    }


class BillingConfig(pydantic.BaseModel):
    enabled: bool = True
    monthly_credit_usd: float = 100.0
    cycle_anchor_day: int = 15
    check_interval_seconds: int = 900
    warn_thresholds_pct: list[float] = pydantic.Field(
        default_factory=lambda: [50.0, 75.0, 90.0, 100.0]
    )
    projected_overrun_warn: bool = True
    min_hours_for_projection: float = 24.0
    min_spend_usd_for_projection: float = 3.0
    hard_stop_pct: float = 95.0
    model_rates: dict[str, ModelRateConfig] = pydantic.Field(default_factory=default_model_rates)


class AgyConfigSyncConfig(pydantic.BaseModel):
    """Optional GitHub repo containing global AGY customizations (hooks, rules, skills, sidecars)."""

    enabled: bool = True
    github_repo: str = ""
    branch: str = "main"


class TargetsConfig(pydantic.BaseModel):
    sentry: SentryGlobalConfig = pydantic.Field(default_factory=SentryGlobalConfig)
    projects: dict[str, ProjectTargetConfig] = pydantic.Field(default_factory=dict)
    slack_coding: SlackCodingConfig = pydantic.Field(default_factory=SlackCodingConfig)
    ci_healer: CiHealerConfig = pydantic.Field(default_factory=CiHealerConfig)
    k8s_watchdog: K8sWatchdogConfig = pydantic.Field(default_factory=K8sWatchdogConfig)
    dep_upgrader: DepUpgraderConfig = pydantic.Field(default_factory=DepUpgraderConfig)
    agy_config: AgyConfigSyncConfig = pydantic.Field(default_factory=AgyConfigSyncConfig)
    channels: ChannelRoutingConfig = pydantic.Field(default_factory=ChannelRoutingConfig)
    billing: BillingConfig = pydantic.Field(default_factory=BillingConfig)
    food_tracker: FoodTrackerConfig = pydantic.Field(default_factory=FoodTrackerConfig)

    @classmethod
    def from_yaml(cls, path: str | Path) -> TargetsConfig:
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        return cls.model_validate(raw)


def _parse_bool(raw: str | None, default: bool) -> bool:
    if raw is None or not raw.strip():
        return default
    clean = raw.strip().lower()
    if clean in {"1", "true", "yes", "on"}:
        return True
    if clean in {"0", "false", "no", "off"}:
        return False
    return default


def _normalize_base_path(raw: str) -> str:
    clean = (raw or "/agy").strip()
    if not clean.startswith("/"):
        clean = f"/{clean}"
    if len(clean) > 1:
        clean = clean.rstrip("/")
    return clean


def parse_github_tokens_from_env(env: Mapping[str, str]) -> dict[str, str]:
    """Parse per-owner and per-repo GitHub tokens from `GITHUB_TOKENS_JSON` and `GITHUB_TOKEN_<OWNER>`.

    Keys are normalized to lowercase (`"owner"` or `"owner/repo"`). For `GITHUB_TOKEN_<OWNER>`,
    underscores in `<OWNER>` register both the raw underscore form and hyphenated form (e.g.
    `GITHUB_TOKEN_LODBROK_APP` matches `lodbrok-app`). Explicit entries in `GITHUB_TOKENS_JSON`
    take precedence over env-var suffix tokens.
    """
    tokens: dict[str, str] = {}
    for key, raw_val in env.items():
        if not key.startswith("GITHUB_TOKEN_"):
            continue
        suffix = key[len("GITHUB_TOKEN_") :].strip()
        val = (raw_val or "").strip()
        if not suffix or not val or suffix.upper() in {"FILE", "JSON"}:
            continue
        owner_lower = suffix.lower()
        tokens[owner_lower] = val
        if "_" in owner_lower:
            tokens[owner_lower.replace("_", "-")] = val

    raw_json = (env.get("GITHUB_TOKENS_JSON") or "").strip()
    if raw_json:
        parsed = json.loads(raw_json)
        if not isinstance(parsed, dict):
            raise ValueError(
                "GITHUB_TOKENS_JSON must be a JSON object mapping owner or owner/repo to token."
            )
        for raw_k, raw_v in parsed.items():
            k = str(raw_k or "").strip().lower()
            v = str(raw_v or "").strip()
            if k and v:
                tokens[k] = v
    return tokens


class ControllerSettings(pydantic.BaseModel):
    namespace: str = "agent-tasks"
    node_name: str | None = None
    state_dir: Path = Path("/state")
    state_pvc_name: str = "agent-tasks-state"
    targets_config_path: Path = Path("/etc/agent-tasks/targets.yaml")

    worker_image: str = "ghcr.io/billthefighter/agent-tasks-worker:latest"
    worker_model: str = DEFAULT_WORKER_MODEL
    worker_service_account: str = "agent-tasks-worker"
    worker_secret_name: str = "agent-tasks-worker-secret"
    image_pull_secret_name: str = "ghcr-pull"
    job_active_deadline_seconds: int = 1800
    handoff_active_deadline_seconds: int = 7200
    handoff_max_deadline_seconds: int = 14400
    worker_idle_timeout_seconds: int = 900
    max_handoff_auto_continues: int = 2
    job_ttl_seconds_after_finished: int = 1800
    max_concurrent_jobs: int = 2

    sentry_auth_token: str = ""
    target_github_token: str
    github_tokens: dict[str, str] = pydantic.Field(default_factory=dict)
    slack_enabled: bool = True
    slack_bot_token: str = ""
    slack_app_token: str = ""
    slack_channel_id: str = ""

    pwa_enabled: bool = True
    pwa_host: str = "0.0.0.0"
    pwa_port: int = 8080
    pwa_base_path: str = "/agy"
    pwa_health_base_path: str = "/health"
    pwa_api_token: str = ""
    pwa_mirror_to_slack: bool = True
    pwa_ca_cert_path: Path = Path("/etc/agent-tasks-ca/ca.crt")
    vapid_subject: str = "mailto:alerts@lodbrok.app"
    vapid_public_key: str = ""
    vapid_private_key: str = ""
    garmin_api_key: str = ""

    def resolve_github_token(self, github_repo: str) -> str:
        """Resolve the best GitHub token for `owner/repo` (exact repo -> owner -> default)."""
        clean = (github_repo or "").strip().lower().removesuffix(".git")
        if clean:
            exact = self.github_tokens.get(clean)
            if exact:
                return exact
            owner = clean.split("/", 1)[0]
            by_owner = self.github_tokens.get(owner)
            if by_owner:
                return by_owner
        return self.target_github_token

    @classmethod
    def from_env(cls) -> ControllerSettings:
        slack_enabled = _parse_bool(os.environ.get("SLACK_ENABLED"), True)
        if slack_enabled:
            slack_bot_token = os.environ["SLACK_BOT_TOKEN"]
            slack_app_token = os.environ["SLACK_APP_TOKEN"]
            slack_channel_id = os.environ["SLACK_CHANNEL_ID"]
        else:
            slack_bot_token = os.environ.get("SLACK_BOT_TOKEN", "")
            slack_app_token = os.environ.get("SLACK_APP_TOKEN", "")
            slack_channel_id = os.environ.get("SLACK_CHANNEL_ID", "")

        github_tokens = parse_github_tokens_from_env(os.environ)
        target_github_token = (
            os.environ.get("TARGET_GITHUB_TOKEN", "")
            if github_tokens
            else os.environ["TARGET_GITHUB_TOKEN"]
        )

        return cls(
            namespace=os.environ.get("NAMESPACE", "agent-tasks"),
            node_name=os.environ.get("MY_NODE_NAME") or None,
            state_dir=Path(os.environ.get("STATE_DIR", "/state")),
            state_pvc_name=os.environ.get("STATE_PVC_NAME", "agent-tasks-state"),
            targets_config_path=Path(
                os.environ.get("TARGETS_CONFIG_PATH", "/etc/agent-tasks/targets.yaml")
            ),
            worker_image=os.environ.get(
                "WORKER_IMAGE", "ghcr.io/billthefighter/agent-tasks-worker:latest"
            ),
            worker_model=os.environ.get("WORKER_MODEL", DEFAULT_WORKER_MODEL),
            worker_service_account=os.environ.get("WORKER_SERVICE_ACCOUNT", "agent-tasks-worker"),
            worker_secret_name=os.environ.get("WORKER_SECRET_NAME", "agent-tasks-worker-secret"),
            image_pull_secret_name=os.environ.get("IMAGE_PULL_SECRET_NAME", "ghcr-pull"),
            job_active_deadline_seconds=int(os.environ.get("JOB_ACTIVE_DEADLINE_SECONDS", "1800")),
            handoff_active_deadline_seconds=int(
                os.environ.get("HANDOFF_ACTIVE_DEADLINE_SECONDS", "7200")
            ),
            handoff_max_deadline_seconds=int(
                os.environ.get("HANDOFF_MAX_DEADLINE_SECONDS", "14400")
            ),
            worker_idle_timeout_seconds=int(os.environ.get("WORKER_IDLE_TIMEOUT_SECONDS", "900")),
            max_handoff_auto_continues=int(os.environ.get("MAX_HANDOFF_AUTO_CONTINUES", "2")),
            job_ttl_seconds_after_finished=int(
                os.environ.get("JOB_TTL_SECONDS_AFTER_FINISHED", "1800")
            ),
            max_concurrent_jobs=int(os.environ.get("MAX_CONCURRENT_JOBS", "2")),
            sentry_auth_token=os.environ.get("SENTRY_AUTH_TOKEN", ""),
            target_github_token=target_github_token,
            github_tokens=github_tokens,
            slack_enabled=slack_enabled,
            slack_bot_token=slack_bot_token,
            slack_app_token=slack_app_token,
            slack_channel_id=slack_channel_id,
            pwa_enabled=_parse_bool(os.environ.get("PWA_ENABLED"), True),
            pwa_host=os.environ.get("PWA_HOST", "0.0.0.0"),
            pwa_port=int(os.environ.get("PWA_PORT", "8080")),
            pwa_base_path=_normalize_base_path(os.environ.get("PWA_BASE_PATH", "/agy")),
            pwa_health_base_path=_normalize_base_path(
                os.environ.get("PWA_HEALTH_BASE_PATH", "/health")
            ),
            pwa_api_token=os.environ.get("PWA_API_TOKEN", "").strip(),
            pwa_mirror_to_slack=_parse_bool(os.environ.get("PWA_MIRROR_TO_SLACK"), True),
            pwa_ca_cert_path=Path(os.environ.get("PWA_CA_CERT_PATH", "/etc/agent-tasks-ca/ca.crt")),
            vapid_subject=os.environ.get("VAPID_SUBJECT", "mailto:alerts@lodbrok.app").strip()
            or "mailto:alerts@lodbrok.app",
            vapid_public_key=os.environ.get("VAPID_PUBLIC_KEY", "").strip(),
            vapid_private_key=os.environ.get("VAPID_PRIVATE_KEY", "").strip(),
            garmin_api_key=os.environ.get("GARMIN_API_KEY", "").strip(),
        )
