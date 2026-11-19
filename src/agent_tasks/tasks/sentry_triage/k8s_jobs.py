from __future__ import annotations

import asyncio
import re

import structlog
from kubernetes import client, config
from kubernetes.client.rest import ApiException

from agent_tasks.common.config import ControllerSettings, ProjectTargetConfig
from agent_tasks.common.state import IssueRecord

log = structlog.get_logger(__name__)


def make_job_name(issue_id: str, turn: int) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", issue_id.lower()).strip("-")[:36]
    return f"agy-triage-{slug}-t{turn}"


def build_triage_job_manifest(
    *,
    settings: ControllerSettings,
    record: IssueRecord,
    target: ProjectTargetConfig,
    worker_backend: str = "sdk",
) -> client.V1Job:
    """Build a locked-down Kubernetes batch/v1 Job for isolated AGY triage on makesense.

    Key security & multi-node guarantees:
    1. `automountServiceAccountToken=False` + `agent-tasks-worker` SA (zero K8s RBAC).
    2. Worker container receives ONLY `agent-tasks-worker-secret` (GEMINI_API_KEY) —
       never GitHub, Slack, or Sentry tokens.
    3. Mounts only subPaths of `agent-tasks-state` (`mirrors/<repo>.git` read-only in
       initContainer, plus `sessions/<id>`, `artifacts/<id>`, and `caches/`).
    4. Co-locates onto the controller's node (`MY_NODE_NAME` + `podAffinity`) so
       makesense's 3-worker `local-path` ReadWriteOnce PVC mounts cleanly every time.
    """
    job_name = make_job_name(record.issue_id, record.turn)
    mirror_subpath = f"mirrors/{record.github_repo.replace('/', '__')}.git"
    session_subpath = f"sessions/{record.issue_id}"
    artifact_subpath = f"artifacts/{record.issue_id}"

    init_script = (
        "set -euo pipefail\n"
        'git clone --branch "$CLONE_REF" --single-branch /mirror /work\n'
        "cd /work\n"
        # Detach origin from /mirror so `git remote` inside /work doesn't point outside
        "git remote remove origin || true\n"
        "if [ -s /artifacts/base_sha.txt ]; then\n"
        "  BASE_SHA=\"$(tr -d '[:space:]' < /artifacts/base_sha.txt)\"\n"
        '  if [ -n "$BASE_SHA" ]; then\n'
        '    if ! git checkout "$BASE_SHA" 2>/dev/null; then\n'
        '      echo "ERROR: BASE_SHA $BASE_SHA not found in /mirror (branch $CLONE_REF). Push your base branch or re-run handoff." >&2\n'
        "      exit 1\n"
        "    fi\n"
        "  fi\n"
        "fi\n"
        "if [ -s /artifacts/fix.patch ]; then\n"
        '  echo "Applying prior fix.patch before starting turn $TURN..."\n'
        "  git apply /artifacts/fix.patch || git apply --3way /artifacts/fix.patch\n"
        "fi\n"
    )

    clone_ref = record.base_ref or record.default_branch
    worker_model = record.model_override or settings.worker_model

    git_safe_dir_env = [
        client.V1EnvVar(name="GIT_CONFIG_COUNT", value="1"),
        client.V1EnvVar(name="GIT_CONFIG_KEY_0", value="safe.directory"),
        client.V1EnvVar(name="GIT_CONFIG_VALUE_0", value="*"),
    ]

    init_container = client.V1Container(
        name="git-clone",
        image=settings.worker_image,
        image_pull_policy="Always",
        command=["/bin/bash", "-lc", init_script],
        env=[
            client.V1EnvVar(name="DEFAULT_BRANCH", value=record.default_branch),
            client.V1EnvVar(name="CLONE_REF", value=clone_ref),
            client.V1EnvVar(name="TURN", value=str(record.turn)),
            *git_safe_dir_env,
        ],
        security_context=client.V1SecurityContext(
            allow_privilege_escalation=False,
            capabilities=client.V1Capabilities(drop=["ALL"]),
        ),
        volume_mounts=[
            client.V1VolumeMount(name="work", mount_path="/work"),
            client.V1VolumeMount(
                name="state",
                mount_path="/mirror",
                sub_path=mirror_subpath,
                read_only=True,
            ),
            client.V1VolumeMount(
                name="state",
                mount_path="/artifacts",
                sub_path=artifact_subpath,
                read_only=True,
            ),
        ],
    )

    use_agy_cli = worker_backend == "agy_cli"
    worker_env = [
        client.V1EnvVar(name="ISSUE_ID", value=record.issue_id),
        client.V1EnvVar(name="SHORT_ID", value=record.short_id),
        client.V1EnvVar(name="PROJECT_SLUG", value=record.project_slug),
        client.V1EnvVar(name="GITHUB_REPO", value=record.github_repo),
        client.V1EnvVar(name="TASK_KIND", value=record.task_kind),
        client.V1EnvVar(name="WORKER_BACKEND", value=worker_backend),
        client.V1EnvVar(name="WORKER_MODEL", value=worker_model),
        client.V1EnvVar(name="TURN", value=str(record.turn)),
        client.V1EnvVar(name="CONVERSATION_ID", value=record.conversation_id or ""),
        client.V1EnvVar(name="FOCUS_SUBDIR", value=target.focus_subdir or ""),
        client.V1EnvVar(name="VERIFY_HINT", value=target.verify_hint),
        client.V1EnvVar(name="WORK_DIR", value="/work"),
        client.V1EnvVar(name="SESSION_DIR", value="/session"),
        client.V1EnvVar(name="ARTIFACTS_DIR", value="/artifacts"),
        client.V1EnvVar(name="MEMORY_DIR", value="/memory"),
        client.V1EnvVar(name="UV_CACHE_DIR", value="/caches/uv"),
        client.V1EnvVar(name="npm_config_cache", value="/caches/npm"),
        *git_safe_dir_env,
    ]
    if use_agy_cli:
        worker_env.extend(
            [
                client.V1EnvVar(name="HOME", value="/home/agy"),
                client.V1EnvVar(name="TZ", value="UTC"),
            ]
        )

    worker_volume_mounts = [
        client.V1VolumeMount(name="work", mount_path="/work"),
        client.V1VolumeMount(
            name="state",
            mount_path="/session",
            sub_path=session_subpath,
        ),
        client.V1VolumeMount(
            name="state",
            mount_path="/artifacts",
            sub_path=artifact_subpath,
        ),
        client.V1VolumeMount(
            name="state",
            mount_path="/memory",
            sub_path="memory",
            read_only=True,
        ),
        client.V1VolumeMount(
            name="state",
            mount_path="/caches",
            sub_path="caches",
        ),
    ]
    if use_agy_cli:
        worker_volume_mounts.append(
            client.V1VolumeMount(
                name="state",
                mount_path="/home/agy/.gemini",
                sub_path="agy-auth",
            )
        )

    worker_container = client.V1Container(
        name="agy-worker",
        image=settings.worker_image,
        image_pull_policy="Always",
        env=worker_env,
        env_from=[
            client.V1EnvFromSource(
                secret_ref=client.V1SecretEnvSource(
                    name=settings.worker_secret_name,
                    optional=use_agy_cli,
                )
            )
        ],
        resources=client.V1ResourceRequirements(
            requests={"cpu": "250m", "memory": "768Mi"},
            limits={"cpu": "2000m", "memory": "4Gi"},
        ),
        security_context=client.V1SecurityContext(
            allow_privilege_escalation=False,
            capabilities=client.V1Capabilities(drop=["ALL"]),
        ),
        volume_mounts=worker_volume_mounts,
    )

    pod_affinity = client.V1Affinity(
        pod_affinity=client.V1PodAffinity(
            required_during_scheduling_ignored_during_execution=[
                client.V1PodAffinityTerm(
                    label_selector=client.V1LabelSelector(
                        match_labels={"app.kubernetes.io/name": "agent-tasks-controller"}
                    ),
                    topology_key="kubernetes.io/hostname",
                )
            ]
        )
    )

    node_selector = {"kubernetes.io/hostname": settings.node_name} if settings.node_name else None

    pod_spec = client.V1PodSpec(
        restart_policy="Never",
        service_account_name=settings.worker_service_account,
        automount_service_account_token=False,
        image_pull_secrets=[client.V1LocalObjectReference(name=settings.image_pull_secret_name)],
        node_selector=node_selector,
        affinity=pod_affinity,
        security_context=client.V1PodSecurityContext(
            run_as_non_root=True,
            run_as_user=1000,
            run_as_group=1000,
            fs_group=1000,
            seccomp_profile=client.V1SeccompProfile(type="RuntimeDefault"),
        ),
        init_containers=[init_container],
        containers=[worker_container],
        volumes=[
            client.V1Volume(name="work", empty_dir=client.V1EmptyDirVolumeSource()),
            client.V1Volume(
                name="state",
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=settings.state_pvc_name
                ),
            ),
        ],
    )

    safe_issue_label = re.sub(r"[^a-zA-Z0-9_.-]+", "-", record.issue_id)[:63].strip("-._") or "task"
    labels = {
        "app.kubernetes.io/name": "agent-tasks-worker",
        "app.kubernetes.io/component": "agy-worker",
        "app.kubernetes.io/part-of": "agent-tasks",
        "agent-tasks.homelab/issue-id": safe_issue_label,
        "agent-tasks.homelab/task-kind": record.task_kind[:63],
        "agent-tasks.homelab/turn": str(record.turn),
    }

    return client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(
            name=job_name,
            namespace=settings.namespace,
            labels=labels,
        ),
        spec=client.V1JobSpec(
            backoff_limit=0,
            active_deadline_seconds=settings.job_active_deadline_seconds,
            ttl_seconds_after_finished=settings.job_ttl_seconds_after_finished,
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels=labels),
                spec=pod_spec,
            ),
        ),
    )


class K8sJobRunner:
    """Thin async wrapper around kubernetes.client.BatchV1Api in-cluster calls."""

    def __init__(self, settings: ControllerSettings) -> None:
        self.settings = settings
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        self._batch = client.BatchV1Api()

    async def spawn_job(
        self,
        record: IssueRecord,
        target: ProjectTargetConfig,
        *,
        worker_backend: str = "sdk",
    ) -> str:
        manifest = build_triage_job_manifest(
            settings=self.settings,
            record=record,
            target=target,
            worker_backend=worker_backend,
        )
        job_name = manifest.metadata.name

        def _create() -> None:
            try:
                self._batch.create_namespaced_job(
                    namespace=self.settings.namespace,
                    body=manifest,
                    _request_timeout=15,
                )
            except ApiException as exc:
                if exc.status != 409:
                    raise

        await asyncio.to_thread(_create)
        log.info(
            "k8s_job.spawned",
            job_name=job_name,
            issue_id=record.issue_id,
            turn=record.turn,
            worker_backend=worker_backend,
        )
        return job_name

    async def get_job_terminal_state(self, issue_id: str, turn: int) -> str | None:
        """Return 'succeeded', 'failed', or None if still running."""
        job_name = make_job_name(issue_id, turn)

        def _read() -> str | None:
            try:
                job = self._batch.read_namespaced_job_status(
                    name=job_name,
                    namespace=self.settings.namespace,
                    _request_timeout=15,
                )
            except ApiException as exc:
                if exc.status == 404:
                    return "failed"
                raise

            status = job.status
            if not status:
                return None
            if status.succeeded and status.succeeded > 0:
                return "succeeded"
            if status.failed and status.failed > 0:
                return "failed"
            for cond in status.conditions or []:
                if cond.type == "Complete" and cond.status == "True":
                    return "succeeded"
                if cond.type == "Failed" and cond.status == "True":
                    return "failed"
            return None

        return await asyncio.to_thread(_read)
