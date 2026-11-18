from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path
from typing import Any

import structlog

from agy_cloud.core.gateway import RecordingWorkerJobService, build_worker_init_script
from agy_cloud.sdk.spec import JobSpec, WorkerJobServiceProtocol, WorkerRunnerConfig

log = structlog.get_logger(__name__)


def make_k8s_job_name(task_id: str, turn: int) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", task_id.lower()).strip("-")[:36]
    return f"agy-worker-{slug}-t{turn}"


def build_k8s_worker_job_dict(
    spec: JobSpec,
    cfg: WorkerRunnerConfig,
) -> dict[str, Any]:
    """Build a locked-down Kubernetes `batch/v1` Job manifest dictionary for `spec`."""
    job_name = make_k8s_job_name(spec.task_id, spec.turn)
    mirror_subpath = f"mirrors/{spec.github_repo.replace('/', '__')}.git"
    session_subpath = f"sessions/{spec.task_id}"
    artifact_subpath = f"artifacts/{spec.task_id}"
    apply_initial = "1" if (spec.initial_patch and spec.initial_patch.strip()) else "0"

    labels = {
        "app.kubernetes.io/name": "agy-cloud-worker",
        "app.kubernetes.io/part-of": "agy-cloud",
        "agy.cloud/task-id": re.sub(r"[^a-zA-Z0-9_.-]+", "-", spec.task_id)[:63].strip("-._")
        or "task",
        "agy.cloud/plugin-id": re.sub(r"[^a-zA-Z0-9_.-]+", "-", spec.plugin_id)[:63].strip("-._")
        or "core",
        "agy.cloud/turn": str(spec.turn),
    }

    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "namespace": cfg.namespace,
            "labels": labels,
        },
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": 1800,
            "ttlSecondsAfterFinished": 3600,
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "restartPolicy": "Never",
                    "serviceAccountName": cfg.worker_service_account,
                    "automountServiceAccountToken": False,
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 1000,
                        "runAsGroup": 1000,
                        "fsGroup": 1000,
                    },
                    "initContainers": [
                        {
                            "name": "git-clone",
                            "image": cfg.worker_image,
                            "command": ["/bin/bash", "-lc", build_worker_init_script()],
                            "env": [
                                {"name": "CLONE_REF", "value": spec.base_branch},
                                {"name": "BASE_SHA", "value": spec.base_sha or ""},
                                {"name": "TURN", "value": str(spec.turn)},
                                {"name": "APPLY_INITIAL_PATCH", "value": apply_initial},
                            ],
                            "volumeMounts": [
                                {"name": "work", "mountPath": "/work"},
                                {
                                    "name": "state",
                                    "mountPath": "/mirror",
                                    "subPath": mirror_subpath,
                                    "readOnly": True,
                                },
                                {
                                    "name": "state",
                                    "mountPath": "/artifacts",
                                    "subPath": artifact_subpath,
                                    "readOnly": True,
                                },
                            ],
                        }
                    ],
                    "containers": [
                        {
                            "name": "agy-worker",
                            "image": cfg.worker_image,
                            "env": [
                                {"name": "TASK_ID", "value": spec.task_id},
                                {"name": "SHORT_ID", "value": spec.short_id},
                                {"name": "PLUGIN_ID", "value": spec.plugin_id},
                                {"name": "GITHUB_REPO", "value": spec.github_repo},
                                {"name": "WORKER_MODEL", "value": spec.model},
                                {"name": "TURN", "value": str(spec.turn)},
                                {"name": "CONVERSATION_ID", "value": spec.conversation_id or ""},
                                {"name": "FOCUS_SUBDIR", "value": spec.focus_subdir},
                                {"name": "VERIFY_HINT", "value": spec.verify_hint},
                                {"name": "WORK_DIR", "value": "/work"},
                                {"name": "SESSION_DIR", "value": "/session"},
                                {"name": "ARTIFACTS_DIR", "value": "/artifacts"},
                            ],
                            "envFrom": [{"secretRef": {"name": cfg.worker_secret_name}}],
                            "volumeMounts": [
                                {"name": "work", "mountPath": "/work"},
                                {
                                    "name": "state",
                                    "mountPath": "/session",
                                    "subPath": session_subpath,
                                },
                                {
                                    "name": "state",
                                    "mountPath": "/artifacts",
                                    "subPath": artifact_subpath,
                                },
                            ],
                        }
                    ],
                    "volumes": [
                        {"name": "work", "emptyDir": {}},
                        {
                            "name": "state",
                            "persistentVolumeClaim": {"claimName": cfg.state_pvc_name},
                        },
                    ],
                },
            },
        },
    }


class LocalSubprocessWorkerJobService(WorkerJobServiceProtocol):
    """Local dev worker runner executing jobs in an isolated temp git checkout without K8s."""

    def __init__(
        self,
        *,
        state_root: Path,
        cfg: WorkerRunnerConfig | None = None,
        repo_resolver: dict[str, Path] | None = None,
    ) -> None:
        self.state_root = state_root
        self.cfg = cfg or WorkerRunnerConfig(mode="local")
        self.repo_resolver = repo_resolver or {}
        self.spawned_jobs: list[JobSpec] = []

    def work_dir(self, task_id: str) -> Path:
        return self.state_root / "local_work" / task_id

    def artifact_dir(self, task_id: str) -> Path:
        path = self.state_root / "artifacts" / task_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    async def spawn_agent_job(self, spec: JobSpec) -> str:
        self.spawned_jobs.append(spec)
        wdir = self.work_dir(spec.task_id)
        art_dir = self.artifact_dir(spec.task_id)
        (art_dir / "task_prompt.md").write_text(spec.prompt, encoding="utf-8")

        # Resolve source git repo from local override map, mirror directory, or git path/URL
        source_repo: str | Path | None = self.repo_resolver.get(spec.github_repo)
        if source_repo is None:
            mirror_candidate = (
                self.state_root / "mirrors" / f"{spec.github_repo.replace('/', '__')}.git"
            )
            if mirror_candidate.exists():
                source_repo = mirror_candidate
            elif Path(spec.github_repo).exists():
                source_repo = Path(spec.github_repo)

        if source_repo is not None and not (wdir / ".git").exists():
            wdir.parent.mkdir(parents=True, exist_ok=True)
            subprocess.run(
                ["git", "clone", str(source_repo), str(wdir)],
                check=True,
                capture_output=True,
                text=True,
            )
            if spec.base_sha:
                subprocess.run(
                    ["git", "-C", str(wdir), "checkout", spec.base_sha],
                    check=False,
                    capture_output=True,
                    text=True,
                )
            if spec.initial_patch and spec.initial_patch.strip():
                patch_file = art_dir / "initial.patch"
                patch_file.write_text(spec.initial_patch, encoding="utf-8")
                subprocess.run(
                    ["git", "-C", str(wdir), "apply", str(patch_file)],
                    check=True,
                    capture_output=True,
                    text=True,
                )

        if self.cfg.local_command and wdir.exists():
            env = {
                **os.environ,
                "TASK_ID": spec.task_id,
                "SHORT_ID": spec.short_id,
                "PLUGIN_ID": spec.plugin_id,
                "GITHUB_REPO": spec.github_repo,
                "WORK_DIR": str(wdir),
                "ARTIFACTS_DIR": str(art_dir),
                "WORKER_MODEL": spec.model,
            }
            subprocess.run(
                self.cfg.local_command,
                cwd=str(wdir),
                env=env,
                check=True,
                capture_output=True,
                text=True,
            )
            if (wdir / ".git").exists():
                subprocess.run(
                    ["git", "-C", str(wdir), "add", "-N", "."],
                    check=False,
                    capture_output=True,
                )
                diff_out = subprocess.run(
                    ["git", "-C", str(wdir), "diff", "--binary", "HEAD"],
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout
                (art_dir / "fix.patch").write_text(diff_out, encoding="utf-8")

        return f"local-job-{spec.task_id}-t{spec.turn}"


def create_worker_job_service(
    cfg: WorkerRunnerConfig,
    *,
    state_root: Path,
) -> WorkerJobServiceProtocol:
    mode = cfg.mode.strip().lower()
    if mode == "local":
        return LocalSubprocessWorkerJobService(state_root=state_root, cfg=cfg)
    return RecordingWorkerJobService()
