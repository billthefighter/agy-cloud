from __future__ import annotations

from typing import Protocol


class LongLivedTask(Protocol):
    """Common lifecycle protocol for long-lived background loops in the `agent-tasks` controller."""

    name: str

    async def run(self) -> None: ...
