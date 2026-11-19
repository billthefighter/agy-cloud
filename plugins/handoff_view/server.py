from __future__ import annotations

from agy_cloud.sdk import AgyCloudPlugin, DefaultPluginConfig


class HandoffViewPlugin(AgyCloudPlugin[DefaultPluginConfig]):
    """Declarative PWA view plugin for visualizing and steering `/handoff` sessions."""

    config_model = DefaultPluginConfig
