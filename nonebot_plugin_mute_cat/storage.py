"""持久化存储模块（基于 nonebot-plugin-localstore）— The Betterest Mute Cat"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from nonebot.log import logger

import nonebot_plugin_localstore as store


class MuteStorage:
    """禁言数据持久化管理器"""

    def __init__(self) -> None:
        self._at_file = store.get_plugin_data_file("at_overrides.json")
        self._states_file = store.get_plugin_data_file("group_states.json")

    # ========== @ 开关覆盖 ==========

    def load_at_overrides(self) -> dict[int, bool]:
        try:
            if not self._at_file.exists():
                return {}
            raw = json.loads(self._at_file.read_text(encoding="utf-8"))
            return {int(k): v for k, v in raw.items()}
        except Exception as e:
            logger.opt(exception=e).error("加载 @ 开关数据失败")
            return {}

    def save_at_overrides(self, data: dict[int, bool]) -> None:
        try:
            serializable = {str(k): v for k, v in data.items()}
            self._at_file.write_text(
                json.dumps(serializable, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            logger.opt(exception=e).error("保存 @ 开关数据失败")

    # ========== 群禁言状态 ==========

    def load_states(self) -> dict[int, dict[str, Any]]:
        try:
            if not self._states_file.exists():
                return {}
            raw = json.loads(self._states_file.read_text(encoding="utf-8"))
            return self._deserialize_states(raw)
        except Exception as e:
            logger.opt(exception=e).error("加载群状态失败")
            return {}

    def save_states(self, data: dict[int, dict[str, Any]]) -> None:
        try:
            serializable = self._serialize_states(data)
            self._states_file.write_text(
                json.dumps(serializable, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            logger.opt(exception=e).error("保存群状态失败")

    # ========== 内部：序列化 ==========

    @staticmethod
    def _serialize_states(data: dict[int, dict[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for group_id, state in data.items():
            wm = state["whole_mute"]
            result[str(group_id)] = {
                "whole_mute": {
                    "enabled": wm["enabled"],
                    "end_time": wm["end_time"].isoformat() if wm["end_time"] else None,
                    "duration": wm["duration"],
                },
                "individual_mutes": {
                    str(uid): {
                        "end_time": info["end_time"].isoformat() if info["end_time"] else None,
                        "duration": info["duration"],
                    }
                    for uid, info in state["individual_mutes"].items()
                },
            }
        return result

    @staticmethod
    def _deserialize_states(raw: dict[str, Any]) -> dict[int, dict[str, Any]]:
        result: dict[int, dict[str, Any]] = {}
        for gid_str, state in raw.items():
            wm = state.get("whole_mute", {})

            wm_end: Optional[datetime] = None
            if wm.get("end_time"):
                try:
                    wm_end = datetime.fromisoformat(wm["end_time"])
                except (ValueError, TypeError):
                    pass

            individual: dict[int, dict[str, Any]] = {}
            for uid_str, info in state.get("individual_mutes", {}).items():
                u_end: Optional[datetime] = None
                if info.get("end_time"):
                    try:
                        u_end = datetime.fromisoformat(info["end_time"])
                    except (ValueError, TypeError):
                        pass
                individual[int(uid_str)] = {
                    "end_time": u_end,
                    "duration": info.get("duration", 0),
                }

            result[int(gid_str)] = {
                "whole_mute": {
                    "enabled": wm.get("enabled", False),
                    "end_time": wm_end,
                    "duration": wm.get("duration"),
                },
                "individual_mutes": individual,
                "tasks": {},
            }
        return result