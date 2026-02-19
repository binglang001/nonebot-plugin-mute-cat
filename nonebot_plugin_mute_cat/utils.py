"""工具函数模块 — The Betterest Mute Cat"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from nonebot.adapters.onebot.v11 import Message, MessageSegment

BEIJING_TZ = ZoneInfo("Asia/Shanghai")


# ==================== 消息解析 ====================


def parse_at_targets(message: Message) -> list[int]:
    """从消息中解析所有被 @ 的用户 ID"""
    return [
        int(seg.data["qq"])
        for seg in message
        if seg.type == "at" and "qq" in seg.data
    ]


def build_at_message(user_ids: list[int]) -> Message:
    """构建 @ 多个用户的消息"""
    msg = Message()
    for i, uid in enumerate(user_ids):
        if i > 0:
            msg += " "
        msg += MessageSegment.at(uid)
    return msg


# ==================== 时间解析 ====================


def parse_duration(text: str) -> Optional[int]:
    """解析持续时间文本，返回分钟数。

    支持格式:
      - 纯数字 → 分钟（"5" → 5）
      - 数字+单位（"30s" "5分钟" "1小时" "2h"）
    """
    if not text or not isinstance(text, str):
        return None

    text = text.strip()
    if not text:
        return None

    if text.isdigit():
        return int(text)

    unit_map: list[tuple[str, float]] = [
        ("sec", 1 / 60),
        ("秒钟", 1 / 60),
        ("秒", 1 / 60),
        ("分钟", 1),
        ("min", 1),
        ("分", 1),
        ("小时", 60),
        ("hour", 60),
        ("时", 60),
        ("h", 60),
        ("s", 1 / 60),
        ("m", 1),
    ]

    for unit, multiplier in unit_map:
        if len(unit) == 1:
            pattern = rf"(\d+)\s*{re.escape(unit)}(?![a-zA-Z\u4e00-\u9fff])"
        else:
            pattern = rf"(\d+)\s*{re.escape(unit)}"
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            raw = int(match.group(1)) * multiplier
            return max(1, round(raw))

    number_match = re.search(r"(\d+)", text)
    if number_match:
        return int(number_match.group(1))

    return None


def parse_time_range(text: str) -> Optional[tuple[datetime, int]]:
    """解析定时禁言的时间范围，返回 (开始时间, 时长分钟)。

    支持格式:
      - "12:00~13:00" / "12:00-13:00"
      - "12:00 13:00"
      - "12:00 30分钟"
      - "12:00"（返回 duration=0，由调用方使用默认时长）
    """
    if not text:
        return None

    now = datetime.now(BEIJING_TZ)

    def _make_time(h: int, m: int) -> datetime:
        t = now.replace(hour=h, minute=m, second=0, microsecond=0)
        if t <= now:
            t += timedelta(days=1)
        return t

    range_match = re.search(
        r"(\d{1,2})[:：](\d{2})\s*[~\-]\s*(\d{1,2})[:：](\d{2})", text
    )
    if range_match:
        sh, sm, eh, em = map(int, range_match.groups())
        start = _make_time(sh, sm)
        end = now.replace(hour=eh, minute=em, second=0, microsecond=0)
        if end <= start:
            end += timedelta(days=1)
        return start, max(1, int((end - start).total_seconds() / 60))

    time_pair = re.findall(r"(\d{1,2})[:：](\d{2})", text)
    if len(time_pair) == 2:
        sh, sm = int(time_pair[0][0]), int(time_pair[0][1])
        eh, em = int(time_pair[1][0]), int(time_pair[1][1])
        start = _make_time(sh, sm)
        end = now.replace(hour=eh, minute=em, second=0, microsecond=0)
        if end <= start:
            end += timedelta(days=1)
        return start, max(1, int((end - start).total_seconds() / 60))

    td_match = re.search(r"(\d{1,2})[:：](\d{2})\s+(.+)", text)
    if td_match:
        h, m = int(td_match.group(1)), int(td_match.group(2))
        dur = parse_duration(td_match.group(3).strip())
        if dur:
            return _make_time(h, m), dur

    only_match = re.match(r"^(\d{1,2})[:：](\d{2})$", text.strip())
    if only_match:
        h, m = int(only_match.group(1)), int(only_match.group(2))
        return _make_time(h, m), 0

    return None


# ==================== 格式化显示 ====================


def format_remaining_time(end_time: datetime, now: Optional[datetime] = None) -> str:
    """将结束时间格式化为可读的剩余时间"""
    if now is None:
        now = datetime.now(BEIJING_TZ)
    secs = max(0, int((end_time - now).total_seconds()))

    if secs < 60:
        return f"{secs}秒"
    if secs < 3600:
        m, s = divmod(secs, 60)
        return f"{m}分{s}秒" if s else f"{m}分钟"
    h, rest = divmod(secs, 3600)
    m = rest // 60
    return f"{h}小时{m}分钟" if m else f"{h}小时"


def format_duration_display(minutes: int) -> str:
    """将分钟数格式化为可读时长"""
    if minutes < 60:
        return f"{minutes}分钟"
    h, m = divmod(minutes, 60)
    return f"{h}小时{m}分钟" if m else f"{h}小时"


def format_time_display(dt: datetime) -> str:
    """格式化时刻（HH:MM）"""
    return dt.strftime("%H:%M")


# ==================== 命令识别 ====================


def is_mute_command(text: str) -> bool:
    return text.startswith("禁言")


def is_cancel_command(text: str) -> bool:
    return any(text.startswith(kw) for kw in ("取消", "解除", "解禁"))


def is_self_mute_command(text: str) -> bool:
    return text.strip() == "禁我"


def is_whole_unmute_command(text: str) -> bool:
    """判断是否为解禁全员命令"""
    text_clean = text.replace(" ", "")
    patterns = [
        "解禁全员", "解禁全体", "解禁所有人",
        "取消全员", "取消全体", "取消所有人", "取消全员禁言", "取消始终禁言",
        "解除全员", "解除全体", "解除所有人", "解除全员禁言", "解除始终禁言",
        "关闭全员禁言", "关闭全体禁言", "关闭始终禁言",
        "全员解禁", "全体解禁", "所有人解禁",
        "全员取消", "全体取消", "所有人取消",
        "全员解除", "全体解除", "所有人解除",
        "全员禁言取消", "全员禁言解除", "全员禁言关闭",
        "全体禁言取消", "全体禁言解除", "全体禁言关闭",
        "始终禁言取消", "始终禁言解除", "始终禁言关闭",
    ]
    return text_clean in patterns


def is_whole_mute_command(text: str) -> bool:
    """判断是否为全员禁言命令（解禁类命令不匹配）"""
    if is_whole_unmute_command(text):
        return False
    text_clean = text.replace(" ", "")
    prefixes = ("全员禁言", "全体禁言", "全员", "全体", "始终禁言")
    return any(text_clean.startswith(p) for p in prefixes)


def is_help_command(text: str) -> bool:
    return text.strip() in ("帮助", "help", "菜单")


def is_status_command(text: str) -> bool:
    return "查看状态" in text


def is_at_toggle_command(text: str) -> Optional[bool]:
    """判断是否为 @ 开关命令。

    Returns:
        True  → 开启 @ 模式
        False → 关闭 @ 模式
        None  → 不是 @ 开关命令
    """
    if "开启at" in text or "开启@" in text:
        return True
    if "关闭at" in text or "关闭@" in text:
        return False
    return None


def extract_whole_mute_duration(text: str) -> Optional[int]:
    """从全员禁言命令中提取时长，返回正整数分钟数或 None（表示永久）"""
    remaining = text
    for prefix in ("全员禁言", "全体禁言", "全员", "全体"):
        if remaining.startswith(prefix):
            remaining = remaining[len(prefix):].strip()
            break

    if not remaining:
        return None

    dur = parse_duration(remaining)
    return dur if dur and dur > 0 else None


def extract_mute_remaining_text(raw_msg: str, message: Message) -> str:
    """从禁言命令消息中剥离 @ 段和「禁言」前缀，提取纯时间参数文本"""
    parts: list[str] = []
    skipped_keyword = False
    for seg in message:
        if seg.type == "at":
            continue
        if seg.type == "text":
            t = seg.data.get("text", "")
            if not skipped_keyword:
                idx = t.find("禁言")
                if idx != -1:
                    t = t[idx + 2:]
                    skipped_keyword = True
            parts.append(t)
    return " ".join(parts).strip()