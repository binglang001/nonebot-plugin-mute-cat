"""
nonebot-plugin-mute-cat — The Betterest Mute Cat 🐱

功能强大的 QQ 群禁言插件，支持定时禁言、全员禁言、解禁全员、禁我等功能，
带有可爱的猫娘语气回复。
"""

from __future__ import annotations

import random
import time
from datetime import datetime, timedelta
from typing import Any, Optional

import nonebot
from nonebot import get_driver, get_plugin_config, on_message, require
from nonebot.adapters.onebot.v11 import (
    Bot,
    GroupMessageEvent,
    Message,
    MessageSegment,
)
from nonebot.log import logger
from nonebot.matcher import Matcher
from nonebot.plugin import PluginMetadata
from nonebot.rule import Rule

from .config import Config

# ==================== 依赖声明 ====================
require("nonebot_plugin_apscheduler")
require("nonebot_plugin_localstore")

from nonebot_plugin_apscheduler import scheduler  # noqa: E402

from .storage import MuteStorage  # noqa: E402
from .utils import (  # noqa: E402
    BEIJING_TZ,
    build_at_message,
    extract_mute_remaining_text,
    extract_whole_mute_duration,
    format_duration_display,
    format_remaining_time,
    format_time_display,
    is_at_toggle_command,
    is_cancel_command,
    is_help_command,
    is_mute_command,
    is_self_mute_command,
    is_status_command,
    is_whole_mute_command,
    is_whole_unmute_command,
    parse_at_targets,
    parse_duration,
    parse_time_range,
)

# ==================== 插件元数据 ====================
__plugin_meta__ = PluginMetadata(
    name="The Betterest Mute Cat",
    description="极致的禁言猫猫 — 功能强大的 QQ 群禁言插件，支持定时禁言、全员禁言、解禁全员、禁我等功能",
    usage=(
        "📖 The Betterest Mute Cat 来教你用禁言功能喵~\n"
        "═══════════════════\n"
        "▎禁言别人\n"
        "  禁言 @猫猫 [时长]\n"
        "  支持：5、5分钟、1小时、30s\n\n"
        "▎取消禁言\n"
        "  取消/解除/解禁 @猫猫\n\n"
        "▎全员禁言\n"
        "  全员禁言 [时长]\n"
        "  不加时长就是永久禁言\n\n"
        "▎解禁全员\n"
        "  解禁全员 / 取消全员 / 解除全员\n\n"
        "▎禁自己\n"
        "  禁我（随机 1/3/5 分钟或不禁言）\n\n"
        "▎定时禁言\n"
        "  禁言 @猫猫 14:30 15:30\n"
        "  禁言 @猫猫 14:30 1小时\n\n"
        "▎查看状态 / 帮助\n"
        "  查看状态 / 帮助"
    ),
    type="application",
    homepage="https://github.com/binglang001/nonebot-plugin-mute-cat",
    config=Config,
    supported_adapters={"~onebot.v11"},
    extra={
        "author": "binglang",
        "version": "1.2.4",
    },
)

# ==================== 配置与存储 ====================
plugin_config = get_plugin_config(Config)
driver = get_driver()
storage = MuteStorage()

group_states: dict[int, dict[str, Any]] = {}
at_overrides: dict[int, bool] = {}


# ==================== 生命周期 ====================


@driver.on_startup
async def _load_data() -> None:
    global group_states, at_overrides
    group_states = storage.load_states()
    at_overrides = storage.load_at_overrides()
    _cleanup_expired_mutes()
    _cleanup_orphan_tasks()
    logger.opt(colors=True).success(
        f"<green>🐱 The Betterest Mute Cat 加载完成</green> | "
        f"群状态: {len(group_states)} | @ 覆盖: {len(at_overrides)}"
    )


def _cleanup_expired_mutes() -> None:
    """清理所有群中已过期的个人禁言记录"""
    now = datetime.now(BEIJING_TZ)
    cleaned = 0
    for state in group_states.values():
        expired = [
            uid for uid, info in state["individual_mutes"].items()
            if info["end_time"] and info["end_time"] <= now
        ]
        for uid in expired:
            del state["individual_mutes"][uid]
            cleaned += 1
    if cleaned:
        storage.save_states(group_states)
        logger.opt(colors=True).info(f"<cyan>🧹 清理了 {cleaned} 条过期禁言记录</cyan>")


def _cleanup_orphan_tasks() -> None:
    """清理 APScheduler 中已不存在的孤儿任务记录（通常由重启导致）"""
    cleaned = 0
    for state in group_states.values():
        for job_id in list(state["tasks"]):
            if scheduler.get_job(job_id) is None:
                del state["tasks"][job_id]
                cleaned += 1
    if cleaned:
        storage.save_states(group_states)
        logger.opt(colors=True).info(f"<cyan>🧹 清理了 {cleaned} 条孤儿任务记录</cyan>")


# ==================== 权限与规则 ====================


async def _check_at_required(event: GroupMessageEvent) -> bool:
    """检查当前群是否满足 @ 触发要求"""
    need_at = at_overrides.get(event.group_id, plugin_config.mute_at_required)
    return not need_at or event.is_tome()


def _is_mute_related(event: GroupMessageEvent) -> bool:
    """判断消息是否为禁言相关命令"""
    text = event.get_plaintext().strip()
    return (
        is_mute_command(text)
        or is_cancel_command(text)
        or is_self_mute_command(text)
        or is_whole_mute_command(text)
        or is_whole_unmute_command(text)
        or is_help_command(text)
        or is_status_command(text)
        or (is_at_toggle_command(text) is not None)
    )


mute_rule = Rule(
    lambda event: isinstance(event, GroupMessageEvent),
    _check_at_required,
    _is_mute_related,
)

at_toggle_rule = Rule(
    lambda event: (
        isinstance(event, GroupMessageEvent)
        and is_at_toggle_command(event.get_plaintext().strip()) is not None
    ),
    lambda event: isinstance(event, GroupMessageEvent) and event.is_tome(),
)

# ==================== 响应器 ====================

at_toggle_matcher = on_message(rule=at_toggle_rule, priority=4, block=True)
mute_matcher = on_message(rule=mute_rule, priority=5, block=True)


# ==================== 辅助函数 ====================


async def is_group_admin(bot: Bot, group_id: int, user_id: int) -> bool:
    """检查用户是否为群管理员或群主"""
    try:
        info = await bot.get_group_member_info(
            group_id=group_id, user_id=user_id, no_cache=True
        )
        return info.get("role", "member") in ("owner", "admin")
    except Exception as e:
        logger.opt(exception=e).warning(f"获取群成员 {user_id} 信息失败")
        return False


def is_superuser(user_id: str) -> bool:
    return user_id in driver.config.superusers


async def check_admin_permission(bot: Bot, group_id: int, user_id: str) -> bool:
    """检查用户是否有执行管理命令的权限"""
    if plugin_config.mute_superuser_only:
        return is_superuser(user_id)
    return is_superuser(user_id) or await is_group_admin(bot, group_id, int(user_id))


def init_group_state(group_id: int) -> None:
    if group_id not in group_states:
        group_states[group_id] = {
            "whole_mute": {"enabled": False, "end_time": None, "duration": 0},
            "individual_mutes": {},
            "tasks": {},
        }


def generate_job_id(group_id: int, prefix: str = "mute") -> str:
    """生成唯一的定时任务 ID，毫秒时间戳加随机后缀避免碰撞"""
    ts = int(time.time() * 1000)
    rnd = random.randint(0, 9999)
    return f"{prefix}_{group_id}_{ts}_{rnd:04d}"


def cancel_tasks_for_target(group_id: int, target_key: str) -> bool:
    if group_id not in group_states:
        return False
    cancelled = False
    for job_id in list(group_states[group_id]["tasks"]):
        if group_states[group_id]["tasks"][job_id].get("target_key") == target_key:
            try:
                scheduler.remove_job(job_id)
            except Exception:
                pass
            del group_states[group_id]["tasks"][job_id]
            cancelled = True
    return cancelled


def cancel_tasks_containing_user(group_id: int, user_id: int) -> bool:
    if group_id not in group_states:
        return False
    cancelled = False
    for job_id in list(group_states[group_id]["tasks"]):
        if user_id in group_states[group_id]["tasks"][job_id].get("targets", []):
            try:
                scheduler.remove_job(job_id)
            except Exception:
                pass
            del group_states[group_id]["tasks"][job_id]
            cancelled = True
    return cancelled


def cancel_all_tasks(group_id: int) -> bool:
    if group_id not in group_states:
        return False
    cancelled = False
    for job_id in list(group_states[group_id]["tasks"]):
        try:
            scheduler.remove_job(job_id)
        except Exception:
            pass
        del group_states[group_id]["tasks"][job_id]
        cancelled = True
    return cancelled


# ==================== 状态管理 ====================


def update_mute_state(group_id: int, user_id: int, duration: int) -> None:
    init_group_state(group_id)
    group_states[group_id]["individual_mutes"][user_id] = {
        "end_time": datetime.now(BEIJING_TZ) + timedelta(minutes=duration),
        "duration": duration,
    }
    storage.save_states(group_states)


def update_whole_mute_state(
    group_id: int, enabled: bool, duration: Optional[int] = None
) -> None:
    init_group_state(group_id)
    wm = group_states[group_id]["whole_mute"]
    wm["enabled"] = enabled
    if enabled:
        if duration is None:
            wm["end_time"] = None
            wm["duration"] = None
        else:
            wm["end_time"] = datetime.now(BEIJING_TZ) + timedelta(minutes=duration)
            wm["duration"] = duration
    else:
        wm["end_time"] = None
        wm["duration"] = 0
    storage.save_states(group_states)


def remove_mute_state(group_id: int, user_id: int) -> None:
    if group_id in group_states:
        group_states[group_id]["individual_mutes"].pop(user_id, None)
        storage.save_states(group_states)


def add_task_record(
    group_id: int,
    job_id: str,
    task_type: str,
    targets: list[int],
    execute_time: datetime,
    duration: int = 0,
    target_key: str = "",
) -> None:
    init_group_state(group_id)
    group_states[group_id]["tasks"][job_id] = {
        "type": task_type,
        "targets": targets,
        "time": execute_time,
        "duration": duration,
        "target_key": target_key,
    }


def remove_task_record(group_id: int, job_id: str) -> None:
    if group_id in group_states:
        group_states[group_id]["tasks"].pop(job_id, None)


# ==================== 定时任务包装器 ====================
# APScheduler 不持久化 job，重启后 job 会丢失。
# 这里用 bot_id（str）代替直接传 Bot 实例，执行时通过 nonebot.get_bot() 重新获取，
# 避免 Bot 断线重连后实例失效导致的错误。


async def _sched_mute(
    bot_id: str, group_id: int, user_ids: list[int], duration: int, job_id: str
) -> None:
    try:
        bot = nonebot.get_bot(bot_id)
    except KeyError:
        logger.warning(f"Bot {bot_id} 当前不在线，跳过定时禁言任务 {job_id}")
        remove_task_record(group_id, job_id)
        return
    await execute_mute(bot, group_id, user_ids, duration, job_id)


async def _sched_whole_mute(
    bot_id: str, group_id: int, duration: Optional[int], job_id: str
) -> None:
    try:
        bot = nonebot.get_bot(bot_id)
    except KeyError:
        logger.warning(f"Bot {bot_id} 当前不在线，跳过定时全员禁言任务 {job_id}")
        remove_task_record(group_id, job_id)
        return
    await execute_whole_mute(bot, group_id, duration, job_id)


async def _sched_whole_unmute(bot_id: str, group_id: int, job_id: str) -> None:
    try:
        bot = nonebot.get_bot(bot_id)
    except KeyError:
        logger.warning(f"Bot {bot_id} 当前不在线，跳过定时解禁任务 {job_id}")
        remove_task_record(group_id, job_id)
        return
    await execute_whole_unmute(bot, group_id, job_id)


# ==================== 执行函数 ====================


async def execute_mute(
    bot: Bot,
    group_id: int,
    user_ids: list[int],
    duration: int,
    job_id: Optional[str] = None,
) -> None:
    """执行禁言操作，处理管理员冲突和各类错误"""
    try:
        success_users: list[int] = []
        admin_users: list[int] = []
        fail_list: list[str] = []

        for uid in user_ids:
            try:
                if await is_group_admin(bot, group_id, uid):
                    admin_users.append(uid)
                    continue
                await bot.set_group_ban(
                    group_id=group_id, user_id=uid, duration=duration * 60
                )
                success_users.append(uid)
                update_mute_state(group_id, uid, duration)
            except Exception as e:
                emsg = str(e)
                if "1200" in emsg or "1287" in emsg:
                    fail_list.append(f"{uid}(人家不是管理喵~)")
                elif "1202" in emsg:
                    fail_list.append(f"{uid}(TA已不在群里了喵~)")
                elif "1203" in emsg:
                    fail_list.append(f"{uid}(人家不能禁言群主哦~)")
                elif "1204" in emsg:
                    fail_list.append(f"{uid}(人家不能禁言管理喵~)")
                else:
                    fail_list.append(f"{uid}({emsg})")

        msg = Message()
        if success_users:
            end_time = datetime.now(BEIJING_TZ) + timedelta(minutes=duration)
            msg += "呜...禁言了 "
            msg += build_at_message(success_users)
            msg += f" {format_duration_display(duration)} 啢~\n"
            msg += f"* {format_time_display(end_time)} 才能说话哦 *"
        if admin_users:
            msg += ("\n⚠️ " if success_users else "⚠️ ")
            msg += "人家不能禁言群管理哦~ "
            msg += build_at_message(admin_users)
        if fail_list:
            msg += "哎呀呀...人家禁言失败了喵~"
            msg += f"\n❌ {', '.join(fail_list)}"
        if not success_users and not admin_users and not fail_list:
            msg += "❌ 没有可以禁言的目标喵~"

        await bot.send_group_msg(group_id=group_id, message=msg)
    except Exception as e:
        logger.opt(exception=e).error("禁言执行失败")
    finally:
        if job_id:
            remove_task_record(group_id, job_id)


async def execute_whole_mute(
    bot: Bot,
    group_id: int,
    duration: Optional[int] = None,
    job_id: Optional[str] = None,
) -> None:
    """执行全员禁言，支持永久模式和定时模式"""
    try:
        await bot.set_group_whole_ban(group_id=group_id, enable=True)
        update_whole_mute_state(group_id, True, duration)

        if not duration or duration <= 0:
            await bot.send_group_msg(
                group_id=group_id,
                message="⌈全员禁言⌋ 开始啦~ 大家要一直安静到天荒地老喵~",
            )
        else:
            end_time = datetime.now(BEIJING_TZ) + timedelta(minutes=duration)
            unmute_job = generate_job_id(group_id, "whole_unmute")
            scheduler.add_job(
                func=_sched_whole_unmute,
                trigger="date",
                run_date=end_time,
                args=[str(bot.self_id), group_id, unmute_job],
                id=unmute_job,
                replace_existing=True,
            )
            add_task_record(
                group_id, unmute_job, "whole_unmute", [],
                end_time, 0, f"whole_unmute_{group_id}",
            )
            await bot.send_group_msg(
                group_id=group_id,
                message=(
                    f"⌈全员禁言⌋ {format_duration_display(duration)} 哦~\n"
                    f"* {format_time_display(end_time)} 才能说话喵 *"
                ),
            )
    except Exception as e:
        emsg = str(e)
        tip = "权限不够啢，人家不是管理员哦~" if ("1200" in emsg or "1287" in emsg) else emsg
        await bot.send_group_msg(group_id=group_id, message=f"❌ 全员禁言失败：{tip}")
    finally:
        if job_id:
            remove_task_record(group_id, job_id)


async def execute_whole_unmute(
    bot: Bot, group_id: int, job_id: Optional[str] = None
) -> None:
    """解除全员禁言（定时任务到期时调用）"""
    try:
        if group_id in group_states and not group_states[group_id]["whole_mute"]["enabled"]:
            return  # 已被手动取消，无需操作
        await bot.set_group_whole_ban(group_id=group_id, enable=False)
        update_whole_mute_state(group_id, False)
        await bot.send_group_msg(
            group_id=group_id,
            message="⌈全员禁言⌋ 结束啦~ 大家可以说话咯",
        )
    except Exception as e:
        logger.opt(exception=e).error("解除全员禁言失败")
    finally:
        if job_id:
            remove_task_record(group_id, job_id)


async def execute_cancel(
    bot: Bot, group_id: int, user_ids: list[int], is_whole: bool = False
) -> None:
    """执行取消禁言（个人或全员）"""
    try:
        if is_whole:
            await _cancel_whole(bot, group_id)
        else:
            await _cancel_individual(bot, group_id, user_ids)
    except Exception as e:
        logger.opt(exception=e).error("取消操作失败")
        await bot.send_group_msg(group_id=group_id, message=f"❌ 操作失败了喵...{e}")


async def _cancel_whole(bot: Bot, group_id: int) -> None:
    """解禁全员，同时解除所有个人禁言并清除该群全部定时任务"""
    init_group_state(group_id)
    state = group_states[group_id]
    was_whole = state["whole_mute"]["enabled"]

    success_users: list[int] = []
    fail_list: list[str] = []
    for uid in list(state["individual_mutes"]):
        try:
            await bot.set_group_ban(group_id=group_id, user_id=uid, duration=0)
            success_users.append(uid)
            remove_mute_state(group_id, uid)
        except Exception as e:
            emsg = str(e)
            if "1200" in emsg or "1287" in emsg:
                fail_list.append(f"{uid}(人家不是管理喵~)")
            elif "1202" in emsg:
                fail_list.append(f"{uid}(TA已不在群里了喵~)")
            elif "1203" in emsg:
                fail_list.append(f"{uid}(人家不能禁言群主哦~)")
            elif "1204" in emsg:
                fail_list.append(f"{uid}(人家不能禁言管理喵~)")
            else:
                fail_list.append(f"{uid}({emsg})")

    whole_closed = False
    if was_whole:
        try:
            await bot.set_group_whole_ban(group_id=group_id, enable=False)
            update_whole_mute_state(group_id, False)
            whole_closed = True
        except Exception as e:
            logger.opt(exception=e).warning("关闭全员禁言失败")

    cancel_all_tasks(group_id)

    msg = Message()
    if success_users:
        msg += "解除了 "
        msg += build_at_message(success_users)
        msg += "\n现在可以说话了喵~ "
        if whole_closed:
            msg += "\n⌈全员禁言⌋ 也关掉啦~"
    elif whole_closed:
        msg += "⌈全员禁言⌋ 关掉啦~"
    elif not was_whole:
        msg += "诶？现在本来就没有全员禁言啢"
    else:
        msg += "现在没有成员被禁言喵~"

    if fail_list:
        msg += f"\n❌ {', '.join(fail_list)}"

    await bot.send_group_msg(group_id=group_id, message=msg)


async def _cancel_individual(bot: Bot, group_id: int, user_ids: list[int]) -> None:
    """解除指定用户的禁言并取消其相关定时任务"""
    success_users: list[int] = []
    fail_list: list[str] = []

    for uid in user_ids:
        try:
            await bot.set_group_ban(group_id=group_id, user_id=uid, duration=0)
            success_users.append(uid)
            remove_mute_state(group_id, uid)
            cancel_tasks_containing_user(group_id, uid)
        except Exception as e:
            emsg = str(e)
            if "1200" in emsg or "1287" in emsg:
                fail_list.append(f"{uid}(人家不是管理喵~)")
            elif "1202" in emsg:
                fail_list.append(f"{uid}(TA已不在群里了喵~)")
            else:
                fail_list.append(f"{uid}({emsg})")

    msg = Message()
    if success_users:
        msg += build_at_message(success_users)
        msg += " 可以说话啦~"
    if fail_list:
        msg += f"\n❌ {', '.join(fail_list)}"
    if not success_users and not fail_list:
        msg += "❌ 没有需要解禁的对象喵~"

    await bot.send_group_msg(group_id=group_id, message=msg)


async def execute_self_mute(bot: Bot, group_id: int, user_id: int) -> None:
    """执行「禁我」功能，从配置的选项中随机抽取时长"""
    try:
        if await is_group_admin(bot, group_id, user_id):
            await bot.send_group_msg(
                group_id=group_id,
                message="你是群管理啢，人家不能禁言你喵~",
            )
            return

        duration = random.choice(plugin_config.mute_self_options)
        if duration <= 0:
            await bot.send_group_msg(
                group_id=group_id,
                message="今天心情好，就不禁言你了喵~",
            )
            return

        await bot.set_group_ban(
            group_id=group_id, user_id=user_id, duration=duration * 60
        )
        update_mute_state(group_id, user_id, duration)
        end_time = datetime.now(BEIJING_TZ) + timedelta(minutes=duration)

        msg = Message()
        msg += "满足你喵~\n禁言 "
        msg += build_at_message([user_id])
        msg += f" {format_duration_display(duration)}\n"
        msg += f"* {format_time_display(end_time)} 结束 *"
        await bot.send_group_msg(group_id=group_id, message=msg)

    except Exception as e:
        emsg = str(e)
        if "1200" in emsg:
            tip = "权限不够啢"
        elif "1202" in emsg:
            tip = "找不到这个人哦"
        else:
            tip = emsg
        await bot.send_group_msg(group_id=group_id, message=f"❌ 禁言失败：{tip}")


# ==================== 处理器 ====================


@at_toggle_matcher.handle()
async def handle_at_toggle(bot: Bot, event: GroupMessageEvent, matcher: Matcher) -> None:
    """处理 @ 开关命令（始终需要 @ 机器人触发）"""
    text = event.get_plaintext().strip()
    group_id = event.group_id
    user_id = str(event.get_user_id())

    if not await check_admin_permission(bot, group_id, user_id):
        await matcher.finish("只有管理员才能设置 @ 开关喵~")

    toggle = is_at_toggle_command(text)
    if toggle is True:
        at_overrides[group_id] = True
        storage.save_at_overrides(at_overrides)
        await matcher.finish("✅ 好啦~ 以后要 @ 人家才能用命令喵~")
    elif toggle is False:
        at_overrides[group_id] = False
        storage.save_at_overrides(at_overrides)
        await matcher.finish("✅ 知道啦~ 现在可以直接用命令不用 @ 人家咯")


@mute_matcher.handle()
async def handle_mute(bot: Bot, event: GroupMessageEvent, matcher: Matcher) -> None:
    """主消息处理器，按命令类型分发执行"""
    raw_msg = event.get_plaintext().strip()
    group_id = event.group_id
    user_id = str(event.get_user_id())
    message = event.message

    # ---------- 帮助 ----------
    if is_help_command(raw_msg):
        at_status = (
            "需要 @ 人家"
            if at_overrides.get(group_id, plugin_config.mute_at_required)
            else "不需要 @ 人家"
        )
        await matcher.finish(
            "📖 The Betterest Mute Cat 来教你用禁言功能喵~\n"
            "═══════════════════\n"
            f"📌 当前模式：{at_status}\n\n"
            "▎@ 开关（必须 @ 人家）\n"
            "  @我 开启at / @我 关闭at\n\n"
            "▎禁言别人\n"
            "  禁言 @人 [时长]\n"
            "  时长示例：5、5分钟、1小时、30s\n\n"
            "▎取消禁言\n"
            "  取消/解除/解禁 @人\n\n"
            "▎全员禁言\n"
            "  全员禁言 [时长]\n\n"
            "▎解禁全员\n"
            "  解禁全员 / 取消全员 / 解除全员\n"
            "  （同时解除所有个人禁言）\n\n"
            "▎禁自己\n"
            "  禁我（随机 1/3/5 分钟或不禁言）\n\n"
            "▎定时禁言\n"
            "  禁言 @人 14:30 15:30\n"
            "  禁言 @人 14:30 1小时\n\n"
            "▎查看状态\n"
            "  查看状态"
        )

    # ---------- 查看状态 ----------
    if is_status_command(raw_msg):
        await _handle_status(bot, matcher, group_id)
        return

    # ---------- 禁我（无需管理员权限） ----------
    if is_self_mute_command(raw_msg):
        await execute_self_mute(bot, group_id, int(user_id))
        return

    # ========== 以下命令需要管理员权限 ==========
    if not await check_admin_permission(bot, group_id, user_id):
        return

    # ---------- 全员解禁 ----------
    if is_whole_unmute_command(raw_msg):
        await execute_cancel(bot, group_id, [], is_whole=True)
        return

    # ---------- 全员禁言 ----------
    if is_whole_mute_command(raw_msg):
        duration = extract_whole_mute_duration(raw_msg)
        cancel_tasks_for_target(group_id, f"whole_mute_{group_id}")
        await execute_whole_mute(bot, group_id, duration)
        return

    # ---------- 取消禁言 ----------
    if is_cancel_command(raw_msg):
        targets = parse_at_targets(message)
        if not targets:
            await matcher.finish("要 @ 你想解禁的人喵~")
        await execute_cancel(bot, group_id, targets)
        return

    # ---------- 禁言 ----------
    if is_mute_command(raw_msg):
        targets = parse_at_targets(message)
        if not targets:
            await matcher.finish("要 @ 你想禁言的人喵~")

        remaining = extract_mute_remaining_text(raw_msg, message)

        time_range = parse_time_range(remaining)
        if time_range:
            start_time, dur = time_range
            if dur == 0:
                dur = plugin_config.mute_default_minutes

            job_id = generate_job_id(group_id, "mute")
            scheduler.add_job(
                func=_sched_mute,
                trigger="date",
                run_date=start_time,
                args=[str(bot.self_id), group_id, targets, dur, job_id],
                id=job_id,
                replace_existing=True,
            )
            target_key = f"mute_{'_'.join(map(str, sorted(targets)))}"
            add_task_record(group_id, job_id, "mute", targets, start_time, dur, target_key)

            end_time = start_time + timedelta(minutes=dur)
            msg = Message()
            msg += f"人家记住了~ 在 {format_time_display(start_time)} 禁言 "
            msg += build_at_message(targets)
            msg += f" {format_duration_display(dur)} 喵~\n"
            msg += f"* {format_time_display(end_time)} 就能说话啦 *"
            await matcher.finish(msg)
        else:
            dur = parse_duration(remaining)
            if dur is None:
                dur = plugin_config.mute_default_minutes
            await execute_mute(bot, group_id, targets, dur)
        return


async def _handle_status(bot: Bot, matcher: Matcher, group_id: int) -> None:
    """查看状态子处理"""
    init_group_state(group_id)
    state = group_states[group_id]
    now = datetime.now(BEIJING_TZ)
    at_status = (
        "需要 @ 人家"
        if at_overrides.get(group_id, plugin_config.mute_at_required)
        else "不需要 @ 人家"
    )

    msg = Message()
    msg += "The Betterest Mute Cat 群状态喵~\n═══════════════════\n"
    msg += f"📌 触发模式：{at_status}\n"

    wm = state["whole_mute"]
    if wm["enabled"]:
        if wm["end_time"] and wm["end_time"] > now:
            msg += f"💥 全员禁言中，还有 {format_remaining_time(wm['end_time'], now)} 就能说话啦~\n"
        else:
            msg += "💥 全员禁言中（永久...）\n"
    else:
        msg += "💥 全员禁言：没有开哦\n"

    muted = {
        uid: info
        for uid, info in state["individual_mutes"].items()
        if info["end_time"] and info["end_time"] > now
    }
    if muted:
        msg += "\n💤 被禁言的可怜孩子：\n"
        for uid, info in muted.items():
            msg += "  "
            msg += MessageSegment.at(uid)
            msg += f" 还要等 {format_remaining_time(info['end_time'], now)}\n"
    else:
        msg += "\n💤 没有被禁言的可怜孩子啢\n"

    active_tasks = {
        jid: t for jid, t in state["tasks"].items()
        if t.get("time") and t["time"] > now
    }
    if active_tasks:
        msg += "\n⏰ 定时任务：\n"
        for jid, t in active_tasks.items():
            time_str = format_time_display(t["time"])
            if t["type"] == "mute":
                msg += f"  • {time_str} 禁言 "
                msg += build_at_message(t["targets"])
                msg += "\n"
            elif t["type"] == "whole_mute":
                msg += f"  • {time_str} 全员禁言\n"
            elif t["type"] == "whole_unmute":
                msg += f"  • {time_str} 解除全员禁言\n"
    else:
        msg += "\n⏰ 定时任务：没有喵~\n"

    await matcher.finish(msg)