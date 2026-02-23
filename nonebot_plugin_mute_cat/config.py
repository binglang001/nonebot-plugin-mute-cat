from pydantic import BaseModel, Field

class Config(BaseModel):
    """禁言猫猫插件配置"""

    mute_default_minutes: int = Field(default=5, description="默认禁言时长（分钟）")
    mute_self_options: list[int] = Field(
        default=[1, 3, 5, 0],
        description="「禁我」随机时长选项（分钟），0 表示本次不禁言",
    )
    mute_at_required: bool = Field(
        default=True,
        description="全局默认：是否需要 @ 机器人才能触发命令",
    )
    mute_superuser_only: bool = Field(
        default=False,
        description="是否仅超级管理员可用禁言管理命令",
    )