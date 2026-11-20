"""请求/响应模型。"""
from __future__ import annotations

from pydantic import BaseModel, Field


class EventIn(BaseModel):
    window_name: str
    biz_key: str
    event_ts: str
    amount: float = Field(gt=-1e12)


class WindowCreateIn(BaseModel):
    name: str
    open_at: str
    close_at: str


class HoldIn(BaseModel):
    window_name: str
    biz_key: str
    event_ts: str
    amount: float


class ReopenIn(BaseModel):
    mode: str = "作废重算"  # 或「增量补丁」——题面择一后落地，母仓仅空桩登记


class MaterializeIn(BaseModel):
    # 分区键仅用于兼容旧单分区入口；留空则按 biz_key 首字符全分区物化。
    partition_key: str | None = None
    # 正确性口径：缺省走分区物化册钉死的「整窗回滚」，传乙路径会被拒写。
    path: str | None = None
    # 夹具：模拟分片失败；fail_simulate=True 时指定哪个分区炸，留空炸首个。
    fail_simulate: bool = False
    fail_partition: str | None = None
