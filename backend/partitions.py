"""分区物化的单一事实源。

口径钉死（甲路径，见 materialize_registry）：任一分片失败，本窗全部物化行
与分区标记同事务回滚，台账零增、不留失败凭证。物化写路径与对账读路径都只
能从本模块取分区规则与合计 SQL，保证「物化台账、分区键、窗内事件、对账互
证、审计」五处同态、读数同源。
"""
from __future__ import annotations

# 分区物化册（当天禁止改口）：只允许整窗回滚，保留成功分区为在册禁路。
CANONICAL_PATH = "整窗回滚"
FORBIDDEN_PATH = "保留成功分区"
REGISTRY_PATHS = (CANONICAL_PATH, FORBIDDEN_PATH)


def partition_key_of(biz_key: str | None) -> str:
    """分区键：业务键首字符大写；空键归入 _。"""
    if not biz_key:
        return "_"
    return biz_key[0].upper()


# 窗内事件按分区键聚合的统一查询（写路径与对账共用，禁止各写一份）。
PARTITION_AGG_SQL = """
SELECT
  UPPER(SUBSTR(COALESCE(biz_key, '_'), 1, 1)) AS partition_key,
  COUNT(*) AS row_count,
  COALESCE(SUM(amount), 0) AS total_amount
FROM events
WHERE window_id = ? AND disposition = '窗内'
GROUP BY partition_key
ORDER BY partition_key
"""


def partition_rows(conn, wid: int) -> list[dict]:
    """返回某窗窗内事件的分区合计，物化与对账同一份读数。"""
    return [dict(r) for r in conn.execute(PARTITION_AGG_SQL, (wid,)).fetchall()]


def aggregate(parts: list[dict]) -> tuple[int, float]:
    return sum(p["row_count"] for p in parts), float(
        sum(p["total_amount"] for p in parts)
    )


def lock_key_for(wid: int) -> str:
    """同一 window_id 的物化锁键，全仓唯一来源。"""
    return f"materialize:{wid}"
