"""关窗后分区物化。

正确性口径由分区物化册钉死为「整窗回滚」（甲路径）：任一分片失败，本窗
全部物化行与分区标记同事务回滚，台账零增、不留失败凭证。乙路径（保留成功
分区）在册但禁写，改口请求拒写并记 partition_drift_rejected（分区偏了）。

同一 window_id 的两路物化须占物化锁；持锁者带每请求令牌（即便是同一用户
双击也不允许重入），锁与物化册均先于业务事务独立提交，后到者真实撞锁，
只记争用、不写业务。
"""
from __future__ import annotations

import uuid
from datetime import date, datetime

from fastapi import APIRouter, HTTPException

from auth import AuthHeader, require_user
from db import connect, write_audit
from locks import release, try_acquire
from models import MaterializeIn
from partitions import (
    CANONICAL_PATH,
    aggregate,
    lock_key_for,
    partition_key_of,
    partition_rows,
)

router = APIRouter(tags=["materialize"])


def materialize_concurrency_hook(phase: str, wid: int) -> None:
    """生产空实现；测试用 monkeypatch 换入屏障，造真实同窗并发争用。"""


def _today() -> str:
    return date.today().isoformat()


def _ensure_registry(conn, actor: str) -> dict:
    """取当天物化册；无则落册（整窗回滚，当天禁止改口）。

    册行是策略事实而非业务数据：INSERT OR IGNORE 幂等，调用方须以独立短
    事务提交，不随分片失败回滚。
    """
    day = _today()
    conn.execute(
        """INSERT OR IGNORE INTO materialize_registry(policy_date,path,frozen,actor,created_at)
           VALUES (?,?,1,?,?)""",
        (day, CANONICAL_PATH, actor, datetime.now().isoformat(timespec="seconds")),
    )
    return dict(
        conn.execute(
            "SELECT * FROM materialize_registry WHERE policy_date=?", (day,)
        ).fetchone()
    )


@router.post("/api/windows/{wid}/materialize")
def materialize(
    wid: int,
    authorization: AuthHeader = None,
    body: MaterializeIn = MaterializeIn(),
):
    user = require_user(authorization)
    lock_key = lock_key_for(wid)
    # 每请求唯一持锁者：同一用户的两路并发也不构成重入，后到必撞争用。
    holder = f"{user}:{uuid.uuid4().hex[:8]}"

    # 第一段：占物化锁并独立提交，使后到连接能真实看到持锁行 → 只记争用。
    with connect() as lock_conn:
        if not try_acquire(lock_conn, lock_key, holder):
            write_audit(
                lock_conn,
                user,
                "materialize_lock_contention",
                str(wid),
                {"lock_key": lock_key, "holder": holder},
            )
            lock_conn.commit()
            raise HTTPException(409, "争用：后到只记争用，不写业务")
        lock_conn.commit()

    # 锁行已独立提交并对其他连接可见：屏障停在这里时，后到者会真实撞锁。
    materialize_concurrency_hook("locked", wid)

    # 物化册独立落册提交，不进业务回滚面；并发两路靠 INSERT OR IGNORE 幂等。
    with connect() as reg_conn:
        registry = _ensure_registry(reg_conn, user)
        reg_conn.commit()

    # 第二段：分区物化业务事务；任一分片失败整窗回滚，锁在 finally 释放。
    conn = connect()
    try:
        # 偏离物化册：乙路径与在册甲路径冲突，拒写并记「分区偏了」。
        requested = body.path
        if requested is not None and requested != registry["path"]:
            conn.rollback()
            with connect() as audit_conn:
                write_audit(
                    audit_conn,
                    user,
                    "partition_drift_rejected",
                    str(wid),
                    {
                        "requested": requested,
                        "registry_path": registry["path"],
                        "policy_date": registry["policy_date"],
                    },
                )
                audit_conn.commit()
            raise HTTPException(
                409,
                f"分区物化册当天钉死「{registry['path']}」，"
                f"改口「{requested}」拒写",
            )

        w = conn.execute("SELECT * FROM windows WHERE id=?", (wid,)).fetchone()
        if not w:
            raise HTTPException(404, "窗口不存在")
        if w["status"] != "已关闭":
            raise HTTPException(409, "未关窗不能物化")

        # 读数同源：分区合计与对账共用 partitions.PARTITION_AGG_SQL。
        parts = partition_rows(conn, wid)
        if body.partition_key:
            wanted = partition_key_of(body.partition_key)
            parts = [p for p in parts if p["partition_key"] == wanted]

        run_id = f"run-{uuid.uuid4().hex[:12]}"
        now = datetime.now().isoformat(timespec="seconds")
        fail_partition = body.fail_partition or (
            parts[0]["partition_key"] if parts else None
        )

        for part in parts:
            # 夹具：指定分片（缺省首个）炸 —— 触发整窗回滚，不留半截。
            if body.fail_simulate and part["partition_key"] == fail_partition:
                conn.rollback()  # 本事务已写的成功分片/分区标记一并撤销
                with connect() as audit_conn:
                    write_audit(
                        audit_conn,
                        user,
                        "materialize_fail_rollback",
                        str(wid),
                        {
                            "run_id": run_id,
                            "failed_partition": fail_partition,
                            "path": CANONICAL_PATH,
                            "note": "任一分片失败，整窗物化行与分区标记同事务回滚，台账零增",
                        },
                    )
                    audit_conn.commit()
                raise HTTPException(500, "分区物化失败：整窗已回滚（甲路径）")

            conn.execute(
                """INSERT INTO materializations
                   (window_id,run_id,status,row_count,total_amount,partition_key,created_at)
                   VALUES (?,?, '成功', ?,?,?,?)""",
                (
                    wid,
                    run_id,
                    part["row_count"],
                    part["total_amount"],
                    part["partition_key"],
                    now,
                ),
            )

        total_count, total_amount = aggregate(parts)
        write_audit(
            conn,
            user,
            "materialize_ok",
            str(wid),
            {
                "run_id": run_id,
                "path": CANONICAL_PATH,
                "partitions": parts,
                "row_count": total_count,
                "total_amount": total_amount,
            },
        )
        conn.commit()
        return {
            "run_id": run_id,
            "path": CANONICAL_PATH,
            "row_count": total_count,
            "total_amount": total_amount,
            "partitions": parts,
        }
    finally:
        with connect() as cleanup:
            release(cleanup, lock_key, holder)
            cleanup.commit()
        conn.close()


@router.get("/api/materializations/registry")
def get_registry(authorization: AuthHeader = None):
    """分区物化册：当天口径只读，整窗回滚钉死、禁止改口。"""
    user = require_user(authorization)
    with connect() as conn:
        registry = _ensure_registry(conn, user)
        conn.commit()
        return {
            "policy_date": registry["policy_date"],
            "path": registry["path"],
            "frozen": bool(registry["frozen"]),
            "actor": registry["actor"],
            "created_at": registry["created_at"],
        }


@router.get("/api/materializations")
def list_mats(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            """SELECT m.*, w.name AS window_name
               FROM materializations m
               JOIN windows w ON w.id=m.window_id
               ORDER BY m.id DESC"""
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.post("/api/materializations/partition-stub")
def partition_stub(authorization: AuthHeader = None):
    """兼容旧入口：分区物化已按甲路径落地，空桩转为在册口径说明。"""
    user = require_user(authorization)
    with connect() as conn:
        registry = _ensure_registry(conn, user)
        write_audit(
            conn,
            user,
            "partition_mat_registry_lookup",
            None,
            {
                "paths": ["整窗回滚", "保留成功分区"],
                "active": registry["path"],
                "note": "甲路径已落地；乙路径在册禁写，偏离拒写",
            },
        )
        conn.commit()
    return {
        "ok": True,
        "stub": False,
        "module": "partition_mat",
        "path": registry["path"],
    }
