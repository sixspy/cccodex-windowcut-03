"""窗内事件与物化结果互证（读数同源：分区合计走 partitions 单一事实源）。"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from auth import AuthHeader, require_user
from db import connect
from partitions import aggregate, partition_rows

router = APIRouter(tags=["reconcile"])


def _reconcile_window(conn, w) -> dict:
    wid = w["id"]
    # 窗内事件侧：与物化写路径完全同一条分区聚合 SQL。
    event_parts = partition_rows(conn, wid)
    ev_count, ev_amount = aggregate(event_parts)

    # 台账侧：取最新一次 run 的全部分片行；无 run 则回退看最新单行（旧数据）。
    latest_run = conn.execute(
        "SELECT run_id FROM materializations WHERE window_id=? ORDER BY id DESC LIMIT 1",
        (wid,),
    ).fetchone()
    mat_rows = []
    if latest_run is not None:
        mat_rows = conn.execute(
            "SELECT * FROM materializations WHERE window_id=? AND run_id=? ORDER BY partition_key",
            (wid, latest_run["run_id"]),
        ).fetchall()
    mat_by_key = {r["partition_key"]: dict(r) for r in mat_rows}

    partitions = []
    for ep in event_parts:
        key = ep["partition_key"]
        m = mat_by_key.get(key)
        partitions.append(
            {
                "partition_key": key,
                "row_count": ep["row_count"],
                "total_amount": ep["total_amount"],
                "materialized_row_count": m["row_count"] if m else 0,
                "materialized_amount": m["total_amount"] if m else 0.0,
                "aligned": bool(
                    m
                    and m["row_count"] == ep["row_count"]
                    and abs(m["total_amount"] - ep["total_amount"]) < 1e-6
                ),
            }
        )

    mat_count, mat_amount = aggregate(
        [
            {"row_count": r["row_count"], "total_amount": r["total_amount"]}
            for r in mat_rows
        ]
    )
    # 整窗对齐：每个事件分区都有同键分片，且笔数/金额全等，缺一分区即假绿破灭。
    aligned = bool(mat_rows) and len(mat_by_key) == len(event_parts) and (
        mat_count == ev_count and abs(mat_amount - ev_amount) < 1e-6
    )

    return {
        "event_partitions": event_parts,
        "partitions": partitions,
        "mat_count": mat_count,
        "mat_amount": mat_amount,
        "ev_count": ev_count,
        "ev_amount": ev_amount,
        "aligned": aligned,
        "latest_run": latest_run["run_id"] if latest_run else None,
    }


@router.get("/api/reconcile/{wid}")
def reconcile(wid: int, authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        w = conn.execute("SELECT * FROM windows WHERE id=?", (wid,)).fetchone()
        if not w:
            raise HTTPException(404, "窗口不存在")
        ev = conn.execute(
            """SELECT COUNT(*) c, COALESCE(SUM(amount),0) s
               FROM events WHERE window_id=? AND disposition='窗内'""",
            (wid,),
        ).fetchone()
        latest = conn.execute(
            "SELECT * FROM materializations WHERE window_id=? ORDER BY id DESC LIMIT 1",
            (wid,),
        ).fetchone()
        late = conn.execute(
            "SELECT COUNT(*) c FROM late_bucket WHERE window_name=?",
            (w["name"],),
        ).fetchone()["c"]
        rejects = conn.execute(
            "SELECT COUNT(*) c FROM reject_slips WHERE window_name=?",
            (w["name"],),
        ).fetchone()["c"]
        rec = _reconcile_window(conn, w)
        return {
            "window": dict(w),
            "window_events": ev["c"],
            "window_amount": ev["s"],
            "materialized": dict(latest) if latest else None,
            "run_id": rec["latest_run"],
            "partitions": rec["partitions"],
            "late_bucket": late,
            "rejects": rejects,
            "aligned": rec["aligned"],
        }


@router.get("/api/reconcile")
def reconcile_all(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        windows = conn.execute("SELECT id,name,status FROM windows ORDER BY id").fetchall()
        items = []
        for w in windows:
            ev = conn.execute(
                """SELECT COUNT(*) c, COALESCE(SUM(amount),0) s
                   FROM events WHERE window_id=? AND disposition='窗内'""",
                (w["id"],),
            ).fetchone()
            late = conn.execute(
                "SELECT COUNT(*) c FROM late_bucket WHERE window_name=?",
                (w["name"],),
            ).fetchone()["c"]
            rec = _reconcile_window(conn, w)
            items.append(
                {
                    "window_id": w["id"],
                    "name": w["name"],
                    "status": w["status"],
                    "aligned": rec["aligned"],
                    "window_events": ev["c"],
                    "window_amount": ev["s"],
                    "mat_row_count": rec["mat_count"],
                    "mat_amount": rec["mat_amount"],
                    "partitions": rec["partitions"],
                    "late_bucket": late,
                }
            )
        return {"items": items}
