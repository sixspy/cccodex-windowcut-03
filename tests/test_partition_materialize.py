"""分区物化命门：整窗回滚（甲路径）与同窗物化锁争用。

夹具在本地 SQLite 上造「半截分区物化假绿」：一个已关窗内有 A/B 两个分区
（biz_key 首字符即分区键），指定 B 分片模拟失败。

两条红测先于一切写路径改动落地：
  1. test_partition_failure_rolls_back_whole_window —— 任一分片失败，本窗
     物化行与分区标记同事务回滚，台账零增、无失败凭证、对账不拿假绿。
  2. test_concurrent_materialize_same_window_lock_contention —— 两路同时
     物化同一 window_id 须占物化锁，后到只记争用，不写业务。
"""
from __future__ import annotations

import threading

import pytest
from fastapi.testclient import TestClient

from app import app, init_db
from db import connect

LOCK_KEY_TMPL = "materialize:{}"


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("db.DB_PATH", tmp_path / "partition.db")
    init_db()
    yield


def _auth(c: TestClient) -> dict:
    tok = c.post(
        "/api/login", json={"username": "admin", "password": "123456"}
    ).json()["token"]
    return {"Authorization": "Bearer " + tok}


def _make_closed_partitioned_window() -> int:
    """造一个已关窗，窗内事件按 biz_key 首字符切成 A/B 两个分区。

    A 分区 2 笔合计 30，B 分区 2 笔合计 70，整窗 4 笔合计 100。
    """
    c = TestClient(app)
    h = _auth(c)
    r = c.post(
        "/api/windows",
        headers=h,
        json={
            "name": "W-PART",
            "open_at": "2026-11-10T00:00:00",
            "close_at": "2026-11-10T23:59:59",
        },
    )
    wid = r.json()["id"]
    events = [
        ("A-1", "2026-11-10T09:00:00", 10.0),
        ("A-2", "2026-11-10T10:00:00", 20.0),
        ("B-1", "2026-11-10T11:00:00", 30.0),
        ("B-2", "2026-11-10T12:00:00", 40.0),
    ]
    with connect() as conn:
        for biz_key, ts, amount in events:
            conn.execute(
                """INSERT INTO events(window_id,biz_key,event_ts,amount,disposition,created_at)
                   VALUES (?,?,?,?, '窗内', ?)""",
                (wid, biz_key, ts, amount, ts),
            )
        conn.commit()
    assert c.post(f"/api/windows/{wid}/close", headers=h).status_code == 200
    return wid


def _mat_count(wid: int) -> int:
    with connect() as conn:
        return conn.execute(
            "SELECT COUNT(*) c FROM materializations WHERE window_id=?", (wid,)
        ).fetchone()["c"]


def _partition_keys(wid: int) -> set[str]:
    with connect() as conn:
        rows = conn.execute(
            "SELECT DISTINCT partition_key FROM materializations WHERE window_id=?",
            (wid,),
        ).fetchall()
    return {r["partition_key"] for r in rows}


def test_partition_failure_rolls_back_whole_window():
    """红测①：B 分片失败 → 整窗物化行与分区标记同事务回滚，台账零增。"""
    wid = _make_closed_partitioned_window()
    assert _mat_count(wid) == 0  # 夹具起点：无任何物化痕迹

    c = TestClient(app)
    h = _auth(c)
    r = c.post(
        f"/api/windows/{wid}/materialize",
        headers=h,
        json={"fail_simulate": True, "fail_partition": "B"},
    )
    # 分片失败是明确的业务失败，不是 200 假绿
    assert r.status_code == 500

    # 台账零增：成功行不存在，也不允许留下 status=失败 的半截凭证
    assert _mat_count(wid) == 0
    assert _partition_keys(wid) == set()

    # 窗内事件原样保留（回滚只回物化，不动事件）
    with connect() as conn:
        ev = conn.execute(
            """SELECT COUNT(*) c, COALESCE(SUM(amount),0) s
               FROM events WHERE window_id=? AND disposition='窗内'""",
            (wid,),
        ).fetchone()
    assert (ev["c"], ev["s"]) == (4, 100.0)

    # 对账不得拿半截窗内合计装对齐；无成功物化行即 aligned=False
    recon = c.get(f"/api/reconcile/{wid}", headers=h).json()
    assert recon["aligned"] is False
    assert recon["materialized"] is None
    # 同态：事件分区仍可下钻，但每个分区在台账侧都没有任何物化痕迹
    assert {p["partition_key"] for p in recon["partitions"]} == {"A", "B"}
    assert all(p["aligned"] is False for p in recon["partitions"])
    assert all(p["materialized_row_count"] == 0 for p in recon["partitions"])
    assert all(p["materialized_amount"] == 0.0 for p in recon["partitions"])

    # 审计：失败与整窗回滚须留痕，但同事务内的成功行/分区标记已回滚
    audits = c.get("/api/audit?limit=50", headers=h).json()["items"]
    actions = [a["action"] for a in audits]
    assert "materialize_fail_rollback" in actions
    assert "materialize_ok" not in actions


def test_partition_success_all_shards_same_run_and_aligned():
    """对照测：全分片成功 → 同一 run_id 多行、按分区键可对账且整窗对齐。"""
    wid = _make_closed_partitioned_window()
    c = TestClient(app)
    h = _auth(c)
    r = c.post(f"/api/windows/{wid}/materialize", headers=h, json={})
    assert r.status_code == 200
    body = r.json()
    assert body["row_count"] == 4
    assert body["total_amount"] == 100.0
    assert sorted(body["partitions"], key=lambda p: p["partition_key"]) == [
        {"partition_key": "A", "row_count": 2, "total_amount": 30.0},
        {"partition_key": "B", "row_count": 2, "total_amount": 70.0},
    ]

    with connect() as conn:
        rows = conn.execute(
            """SELECT run_id, partition_key, row_count, total_amount, status
               FROM materializations WHERE window_id=? ORDER BY partition_key""",
            (wid,),
        ).fetchall()
    assert len(rows) == 2
    assert {r["run_id"] for r in rows} == {r0_run_id := rows[0]["run_id"]}
    assert all(r["status"] == "成功" for r in rows)
    assert [(r["partition_key"], r["row_count"], r["total_amount"]) for r in rows] == [
        ("A", 2, 30.0),
        ("B", 2, 70.0),
    ]

    recon = c.get(f"/api/reconcile/{wid}", headers=h).json()
    assert recon["aligned"] is True
    by_key = {p["partition_key"]: p for p in recon["partitions"]}
    assert set(by_key) == {"A", "B"}
    for key, cnt, amt in (("A", 2, 30.0), ("B", 2, 70.0)):
        assert by_key[key]["row_count"] == cnt
        assert by_key[key]["total_amount"] == amt
        assert by_key[key]["aligned"] is True


def test_registry_pins_path_and_rejects_drift():
    """分区物化册钉死甲路径；当天改口走乙路径拒写并记「分区偏了」。"""
    wid = _make_closed_partitioned_window()
    c = TestClient(app)
    h = _auth(c)

    reg = c.get("/api/materializations/registry", headers=h).json()
    assert reg["path"] == "整窗回滚"
    assert reg["frozen"] is True

    # 乙路径（保留成功分区）与册冲突：拒写，台账零增
    r = c.post(
        f"/api/windows/{wid}/materialize",
        headers=h,
        json={"path": "保留成功分区"},
    )
    assert r.status_code == 409
    assert _mat_count(wid) == 0

    audits = c.get("/api/audit?limit=50", headers=h).json()["items"]
    drift = [a for a in audits if a["action"] == "partition_drift_rejected"]
    assert drift and drift[0]["target"] == str(wid)

    # 再以甲路径正常物化，证明只是偏离被拒、正路不封
    assert c.post(
        f"/api/windows/{wid}/materialize", headers=h, json={"path": "整窗回滚"}
    ).status_code == 200
    assert _mat_count(wid) == 2


def _run_concurrent_materialize(monkeypatch, user_a, password_a, user_b, password_b):
    """两路同时物化同一窗；返回 (状态码字典, 窗口 id, 锁键)。"""
    wid = _make_closed_partitioned_window()
    lock_key = LOCK_KEY_TMPL.format(wid)

    # 测试缝：让先到者占住物化锁并停在事务中段，后到者此刻真实撞锁。
    locked = threading.Event()
    release_barrier = threading.Event()

    def concurrency_hook(phase: str, _wid: int) -> None:
        if phase == "locked":
            locked.set()
            release_barrier.wait(timeout=10)

    import materialize_svc

    monkeypatch.setattr(
        materialize_svc, "materialize_concurrency_hook", concurrency_hook
    )

    outcomes: dict[str, int] = {}

    def runner(name: str, username: str, password: str) -> None:
        cc = TestClient(app)
        tok = cc.post(
            "/api/login", json={"username": username, "password": password}
        ).json()["token"]
        hh = {"Authorization": "Bearer " + tok}
        rr = cc.post(f"/api/windows/{wid}/materialize", headers=hh, json={})
        outcomes[name] = rr.status_code

    t1 = threading.Thread(target=runner, args=("first", user_a, password_a))
    t2 = threading.Thread(target=runner, args=("second", user_b, password_b))
    t1.start()
    assert locked.wait(timeout=10), "先到者未占住物化锁"
    t2.start()
    t2.join(timeout=10)
    release_barrier.set()
    t1.join(timeout=10)
    assert not t1.is_alive() and not t2.is_alive()
    return outcomes, wid, lock_key


def _assert_single_run_and_clean_lock(wid: int, lock_key: str) -> None:
    c = TestClient(app)
    h = _auth(c)
    contention = c.get("/api/contention", headers=h).json()["items"]
    hits = [x for x in contention if x["lock_key"] == lock_key]
    assert len(hits) == 1
    assert hits[0]["action"] == "争用"

    # 业务只写一次：两个分区同属一个 run，无重复台账
    with connect() as conn:
        rows = conn.execute(
            "SELECT run_id, partition_key FROM materializations WHERE window_id=?",
            (wid,),
        ).fetchall()
    assert len(rows) == 2
    assert len({r["run_id"] for r in rows}) == 1

    with connect() as conn:
        held = conn.execute(
            "SELECT COUNT(*) c FROM window_locks WHERE lock_key=?", (lock_key,)
        ).fetchone()["c"]
    assert held == 0

    recon = c.get(f"/api/reconcile/{wid}", headers=h).json()
    assert recon["aligned"] is True
    assert sum(p["row_count"] for p in recon["partitions"]) == 4


def test_concurrent_materialize_same_window_lock_contention(monkeypatch):
    """红测②：两路同时物化同一 window_id，后到只记争用、不写业务。"""
    outcomes, wid, lock_key = _run_concurrent_materialize(
        monkeypatch, "admin", "123456", "closer", "close123"
    )
    assert outcomes == {"first": 200, "second": 409}
    _assert_single_run_and_clean_lock(wid, lock_key)


def test_same_user_double_click_materialize_still_contends(monkeypatch):
    """同一用户双击也是两路并发：持锁者带每请求令牌，后到必须撞 409 而非重入。"""
    outcomes, wid, lock_key = _run_concurrent_materialize(
        monkeypatch, "admin", "123456", "admin", "123456"
    )
    assert outcomes == {"first": 200, "second": 409}
    _assert_single_run_and_clean_lock(wid, lock_key)
