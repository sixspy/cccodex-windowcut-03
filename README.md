# 业务窗关账

> 遵守 `ops/底座生成前必读_效率与技术栈.md`（**≥3× 厚度**）。  
> FastAPI + Vue 3 + SQLite；交付须 `docker compose up -d --build` 一键起。  
> **禁止同构换皮**：本仓主链路、表结构、接口与页面均独立。

## 骨架一句话

事件按业务窗裁切，迟到进旁路或拒收，关窗后物化结果并与窗口对账。

## 基本信息

| 项 | 值 |
|----|-----|
| 产品名（首页） | 业务窗关账 |
| 目录 | `08-windowcut` |
| 接口 | http://127.0.0.1:11013 |
| 库 | `data/app.db` |
| 账号 | admin / 123456 |

## 厚度（相对薄壳 ≥3×）

| 项 | 目标 |
|----|------|
| 业务 LOC | ≥900（目标 1000+） |
| 后端具名模块 | `db` / `auth` / `locks` / `windows_svc` / `ingest_svc` / `late_svc` / `materialize_svc` / `partitions` / `reconcile_svc` / `audit_svc` / `hold_svc` / `reopen_svc` |
| 前端视图 | 业务窗 / 入账 / 迟到桶 / 物化 / 对账 / 审计争用 |
| 空桩 | hold 晋升、迟到补入、窗重开 |

## 分区物化命门（已落地）

正确性口径由分区物化册 `materialize_registry` 按天钉死为 **甲路径·整窗回滚**，
当天禁止改口；乙路径（保留成功分区）在册但禁写，改口请求返回 409 并记
`partition_drift_rejected`（分区偏了）。

- **甲路径**：按 `biz_key` 首字符切分区，任一分片失败则本窗全部物化行与分区
  标记同事务回滚，`materializations` 台账零增、不留失败凭证；失败留痕走
  `materialize_fail_rollback` 审计（独立事务，回滚之后写）。
- **物化锁**：同一 `window_id` 的两路物化争用 `materialize:{wid}` 锁，持锁者
  带每请求令牌（同一用户双击也不重入）；锁先于业务事务提交，后到者真实撞锁，
  只记争用（`contention_log` + `materialize_lock_contention`），不写业务。
- **五处同态、读数同源**：物化台账、分区键、窗内事件、对账互证、审计的分区
  合计统一走 `partitions.PARTITION_AGG_SQL`；对账按分区键下钻，缺任一成功分片
  即判不对齐，半截窗内合计无法装绿。同一批分片共享 `run_id`。
- 红测先行：`tests/test_partition_materialize.py` 先造半截分区假绿夹具与同窗
  争用两条红测，再改 `materialize_svc` / `reconcile_svc` / `windows_svc` 写路径。

## 为何能出多题

开窗关窗、迟到策略、窗重开、部分物化失败回滚、旁路暂存 vs 拒收、争用锁与审计。

## 互斥切口候选

- 迟到拒收 vs 进迟到桶
- 物化失败整窗回滚（**已选定落地**）vs 保留成功分区（在册禁写）
- 窗重开作废重算 vs 增量补丁合并
- 切版旁路暂存 vs 开始瞬间拒收

## 启动（一键）

```powershell
cd projects/08-windowcut
docker compose up -d --build
```

浏览器：http://127.0.0.1:11013/  
健康检查：GET http://127.0.0.1:11013/api/health

本机开发：

```powershell
python -m pip install -r requirements.txt
python -m uvicorn app:app --app-dir backend --host 127.0.0.1 --port 10605
python -m pytest -q
```
