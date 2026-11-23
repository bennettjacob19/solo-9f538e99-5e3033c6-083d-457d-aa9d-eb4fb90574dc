# Shuffle Handoff Drill（执行器下线 Shuffle 文件本地交接演练）

用 FastAPI 模拟 MapReduce 场景：Reduce 正在读取某执行器的 Shuffle（词频）文件时，
该执行器要下线。服务需要在**复制字节预算内**把它的分片交接给另一台执行器，
最多允许从原文重算一个分片；交接与归并并发执行、互不等待，且每个分片在任何
时刻都必须被**恰好计入一次**。

## 1. 目录与文件系统布局

```
/data
├── originals/            # 原始文本，位于两台执行器目录之外（第三个文件系统）
│   └── <job>/shard-<n>.txt
├── exec-a/<job>/shard-<n>.wc.json   # 执行器 A 的分片词频（Shuffle）文件
├── exec-b/<job>/shard-<n>.wc.json   # 执行器 B 的分片词频文件
└── state/manifest.json   # 分片位置清单（唯一的状态真相来源，原子落盘）
```

- `docker compose up` 时，`exec-a`、`exec-b` 分别挂载**两个独立 tmpfs**，
  `st_dev` 不同（`GET /filesystems` 可验证），因此跨执行器交接不能用 rename，
  只能按字节复制（对应真实的 EXDEV 语义）。
- 原文目录与清单目录使用普通命名卷，与执行器目录也不在同一文件系统。

## 2. API（启动后访问 `/docs` 交互式提交）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/jobs/shards` | 一次提交一个作业的一个文本分片及其所属执行器；按空白分词，写分片词频文件；原文写到执行器目录之外 |
| GET | `/jobs/{job_id}/word-count` | 读取并归并该作业所有分片的词频文件，返回总词频与每个分片的实际读取位置 |
| POST | `/executors/decommission` | 下线执行器；请求体给 `copy_byte_budget`（中间文件复制字节上限） |
| GET | `/executors` | 执行器状态（ACTIVE/DECOMMISSIONING/DECOMMISSIONED）及名下分片 |
| GET | `/filesystems` | 各目录的设备号，证明执行器目录位于不同文件系统 |
| POST | `/admin/reset` | 演练用，清空全部数据回到初始状态 |

下线响应（成功）包含：`copied_bytes`（实际复制字节数）、`copied_shard_ids`、
`recomputed_shard_id`（重算的分片号，最多一个）、`moved_shards`（新位置）。
预算不足时 HTTP 200 且 `success=false`，带 `reason`，不复制、不删除任何文件；
交接过程出错返回 500 并自动回滚。

## 3. 实现要点

### 3.1 清单（manifest）状态机
`app/store.py` 中的 JSON 清单记录每个分片当前所在的执行器与词频文件路径，
是唯一权威状态。所有变更都通过「临时文件 + `os.replace`」原子重写，
读端要么看到旧版本、要么看到新版本，不会看到半写文件。进程内用一把
`RLock` 保护清单结构；文件 IO 在锁外进行，因此归并不会阻塞交接。

### 3.2 下线交接三阶段（失败可完整回滚）
1. **暂存（staging）**：清单仍指向原文件，把所有待复制分片逐字节复制到对端；
   最大的一个分片不复制，改为从原文重算（最多一个，且不计入复制字节）。
   此阶段任何失败都只清理对端临时文件，清单与源文件完全未动。
2. **切换（switch）**：所有目标文件就绪后，在一次锁内改写全部分片条目并把
   执行器置为 DECOMMISSIONED，一次原子落盘。
3. **清理（cleanup）**：清单切换完成后才删除旧文件并清空源目录。

预算是硬上限：规划阶段先算出「最大分片重算后，其余分片的复制总字节」，
超出预算直接拒绝，一个字节都不会复制。

### 3.3 归并与交接并发、恰好一次
- 归并先在锁内对作业的分片列表做**快照**，之后 IO 在锁外执行，因此与交接
  互不等待（有测试用计时验证交接不被归并 IO 拖住）。
- 快照决定「归并哪些分片」，每个分片固定出现一次，杜绝重复计入。
- 若打开某分片旧路径时文件已被清理，归并按 `(job, shard)` 回到清单查询该
  **同一分片**的当前路径再读，只改变读取位置、不改变分片身份；重试有界。
- 因为旧文件在清单切换之后才删除，并发归并只有两种结果：读到旧文件，或按
  新清单读到对端文件，二者词频内容一致。
- 下线成功后新请求只接受 ACTIVE 执行器的提交；对端若已 DECOMMISSIONED，
  下线请求以 409 拒绝。

### 3.4 持久化与崩溃恢复
清单每次变更都 fsync 落盘。重启时若发现执行器停在 DECOMMISSIONING（进程在
阶段 1/2 之间崩溃），自动恢复为 ACTIVE——此时源文件从未被删除，数据无损失。

## 4. 启动方式

### Docker Compose（推荐，两个 tmpfs 即两个文件系统）
```bash
docker compose up --build
# 打开 http://localhost:8000/docs
curl -s localhost:8000/filesystems | python -m json.tool
python scripts/smoke_demo.py            # 另开一个终端跑端到端演练
```

### 本地直接运行（无 Docker 时）
```bash
python3 -m venv --without-pip .venv     # 若系统带 ensurepip 可省略 get-pip 步骤
.venv/bin/python get-pip.py             # 仅在 venv 缺 pip 时需要
.venv/bin/pip install -r requirements.txt
./scripts/run_local.sh                  # 默认数据目录 ./data，端口 8000
python scripts/smoke_demo.py
```
本地普通目录共享一个块设备（`/filesystems` 会显示相同 device），跨文件系统
行为由 `tests/test_cross_device.py` 用 `/tmp`(overlay) 与 `/dev/shm`(tmpfs)
两个真实设备覆盖。

## 5. 测试

```bash
.venv/bin/python -m pytest -q           # 23 个用例
```

| 测试文件 | 覆盖内容 |
| --- | --- |
| `test_basic.py` | 提交/分词/词频文件位置、原文在执行器目录外、归并、成功下线清空源目录、空执行器下线、重复/非法 ID、无可用对端、重复下线 |
| `test_budget.py` | **预算不足**：拒绝且零复制零删除，原位置仍可归并；边界预算恰好成功；单分片零预算靠重算成功；原文不落在执行器树内 |
| `test_concurrency.py` | **归并途中下线**（注入延时稳定命中竞态）、6 线程归并与下线并行、预算不足不干扰并发归并、交接不等待归并锁的计时断言 |
| `test_persistence.py` | 清单重启恢复、崩溃在 DECOMMISSIONING 的恢复、模拟磁盘故障后的回滚、原文缺失导致重算失败并回滚 |
| `test_cross_device.py` | 两个真实不同块设备（overlay vs tmpfs）之间的交接，验证按字节复制而非 rename |

并发测试已连续多轮重复执行无抖动；每次归并结果都断言
`shards_merged == 分片数` 且总词频与串行期望值完全一致。

## 6. 实测记录（本仓库环境）

- `pytest -q`：**23 passed**；并发套件连跑 5 轮全部通过。
- 真实 uvicorn + `scripts/smoke_demo.py` 端到端：预算 0 被拒（复制需求 86B
  超预算，已含最大分片重算）；给足预算后成功下线，`copied_bytes=86`、
  `copied_shards=[0,1]`、`recomputed_shard=2`，源目录清空；下线后归并仍为
  4 个分片、词频不变；向已下线对端再次交接返回 409。
- 跨设备测试确认 `/tmp`(st_dev=58) → `/dev/shm`(st_dev=111) 交接成功，
  迁移后的文件物理位于对端设备。

## 7. 已知缺项 / 简化（与真实 Shuffle 服务的差距）

1. **单进程内存状态**：清单保存在单文件、单 uvicorn worker 内，靠线程锁保证
   一致性；没有多副本/选主，水平扩展需引入外部注册中心（如 etcd）。compose
   也固定 `--workers 1`。
2. **故障窗口**：阶段 2（清单已切换）之后、阶段 3 清理完成之前进程崩溃，
   重启后清单指向对端而旧文件可能残留；当前恢复逻辑只处理
   DECOMMISSIONING，不清理这种 DECOMMISSIONED 残留（不影响正确性，仅留垃圾）。
3. **重算能力上限为 1 个分片**，且固定选最大分片；这是题目约束的直接实现，
   未做更复杂的背包/多选优化。
4. **无鉴权与多租户**：`/admin/reset` 等接口仅适用于本地演练。
5. **删除依赖 Linux 语义**：归并已打开的文件在 unlink 后仍可读（测试基于
   Linux tmpfs/overlay）；Windows 上该竞态语义不同，未适配。
6. **tokenizer 仅按空白切分**，不做大小写归一、标点剔除；空文本产生空词频
   文件，仍作为一个分片参与归并。
7. 本环境没有 Docker 守护进程，compose 文件做了 YAML 校验与等价的跨设备
   测试，但未在本机实际 `docker compose up` 验证。
