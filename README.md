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
├── exec-a/gen-<epoch>/<job>/shard-<n>.wc.json   # 执行器 A 的分片词频（Shuffle）文件
├── exec-b/gen-<epoch>/<job>/shard-<n>.wc.json   # 执行器 B 的分片词频文件
└── state/manifest.json   # 分片位置清单（唯一的状态真相来源，原子落盘，含 epoch）
```

- `gen-<epoch>` 是**演练代际目录**：每次 `/admin/reset` 让清单里的 `epoch` 加一，
  新一轮的词频文件全部落在新的 `gen-<epoch>` 目录里，旧下线流程拿到的旧路径
  物理上无法触及新一轮文件（详见 3.5）。
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
| POST | `/admin/reset` | 演练用，立刻进入新的一代（epoch+1）并复用全部编号；不等待在途下线 |

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
启动时还会清扫与当前 epoch 不符的遗留 `gen-*` 目录，回收崩溃残留占用的 tmpfs。

### 3.5 重置（reset）与在途下线的代际隔离
在 `/docs` 反复演练时，可能在上一个下线还在复制/重算/清理时就重置，并用**同一
作业号、同一分片号**重新提交。旧实现按裸路径清理，会把刚收下的新一轮词频文件
误删。修复依赖两层机制：

1. **代际目录（命名空间隔离）**：清单带单调递增 `epoch`，词频文件路径为
   `<executor>/gen-<epoch>/<job>/shard-<n>.wc.json`。重置后新文件进入新的
   `gen-<epoch+1>`，旧下线持有的所有路径（复制源/目标、清理目标、回滚目标）
   都在旧 `gen-<epoch>` 内，目录结构保证它无法覆盖或删除新一轮文件。
2. **epoch 围栏（状态隔离）**：下线在持锁规划时捕获 `start_epoch`，并在阶段 1
   每个分片处理前、重算前、阶段 2 清单切换前重新校验；IO 异常的错误路径也会
   回查。重置使 epoch 变化后：
   - 尚未完成清单切换的旧下线一律得到 **409 Conflict**（含复制中途被重置
     unlink 掉源文件的情况），其阶段 2 永远不会执行，新一轮清单与执行器状态
     不被改动；
   - 已经完成阶段 2 切换的旧下线**保留原来的成功回执**，其阶段 3 清理只作用于
     它捕获的旧代际目录，对新一轮是 no-op。

`reset()` 自身在锁内只做元数据（epoch+1、清单清空、执行器回 ACTIVE、一次原子
落盘），**不复制、不重算、不等待任何在途下线**，回包后编号立即可复用。锁外只做
unlink：未打开的旧词频文件在回包前就释放 tmpfs 空间（**绝不靠复制原文腾位置**）；
仍被归并请求打开的文件按 Linux unlink 语义保留 inode，最后一个持有它的请求退出
后自动释放。旧代际归并若在重置后才找不到文件，其有界重试在新清单中查不到该分片，
返回 500——它不会影响新一轮（见第 7 节缺项）。

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

重置竞态的真实服务端演练（自动起 uvicorn、用暂停钩子把旧下线停在复制/清理
阶段，数据根默认放在 `/dev/shm` tmpfs）：
```bash
.venv/bin/python -I scripts/run_reset_demo.py
```

## 5. 测试

```bash
.venv/bin/python -m pytest -q           # 30 个用例
```

| 测试文件 | 覆盖内容 |
| --- | --- |
| `test_basic.py` | 提交/分词/词频文件位置、原文在执行器目录外、归并、成功下线清空源目录、空执行器下线、重复/非法 ID、无可用对端、重复下线 |
| `test_budget.py` | **预算不足**：拒绝且零复制零删除，原位置仍可归并；边界预算恰好成功；单分片零预算靠重算成功；原文不落在执行器树内 |
| `test_concurrency.py` | **归并途中下线**（注入延时稳定命中竞态）、6 线程归并与下线并行、预算不足不干扰并发归并、交接不等待归并锁的计时断言 |
| `test_persistence.py` | 清单重启恢复、崩溃在 DECOMMISSIONING 的恢复、模拟磁盘故障后的回滚、原文缺失导致重算失败并回滚 |
| `test_cross_device.py` | 两个真实不同块设备（overlay vs tmpfs）之间的交接，验证按字节复制而非 rename |
| `test_reset_handoff.py` | **重置与在途下线竞态**：复制暂停时重置→旧下线 409、新轮同号文件不被覆盖/删除；清理暂停时重置→已切换的旧下线保留成功回执且碰不到新轮；重置回包前 unlink 回收 tmpfs（不复制原文）；打开的文件在最后 fd 关闭后释放；reset 后 `/filesystems` 与重启恢复；同轮归并读全分片且与下线并行 |

并发测试已连续多轮重复执行无抖动；每次归并结果都断言
`shards_merged == 分片数` 且总词频与串行期望值完全一致。

## 6. 实测记录（本仓库环境）

- `pytest -q`：**30 passed**；完整套件（含竞态用例）连跑 6 轮全部通过，无抖动。
- 真实 uvicorn + `scripts/smoke_demo.py`（数据目录放在 `/dev/shm` tmpfs）端到端：
  预算 0 被拒（复制需求 86B 超预算，已含最大分片重算）；给足预算后成功下线，
  `copied_bytes=86`、`copied_shards=[0,1]`、`recomputed_shard=2`，源代际目录清空；
  下线后归并仍为 4 个分片、词频不变；向已下线对端再次交接返回 409。
- 真实 uvicorn + `scripts/run_reset_demo.py`（用暂停钩子把旧下线分别停在
  **复制阶段**和**清理阶段**再 reset）：
  - 复制暂停：reset 耗时 0.00s 立即返回，同作业号/分片号立刻可复用；旧下线
    恢复后返回 **409**，新文件 inode 与内容均未被改动，旧 `gen-*` 目录全部回收；
  - 清理暂停（清单已切换）：旧下线恢复后保留 **200 成功回执**，但其 unlink
    全部落在旧代际，新一轮同号文件 inode 不变、归并只计新轮分片；
  - tmpfs 空间：未打开文件在 reset 回包前即回收（实测单次回收 688128B）；
    被打开的文件 reset 后目录项消失但 inode 仍可读，fd 关闭时才释放其 73728B。
- 跨设备测试确认 `/tmp`(overlay) → `/dev/shm`(tmpfs) 交接成功，
  迁移后的文件物理位于对端设备。
- 本环境没有 Docker 守护进程，`docker-compose.yml` 未实际 `up`；其两个
  `tmpfs` 挂载与卷布局保持不变，上述 tmpfs 实测覆盖了等价的文件系统语义。

## 7. 已知缺项 / 简化（与真实 Shuffle 服务的差距）

1. **单进程内存状态**：清单保存在单文件、单 uvicorn worker 内，靠线程锁保证
   一致性；没有多副本/选主，水平扩展需引入外部注册中心（如 etcd）。compose
   也固定 `--workers 1`。
2. **故障窗口**：阶段 2（清单已切换）之后、阶段 3 清理完成之前进程崩溃，
   重启后清单指向对端而旧文件可能残留；启动清扫会按 epoch 回收这类
   `gen-*` 残留（不影响正确性，仅留待下次启动清理）。
3. **重算能力上限为 1 个分片**，且固定选最大分片；这是题目约束的直接实现，
   未做更复杂的背包/多选优化。
4. **无鉴权与多租户**：`/admin/reset` 等接口仅适用于本地演练。
5. **删除依赖 Linux 语义**：归并已打开的文件在 unlink 后仍可读（测试基于
   Linux tmpfs/overlay）；Windows 上该竞态语义不同，未适配。
6. **tokenizer 仅按空白切分**，不做大小写归一、标点剔除；空文本产生空词频
   文件，仍作为一个分片参与归并。
7. **旧代际在途归并遇到 reset**：若归并在 reset 前快照、reset 后才打开文件，
   其有界 fallback 在新清单中找不到旧分片，会返回 500；该请求属于已被重置
   废弃的演练轮次，不会污染新一轮（新一轮归并独立验证通过）。要让旧请求也
   成功，需要保留旧代际直到所有在途请求结束，与"回包前腾出 tmpfs"的要求
   相冲突，故按当前取舍实现。
8. **暂停钩子是测试专用接口**：`Store.on_handoff_stage / on_handoff_cleanup /
   on_merge_file_open` 仅用于稳定复现竞态，生产路径默认全为 `None`，零开销。
