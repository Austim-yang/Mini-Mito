# Mini Mito

[![Rust](https://img.shields.io/badge/rust-2024%20edition-blue)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

一个基于 Rust 的 LSM-Tree 存储引擎演示。灵感来源于 GreptimeDB 的 Mito 引擎。

LSM-Tree（Log-Structured Merge-Tree）是现代高性能数据库（如 GreptimeDB、LevelDB、RocksDB）的核心存储架构。它将随机写转换为顺序写，大幅提升写入吞吐量，非常适合时序数据和日志型数据。

## 当前状态

| 组件 | 说明 |
| :--- | :--- |
| **预写日志（WAL）** | 二进制帧格式（len + CRC-32 + payload），整批一次写入；崩溃恢复容忍截断尾部并校验每帧 CRC，损坏帧及其后内容安全丢弃；`SyncPolicy` 控制落盘强度（默认 Interval）。 |
| **Memtable（列式）** | 按 series（tags）组织的列式缓冲，16 分片降低写锁竞争；值以 `Arc` 共享存储降低写放大与冻结拷贝开销；追加写不去重，交给查询层按 seq 合并；`fork`/`freeze` 实现 O(1) 冻结。 |
| **快照隔离事务** | `Region::begin` 钉住时点快照，写缓冲本地暂存；提交时逐 key first-committer-wins 冲突检测，整个事务单帧落 WAL、在提交锁内原子应用——读者永不看到半截事务；未提交即丢弃自动回滚。 |
| **Region & Version** | Region 管理 active/immutable/ssts 与 seq 水位；freeze 在写线程完成，Parquet 落盘与压缩由后台线程异步执行；查询基于不可变快照。 |
| **SSTable** | Parquet 存储；索引含 bloom + row-group 元数据（键范围 + min_ts/max_ts）；点查只解码所需列并配合有界行组缓存（FIFO）；批级扫描支持时间范围剪枝与列投影下推到 Parquet 解码层。 |
| **Manifest 管理** | 记录所有 SSTable 元信息（ID、路径、键范围、条目数），原子写入；丢失时自动扫描目录重建 |
| **时间范围谓词剪枝** | 从 DataFusion 过滤条件提取 `TimeRange`，对 sst、row-group 双层剪枝，并做行级 ts 过滤。 |
| **TWCS 压缩 + TTL** | 按 `ts / window_size`（默认 1 小时）分窗，窗内 sst 数达 `compact_threshold`（默认 4）时合并；写路径自动触发压缩；TTL（默认关闭）在压缩时物理删除过期行，读路径按 `ttl_cutoff` 钳制。 |
| **DataFusion 集成** | 实现 `TableProvider` 和 `ExecutionPlan`，将 LSM 存储暴露为 SQL 表（`tags`、`timestamp`、`fields` 三列）；支持投影、过滤和时间范围谓词下推 |
| **单元测试** | 覆盖 WAL 截断恢复、列式 Memtable、SSTable、Region 并发、查询管线、剪枝与 TTL，全部通过。 |
| **基准测试** | 使用 Criterion 进行多维度性能测量（插入、查询、扫描、刷新、压缩、全表扫描、TWCS 同窗/多窗对比等） |

## 技术栈

- Rust 2024 Edition
- `serde` + `serde_json` + `base64`
- `datafusion`
- `crc32fast`
- `tokio` + `futures`
- `rand`
- `tempfile`
- `parquet` + `arrow` + `arrow-schema`
- `criterion`

## 构建与运行

```bash
git clone https://github.com/Austim-yang/Mini-Mito.git
cd mini_mito
cargo build
cargo test
```

## 运行基准测试

```bash
cargo bench
```

## 参考资料

- [GreptimeDB Mito 存储引擎设计](https://docs.greptime.com/contributor-guide/storage-engine/overview) —— 本项目的主要灵感来源
- [The Log-Structured Merge-Tree (LSM-Tree)](https://www.cs.umb.edu/~poneil/lsmtree.pdf) —— 原始论文

## License

[MIT](LICENSE)
