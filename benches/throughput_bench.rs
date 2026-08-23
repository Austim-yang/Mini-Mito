use std::{hint::black_box, sync::Arc};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use mini_mito::{
    Key, Region, Value,
    memtable::{Wal, version::Source, wal::Operation},
    query::merge::MergeBatchIter,
    schema::TableSchema,
    sstable::sstable::SSTable,
};
use tempfile::tempdir;

fn key(i: u64) -> Key {
    (
        vec![
            ((i >> 24) & 0xff) as u8,
            ((i >> 16) & 0xff) as u8,
            ((i >> 8) & 0xff) as u8,
            (i & 0xff) as u8,
        ],
        i as i64,
    )
}

fn value(i: u64) -> Value {
    format!("v{}", i).into_bytes()
}

fn make_rows(n: usize) -> Vec<(Key, u64, Option<Value>)> {
    (0..n)
        .map(|i| (key(i as u64), i as u64, Some(value(i as u64))))
        .collect()
}

fn bench_memtable_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("memtable_bulk_insert");
    for size in [10, 50, 100, 500].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let dir = tempdir().unwrap();
                let wal_path = dir.path().join("wal.log");
                let region = Region::new(&wal_path).unwrap();
                for i in 0..size {
                    let _ = region.write(key(i), value(i));
                }
                black_box(region);
            });
        });
    }
    group.finish();
}

fn bench_create_sstable(c: &mut Criterion) {
    let mut group = c.benchmark_group("sstable_create");
    for size in [100, 500, 1000, 5000].iter() {
        let approx_bytes = (*size as u64) * 50;
        group.throughput(Throughput::Bytes(approx_bytes));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                let rows = make_rows(size);
                let dir = tempdir().unwrap();
                let path = dir.path().join("temp.sst");
                let sst = SSTable::create_from_rows(&rows, 0, &path, &TableSchema::default_table())
                    .unwrap();
                black_box(sst);
            });
        });
    }
    group.finish();
}

fn bench_sstable_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("sstable_scan");
    for size in [100, 500, 1000, 5000].iter() {
        let rows = make_rows(*size);
        let dir = tempdir().unwrap();
        let path = dir.path().join("scan.sst");
        let sstable =
            SSTable::create_from_rows(&rows, 0, &path, &TableSchema::default_table()).unwrap();
        let min = sstable.min_key().clone();
        let max = sstable.max_key().clone();

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &sstable, |b, sst| {
            b.iter(|| {
                let n: usize = sst
                    .scan_batches(&min, &max, None)
                    .unwrap()
                    .map(|b| b.unwrap().num_rows())
                    .sum();
                black_box(n);
            });
        });
    }
    group.finish();
}

fn bench_compaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction");
    let per_sst_size = 100;
    for num_ssts in [4, 8, 16].iter() {
        let total_elements = (per_sst_size * num_ssts) as u64;
        group.throughput(Throughput::Elements(total_elements));

        let dir = tempdir().unwrap();
        let mut sstables = Vec::new();
        for id in 0..*num_ssts {
            let start = id * per_sst_size;
            let rows: Vec<(Key, u64, Option<Value>)> = (0..per_sst_size)
                .map(|i| {
                    (
                        key((start + i) as u64),
                        i as u64,
                        Some(value((start + i) as u64)),
                    )
                })
                .collect();
            let path = dir.path().join(format!("{}.sst", id));
            let sst =
                SSTable::create_from_rows(&rows, id as usize, &path, &TableSchema::default_table())
                    .unwrap();
            sstables.push(sst);
        }

        group.bench_with_input(
            BenchmarkId::from_parameter(num_ssts),
            &(dir.path().to_path_buf(), sstables),
            |b, (dir, ssts)| {
                b.iter(|| {
                    let sources = ssts
                        .iter()
                        .map(|sst| {
                            Source::Sst(
                                sst.scan_batches(sst.min_key(), sst.max_key(), None)
                                    .unwrap(),
                            )
                        })
                        .collect();
                    let merge =
                        MergeBatchIter::new(sources, Arc::new(TableSchema::default_table()));
                    let merged: Vec<_> = merge.map(|b| Arc::new(b.unwrap())).collect();
                    let _new_sst = SSTable::create_from_batches(
                        &merged,
                        99,
                        dir.join("merged.sst"),
                        &TableSchema::default_table(),
                    )
                    .unwrap();
                });
            },
        );
    }
    group.finish();
}

fn bench_wal_append_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_bulk_append");
    for batch_size in [10, 50, 100, 500].iter() {
        let ops: Vec<Operation> = (0..*batch_size)
            .map(|i| Operation::Insert {
                key: key(i),
                seq: i,
                value: value(i),
            })
            .collect();
        let approx_bytes = (*batch_size as u64) * 60;
        group.throughput(Throughput::Bytes(approx_bytes));
        group.bench_with_input(BenchmarkId::from_parameter(batch_size), &ops, |b, ops| {
            b.iter(|| {
                let dir = tempdir().unwrap();
                let path = dir.path().join("test_wal.log");
                let mut wal = Wal::new(&path).unwrap();
                for op in ops {
                    wal.append(op).unwrap();
                }
                wal.close().unwrap();
                black_box(wal);
            });
        });
    }
    group.finish();
}

// ---------- 组合所有 benchmark ----------
criterion_group!(
    benches,
    bench_memtable_insert,
    bench_create_sstable,
    bench_sstable_scan,
    bench_compaction,
    bench_wal_append_batch,
);
criterion_main!(benches);
