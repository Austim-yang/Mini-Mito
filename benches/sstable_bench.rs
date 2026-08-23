use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mini_mito::{Key, schema::TableSchema, sstable::sstable::SSTable};
use rand::{RngExt, rng};
use tempfile::tempdir;

fn key(i: u64) -> (Vec<u8>, i64) {
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

fn value(i: u64) -> Vec<u8> {
    format!("v{}", i).into_bytes()
}

fn make_rows(n: u64) -> Vec<(Key, u64, Option<Vec<u8>>)> {
    (0..n).map(|i| (key(i), i, Some(value(i)))).collect()
}

fn bench_sstable_create(c: &mut Criterion) {
    let mut group = c.benchmark_group("sstable_create");
    for size in [1_000, 10_000, 100_000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || (make_rows(size), tempdir().unwrap()),
                |(rows, dir)| {
                    let path = dir.path().join("test.sst");
                    let _ =
                        SSTable::create_from_rows(&rows, 1, &path, &TableSchema::default_table())
                            .unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

fn bench_sstable_get(c: &mut Criterion) {
    let size = 10_000;
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");
    let sst = SSTable::create_from_rows(&make_rows(size), 1, &path, &TableSchema::default_table())
        .unwrap();
    let mut rng = rng();

    c.bench_function("sstable_get_hit", |b| {
        b.iter(|| {
            let idx = black_box(rng.random_range(0..size) as u64);
            let _ = sst.get(&key(idx)).unwrap();
        });
    });
    c.bench_function("sstable_get_miss", |b| {
        b.iter(|| {
            let idx = black_box(rng.random_range(size..(size * 2)) as u64);
            let _ = sst.get(&key(idx)).unwrap();
        });
    });
}

fn bench_sstable_scan(c: &mut Criterion) {
    let size = 10_000;
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");
    let sst = SSTable::create_from_rows(&make_rows(size), 1, &path, &TableSchema::default_table())
        .unwrap();

    c.bench_function("sstable_scan_all", |b| {
        b.iter(|| {
            let n: usize = sst
                .scan_batches(&key(0), &key(size - 1), None)
                .unwrap()
                .map(|b| b.unwrap().num_rows())
                .sum();
            black_box(n);
        });
    });

    c.bench_function("sstable_scan_range_10pct", |b| {
        b.iter(|| {
            let start = black_box(size / 10);
            let end = black_box(size / 10 * 2);
            let n: usize = sst
                .scan_batches(&key(start as u64), &key(end as u64), None)
                .unwrap()
                .map(|b| b.unwrap().num_rows())
                .sum();
            black_box(n);
        });
    });
}

fn bench_sstable_scan_time_range(c: &mut Criterion) {
    let size: u64 = 100_000;
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");
    let sst = SSTable::create_from_rows(&make_rows(size), 1, &path, &TableSchema::default_table())
        .unwrap();
    let min = sst.min_key().clone();
    let max = sst.max_key().clone();

    c.bench_function("sstable_scan_iter_all_100k", |b| {
        b.iter(|| {
            let n: usize = sst
                .scan_batches(&min, &max, None)
                .unwrap()
                .map(|b| b.unwrap().num_rows())
                .sum();
            black_box(n);
        });
    });
    c.bench_function("sstable_scan_iter_time_range_100k", |b| {
        b.iter(|| {
            let n: usize = sst
                .scan_batches(&min, &max, Some((20_000, 40_000)))
                .unwrap()
                .map(|b| b.unwrap().num_rows())
                .sum();
            black_box(n);
        });
    });
}

fn bench_sstable_get_100k(c: &mut Criterion) {
    let size: u64 = 100_000;
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");
    let sst = SSTable::create_from_rows(&make_rows(size), 1, &path, &TableSchema::default_table())
        .unwrap();
    let mut rng = rng();

    c.bench_function("sstable_get_hit_100k", |b| {
        b.iter(|| {
            let idx = black_box(rng.random_range(0..size));
            let _ = sst.get(&key(idx)).unwrap();
        });
    });
    c.bench_function("sstable_get_miss_100k", |b| {
        b.iter(|| {
            let idx = black_box(rng.random_range(size..(size * 2)));
            let _ = sst.get(&key(idx)).unwrap();
        });
    });
}

fn bench_sstable_scan_100k(c: &mut Criterion) {
    let size: u64 = 100_000;
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.sst");
    let sst = SSTable::create_from_rows(&make_rows(size), 1, &path, &TableSchema::default_table())
        .unwrap();

    c.bench_function("sstable_scan_all_100k", |b| {
        b.iter(|| {
            let n: usize = sst
                .scan_batches(&key(0), &key(size - 1), None)
                .unwrap()
                .map(|b| b.unwrap().num_rows())
                .sum();
            black_box(n);
        });
    });
    c.bench_function("sstable_scan_range_10pct_100k", |b| {
        b.iter(|| {
            let start = black_box(size / 10);
            let end = black_box(size / 10 * 2);
            let n: usize = sst
                .scan_batches(&key(start), &key(end), None)
                .unwrap()
                .map(|b| b.unwrap().num_rows())
                .sum();
            black_box(n);
        });
    });
}

criterion_group!(
    benches,
    bench_sstable_create,
    bench_sstable_get,
    bench_sstable_get_100k,
    bench_sstable_scan,
    bench_sstable_scan_time_range,
    bench_sstable_scan_100k,
);
criterion_main!(benches);
