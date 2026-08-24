use std::{
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    io,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

use arrow::array::{ArrayRef, BinaryArray, Int8Array, Int64Array, RecordBatch};
use arrow_schema::DataType;

use crate::{
    Key, Value, memtable::{ImmutableMemtable, Memtable, Wal, wal::Operation}, schema::{BatchView, SemanticType, TableSchema}, sstable::sstable::{OP_DELETE, OP_PUT, internal_batch_from_rows, key_at, sst_schema},
};

const DEFAULT_SHARDS: usize = 16;
const ROW_FIXED_BYTES: usize = 17;
const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

type SeriesRows = Vec<(i64, u64, Option<Arc<Value>>)>;

struct Shard {
    series: HashMap<Box<[u8]>, SeriesRows>,
}

fn series_hash(tags: &[u8]) -> u64 {
    let mut h: u64 = FNV_OFFSET;
    for &b in tags {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

fn find_old(rows: &SeriesRows, ts: i64) -> Option<Value> {
    let mut best: Option<(u64, Option<Arc<Value>>)> = None;
    for &(row_ts, row_seq, ref v) in rows {
        if row_ts == ts {
            match &best {
                Some((s, _)) if *s >= row_seq => {}
                _ => best = Some((row_seq, v.clone())),
            }
        }
    }
    best.and_then(|(_, v)| v.map(|a| a.as_ref().clone()))
}

fn materialize_series(
    tags: &[u8],
    rows: &SeriesRows,
    schema: &TableSchema,
) -> io::Result<RecordBatch> {
    let mut idx: Vec<u32> = (0..rows.len() as u32).collect();
    idx.sort_by(|&a, &b| {
        let (row_a, row_b) = (&rows[a as usize], &rows[b as usize]);
        row_a.0.cmp(&row_b.0).then(row_b.1.cmp(&row_a.1))
    });
    let mut last_ts: Option<i64> = None;
    let kept: Vec<u32> = idx
        .into_iter()
        .filter(|&i| {
            let ts = rows[i as usize].0;
            let keep = last_ts != Some(ts);
            if keep {
                last_ts = Some(ts);
            }
            keep
        })
        .collect();

    let nfields = schema
        .columns
        .iter()
        .filter(|c| c.semantic == SemanticType::Field)
        .count();
    let field_pos = schema
        .columns
        .iter()
        .position(|c| c.semantic == SemanticType::Field);
    let fast = schema.primary_key.len() == 1
        && nfields == 1
        && schema.columns.len() == 3
        && schema.columns[schema.primary_key[0]].data_type == DataType::Binary
        && schema.columns[field_pos.unwrap()].data_type == DataType::Binary;

    if !fast {
        let sorted: Vec<(Key, u64, Option<Value>)> = kept
            .iter()
            .map(|&i| {
                let (ts, seq, v) = &rows[i as usize];
                (
                    (tags.to_vec(), *ts),
                    *seq,
                    v.as_ref().map(|a| a.as_ref().clone()),
                )
            })
            .collect();
        return internal_batch_from_rows(&sorted, schema);
    }

    let pk = schema.primary_key[0];
    let fi = field_pos.unwrap();
    let mut slots: Vec<Option<ArrayRef>> = (0..schema.columns.len()).map(|_| None).collect();
    slots[pk] = Some(Arc::new(BinaryArray::from_iter_values(
        std::iter::repeat(tags).take(kept.len()),
    )));
    slots[schema.time_index] = Some(Arc::new(Int64Array::from_iter(
        kept.iter().map(|&i| Some(rows[i as usize].0)),
    )));
    slots[fi] =
        Some(Arc::new(BinaryArray::from_iter(kept.iter().map(|&i| {
            rows[i as usize].2.as_ref().map(|a| a.as_slice())
        }))));

    let mut arrays: Vec<ArrayRef> = slots
        .into_iter()
        .map(|s| s.expect("all user columns populated"))
        .collect();
    arrays.push(Arc::new(Int64Array::from_iter(
        kept.iter().map(|&i| Some(rows[i as usize].1 as i64)),
    )));
    arrays.push(Arc::new(Int8Array::from_iter(kept.iter().map(|&i| {
        if rows[i as usize].2.is_some() {
            OP_PUT
        } else {
            OP_DELETE
        }
    }))));
    RecordBatch::try_new(Arc::new(sst_schema(schema)), arrays)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

pub struct ColumnarMemtable {
    shards: Box<[Mutex<Shard>]>,
    mask: u64,
    shard_count: usize,
    row_count: AtomicUsize,
    estimated_bytes: AtomicUsize,
    max_seq: AtomicU64,
    schema: Arc<TableSchema>,
    wal: Arc<Mutex<Wal>>,
    wal_path: PathBuf,
}

impl ColumnarMemtable {
    pub fn with_default_config(schema: Arc<TableSchema>, wal_path: PathBuf) -> io::Result<Self> {
        Self::new(schema, DEFAULT_SHARDS, wal_path)
    }

    pub fn new(
        schema: Arc<TableSchema>,
        shard_count: usize,
        wal_path: PathBuf,
    ) -> io::Result<Self> {
        let count = shard_count.max(1).next_power_of_two();
        let shards = (0..count)
            .map(|_| {
                Mutex::new(Shard {
                    series: HashMap::new(),
                })
            })
            .collect::<Vec<_>>();
        let wal = Arc::new(Mutex::new(Wal::new(&wal_path)?));
        let mem = Self {
            shards: shards.into_boxed_slice(),
            mask: (count - 1) as u64,
            shard_count: count,
            row_count: AtomicUsize::new(0),
            estimated_bytes: AtomicUsize::new(0),
            max_seq: AtomicU64::new(0),
            schema,
            wal,
            wal_path,
        };

        {
            let wal = mem.wal.lock().unwrap();
            wal.recover(&mut |op: &Operation| {
                let _ = mem.replay(op);
            })?;
        }
        Ok(mem)
    }

    pub fn wal_path(&self) -> &Path {
        &self.wal_path
    }

    fn shard_for(&self, tags: &[u8]) -> &Mutex<Shard> {
        &self.shards[(series_hash(tags) & self.mask) as usize]
    }

    fn insert_row(
        &self,
        tags: Box<[u8]>,
        ts: i64,
        seq: u64,
        value: Option<Value>,
    ) -> Option<Value> {
        let stored = value.map(Arc::new);
        let bytes = ROW_FIXED_BYTES + stored.as_ref().map_or(0, |v| v.len());
        let old = {
            let mut shard = self.shard_for(&tags).lock().unwrap();
            let rows = shard.series.entry(tags).or_default();
            let old = find_old(rows, ts);
            rows.push((ts, seq, stored));
            old
        };
        self.row_count.fetch_add(1, Ordering::Relaxed);
        self.estimated_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.max_seq.fetch_max(seq, Ordering::Relaxed);
        old
    }

    fn scan_series(rows: &SeriesRows, ts: i64) -> Option<(u64, Option<Value>)> {
        let mut best: Option<(u64, Option<Arc<Value>>)> = None;
        for &(row_ts, seq, ref v) in rows {
            if row_ts == ts {
                match &best {
                    Some((s, _)) if *s >= seq => {}
                    _ => best = Some((seq, v.clone())),
                }
            }
        }
        best.map(|(s, v)| (s, v.map(|a| a.as_ref().clone())))
    }
}

impl Memtable for ColumnarMemtable {
    fn write(&self, key: Key, seq: u64, value: Option<Value>) -> io::Result<Option<Value>> {
        let op = match &value {
            Some(v) => Operation::Insert {
                key: key.clone(),
                seq,
                value: v.clone(),
            },
            None => Operation::Delete {
                key: key.clone(),
                seq,
            },
        };
        self.wal.lock().unwrap().append(&op)?;
        Ok(self.insert_row(key.0.into_boxed_slice(), key.1, seq, value))
    }

    fn write_batch(&self, entries: Vec<(Key, u64, Option<Value>)>) -> io::Result<()> {
        let ops: Vec<Operation> = entries
            .iter()
            .map(|(key, seq, value)| match value {
                Some(v) => Operation::Insert {
                    key: key.clone(),
                    seq: *seq,
                    value: v.clone(),
                },
                None => Operation::Delete {
                    key: key.clone(),
                    seq: *seq,
                },
            })
            .collect();
        self.wal.lock().unwrap().append_batch(&ops)?;
        for (key, seq, value) in entries {
            self.insert_row(key.0.into_boxed_slice(), key.1, seq, value);
        }
        Ok(())
    }

    fn replay(&self, op: &Operation) -> io::Result<()> {
        match op {
            Operation::Insert { key, seq, value } | Operation::Update { key, seq, value } => {
                self.insert_row(
                    key.0.clone().into_boxed_slice(),
                    key.1,
                    *seq,
                    Some(value.clone()),
                );
            }
            Operation::Delete { key, seq } => {
                self.insert_row(key.0.clone().into_boxed_slice(), key.1, *seq, None);
            }
        }
        Ok(())
    }

    fn get(&self, key: &Key) -> io::Result<Option<(u64, Option<Value>)>> {
        let shard = self.shard_for(&key.0).lock().unwrap();
        Ok(shard
            .series
            .get(key.0.as_slice())
            .and_then(|r| Self::scan_series(r, key.1)))
    }

    fn max_seq(&self) -> u64 {
        self.max_seq.load(Ordering::Relaxed)
    }

    fn to_batches(&self, schema: &TableSchema) -> io::Result<Vec<Arc<RecordBatch>>> {
        let mut batches = Vec::new();
        for shard in self.shards.iter() {
            let s = shard.lock().unwrap();
            for (tags, rows) in s.series.iter() {
                batches.push(Arc::new(materialize_series(tags, rows, schema)?));
            }
        }
        batches.sort_by_key(|b| {
            let view = BatchView::new(b, schema);
            key_at(&view, schema, 0)
        });
        Ok(batches)
    }

    fn len(&self) -> usize {
        self.row_count.load(Ordering::Relaxed)
    }

    fn estimated_size(&self) -> usize {
        self.estimated_bytes.load(Ordering::Relaxed)
    }

    fn freeze(&self) -> io::Result<Box<dyn ImmutableMemtable>> {
        self.wal.lock().unwrap().close()?;
        let mut series = BTreeMap::new();
        for shard in self.shards.iter() {
            let s = shard.lock().unwrap();
            for (tags, rows) in s.series.iter() {
                series.insert(tags.clone(), rows.clone());
            }
        }
        Ok(Box::new(FrozenColumnar {
            series,
            row_count: self.row_count.load(Ordering::Relaxed),
            total_bytes: self.estimated_bytes.load(Ordering::Relaxed),
            max_seq_v: self.max_seq.load(Ordering::Relaxed),
            wal_path: self.wal_path.clone(),
            schema: Arc::clone(&self.schema),
            cached: Mutex::new(None),
        }))
    }

    fn fork(&self) -> io::Result<Box<dyn Memtable>> {
        let parent = self.wal_path.parent().unwrap();
        let stem = self.wal_path.file_stem().unwrap().to_str().unwrap();
        let n: usize = stem.trim_start_matches("wal_").parse().unwrap_or(0);
        let new_path = parent.join(format!("wal_{:03}.log", n + 1));
        Ok(Box::new(ColumnarMemtable::new(
            Arc::clone(&self.schema),
            self.shard_count,
            new_path,
        )?))
    }

    fn close(&self) -> io::Result<()> {
        self.wal.lock().unwrap().close()
    }
}

impl Debug for ColumnarMemtable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Memtable")
            .field("wal_path", &self.wal_path)
            .finish()
    }
}

struct FrozenColumnar {
    series: BTreeMap<Box<[u8]>, SeriesRows>,
    row_count: usize,
    total_bytes: usize,
    max_seq_v: u64,
    wal_path: PathBuf,
    schema: Arc<TableSchema>,
    cached: Mutex<Option<Vec<Arc<RecordBatch>>>>,
}

impl FrozenColumnar {
    fn materialize_all(&self) -> io::Result<Vec<Arc<RecordBatch>>> {
        let mut out = Vec::with_capacity(self.series.len());
        for (tags, rows) in self.series.iter() {
            out.push(Arc::new(materialize_series(tags, rows, &self.schema)?));
        }
        Ok(out)
    }
}

impl ImmutableMemtable for FrozenColumnar {
    fn get(&self, key: &Key) -> io::Result<Option<(u64, Option<Value>)>> {
        Ok(self
            .series
            .get(key.0.as_slice())
            .and_then(|r| ColumnarMemtable::scan_series(r, key.1)))
    }

    fn max_seq(&self) -> u64 {
        self.max_seq_v
    }

    fn len(&self) -> usize {
        self.row_count
    }

    fn estimated_size(&self) -> usize {
        self.total_bytes
    }

    fn to_batches(&self, _schema: &TableSchema) -> io::Result<Vec<Arc<RecordBatch>>> {
        let mut cache = self.cached.lock().unwrap();
        if cache.is_none() {
            *cache = Some(self.materialize_all()?);
        }
        Ok(cache.as_ref().unwrap().clone())
    }

    fn wal_path(&self) -> &Path {
        &self.wal_path
    }
}

#[cfg(test)]
mod tests {
    use tempfile::env;

    use super::*;

    fn k(tag: u8, ts: i64) -> Key {
        (vec![tag], ts)
    }
    fn v(s: &str) -> Value {
        s.as_bytes().to_vec()
    }
    fn schema() -> Arc<TableSchema> {
        Arc::new(TableSchema::default_table())
    }
    fn fresh(name: &str) -> PathBuf {
        let dir = env::temp_dir()
            .join(format!("mini_mito_col_{}", std::process::id()))
            .join(name);
        let _ = std::fs::create_dir_all(&dir);
        dir.join("wal_000.log")
    }

    #[test]
    fn write_get_unsorted_timestamps() {
        let m = ColumnarMemtable::with_default_config(schema(), fresh("unsorted")).unwrap();
        assert_eq!(m.write(k(1, 30), 1, Some(v("c"))).unwrap(), None);
        assert_eq!(m.write(k(1, 10), 2, Some(v("a"))).unwrap(), None);
        assert_eq!(m.write(k(1, 20), 3, Some(v("b"))).unwrap(), None);
        assert_eq!(m.get(&k(1, 10)).unwrap(), Some((2, Some(v("a")))));
        assert_eq!(m.get(&k(1, 99)).unwrap(), None);
        assert_eq!(m.len(), 3);
        let batches = m.to_batches(&schema()).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[test]
    fn duplicate_key_newest_wins() {
        let m = ColumnarMemtable::with_default_config(schema(), fresh("dup")).unwrap();
        m.write(k(5, 0), 1, Some(v("old"))).unwrap();
        assert_eq!(m.write(k(5, 0), 2, Some(v("new"))).unwrap(), Some(v("old")));
        assert_eq!(
            m.write(k(5, 0), 1, Some(v("late"))).unwrap(),
            Some(v("new"))
        );
        assert_eq!(m.get(&k(5, 0)).unwrap(), Some((2, Some(v("new")))));
        assert_eq!(m.len(), 3);
    }

    #[test]
    fn tombstone_and_reput() {
        let m = ColumnarMemtable::with_default_config(schema(), fresh("tomb")).unwrap();
        m.write(k(7, 1), 1, Some(v("x"))).unwrap();
        assert_eq!(m.write(k(7, 1), 2, None).unwrap(), Some(v("x")));
        assert_eq!(m.get(&k(7, 1)).unwrap(), Some((2, None)));
        assert_eq!(m.write(k(7, 1), 3, Some(v("y"))).unwrap(), None);
        assert_eq!(m.get(&k(7, 1)).unwrap(), Some((3, Some(v("y")))));
    }

    #[test]
    fn freeze_roundtrip_multi_series() {
        let m = ColumnarMemtable::new(schema(), 4, fresh("frozen")).unwrap();
        m.write(k(1, 10), 1, Some(v("a"))).unwrap();
        m.write(k(2, 20), 2, Some(v("b"))).unwrap();
        m.write(k(1, 11), 3, None).unwrap();
        let frozen = m.freeze().unwrap();
        assert_eq!(frozen.get(&k(1, 10)).unwrap(), Some((1, Some(v("a")))));
        assert_eq!(frozen.get(&k(1, 11)).unwrap(), Some((3, None)));
        assert_eq!(frozen.max_seq(), 3);
        assert_eq!(frozen.len(), 3);
        let batches = frozen.to_batches(&schema()).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        let again = frozen.to_batches(&schema()).unwrap();
        assert_eq!(again.len(), batches.len());
    }

    #[test]
    fn persistence_reopen() {
        let path = fresh("persist");
        {
            let m = ColumnarMemtable::with_default_config(schema(), path.clone()).unwrap();
            m.write(k(9, 100), 1, Some(v("kept"))).unwrap();
            m.write(k(9, 101), 2, None).unwrap();
        }
        let m2 = ColumnarMemtable::with_default_config(schema(), path).unwrap();
        assert_eq!(m2.get(&k(9, 100)).unwrap(), Some((1, Some(v("kept")))));
        assert_eq!(m2.get(&k(9, 101)).unwrap(), Some((2, None)));
        assert_eq!(m2.max_seq(), 2);
    }

    #[test]
    fn fork_creates_fresh_writable() {
        let dir = std::env::temp_dir()
            .join(format!("mini_mito_col_{}", std::process::id()))
            .join("fork");
        let _ = std::fs::create_dir_all(&dir);
        let m = ColumnarMemtable::with_default_config(schema(), dir.join("wal_000.log")).unwrap();
        m.write(k(1, 1), 1, Some(v("x"))).unwrap();
        let f = m.fork().unwrap();
        assert_eq!(f.get(&k(1, 1)).unwrap(), None);
        f.write(k(2, 5), 1, Some(v("fresh"))).unwrap();
        assert_eq!(
            f.get(&k(2, 5)).unwrap().map(|(_, v)| v),
            Some(Some(v("fresh")))
        );
    }

    #[test]
    fn estimated_size_tracks_payload() {
        let m = ColumnarMemtable::with_default_config(schema(), fresh("size")).unwrap();
        m.write(k(1, 1), 1, Some(vec![0u8; 16])).unwrap();
        let small = m.estimated_size();
        m.write(k(1, 2), 2, Some(vec![0u8; 4096])).unwrap();
        assert!(m.estimated_size() - small >= 4096);
    }
}
