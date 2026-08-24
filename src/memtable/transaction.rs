use std::{
    collections::{BTreeMap, HashMap},
    fmt::Display,
    io,
    sync::Arc,
};

use crate::{
    Key, Value,
    memtable::version::Source,
    schema::{BatchView, SemanticType, TableSchema},
    sstable::sstable::{key_at, value_at},
};

pub struct Transaction {
    pub(crate) snapshot_seq: u64,
    index: HashMap<Key, (u64, Option<Value>)>,
    pub(crate) writes: BTreeMap<Key, Option<Value>>,
    pub(crate) committed: bool,
}

#[derive(Debug)]
pub enum TxError {
    Conflict,
    Io(io::Error),
}

impl Display for TxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TxError::Conflict => f.write_str("transaction conflict: key modified since begin"),
            TxError::Io(e) => write!(f, "transaction io error: {e}"),
        }
    }
}

impl std::error::Error for TxError {}

impl From<io::Error> for TxError {
    fn from(value: io::Error) -> Self {
        TxError::Io(value)
    }
}

impl Transaction {
    pub(crate) fn new(
        sources: Vec<Source>,
        snapshot_seq: u64,
        schema: &Arc<TableSchema>,
    ) -> io::Result<Self> {
        let mut index: HashMap<Key, (u64, Option<Value>)> = HashMap::new();
        let field_cols: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.semantic == SemanticType::Field)
            .map(|(c, _)| c)
            .collect();
        for mut src in sources {
            while let Some(batch) = src.next_batch()? {
                let view = BatchView::new(&batch, schema);
                for i in 0..batch.num_rows() {
                    let seq = view.seq_value(i) as u64;
                    if seq >= snapshot_seq {
                        continue;
                    }
                    let k = key_at(&view, schema, i);
                    let value = value_at(&view, schema, &field_cols, i);
                    index
                        .entry(k)
                        .and_modify(|(s, cur)| {
                            if seq > *s {
                                *s = seq;
                                *cur = value.clone();
                            }
                        })
                        .or_insert((seq, value));
                }
            }
        }
        Ok(Transaction {
            snapshot_seq,
            index,
            writes: BTreeMap::new(),
            committed: false,
        })
    }

    pub fn write(&mut self, key: Key, value: Value) {
        self.writes.insert(key, Some(value));
    }

    pub fn delete(&mut self, key: Key) {
        self.writes.insert(key, None);
    }

    pub fn get(&self, key: &Key) -> Option<Value> {
        match self.writes.get(key) {
            Some(v) => v.clone(),
            None => self.index.get(key).map(|(_, v)| v.clone())?,
        }
    }

    pub fn snapshot_seq(&self) -> u64 {
        self.snapshot_seq
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tempfile::tempdir;

    fn k(tag: u8, ts: i64) -> Key {
        (vec![tag], ts)
    }
    fn v(s: &str) -> Value {
        s.as_bytes().to_vec()
    }

    fn fresh_region(dir: &std::path::Path) -> Arc<crate::memtable::Region> {
        Arc::new(crate::memtable::Region::new(dir.join("wal.log")).unwrap())
    }

    #[test]
    fn test_txn_uncommitted_invisible_then_visible() {
        let dir = tempdir().unwrap();
        let region = fresh_region(dir.path());
        region.write(k(1, 100), v("base")).unwrap();

        let mut txn = region.begin().unwrap();
        txn.write(k(2, 200), v("pending"));

        assert_eq!(region.get(k(2, 200)).unwrap(), None);

        region.commit(txn).unwrap();
        assert_eq!(
            region.get(k(2, 200)).unwrap(),
            Some(v("pending")),
            "committed write must be visible"
        );
        assert_eq!(region.get(k(1, 100)).unwrap(), Some(v("base")));
    }

    #[test]
    fn test_txn_drop_rolls_back() {
        let dir = tempdir().unwrap();
        let region = fresh_region(dir.path());
        {
            let mut txn = region.begin().unwrap();
            txn.write(k(3, 300), v("doomed"));
        }
        assert_eq!(region.get(k(3, 300)).unwrap(), None);
        assert_eq!(region.len(), 0);
    }

    #[test]
    fn test_txn_read_your_own_writes() {
        let dir = tempdir().unwrap();
        let region = fresh_region(dir.path());
        region.write(k(1, 10), v("old")).unwrap();

        let mut txn = region.begin().unwrap();
        txn.write(k(1, 10), v("mine"));
        txn.delete(k(1, 999));

        assert_eq!(txn.get(&k(1, 10)), Some(v("mine")));
        assert_eq!(txn.get(&k(1, 999)), None);
        assert_eq!(txn.get(&k(1, 11)), None);
    }

    #[test]
    fn test_txn_conflict_aborts_second_committer() {
        let dir = tempdir().unwrap();
        let region = fresh_region(dir.path());

        let mut t1 = region.begin().unwrap();
        let mut t2 = region.begin().unwrap();
        t1.write(k(7, 70), v("first"));
        t2.write(k(7, 70), v("second"));

        region.commit(t1).unwrap();
        match region.commit(t2) {
            Err(TxError::Conflict) => {}
            other => panic!("expected Conflict, got {other:?}"),
        }
        assert_eq!(region.get(k(7, 70)).unwrap(), Some(v("first")));

        let t3 = region.begin().unwrap();
        assert_eq!(t3.get(&k(7, 70)), Some(v("first")));
    }

    #[test]
    fn test_txn_delete_conflicts_too() {
        let dir = tempdir().unwrap();
        let region = fresh_region(dir.path());
        region.write(k(5, 50), v("x")).unwrap();

        let mut t1 = region.begin().unwrap();
        let mut t2 = region.begin().unwrap();
        t1.delete(k(5, 50));
        t2.write(k(5, 50), v("y"));
        region.commit(t1).unwrap();
        assert!(matches!(region.commit(t2), Err(TxError::Conflict)));
        assert_eq!(region.get(k(5, 50)).unwrap(), None);
    }

    #[test]
    fn test_txn_snapshot_isolation_no_dirty_read() {
        let dir = tempdir().unwrap();
        let region = fresh_region(dir.path());
        region.write(k(1, 1), v("v1")).unwrap();

        let txn = region.begin().unwrap();
        region.write(k(1, 2), v("later")).unwrap();
        assert_eq!(txn.get(&k(1, 2)), None);
    }

    #[test]
    fn test_txn_multi_op_atomic_under_concurrent_reader() {
        let dir = tempdir().unwrap();
        let region = fresh_region(dir.path());
        let ka = k(10, 0);
        let kb = k(11, 0);

        let stop = Arc::new(AtomicUsize::new(0));
        let reader = {
            let region = region.clone();
            let stop = stop.clone();
            let ka = ka.clone();
            let kb = kb.clone();
            std::thread::spawn(move || {
                while stop.load(Ordering::Relaxed) == 0 {
                    let a = region.get(ka.clone()).unwrap().is_some();
                    let b = region.get(kb.clone()).unwrap().is_some();
                    assert_eq!(a, b, "multi-op txn observed partially");
                }
            })
        };

        let mut txn = region.begin().unwrap();
        txn.write(ka.clone(), v("a"));
        txn.write(kb.clone(), v("b"));
        region.commit(txn).unwrap();

        stop.store(1, Ordering::Relaxed);
        reader.join().unwrap();
    }

    #[test]
    fn test_txn_concurrent_stress_pair_invariant() {
        let dir = tempdir().unwrap();
        let region = fresh_region(dir.path());
        const WRITERS: usize = 8;
        const ROUNDS: usize = 30;

        let successes: Vec<_> = (0..WRITERS)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect();

        let handles: Vec<_> = (0..WRITERS)
            .map(|w| {
                let region = region.clone();
                let succ = successes[w].clone();
                std::thread::spawn(move || {
                    for r in 0..ROUNDS {
                        let ka = k((w * 2) as u8, r as i64);
                        let kb = k((w * 2 + 1) as u8, r as i64);
                        loop {
                            let mut txn = region.begin().unwrap();
                            txn.write(ka.clone(), v("pa"));
                            txn.write(kb.clone(), v("pb"));
                            match region.commit(txn) {
                                Ok(()) => {
                                    succ.fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                                Err(TxError::Conflict) => continue,
                                Err(TxError::Io(e)) => panic!("io: {e}"),
                            }
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        for w in 0..WRITERS {
            for r in 0..ROUNDS {
                let a = region.get(k((w * 2) as u8, r as i64)).unwrap();
                let b = region.get(k((w * 2 + 1) as u8, r as i64)).unwrap();
                assert_eq!(a.is_some(), b.is_some(), "pair torn at w={w} r={r}");
            }
        }
        let total: usize = successes.iter().map(|s| s.load(Ordering::Relaxed)).sum();
        assert_eq!(total, WRITERS * ROUNDS);
    }

    #[test]
    fn test_txn_wal_replay_whole_frame() {
        let dir = tempdir().unwrap();
        let wal = dir.path().join("wal_replay.log");
        {
            let region = Arc::new(crate::memtable::Region::new(&wal).unwrap());
            let mut txn = region.begin().unwrap();
            txn.write(k(20, 0), v("r1"));
            txn.write(k(21, 0), v("r2"));
            region.commit(txn).unwrap();
            region.close().unwrap();
        }
        let reopened = crate::memtable::Region::new(&wal).unwrap();
        assert_eq!(reopened.get(k(20, 0)).unwrap(), Some(v("r1")));
        assert_eq!(reopened.get(k(21, 0)).unwrap(), Some(v("r2")));
    }
}
