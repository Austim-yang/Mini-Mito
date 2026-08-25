use std::{
    collections::{BTreeMap, HashMap},
    fs::{self},
    io::{self, BufRead, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
    thread::JoinHandle,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::array::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::{
    memtable::{
        Wal,
        columnar::ColumnarMemtable,
        traits::{ImmutableMemtable, Memtable},
        transaction::{Transaction, TxError},
        version::{Source, Version},
        wal::Operation,
    },
    query::merge::MergeBatchIter,
    schema::{BatchView, SemanticType, TableSchema},
    sstable::sstable::{SSTable, SstableIndex, key_at, value_at},
    types::{Key, Value},
};

#[derive(Serialize, Deserialize)]
pub struct ManifestEntry {
    id: usize,
    path: String,
    min_key: Key,
    max_key: Key,
    entry_count: usize,
}

enum Job {
    Flush(Arc<dyn ImmutableMemtable>),
    Compact,
    Sync(mpsc::Sender<()>),
    Shutdown,
}

struct WorkerState {
    version: Arc<Mutex<Arc<Version>>>,
    schema: Arc<TableSchema>,
    base_dir: PathBuf,
    manifest_path: PathBuf,
    sst_id: Arc<AtomicUsize>,
    window_size: Arc<AtomicI64>,
    ttl: Arc<AtomicI64>,
    compact_threshold: Arc<AtomicUsize>,
    error: Arc<Mutex<Option<String>>>,
    pending_deletes: Mutex<Vec<PathBuf>>,
}

impl WorkerState {
    fn record_error(&self, e: io::Error) {
        let mut slot = self.error.lock().unwrap();
        if slot.is_none() {
            *slot = Some(e.to_string());
        }
    }

    fn flush_one(&self, imm: &Arc<dyn ImmutableMemtable>) -> io::Result<()> {
        let id = self.sst_id.load(Ordering::SeqCst);
        let path = self.base_dir.join(format!("{:04}.sst", id));
        let batches = imm.to_batches(&self.schema)?;
        let sst = SSTable::create_from_batches(&batches, id, &path, &self.schema)?;
        self.sst_id.fetch_add(1, Ordering::SeqCst);

        {
            let mut cur = self.version.lock().unwrap();
            let v = (*cur).clone();
            let mut ssts = v.ssts.clone();
            ssts.push(sst);
            ssts.sort_by_key(|s| s.id());
            let immutables: Vec<Arc<dyn ImmutableMemtable>> = v
                .immutables
                .iter()
                .filter(|i| !Arc::ptr_eq(*i, imm))
                .cloned()
                .collect();
            *cur = Arc::new(Version {
                active: v.active.clone(),
                immutables,
                ssts,
                seq: v.seq,
            });
        }
        write_manifest(&self.version, &self.manifest_path)?;
        deferred_remove(imm.wal_path(), &self.pending_deletes);
        Ok(())
    }

    fn compact(&self) -> io::Result<()> {
        let v = self.version.lock().unwrap().clone();
        if v.ssts.len() < self.compact_threshold.load(Ordering::SeqCst) {
            return Ok(());
        }

        let window = self.window_size.load(Ordering::SeqCst).max(1);
        let cutoff = {
            let ttl = self.ttl.load(Ordering::SeqCst);
            if ttl == i64::MIN {
                None
            } else {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?
                    .as_nanos() as i64;
                Some(now.saturating_sub(ttl))
            }
        };

        let mut groups: BTreeMap<i64, Vec<usize>> = BTreeMap::new();
        for (i, sst) in v.ssts.iter().enumerate() {
            let Some((min_ts, _)) = sst.ts_extent() else {
                continue;
            };
            groups.entry(min_ts.div_euclid(window)).or_default().push(i);
        }
        let Some((_, idxs)) = groups
            .iter()
            .find(|(_, idxs)| idxs.len() >= self.compact_threshold.load(Ordering::SeqCst))
        else {
            return Ok(());
        };
        let target_ids: Vec<usize> = idxs.iter().map(|&i| v.ssts[i].id()).collect();

        let old_paths: Vec<PathBuf> = idxs.iter().map(|&i| v.ssts[i].path().clone()).collect();
        let clamp = cutoff.map(|c| (c, i64::MAX));
        let mut sources = Vec::with_capacity(idxs.len());
        for &i in idxs {
            let sst = &v.ssts[i];
            sources.push(Source::Sst(sst.scan_batches(
                sst.min_key(),
                sst.max_key(),
                clamp,
            )?));
        }

        let mut merge = MergeBatchIter::new(sources, self.schema.clone());
        let mut merged_batches: Vec<Arc<RecordBatch>> = Vec::new();
        loop {
            match merge.next_batch()? {
                Some(batch) => merged_batches.push(Arc::new(batch)),
                None => break,
            }
        }

        {
            let mut cur = self.version.lock().unwrap();
            let cv = (*cur).clone();
            let mut ssts: Vec<SSTable> = cv
                .ssts
                .iter()
                .filter(|s| !target_ids.contains(&s.id()))
                .cloned()
                .collect();
            if !merged_batches.is_empty() {
                let id = self.sst_id.load(Ordering::SeqCst);
                let path = self.base_dir.join(format!("{:04}.sst", id));
                let new_sst =
                    SSTable::create_from_batches(&merged_batches, id, &path, &self.schema)?;
                self.sst_id.fetch_add(1, Ordering::SeqCst);
                ssts.push(new_sst);
            }
            ssts.sort_by_key(|s| s.id());
            *cur = Arc::new(Version {
                active: cv.active.clone(),
                immutables: cv.immutables.clone(),
                ssts,
                seq: cv.seq,
            });
        }
        write_manifest(&self.version, &self.manifest_path)?;
        for p in old_paths {
            deferred_remove(&p, &self.pending_deletes);
        }
        Ok(())
    }
}

fn worker_loop(rx: Receiver<Job>, st: Arc<WorkerState>) {
    while let Ok(job) = rx.recv() {
        retry_pending_deletes(&st.pending_deletes);
        if matches!(job, Job::Shutdown) {
            break;
        }
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match job {
            Job::Flush(imm) => {
                if let Err(e) = st.flush_one(&imm) {
                    st.record_error(e);
                }
            }
            Job::Compact => {
                if st.error.lock().unwrap().is_none() {
                    if let Err(e) = st.compact() {
                        st.record_error(e);
                    }
                }
            }
            Job::Sync(tx) => {
                let _ = tx.send(());
            }
            Job::Shutdown => {}
        }));
        if let Err(payload) = outcome {
            let msg = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_else(|| "non-string panic payload".to_string());
            st.record_error(io::Error::new(
                io::ErrorKind::Other,
                format!("worker panicked: {msg}"),
            ));
        }
    }
}

fn write_manifest(version: &Mutex<Arc<Version>>, manifest_path: &Path) -> io::Result<()> {
    let tmp_path = manifest_path.with_extension("tmp");
    let file = fs::File::create(&tmp_path)?;
    let mut writer = io::BufWriter::new(file);
    let ssts = &version.lock().unwrap().clone().ssts;
    for sst in ssts.iter() {
        let entry = super::memtable::ManifestEntry {
            id: sst.id(),
            path: sst
                .path()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            min_key: sst.min_key().clone(),
            max_key: sst.max_key().clone(),
            entry_count: sst.entry_count(),
        };
        let line = serde_json::to_string(&entry)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        writer.write_all(line.as_bytes())?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    fs::rename(&tmp_path, manifest_path)?;
    Ok(())
}

fn deferred_remove(path: &Path, pending: &Mutex<Vec<PathBuf>>) {
    if let Err(e) = fs::remove_file(path) {
        eprintln!("deferring delete of {}: {e}", path.display());
        pending.lock().unwrap().push(path.to_path_buf());
    }
}

fn retry_pending_deletes(pending: &Mutex<Vec<PathBuf>>) {
    let mut list = pending.lock().unwrap();
    if list.is_empty() {
        return;
    }
    list.retain(|p| match fs::remove_file(p) {
        Ok(()) => false,
        Err(_) => true,
    });
}

fn sweep_tmp_files(base_dir: &Path) {
    let Ok(entries) = fs::read_dir(base_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "tmp")
            && let Err(e) = fs::remove_file(&path)
        {
            eprintln!("stale tmp cleanup failed {}: {e}", path.display());
        }
    }
}

pub struct Region {
    version: Arc<Mutex<Arc<Version>>>,
    sst_id: Arc<AtomicUsize>,
    seq: AtomicU64,
    base_dir: PathBuf,
    max_memory_bytes: usize,
    flush_threshold: usize,
    manifest_path: PathBuf,
    schema: Arc<TableSchema>,
    write_gate: Arc<RwLock<()>>,
    commit_gate: Arc<RwLock<()>>,
    ttl: Arc<AtomicI64>,
    window_size: Arc<AtomicI64>,
    compact_threshold: Arc<AtomicUsize>,
    job_tx: SyncSender<Job>,
    worker: Mutex<Option<JoinHandle<()>>>,
    bg_error: Arc<Mutex<Option<String>>>,
}

impl Region {
    pub fn new<P: AsRef<Path>>(wal_path: P) -> io::Result<Self> {
        Self::with_schema(wal_path, Arc::new(TableSchema::default_table()))
    }

    pub fn with_schema<P: AsRef<Path>>(wal_path: P, schema: Arc<TableSchema>) -> io::Result<Self> {
        let base_dir = wal_path
            .as_ref()
            .parent()
            .unwrap_or(Path::new("."))
            .to_path_buf();
        let manifest_path = base_dir.join("manifest");
        let initial_active = Arc::from(Box::new(ColumnarMemtable::with_default_config(
            schema.clone(),
            base_dir.join("wal_000.log"),
        )?) as Box<dyn Memtable>);
        let (job_tx, job_rx) = mpsc::sync_channel::<Job>(8);
        let mut region = Self {
            version: Arc::new(Mutex::new(Arc::new(Version::new(
                initial_active,
                Vec::new(),
                0,
            )))),
            sst_id: Arc::new(AtomicUsize::new(0)),
            seq: AtomicU64::new(0),
            base_dir: base_dir.clone(),
            max_memory_bytes: 1024 * 1024 * 10,
            flush_threshold: 1000,
            manifest_path: manifest_path.clone(),
            schema,
            write_gate: Arc::new(RwLock::new(())),
            commit_gate: Arc::new(RwLock::new(())),
            ttl: Arc::new(AtomicI64::new(i64::MIN)),
            window_size: Arc::new(AtomicI64::new(3_600_000_000_000)),
            compact_threshold: Arc::new(AtomicUsize::new(4)),
            job_tx,
            worker: Mutex::new(None),
            bg_error: Arc::new(Mutex::new(None)),
        };
        let st = Arc::new(WorkerState {
            version: region.version.clone(),
            schema: region.schema.clone(),
            base_dir: region.base_dir.clone(),
            manifest_path: region.manifest_path.clone(),
            sst_id: region.sst_id.clone(),
            window_size: region.window_size.clone(),
            ttl: region.ttl.clone(),
            compact_threshold: region.compact_threshold.clone(),
            error: region.bg_error.clone(),
            pending_deletes: Mutex::new(Vec::new()),
        });
        let handle = std::thread::Builder::new()
            .name("mini-mito-flusher".into())
            .spawn(move || worker_loop(job_rx, st))?;
        region.worker = Mutex::new(Some(handle));
        let mut ssts: Vec<SSTable> = Vec::new();
        region.load_manifest(&mut ssts)?;
        let found = region.scan_sst_files(&mut ssts)?;
        ssts.sort_by_key(|s| s.id());

        let mut wal_files: Vec<PathBuf> = fs::read_dir(&region.base_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                let name = e.file_name();
                name.to_str()
                    .map(|s| s.starts_with("wal_") && s.ends_with(".log"))
                    .unwrap_or(false)
            })
            .map(|e| e.path())
            .collect();
        wal_files.sort();

        let active: Arc<dyn Memtable> = match wal_files.pop() {
            Some(last_wal) => Arc::from(Box::new(ColumnarMemtable::with_default_config(
                region.schema.clone(),
                last_wal,
            )?) as Box<dyn Memtable>),
            None => region.version.lock().unwrap().active.clone(),
        };
        region.merge_orphan_wals(&wal_files, active.as_ref())?;

        let watermark = {
            let mut w = 0u64;
            w = w.max(active.max_seq());
            for sst in ssts.iter() {
                w = w.max(sst.max_seq());
            }
            w
        };
        region.seq.store(watermark + 1, Ordering::SeqCst);

        region.swap_version(Version::new(active, ssts, watermark));

        if !region.manifest_path.exists() || found > 0 {
            region.write_manifest()?;
        }
        sweep_tmp_files(&region.base_dir);
        Ok(region)
    }

    fn swap_version(&self, v: Version) {
        *self.version.lock().unwrap() = Arc::new(v);
    }

    pub fn schema(&self) -> Arc<TableSchema> {
        self.schema.clone()
    }

    fn merge_orphan_wals(&self, wal_files: &[PathBuf], active: &dyn Memtable) -> io::Result<()> {
        for path in wal_files {
            let wal = Wal::new(path)?;
            let mut latest: HashMap<Key, (u64, Option<Value>)> = HashMap::new();
            wal.recover(&mut |op: &Operation| {
                let (key, seq, value) = match op {
                    Operation::Insert { key, seq, value }
                    | Operation::Update { key, seq, value } => (key, *seq, Some(value.clone())),
                    Operation::Delete { key, seq } => (key, *seq, None),
                };
                match latest.get_mut(key) {
                    Some(e) if e.0 >= seq => {}
                    _ => {
                        latest.insert(key.clone(), (seq, value));
                    }
                }
            })?;
            if latest.is_empty() {
                if let Err(e) = fs::remove_file(path) {
                    eprintln!("orphan wal cleanup failed {}: {e}", path.display());
                }
                continue;
            }
            for (key, (seq, value)) in latest {
                match active.get(&key)? {
                    Some((s, _)) if s >= seq => {}
                    _ => {
                        active.write(key, seq, value)?;
                    }
                }
            }
            if let Err(e) = fs::remove_file(path) {
                eprintln!("orphan wal cleanup failed {}: {e}", path.display());
            }
        }
        Ok(())
    }

    fn scan_sst_files(&mut self, ssts: &mut Vec<SSTable>) -> io::Result<usize> {
        let mut files: Vec<PathBuf> = fs::read_dir(&self.base_dir)?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .map(|s| s.ends_with(".sst"))
                    .unwrap_or(false)
            })
            .map(|e| e.path())
            .collect();

        let existing: Vec<PathBuf> = ssts.iter().map(|s| s.path().clone()).collect();

        files.sort_by_key(|p| {
            p.file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(usize::MAX)
        });

        let max_manifest_id = ssts.iter().map(|s| s.id()).max();
        let mut added = 0;
        for path in files {
            if existing.contains(&path) {
                continue;
            }
            let Some(id) = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<usize>().ok())
            else {
                continue;
            };
            if max_manifest_id.is_some_and(|max| id <= max) {
                eprintln!(
                    "ignoring stale untracked sst {} (id <= manifest max)",
                    path.display()
                );
                continue;
            }
            let sst = SSTable::open_from_path(&path, &self.schema)?;
            let current = self.sst_id.load(Ordering::SeqCst);
            if id >= current {
                self.sst_id.store(id + 1, Ordering::SeqCst);
            }
            ssts.push(sst);
            added += 1;
        }
        ssts.sort_by_key(|s| s.id());
        Ok(added)
    }

    fn load_manifest(&mut self, ssts: &mut Vec<SSTable>) -> io::Result<()> {
        if !self.manifest_path.exists() {
            return Ok(());
        }
        let file = fs::File::open(&self.manifest_path)?;
        let reader = io::BufReader::new(file);
        let mut entries = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            let entry: super::memtable::ManifestEntry = serde_json::from_str(&line)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            entries.push(entry);
        }
        entries.sort_by_key(|e| e.id);
        for entry in entries {
            let path = self.base_dir.join(&entry.path);
            if path.exists() {
                let index = SstableIndex::load_from_file(&path)?;
                let sst = SSTable::new(
                    entry.id,
                    path,
                    entry.min_key,
                    entry.max_key,
                    entry.entry_count,
                    self.schema.clone(),
                    index,
                );
                ssts.push(sst);
                let current = self.sst_id.load(Ordering::SeqCst);
                if entry.id >= current {
                    self.sst_id.store(entry.id + 1, Ordering::SeqCst);
                }
            }
        }
        Ok(())
    }

    fn write_manifest(&self) -> io::Result<()> {
        write_manifest(&self.version, &self.manifest_path)
    }

    pub fn write(&self, key: Key, value: Value) -> io::Result<Option<Value>> {
        self.write_inner(key, Some(value))
    }

    pub fn delete(&self, key: Key) -> io::Result<Option<Value>> {
        self.write_inner(key, None)
    }

    pub fn write_batch(&self, entries: Vec<(Key, Option<Value>)>) -> io::Result<()> {
        let _commit_exclusive = self.commit_gate.write().unwrap();
        let n = entries.len() as u64;
        let start = self.seq.fetch_add(n, Ordering::SeqCst);
        let entries: Vec<(Key, u64, Option<Value>)> = entries
            .into_iter()
            .enumerate()
            .map(|(i, (key, value))| (key, start + i as u64, value))
            .collect();
        {
            let _gate = self.write_gate.read().unwrap();
            let v = self.version.lock().unwrap().clone();
            v.active.write_batch(entries)?;
        }
        drop(_commit_exclusive);
        self.maybe_flush()?;
        self.maybe_compact()?;
        Ok(())
    }

    fn write_inner(&self, key: Key, value: Option<Value>) -> io::Result<Option<Value>> {
        let result = {
            let _commit_shared = self.commit_gate.read().unwrap();
            let seq = self.seq.fetch_add(1, Ordering::SeqCst);
            let _gate = self.write_gate.read().unwrap();
            let v = self.version.lock().unwrap().clone();
            v.active.write(key, seq, value)?
        };
        self.maybe_flush()?;
        self.maybe_compact()?;
        Ok(result)
    }

    pub fn begin(self: &Arc<Self>) -> io::Result<Transaction> {
        let _commit_exclusive = self.commit_gate.write().unwrap();
        let snapshot_seq = self.seq.load(Ordering::SeqCst);
        let sources = self.snapshot_columnar_sources_inner(None, None)?;
        Transaction::new(sources, snapshot_seq, &self.schema)
    }

    pub fn commit(&self, mut txn: Transaction) -> Result<(), TxError> {
        if txn.writes.is_empty() {
            txn.committed = true;
            return Ok(());
        }
        let _cg = self.commit_gate.write().unwrap();
        let _wg = self.write_gate.read().unwrap();
        let v = self.version.lock().unwrap().clone();

        for key in txn.writes.keys() {
            if let Some(cur) = self.visible_seq(&v, key)? {
                if cur >= txn.snapshot_seq {
                    return Err(TxError::Conflict);
                }
            }
        }

        let start = self
            .seq
            .fetch_add(txn.writes.len() as u64, Ordering::SeqCst);
        let entries: Vec<(Key, u64, Option<Value>)> = txn
            .writes
            .iter()
            .enumerate()
            .map(|(i, (key, value))| (key.clone(), start + i as u64, value.clone()))
            .collect();
        v.active.write_batch(entries)?;

        txn.committed = true;
        drop(_cg);
        drop(_wg);
        self.maybe_flush()?;
        self.maybe_compact()?;
        Ok(())
    }

    fn visible_seq(&self, v: &Arc<Version>, key: &Key) -> io::Result<Option<u64>> {
        let mut best: Option<u64> = None;
        if let Some((s, _)) = v.active.get(key)? {
            best = Some(s);
        }
        for imm in v.immutables.iter().rev() {
            if imm.max_seq() > best.unwrap_or(0)
                && let Some((s, _)) = imm.get(key)?
            {
                best = Some(best.map_or(s, |b| b.max(s)));
            }
        }
        for sst in v.ssts.iter().rev() {
            if sst.max_seq() > best.unwrap_or(0)
                && let Some((s, _)) = sst.get(key)?
            {
                best = Some(best.map_or(s, |b| b.max(s)));
            }
        }
        Ok(best)
    }

    fn freeze_active(&self) -> io::Result<Option<Arc<dyn ImmutableMemtable>>> {
        let _gate = self.write_gate.write().unwrap();
        let v = self.version.lock().unwrap().clone();
        if v.active.len() == 0 {
            return Ok(None);
        }
        let new_active: Arc<dyn Memtable> = Arc::from(v.active.fork()?);
        let imm: Arc<dyn ImmutableMemtable> = Arc::from(v.active.freeze()?);
        {
            let mut cur = self.version.lock().unwrap();
            let cv = (**cur).clone();
            let mut immutables = cv.immutables;
            immutables.push(imm.clone());
            *cur = Arc::new(Version {
                active: new_active,
                immutables,
                ssts: cv.ssts,
                seq: cv.seq,
            });
        }
        Ok(Some(imm))
    }

    fn maybe_flush(&self) -> io::Result<()> {
        let v = self.version.lock().unwrap().clone();
        let active_len = v.active.len();
        let total = v.active.estimated_size()
            + v.immutables
                .iter()
                .map(|i| i.estimated_size())
                .sum::<usize>();
        if total > self.max_memory_bytes || active_len >= self.flush_threshold {
            if let Some(imm) = self.freeze_active()? {
                self.enqueue(Job::Flush(imm))?;
            }
        }
        Ok(())
    }

    fn maybe_compact(&self) -> io::Result<()> {
        let v = self.version.lock().unwrap().clone();
        let n = v.ssts.len() + v.immutables.len();
        if n >= self.compact_threshold.load(Ordering::SeqCst) {
            self.enqueue(Job::Compact)?;
        }
        Ok(())
    }

    pub fn set_flush_threshold(&mut self, threshold: usize) {
        self.flush_threshold = threshold;
    }

    pub fn set_ttl(&mut self, ttl: Option<i64>) {
        self.ttl.store(ttl.unwrap_or(i64::MIN), Ordering::SeqCst);
    }

    pub fn set_window_size(&mut self, w: i64) {
        self.window_size.store(w.max(1), Ordering::SeqCst);
    }

    pub fn set_compact_threshold(&mut self, t: usize) {
        self.compact_threshold.store(t, Ordering::SeqCst);
    }

    pub fn ttl_cutoff(&self) -> Option<i64> {
        let ttl = self.ttl.load(Ordering::SeqCst);
        if ttl == i64::MIN {
            return None;
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        Some(now.saturating_sub(ttl))
    }

    pub fn sst_id(&self) -> usize {
        self.sst_id.load(Ordering::SeqCst)
    }

    pub fn get(&self, key: Key) -> io::Result<Option<Value>> {
        let _commit_shared = self.commit_gate.read().unwrap();
        if let Some(c) = self.ttl_cutoff()
            && key.1 < c
        {
            return Ok(None);
        }

        let v = self.version.lock().unwrap().clone();
        let mut best: Option<(u64, Option<Value>)> = None;

        if v.active.max_seq() > best.as_ref().map(|(s, _)| *s).unwrap_or(0)
            && let Some(e) = v.active.get(&key)?
        {
            best = Some(e);
        }

        for imm in v.immutables.iter().rev() {
            if imm.max_seq() > best.as_ref().map(|(s, _)| *s).unwrap_or(0)
                && let Some(e) = imm.get(&key)?
            {
                best = Some(e);
            }
        }

        for sst in v.ssts.iter().rev() {
            if sst.max_seq() > best.as_ref().map(|(s, _)| *s).unwrap_or(0)
                && let Some(e) = sst.get(&key)?
            {
                best = Some(e);
            }
        }

        Ok(best.map(|(_, v)| v).flatten())
    }

    pub fn flush(&self) -> io::Result<()> {
        if let Some(imm) = self.freeze_active()? {
            self.enqueue(Job::Flush(imm))?;
        }
        self.flush_barrier()
    }

    pub fn compact(&self) -> io::Result<()> {
        self.enqueue(Job::Compact)?;
        self.flush_barrier()
    }

    pub fn len(&self) -> usize {
        let v = self.version.lock().unwrap().clone();
        let imm_len: usize = v.immutables.iter().map(|i| i.len()).sum();
        let sst_len: usize = v.ssts.iter().map(|s| s.entry_count()).sum();
        v.active.len() + imm_len + sst_len
    }

    pub fn estimated_total_memory(&self) -> usize {
        let v = self.version.lock().unwrap().clone();
        v.active.estimated_size()
            + v.immutables
                .iter()
                .map(|i| i.estimated_size())
                .sum::<usize>()
    }

    pub fn get_immutable_ssts(&self) -> Vec<SSTable> {
        self.version.lock().unwrap().clone().ssts.clone()
    }

    pub fn iter_all_data(&self) -> io::Result<impl Iterator<Item = (Key, Option<Value>)>> {
        let sources = self.snapshot_columnar_sources(None, None)?;
        let schema = Arc::new(TableSchema::clone(&self.schema));
        let mut map: BTreeMap<Key, (u64, Option<Value>)> = BTreeMap::new();
        let field_cols: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.semantic == SemanticType::Field)
            .map(|(c, _)| c)
            .collect();
        for mut src in sources {
            while let Some(batch) = src.next_batch()? {
                let view = BatchView::new(&batch, &schema);
                for i in 0..batch.num_rows() {
                    let k = key_at(&view, &schema, i);
                    let seq = view.seq_value(i) as u64;
                    let value = value_at(&view, &schema, &field_cols, i);
                    map.entry(k)
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
        Ok(map.into_iter().map(|(k, (_, value))| (k, value)))
    }

    pub fn snapshot_columnar_sources(
        &self,
        bounds: Option<(i64, i64)>,
        user_cols: Option<&[usize]>,
    ) -> io::Result<Vec<Source>> {
        let _commit_shared = self.commit_gate.read().unwrap();
        self.snapshot_columnar_sources_inner(bounds, user_cols)
    }

    fn snapshot_columnar_sources_inner(
        &self,
        bounds: Option<(i64, i64)>,
        user_cols: Option<&[usize]>,
    ) -> io::Result<Vec<Source>> {
        let v = self.version.lock().unwrap().clone();
        let mut out = Vec::new();
        if v.active.len() > 0 {
            let batches = v.active.to_batches(&self.schema)?;
            out.push(Source::memtable(batches));
        }

        for imm in v.immutables.iter().rev() {
            let batches = imm.to_batches(&self.schema)?;
            out.push(Source::memtable(batches));
        }

        for sst in v.ssts.iter().rev() {
            let overlaps = bounds.map_or(true, |(low, high)| {
                sst.ts_extent()
                    .map_or(true, |(s_low, s_high)| s_high >= low && s_low <= high)
            });
            if !overlaps {
                continue;
            }
            let ts_range = bounds.filter(|&(low, high)| !(low == i64::MIN && high == i64::MAX));
            let iter = match user_cols {
                Some(cols) => {
                    sst.scan_batches_projected(sst.min_key(), sst.max_key(), ts_range, cols)?
                }
                None => sst.scan_batches(sst.min_key(), sst.max_key(), ts_range)?,
            };
            out.push(Source::Sst(iter));
        }

        Ok(out)
    }

    pub fn close(&self) -> io::Result<()> {
        self.flush_barrier()?;
        self.shutdown_worker();
        self.check_bg_error()?;
        let v = self.version.lock().unwrap().clone();
        v.active.close()
    }

    fn enqueue(&self, job: Job) -> io::Result<()> {
        self.check_bg_error()?;
        self.job_tx
            .send(job)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "flusher terminated"))
    }

    fn check_bg_error(&self) -> io::Result<()> {
        let guard = self.bg_error.lock().unwrap();
        match &*guard {
            Some(msg) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("background flusher failed: {msg}"),
            )),
            None => Ok(()),
        }
    }

    pub fn flush_barrier(&self) -> io::Result<()> {
        self.check_bg_error()?;
        let (tx, rx) = mpsc::channel();
        self.enqueue(Job::Sync(tx))?;
        rx.recv().map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "flusher terminated before barrier",
            )
        })?;
        self.check_bg_error()
    }

    fn shutdown_worker(&self) {
        if let Some(handle) = self.worker.lock().unwrap().take() {
            let _ = self.job_tx.send(Job::Shutdown);
            let _ = handle.join();
        }
    }
}

impl std::fmt::Debug for Region {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let v = self.version.lock().unwrap().clone();
        f.debug_struct("Region")
            .field("sst_id", &self.sst_id.load(Ordering::SeqCst))
            .field("seq", &self.seq.load(Ordering::SeqCst))
            .field("base_dir", &self.base_dir)
            .field("max_memory_bytes", &self.max_memory_bytes)
            .field("flush_threshold", &self.flush_threshold)
            .field("ssts_count", &v.ssts.len())
            .field("immutables_count", &v.immutables.len())
            .finish()
    }
}

impl Drop for Region {
    fn drop(&mut self) {
        let _ = self.flush_barrier();
        self.shutdown_worker();
        let _ = self.check_bg_error();
        if let Ok(v) = self.version.lock() {
            let _ = v.active.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::schema::{ColumnDef, SemanticType};
    use arrow_schema::DataType;
    use tempfile::tempdir;

    fn k(tag: u8, ts: i64) -> Key {
        (vec![tag], ts)
    }
    fn v(s: &str) -> Value {
        s.as_bytes().to_vec()
    }

    fn now_nanos() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64
    }

    #[test]
    fn test_memtable_insert_get_remove() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");
        let region = Region::new(&path).unwrap();
        assert_eq!(region.len(), 0);

        assert_eq!(region.write(k(1, 0), v("one")).unwrap(), None);
        assert_eq!(region.write(k(2, 0), v("two")).unwrap(), None);
        assert_eq!(region.len(), 2);

        assert_eq!(region.get(k(1, 0)).unwrap(), Some(v("one")));
        assert_eq!(region.get(k(3, 0)).unwrap(), None);

        assert_eq!(region.write(k(1, 0), v("uno")).unwrap(), Some(v("one")));
        assert_eq!(region.get(k(1, 0)).unwrap(), Some(v("uno")));

        assert_eq!(region.delete(k(2, 0)).unwrap(), Some(v("two")));
        assert_eq!(region.len(), 4);
        assert_eq!(region.get(k(2, 0)).unwrap(), None);
        assert_eq!(region.delete(k(3, 0)).unwrap(), None);

        region.close().unwrap();
    }

    #[test]
    fn test_memtable_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");

        {
            let region = Region::new(&path).unwrap();
            region.write(k(1, 0), v("one")).unwrap();
            region.write(k(2, 0), v("two")).unwrap();
            region.close().unwrap();
        }

        {
            let region = Region::new(&path).unwrap();
            assert_eq!(region.len(), 2);
            assert_eq!(region.get(k(1, 0)).unwrap(), Some(v("one")));
            assert_eq!(region.get(k(2, 0)).unwrap(), Some(v("two")));
        }
    }

    #[test]
    fn test_memtable_empty_recover() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.log");

        {
            let region = Region::new(&path).unwrap();
            assert_eq!(region.len(), 0);
            region.close().unwrap();
        }

        {
            let region = Region::new(&path).unwrap();
            assert_eq!(region.len(), 0);
        }
    }

    #[test]
    fn test_memtable_flush() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");
        let region = Region::new(&path).unwrap();
        region.write(k(1, 0), v("one")).unwrap();
        region.flush().unwrap();
        region.close().unwrap();

        let region2 = Region::new(&path).unwrap();
        assert_eq!(region2.get(k(1, 0)).unwrap(), Some(v("one")));
    }

    #[test]
    fn test_memtable_flush_multiple() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let mut region = Region::new(&wal_path)?;
        region.set_flush_threshold(2);

        region.write(k(1, 0), v("a"))?;
        region.write(k(2, 0), v("b"))?;
        region.flush_barrier()?;
        assert_eq!(region.get_immutable_ssts().len(), 1);

        region.write(k(3, 0), v("c"))?;
        region.write(k(4, 0), v("d"))?;
        region.flush_barrier()?;
        assert_eq!(region.get_immutable_ssts().len(), 2);

        assert_eq!(region.get(k(1, 0))?, Some(v("a")));
        assert_eq!(region.get(k(4, 0))?, Some(v("d")));
        Ok(())
    }

    #[test]
    fn test_memtable_manifest_fallback_scan() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let manifest_path = dir.path().join("manifest");

        {
            let mut region = Region::new(&wal_path)?;
            region.set_flush_threshold(2);

            region.write(k(1, 0), v("a"))?;
            region.write(k(2, 0), v("b"))?;
            region.write(k(3, 0), v("c"))?;
            region.write(k(4, 0), v("d"))?;

            region.flush()?;
            assert!(region.get_immutable_ssts().len() >= 2);
            assert!(manifest_path.exists());
            region.close()?;
        }

        fs::remove_file(&manifest_path)?;
        assert!(!manifest_path.exists());

        {
            let region = Region::new(&wal_path)?;
            assert!(region.get_immutable_ssts().len() >= 2);
            assert_eq!(region.get(k(1, 0))?, Some(v("a")));
            assert_eq!(region.get(k(2, 0))?, Some(v("b")));
            assert_eq!(region.get(k(3, 0))?, Some(v("c")));
            assert_eq!(region.get(k(4, 0))?, Some(v("d")));
            assert!(manifest_path.exists());
        }

        {
            let region = Region::new(&wal_path)?;
            assert!(region.get_immutable_ssts().len() >= 2);
            assert_eq!(region.get(k(1, 0))?, Some(v("a")));
            assert_eq!(region.get(k(4, 0))?, Some(v("d")));
        }

        Ok(())
    }

    #[test]
    fn test_compaction() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let mut region = Region::new(&wal_path)?;
        region.set_flush_threshold(2);
        region.set_compact_threshold(2);

        for i in 0..8 {
            let key = (vec![i as u8], i as i64);
            region.write(key, format!("v{}", i).into_bytes())?;
        }
        region.flush_barrier()?;
        assert_eq!(region.get_immutable_ssts().len(), 1);

        for i in 0..8 {
            let key = (vec![i as u8], i as i64);
            assert_eq!(region.get(key)?, Some(format!("v{}", i).into_bytes()));
        }

        region.compact()?;
        assert_eq!(region.get_immutable_ssts().len(), 1);

        for i in 0..8 {
            let key = (vec![i as u8], i as i64);
            assert_eq!(region.get(key)?, Some(format!("v{}", i).into_bytes()));
        }

        region.delete(k(3, 0))?;
        region.delete(k(5, 0))?;
        region.write(k(8, 0), v("v8"))?;
        region.write(k(9, 0), v("v9"))?;
        region.write(k(10, 0), v("v10"))?;
        region.write(k(11, 0), v("v11"))?;
        region.flush_barrier()?;
        assert_eq!(region.get_immutable_ssts().len(), 1);
        region.compact()?;
        assert_eq!(region.get_immutable_ssts().len(), 1);

        assert_eq!(region.get(k(3, 0))?, None);
        assert_eq!(region.get(k(5, 0))?, None);
        assert_eq!(region.get(k(0, 0))?, Some(v("v0")));
        assert_eq!(region.get(k(8, 0))?, Some(v("v8")));
        assert_eq!(region.get(k(9, 0))?, Some(v("v9")));
        assert_eq!(region.get(k(10, 0))?, Some(v("v10")));
        assert_eq!(region.get(k(11, 0))?, Some(v("v11")));

        Ok(())
    }

    fn schema3() -> TableSchema {
        TableSchema {
            columns: vec![
                ColumnDef {
                    name: "host".into(),
                    data_type: DataType::Utf8,
                    semantic: SemanticType::Tag,
                },
                ColumnDef {
                    name: "cpu".into(),
                    data_type: DataType::Utf8,
                    semantic: SemanticType::Tag,
                },
                ColumnDef {
                    name: "timestamp".into(),
                    data_type: DataType::Int64,
                    semantic: SemanticType::Timestamp,
                },
                ColumnDef {
                    name: "value".into(),
                    data_type: DataType::Float64,
                    semantic: SemanticType::Field,
                },
                ColumnDef {
                    name: "note".into(),
                    data_type: DataType::Utf8,
                    semantic: SemanticType::Field,
                },
            ],
            primary_key: vec![0, 1],
            time_index: 2,
        }
    }

    fn mkkey(s: &TableSchema, host: &str, cpu: &str, ts: i64) -> Key {
        s.key(&[host.as_bytes().to_vec(), cpu.as_bytes().to_vec()], ts)
    }
    fn mkval(s: &TableSchema, value: f64, note: &str) -> Value {
        s.value(&[value.to_le_bytes().to_vec(), note.as_bytes().to_vec()])
    }

    #[test]
    fn test_manager_multi_tag_flush_compact() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let schema = Arc::new(schema3());
        let mut region = Region::with_schema(&wal_path, schema.clone())?;
        region.set_flush_threshold(2);

        let rows = vec![
            (("h1", "c1", 10), 1.5, "a"),
            (("h1", "c2", 10), 2.5, "b"),
            (("h1", "c1", 20), 3.5, "c"),
            (("h2", "c1", 10), 4.5, "d"),
            (("h2", "c2", 10), 5.5, "e"),
            (("h2", "c2", 20), 6.5, "f"),
            (("h1", "c2", 30), 7.5, "g"),
            (("h2", "c1", 30), 8.5, "h"),
        ];
        for ((host, cpu, ts), value, note) in rows.clone() {
            region.write(mkkey(&schema, host, cpu, ts), mkval(&schema, value, note))?;
        }
        region.flush()?;
        assert_eq!(region.get_immutable_ssts().len(), 1);

        region.compact()?;
        assert_eq!(region.get_immutable_ssts().len(), 1);

        for ((host, cpu, ts), value, note) in rows.clone() {
            let got = region.get(mkkey(&schema, host, cpu, ts))?;
            assert_eq!(
                got,
                Some(mkval(&schema, value, note)),
                "{} {} {}",
                host,
                cpu,
                ts
            );
        }
        assert_eq!(region.get(mkkey(&schema, "h1", "c1", 99))?, None);

        region.delete(mkkey(&schema, "h1", "c1", 20))?;
        region.flush()?;
        region.compact()?;
        assert_eq!(region.get(mkkey(&schema, "h1", "c1", 20))?, None);
        assert_eq!(
            region.get(mkkey(&schema, "h2", "c1", 10))?,
            Some(mkval(&schema, 4.5, "d"))
        );

        Ok(())
    }

    #[test]
    fn test_get_newest_write_wins_across_flush() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let mut region = Region::new(&wal_path)?;
        region.set_flush_threshold(2);
        region.write(k(1, 0), v("old"))?;
        region.write(k(2, 0), v("b"))?;
        region.write(k(1, 0), v("new"))?;

        assert_eq!(region.get(k(1, 0))?, Some(v("new")));
        region.flush()?;
        assert_eq!(region.get(k(1, 0))?, Some(v("new")));
        region.compact()?;
        assert_eq!(region.get(k(1, 0))?, Some(v("new")));
        assert_eq!(region.get(k(2, 0))?, Some(v("b")));
        Ok(())
    }

    #[test]
    fn test_seq_survives_restart_and_compact() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");

        {
            let region = Region::new(&wal_path)?;
            region.write(k(1, 0), v("a"))?;
            region.write(k(2, 0), v("b"))?;
            region.flush()?;
            let ssts = region.get_immutable_ssts();
            assert_eq!(ssts.len(), 1);
            assert_eq!(ssts[0].max_seq(), 2);
            region.close()?;
        }

        {
            let region = Region::new(&wal_path)?;
            region.write(k(3, 0), v("c"))?;
            region.flush()?;
            let ssts = region.get_immutable_ssts();
            assert!(ssts.iter().any(|s| s.max_seq() == 3));

            assert_eq!(region.get(k(1, 0))?, Some(v("a")));
            assert_eq!(region.get(k(2, 0))?, Some(v("b")));
            assert_eq!(region.get(k(3, 0))?, Some(v("c")));
            Ok(())
        }
    }

    #[test]
    fn test_region_reader_never_sees_partial_batch_during_flush() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let region = Arc::new(Region::new(&wal_path)?);

        let writer = {
            let region = region.clone();
            std::thread::spawn(move || -> io::Result<()> {
                for b in 0u8..40 {
                    for i in 0..64u64 {
                        region.write(k(b, i as i64), format!("b{}i{}", b, i).into_bytes())?;
                    }
                    region.write(k(0xFF, b as i64), vec![b])?;
                }
                Ok(())
            })
        };
        let reader = {
            let region = region.clone();
            std::thread::spawn(move || -> io::Result<()> {
                for _ in 0..400 {
                    for b in 0u8..40 {
                        if let Some(marker) = region.get(k(0xFF, b as i64))? {
                            let mb = marker[0];
                            for i in 0..64u64 {
                                let expect = format!("b{}i{}", mb, i).into_bytes();
                                if region.get(k(mb, i as i64))? != Some(expect) {
                                    return Err(io::Error::other("reader saw partial batch"));
                                }
                            }
                        }
                    }
                }
                Ok(())
            })
        };
        let flusher = {
            let region = region.clone();
            std::thread::spawn(move || -> io::Result<()> {
                for _ in 0..300 {
                    region.flush()?;
                }
                Ok(())
            })
        };

        writer.join().unwrap()?;
        flusher.join().unwrap()?;
        reader.join().unwrap()?;

        for b in 0u8..40 {
            for i in 0..64u64 {
                assert_eq!(
                    region.get(k(b, i as i64))?,
                    Some(format!("b{}i{}", b, i).into_bytes())
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_region_snapshot_consistent_during_flush() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let region = Arc::new(Region::new(&wal_path)?);
        for i in 0..5000u64 {
            region.write(
                k((i % 250) as u8, (i / 250) as i64),
                i.to_le_bytes().to_vec(),
            )?;
        }

        let snapshot = region.snapshot_columnar_sources(None, None)?;
        let flusher = {
            let region = region.clone();
            std::thread::spawn(move || {
                for _ in 0..10 {
                    region.flush().unwrap();
                }
            })
        };

        let mut seen = std::collections::HashSet::new();
        let schema = TableSchema::default_table();
        for mut src in snapshot {
            while let Some(batch) = src.next_batch()? {
                let view = BatchView::new(&batch, &schema);
                for i in 0..batch.num_rows() {
                    seen.insert(key_at(&view, &schema, i));
                }
            }
        }
        flusher.join().unwrap();

        assert_eq!(seen.len(), 5000);
        for i in 0..5000u64 {
            assert_eq!(
                region.get(k((i % 250) as u8, (i / 250) as i64))?,
                Some(i.to_le_bytes().to_vec())
            );
        }
        Ok(())
    }

    #[test]
    fn test_region_concurrent_flush_reopen_no_loss() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        {
            let region = Arc::new(Region::new(&wal_path)?);
            let mut handles = Vec::new();
            for t in 0..4u64 {
                let region = region.clone();
                handles.push(std::thread::spawn(move || -> io::Result<()> {
                    for i in 0..250u64 {
                        region.write(k(t as u8, i as i64), i.to_le_bytes().to_vec())?;
                    }
                    Ok(())
                }));
            }
            for h in handles {
                h.join().unwrap()?;
            }
            region.flush()?;
            region.close()?;
        }
        let region = Region::new(&wal_path)?;
        for t in 0..4u64 {
            for i in 0..250u64 {
                assert_eq!(
                    region.get(k(t as u8, i as i64))?,
                    Some(i.to_le_bytes().to_vec())
                );
            }
        }
        Ok(())
    }

    #[test]
    fn test_region_sources_with_range_prunes_ssts() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let region = Region::new(&wal_path)?;
        for i in 0..200i64 {
            region.write(k(1, 1000 + i), v("low"))?;
        }
        region.flush()?;
        for i in 0..200i64 {
            region.write(k(1, 5000 + i), v("high"))?;
        }
        region.flush()?;

        let sources = region.snapshot_columnar_sources(Some((5000, 5100)), None)?;
        let mut rows = Vec::new();
        let schema = TableSchema::default_table();
        let field_cols: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.semantic == SemanticType::Field)
            .map(|(c, _)| c)
            .collect();
        for mut src in sources {
            while let Some(batch) = src.next_batch()? {
                let view = BatchView::new(&batch, &schema);
                for i in 0..batch.num_rows() {
                    rows.push((
                        key_at(&view, &schema, i),
                        value_at(&view, &schema, &field_cols, i),
                    ));
                }
            }
        }
        assert_eq!(rows.len(), 101);
        assert!(rows.iter().all(|(k, _)| k.1 >= 5000 && k.1 <= 5100));
        Ok(())
    }

    #[test]
    fn test_twcs_does_not_merge_across_windows() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let mut region = Region::new(&wal_path)?;
        region.set_flush_threshold(2);
        region.set_compact_threshold(4);
        region.set_window_size(10);

        for i in 0..8i64 {
            region.write(k(1, i), v("old"))?;
        }
        region.flush_barrier()?;
        let win0 = region.get_immutable_ssts();
        assert_eq!(win0.len(), 1);
        assert_eq!(win0[0].ts_extent(), Some((0, 7)));

        for i in 20..28i64 {
            region.write(k(1, i), v("new"))?;
        }
        region.flush_barrier()?;
        let ssts = region.get_immutable_ssts();
        assert_eq!(ssts.len(), 2);
        for sst in &ssts {
            let (lo, hi) = sst.ts_extent().unwrap();
            assert!((lo < 10 && hi <= 9) || lo >= 20, "merged across windows");
        }
        assert_eq!(region.get(k(1, 5))?, Some(v("old")));
        assert_eq!(region.get(k(1, 25))?, Some(v("new")));
        Ok(())
    }

    #[test]
    fn test_ttl_compact_removes_expired() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let mut region = Region::new(&wal_path)?;
        region.set_flush_threshold(2);
        region.set_compact_threshold(4);
        region.set_ttl(Some(1_000_000_000));
        let expired = now_nanos() - 60_000_000_000;

        for i in 0..8i64 {
            region.write(k(1, expired + i), v("expired"))?;
        }
        region.flush_barrier()?;
        assert_eq!(region.get_immutable_ssts().len(), 0);
        assert_eq!(region.get(k(1, expired))?, None);
        assert_eq!(region.get(k(1, expired + 7))?, None);

        let fresh = now_nanos();
        region.write(k(1, fresh), v("fresh"))?;
        assert_eq!(region.get(k(1, fresh))?, Some(v("fresh")));
        Ok(())
    }

    #[test]
    fn test_ttl_read_clamp_without_compact() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let mut region = Region::new(&wal_path)?;
        region.set_ttl(Some(1_000_000_000));
        let now = now_nanos();

        region.write(k(1, now - 60_000_000_000), v("old"))?;
        region.write(k(1, now), v("fresh"))?;

        assert_eq!(region.get(k(1, now - 60_000_000_000))?, None);
        assert_eq!(region.get(k(1, now))?, Some(v("fresh")));
        Ok(())
    }

    #[test]
    fn test_ttl_tombstone_and_seq() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let mut region = Region::new(&wal_path)?;
        region.set_flush_threshold(2);
        region.set_compact_threshold(2);
        region.set_ttl(Some(1_000_000_000));
        let now = now_nanos();

        region.write(k(1, now), v("a"))?;
        region.flush()?;
        region.delete(k(1, now))?;
        region.flush()?;
        assert_eq!(region.get(k(1, now))?, None);
        region.compact()?;
        assert_eq!(region.get(k(1, now))?, None);

        let expired = now - 60_000_000_000;
        region.write(k(2, expired), v("b"))?;
        region.flush()?;
        region.delete(k(2, expired))?;
        region.flush()?;
        assert_eq!(region.get(k(2, expired))?, None);
        region.compact()?;
        assert_eq!(region.get(k(2, expired))?, None);
        let mut found = false;
        let schema = TableSchema::default_table();
        for sst in region.get_immutable_ssts() {
            for batch in sst.scan_batches(sst.min_key(), sst.max_key(), None)? {
                let batch = batch?;
                let view = BatchView::new(&batch, &schema);
                for i in 0..batch.num_rows() {
                    if key_at(&view, &schema, i) == k(2, expired) {
                        found = true;
                    }
                }
            }
        }
        assert!(!found, "expired tombstone should be physically removed");
        Ok(())
    }

    #[test]
    fn test_empty_window_compact_no_orphan_sst() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let mut region = Region::new(&wal_path)?;
        region.set_flush_threshold(2);
        region.set_compact_threshold(2);
        region.set_ttl(Some(1_000_000_000));
        let expired = now_nanos() - 60_000_000_000;

        for i in 0..4i64 {
            region.write(k(1, expired + i), v("x"))?;
        }
        region.flush_barrier()?;
        assert_eq!(region.get_immutable_ssts().len(), 0);
        Ok(())
    }

    #[test]
    fn test_maybe_compact_auto_triggers() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let mut region = Region::new(&wal_path)?;
        region.set_flush_threshold(2);
        region.set_compact_threshold(2);

        for i in 0..6i64 {
            region.write(k(1, i), v("x"))?;
        }
        region.flush_barrier()?;
        let n = region.get_immutable_ssts().len();
        assert!(
            (1..=2).contains(&n),
            "auto compaction should bound sst count, got {n}"
        );
        for i in 0..6i64 {
            assert_eq!(region.get(k(1, i))?, Some(v("x")));
        }
        Ok(())
    }

    #[test]
    fn test_ttl_compact_survives_reopen() -> io::Result<()> {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        let expired = now_nanos() - 60_000_000_000;
        let fresh = now_nanos();
        {
            let mut region = Region::new(&wal_path)?;
            region.set_flush_threshold(2);
            region.set_compact_threshold(2);
            region.set_ttl(Some(1_000_000_000));
            for i in 0..4i64 {
                region.write(k(1, expired + i), v("old"))?;
            }
            for i in 0..4i64 {
                region.write(k(2, fresh + i), v("new"))?;
            }
            region.close()?;
        }
        let region = Region::new(&wal_path)?;
        assert_eq!(region.get(k(1, expired))?, None);
        assert_eq!(region.get(k(2, fresh))?, Some(v("new")));
        Ok(())
    }

    fn find_sst_file(dir: &Path) -> Option<PathBuf> {
        fs::read_dir(dir)
            .ok()?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.extension().map_or(false, |x| x == "sst"))
    }

    #[test]
    fn test_recovery_does_not_resurrect_stale_sst() -> io::Result<()> {
        let dir = tempdir()?;
        let wal = dir.path().join("wal.log");
        let schema = Arc::new(TableSchema::default_table());
        {
            let region = Region::with_schema(&wal, schema.clone())?;
            region.write(k(1, 1), v("a"))?;
            region.flush()?;
            assert_eq!(region.get_immutable_ssts().len(), 1);
            region.close()?;
        }
        let src = find_sst_file(dir.path()).expect("flushed sst should exist");
        fs::copy(&src, dir.path().join("0099.sst"))?;

        let manifest = dir.path().join("manifest");
        let first_line = fs::read_to_string(&manifest)?
            .lines()
            .next()
            .expect("manifest should have exactly one entry")
            .to_string();
        let mut entry: serde_json::Value = serde_json::from_str(&first_line)?;
        entry["id"] = serde_json::json!(99usize);
        entry["path"] = serde_json::json!("0099.sst");
        fs::write(&manifest, serde_json::to_string(&entry)? + "\n")?;

        let region = Region::with_schema(&wal, schema)?;
        assert_eq!(region.get_immutable_ssts().len(), 1);
        assert_eq!(region.len(), 1);
        region.close()?;
        Ok(())
    }

    #[test]
    fn test_recovery_adopts_untracked_high_id_sst() -> io::Result<()> {
        let dir = tempdir()?;
        let wal = dir.path().join("wal.log");
        let schema = Arc::new(TableSchema::default_table());
        {
            let region = Region::with_schema(&wal, schema.clone())?;
            region.write(k(1, 1), v("a"))?;
            region.flush()?;
            region.close()?;
        }

        let src = find_sst_file(dir.path()).expect("flushed sst should exist");
        fs::copy(&src, dir.path().join("0042.sst"))?;

        let region = Region::with_schema(&wal, schema)?;
        assert_eq!(region.get_immutable_ssts().len(), 2);
        assert_eq!(region.len(), 2);
        region.close()?;
        Ok(())
    }

    #[test]
    fn test_drop_without_flush_recovers_from_wal() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        {
            let region = Region::new(&wal_path).unwrap();
            region.write(k(1, 10), v("a")).unwrap();
            region.write(k(2, 20), v("b")).unwrap();
            region.delete(k(1, 10)).unwrap();
        }

        let reopened = Region::new(&wal_path).unwrap();
        assert_eq!(reopened.get(k(1, 10)).unwrap(), None);
        assert_eq!(reopened.get(k(2, 20)).unwrap(), Some(v("b")));
        reopened.write(k(3, 30), v("c")).unwrap();
        assert_eq!(reopened.get(k(3, 30)).unwrap(), Some(v("c")));
    }

    #[test]
    fn test_open_sweeps_stale_tmp_files() {
        let dir = tempdir().unwrap();
        let wal_path = dir.path().join("wal.log");
        std::fs::write(dir.path().join("0003.sst.tmp"), b"garbage").unwrap();
        std::fs::write(dir.path().join("manifest.tmp"), b"garbage").unwrap();

        Region::new(&wal_path).unwrap();

        assert!(!dir.path().join("0003.sst.tmp").exists());
        assert!(!dir.path().join("manifest.tmp").exists());
    }

    #[test]
    fn test_deleted_key_stays_hidden_across_window_compaction() {
        let dir = tempdir().unwrap();
        let mut region = Region::new(dir.path().join("region")).unwrap();
        region.set_window_size(50);
        region.set_compact_threshold(2);

        region.write((vec![7], 10), b"anchor".to_vec()).unwrap();
        region.write((vec![7], 100), b"v1".to_vec()).unwrap();
        region.flush().unwrap();
        region.flush_barrier().unwrap();

        region.delete((vec![7], 100)).unwrap();
        region.write((vec![8], 130), b"w2b".to_vec()).unwrap();
        region.flush().unwrap();
        region.write((vec![9], 140), b"w2c".to_vec()).unwrap();
        region.flush().unwrap();
        region.flush_barrier().unwrap();

        assert_eq!(
            region.get((vec![7], 100)).unwrap(),
            None,
            "deleted key must stay dead"
        );
        for (_, v) in region.iter_all_data().unwrap() {
            assert_ne!(
                v.as_deref(),
                Some(b"v1".as_slice()),
                "PUT version resurrected after windowed compaction"
            );
        }
    }

    #[test]
    fn test_txn_commit_mixed_with_autowrites_stays_live() {
        let dir = tempdir().unwrap();
        let region = Arc::new(Region::new(dir.path().join("region")).unwrap());

        let worker = {
            let region = region.clone();
            std::thread::spawn(move || {
                let mut handles = Vec::new();
                for _ in 0..2 {
                    let r = region.clone();
                    handles.push(std::thread::spawn(move || {
                        for _ in 0..50 {
                            let mut txn = r.begin().unwrap();
                            let cur = txn.get(&(vec![1], 0)).and_then(|v| {
                                String::from_utf8(v)
                                    .ok()
                                    .and_then(|s| s.parse::<u64>().ok())
                            });
                            txn.write(
                                (vec![1], 0),
                                (cur.unwrap_or(0) + 1).to_string().into_bytes(),
                            );
                            r.commit(txn).ok();
                        }
                    }));
                }
                for t in 0..2u8 {
                    let r = region.clone();
                    handles.push(std::thread::spawn(move || {
                        for i in 0..200u64 {
                            r.write((vec![t + 10], i as i64), b"x".to_vec()).unwrap();
                        }
                    }));
                }
                for h in handles {
                    h.join().unwrap();
                }
                let final_result = region.get((vec![1], 0)).unwrap().unwrap();
                assert!(
                    String::from_utf8(final_result)
                        .unwrap()
                        .parse::<u64>()
                        .is_ok()
                );
            })
        };

        let deadline = Instant::now() + Duration::from_secs(15);
        while !worker.is_finished() {
            assert!(
                Instant::now() < deadline,
                "deadlock suspected: mixed txn/autowrite did not finish in 15s"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
        worker.join().unwrap();
    }
}
