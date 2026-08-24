use std::{
    fs::{File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    path::Path,
};

use serde::{Deserialize, Serialize};

use crate::types::{Key, Value};

const KIND_INSERT: u8 = 0;
const KIND_UPDATE: u8 = 1;
const KIND_DELETE: u8 = 2;

const MAX_FRAME_LEN: usize = 256 << 20;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Operation {
    Insert { key: Key, seq: u64, value: Value },
    Update { key: Key, seq: u64, value: Value },
    Delete { key: Key, seq: u64 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncPolicy {
    Never,
    Interval(u32),
    Always,
}

impl Default for SyncPolicy {
    fn default() -> Self {
        SyncPolicy::Interval(100)
    }
}

pub struct Wal {
    writer: BufWriter<File>,
    path: String,
    sync_policy: SyncPolicy,
    ops_since_sync: u32,
}

impl Wal {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        Self::with_sync_policy(path, SyncPolicy::default())
    }

    pub fn with_sync_policy<P: AsRef<Path>>(path: P, sync_policy: SyncPolicy) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path.as_ref())?;
        Ok(Wal {
            writer: BufWriter::new(file),
            path: path.as_ref().to_string_lossy().into_owned(),
            sync_policy,
            ops_since_sync: 0,
        })
    }

    pub fn append(&mut self, op: &Operation) -> io::Result<()> {
        self.append_batch(std::slice::from_ref(op))
    }

    pub fn append_batch(&mut self, ops: &[Operation]) -> io::Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let mut frame = Vec::with_capacity(ops.len() * 32);
        encode_ops(ops, &mut frame);
        self.writer.write_all(&(frame.len() as u32).to_le_bytes())?;
        self.writer
            .write_all(&crc32fast::hash(&frame).to_le_bytes())?;
        self.writer.write_all(&frame)?;
        self.maybe_sync(ops.len() as u32)
    }

    fn maybe_sync(&mut self, n_ops: u32) -> io::Result<()> {
        match self.sync_policy {
            SyncPolicy::Never => Ok(()),
            SyncPolicy::Always => {
                self.writer.flush()?;
                self.writer.get_ref().sync_data()
            }
            SyncPolicy::Interval(n) => {
                self.ops_since_sync += n_ops;
                if n > 0 && self.ops_since_sync >= n {
                    self.ops_since_sync = 0;
                    self.writer.flush()?;
                    self.writer.get_ref().sync_data()?;
                }
                Ok(())
            }
        }
    }

    pub fn recover(&self, sink: &mut dyn FnMut(&Operation)) -> io::Result<()> {
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut len_buf = [0u8; 4];
        loop {
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
                Err(e) => return Err(e),
            }
            let frame_len = u32::from_le_bytes(len_buf) as usize;
            if frame_len == 0 || frame_len > MAX_FRAME_LEN {
                return Ok(());
            }
            let mut crc_buf = [0u8; 4];
            if reader.read_exact(&mut crc_buf).is_err() {
                return Ok(());
            }
            let mut frame = vec![0u8; frame_len];
            if reader.read_exact(&mut frame).is_err() {
                return Ok(());
            }
            if crc32fast::hash(&frame) != u32::from_le_bytes(crc_buf) {
                return Ok(());
            }
            for op in decode_ops(&frame) {
                sink(&op);
            }
        }
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }

    pub fn close(&mut self) -> io::Result<()> {
        self.flush()?;
        self.writer.get_ref().sync_all()?;
        Ok(())
    }
}

fn encode_ops(ops: &[Operation], buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(ops.len() as u32).to_le_bytes());
    for op in ops {
        match op {
            Operation::Insert { key, seq, value } => {
                buf.push(KIND_INSERT);
                encode_kv(key, *seq, Some(value), buf);
            }
            Operation::Update { key, seq, value } => {
                buf.push(KIND_UPDATE);
                encode_kv(key, *seq, Some(value), buf);
            }
            Operation::Delete { key, seq } => {
                buf.push(KIND_DELETE);
                encode_kv(key, *seq, None, buf);
            }
        }
    }
}

fn encode_kv(key: &Key, seq: u64, value: Option<&Value>, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&(key.0.len() as u16).to_le_bytes());
    buf.extend_from_slice(&key.0);
    buf.extend_from_slice(&key.1.to_le_bytes());
    buf.extend_from_slice(&seq.to_le_bytes());
    match value {
        Some(v) => {
            buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
            buf.extend_from_slice(v);
        }
        None => {}
    }
}

fn read_u8(d: &mut &[u8]) -> Option<u8> {
    let (first, rest) = d.split_first()?;
    *d = rest;
    Some(*first)
}

fn read_u16(d: &mut &[u8]) -> Option<u16> {
    if d.len() < 2 {
        return None;
    }
    let v = u16::from_le_bytes([d[0], d[1]]);
    *d = &d[2..];
    Some(v)
}

fn read_u32(d: &mut &[u8]) -> Option<u32> {
    if d.len() < 4 {
        return None;
    }
    let v = u32::from_le_bytes([d[0], d[1], d[2], d[3]]);
    *d = &d[4..];
    Some(v)
}

fn read_u64(d: &mut &[u8]) -> Option<u64> {
    if d.len() < 8 {
        return None;
    }
    let mut b = [0u8; 8];
    b.copy_from_slice(&d[..8]);
    *d = &d[8..];
    Some(u64::from_le_bytes(b))
}

fn read_vec(d: &mut &[u8], n: usize) -> Option<Vec<u8>> {
    if d.len() < n {
        return None;
    }
    let v = d[..n].to_vec();
    *d = &d[n..];
    Some(v)
}

fn decode_ops(payload: &[u8]) -> Vec<Operation> {
    let mut d = payload;
    let mut out = Vec::new();
    let Some(count) = read_u32(&mut d) else {
        return out;
    };
    for _ in 0..count {
        let Some(kind) = read_u8(&mut d) else {
            return out;
        };
        let Some(tag_len) = read_u16(&mut d) else {
            return out;
        };
        let Some(tags) = read_vec(&mut d, tag_len as usize) else {
            return out;
        };
        let ts_lo = read_u64(&mut d);
        let Some(ts) = ts_lo.map(|v| v as i64) else {
            return out;
        };
        let Some(seq) = read_u64(&mut d) else {
            return out;
        };
        let op = match kind {
            KIND_DELETE => Operation::Delete {
                key: (tags, ts),
                seq,
            },
            KIND_UPDATE => {
                let Some(vlen) = read_u32(&mut d) else {
                    return out;
                };
                let Some(value) = read_vec(&mut d, vlen as usize) else {
                    return out;
                };
                Operation::Update {
                    key: (tags, ts),
                    seq,
                    value,
                }
            }
            _ => {
                let Some(vlen) = read_u32(&mut d) else {
                    return out;
                };
                let Some(value) = read_vec(&mut d, vlen as usize) else {
                    return out;
                };
                Operation::Insert {
                    key: (tags, ts),
                    seq,
                    value,
                }
            }
        };
        out.push(op);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn k(tag: u8, ts: i64) -> Key {
        (vec![tag], ts)
    }
    fn v(s: &str) -> Value {
        s.as_bytes().to_vec()
    }

    #[derive(Default)]
    struct Rows(Vec<(Key, u64, Option<Value>)>);

    impl Rows {
        fn push(&mut self, key: Key, seq: u64, value: Option<Value>) {
            self.0.push((key, seq, value));
        }
        fn get(&self, key: &Key) -> Option<(u64, Option<Value>)> {
            self.0
                .iter()
                .rev()
                .find(|(k, _, _)| k == key)
                .map(|(_, s, v)| (*s, v.clone()))
        }
        fn len(&self) -> usize {
            self.0.len()
        }
    }

    fn replay_into(rows: &mut Rows) -> impl FnMut(&Operation) + '_ {
        move |op: &Operation| match op {
            Operation::Insert { key, seq, value } | Operation::Update { key, seq, value } => {
                rows.push(key.clone(), *seq, Some(value.clone()));
            }
            Operation::Delete { key, seq } => {
                rows.push(key.clone(), *seq, None);
            }
        }
    }

    fn write_batch_and_close(wal: &mut Wal, ops: &[Operation]) {
        wal.append_batch(ops).unwrap();
        wal.close().unwrap();
    }

    #[test]
    fn test_wal_insert_and_recover() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");
        let mut wal = Wal::new(&path).unwrap();
        write_batch_and_close(
            &mut wal,
            &[
                Operation::Insert {
                    key: k(1, 0),
                    seq: 1,
                    value: v("one"),
                },
                Operation::Insert {
                    key: k(2, 0),
                    seq: 2,
                    value: v("two"),
                },
            ],
        );

        let rows = {
            let wal_recover = Wal::new(&path).unwrap();
            let mut rows = Rows::default();
            wal_recover.recover(&mut replay_into(&mut rows)).unwrap();
            rows
        };
        assert_eq!(rows.get(&k(1, 0)), Some((1, Some(v("one")))));
        assert_eq!(rows.get(&k(2, 0)), Some((2, Some(v("two")))));
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_wal_update_and_delete() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.log");
        let mut wal = Wal::new(&path).unwrap();
        write_batch_and_close(
            &mut wal,
            &[
                Operation::Insert {
                    key: k(10, 0),
                    seq: 1,
                    value: v("old"),
                },
                Operation::Update {
                    key: k(10, 0),
                    seq: 2,
                    value: v("new"),
                },
                Operation::Delete {
                    key: k(10, 0),
                    seq: 3,
                },
            ],
        );

        let rows = {
            let wal_recover = Wal::new(&path).unwrap();
            let mut rows = Rows::default();
            wal_recover.recover(&mut replay_into(&mut rows)).unwrap();
            rows
        };
        assert_eq!(rows.get(&k(10, 0)), Some((3, None)));
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_wal_empty_recover() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.log");
        Wal::new(&path).unwrap().close().unwrap();

        let wal = Wal::new(&path).unwrap();
        let mut rows = Rows::default();
        wal.recover(&mut replay_into(&mut rows)).unwrap();
        assert_eq!(rows.len(), 0);
    }

    #[test]
    fn test_wal_roundtrip_preserves_seq() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seq.log");
        let mut wal = Wal::new(&path).unwrap();
        write_batch_and_close(
            &mut wal,
            &[
                Operation::Insert {
                    key: k(1, 0),
                    seq: 42,
                    value: v("x"),
                },
                Operation::Delete {
                    key: k(1, 0),
                    seq: 43,
                },
            ],
        );

        let rows = {
            let wal_recover = Wal::new(&path).unwrap();
            let mut rows = Rows::default();
            wal_recover.recover(&mut replay_into(&mut rows)).unwrap();
            rows
        };
        assert_eq!(rows.get(&k(1, 0)), Some((43, None)));
        assert_eq!(
            rows.get(&k(1, 0))
                .unwrap()
                .0
                .max(rows.0.iter().map(|r| r.1).max().unwrap()),
            43
        );
    }

    #[test]
    fn test_wal_torn_tail_recovered_gracefully() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("torn.log");
        {
            let mut wal = Wal::new(&path).unwrap();
            write_batch_and_close(
                &mut wal,
                &[Operation::Insert {
                    key: k(1, 0),
                    seq: 1,
                    value: v("a"),
                }],
            );
        }
        {
            use std::io::Write as _;
            let ops = [Operation::Insert {
                key: k(2, 0),
                seq: 2,
                value: v("b"),
            }];
            let mut frame = Vec::new();
            encode_ops(&ops, &mut frame);
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            file.write_all(&(frame.len() as u32).to_le_bytes()).unwrap();
            file.write_all(&crc32fast::hash(&frame).to_le_bytes())
                .unwrap();
            file.write_all(&frame).unwrap();
            file.write_all(&(frame.len() as u32).to_le_bytes()).unwrap();
            file.write_all(&crc32fast::hash(&frame).to_le_bytes())
                .unwrap();
            file.write_all(&frame[..frame.len() / 2]).unwrap();
        }

        let wal = Wal::new(&path).unwrap();
        let mut rows = Rows::default();
        wal.recover(&mut replay_into(&mut rows)).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows.get(&k(2, 0)), Some((2, Some(v("b")))));
    }

    #[test]
    fn test_wal_corrupt_length_prefix_stops_cleanly() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt.log");
        {
            let mut wal = Wal::new(&path).unwrap();
            write_batch_and_close(
                &mut wal,
                &[Operation::Insert {
                    key: k(1, 0),
                    seq: 1,
                    value: v("a"),
                }],
            );
        }
        {
            use std::io::Write as _;
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            file.write_all(&u32::MAX.to_le_bytes()).unwrap();
        }

        let wal = Wal::new(&path).unwrap();
        let mut rows = Rows::default();
        wal.recover(&mut replay_into(&mut rows)).unwrap();
        assert_eq!(rows.len(), 1); // 垃圾长度前缀之前的帧完整保留
    }

    #[test]
    fn test_wal_sync_policy_smoke() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sync.log");
        let mut wal = Wal::with_sync_policy(&path, SyncPolicy::Interval(2)).unwrap();
        for i in 0..5u64 {
            wal.append(&Operation::Insert {
                key: k(1, i as i64),
                seq: i,
                value: v("x"),
            })
            .unwrap();
        }
        wal.close().unwrap();

        let mut always =
            Wal::with_sync_policy(dir.path().join("always.log"), SyncPolicy::Always).unwrap();
        always
            .append(&Operation::Delete {
                key: k(1, 0),
                seq: 9,
            })
            .unwrap();
        always.close().unwrap();
    }

    fn frame_payload_ranges(bytes: &[u8]) -> Vec<(usize, usize)> {
        let mut out = Vec::new();
        let mut pos = 0usize;
        while pos + 8 <= bytes.len() {
            let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
            if len == 0 || pos + 8 + len > bytes.len() {
                break;
            }
            out.push((pos + 8, len));
            pos += 8 + len;
        }
        out
    }

    #[test]
    fn test_wal_bit_flip_in_payload_detected_by_crc() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("crc_payload.log");
        {
            let mut wal = Wal::new(&path).unwrap();
            wal.append(&Operation::Insert {
                key: k(1, 0),
                seq: 1,
                value: v("a"),
            })
            .unwrap();
            wal.append(&Operation::Insert {
                key: k(2, 0),
                seq: 2,
                value: v("b"),
            })
            .unwrap();
            wal.close().unwrap();
        }
        let mut bytes = std::fs::read(&path).unwrap();
        let ranges = frame_payload_ranges(&bytes);
        assert_eq!(ranges.len(), 2);
        let (start, len) = ranges[1];
        bytes[start + len - 1] ^= 0x01;
        std::fs::write(&path, &bytes).unwrap();

        let wal = Wal::new(&path).unwrap();
        let mut rows = Rows::default();
        wal.recover(&mut replay_into(&mut rows)).unwrap();
        assert_eq!(rows.get(&k(1, 0)), Some((1, Some(v("a")))));
        assert_eq!(rows.get(&k(2, 0)), None, "corrupted frame must be dropped");
    }

    #[test]
    fn test_wal_crc_field_flip_detected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("crc_field.log");
        {
            let mut wal = Wal::new(&path).unwrap();
            wal.append(&Operation::Insert {
                key: k(1, 0),
                seq: 1,
                value: v("a"),
            })
            .unwrap();
            wal.append(&Operation::Insert {
                key: k(2, 0),
                seq: 2,
                value: v("b"),
            })
            .unwrap();
            wal.close().unwrap();
        }
        let mut bytes = std::fs::read(&path).unwrap();
        let ranges = frame_payload_ranges(&bytes);
        assert_eq!(ranges.len(), 2);
        let (start, _) = ranges[1];
        bytes[start - 4] ^= 0x80;
        std::fs::write(&path, &bytes).unwrap();

        let wal = Wal::new(&path).unwrap();
        let mut rows = Rows::default();
        wal.recover(&mut replay_into(&mut rows)).unwrap();
        assert_eq!(rows.get(&k(1, 0)), Some((1, Some(v("a")))));
        assert_eq!(rows.get(&k(2, 0)), None);
    }
}
