use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
    sync::Arc,
};

use arrow::array::{ArrayRef, Int8Array, Int8Builder, Int64Array, Int64Builder, RecordBatch};
use arrow_schema::Schema;
use futures::io;

use crate::{
    memtable::version::Source,
    schema::{BatchView, TableSchema, TypedBuilder},
    sstable::sstable::{OP_DELETE, sst_schema},
};

const BATCH_ROWS: usize = 10_000;

#[derive(Clone, Debug)]
struct CursorKey {
    tags: Box<[u8]>,
    ts: i64,
    seq: u64,
    op: i8,
}

struct CursorEntry {
    key: CursorKey,
    src: usize,
    row: usize,
}

impl PartialEq for CursorEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key.tags == other.key.tags
            && self.key.ts == other.key.ts
            && self.key.seq == other.key.seq
            && self.src == other.src
    }
}
impl Eq for CursorEntry {}
impl PartialOrd for CursorEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for CursorEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key
            .tags
            .cmp(&other.key.tags)
            .then_with(|| self.key.ts.cmp(&other.key.ts))
            .then_with(|| other.key.seq.cmp(&self.key.seq))
            .then_with(|| self.src.cmp(&other.src))
    }
}

struct SourceCursor {
    batch: Option<Arc<RecordBatch>>,
    keys: Vec<CursorKey>,
}

pub struct MergeBatchIter {
    sources: Vec<Source>,
    cursors: Vec<SourceCursor>,
    heap: BinaryHeap<Reverse<CursorEntry>>,
    schema: Arc<TableSchema>,
    out_schema: Arc<Schema>,
    cols: Vec<TypedBuilder>,
    seqs: Int64Builder,
    ops: Int8Builder,
    last_keys: Vec<Option<CursorKey>>,
    buffered: usize,
    primed: bool,
}

fn check_sorted(prev: &CursorKey, cur: &CursorKey, ctx: &str) -> io::Result<()> {
    let ok = prev.tags < cur.tags || (prev.tags == cur.tags && prev.ts < cur.ts);
    if ok {
        Ok(())
    } else {
        Err(io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{ctx}: {cur:?} does not follow {prev:?} (strictly increasing required)"),
        ))
    }
}

fn extract_keys(
    schema: &TableSchema,
    batch: &RecordBatch,
    prev_tail: Option<&CursorKey>,
    src: usize,
) -> io::Result<Vec<CursorKey>> {
    let view = BatchView::new(batch, schema);
    let multi = schema.primary_key.len() > 1;
    let mut out = Vec::with_capacity(batch.num_rows());
    let mut prev = prev_tail.cloned();
    for i in 0..batch.num_rows() {
        let tags: Box<[u8]> = if !multi {
            view.cell(schema.primary_key[0], i)
                .unwrap_or_default()
                .into_boxed_slice()
        } else {
            let mut cells = vec![Vec::new(); schema.columns.len()];
            for &idx in &schema.primary_key {
                cells[idx] = view.cell(idx, i).unwrap_or_default()
            }
            schema.encode_tags(&cells).into_boxed_slice()
        };
        let key = CursorKey {
            tags,
            ts: view.ts_value(schema.time_index, i),
            seq: view.seq_value(i) as u64,
            op: view.op_type(i),
        };
        if let Some(p) = &prev {
            check_sorted(p, &key, &format!("source {src}"))?;
        }
        prev = Some(key);
        out.push(prev.clone().unwrap());
    }
    Ok(out)
}

impl MergeBatchIter {
    pub fn new(sources: Vec<Source>, schema: Arc<TableSchema>) -> Self {
        let cursors = sources
            .iter()
            .map(|_| SourceCursor {
                batch: None,
                keys: Vec::new(),
            })
            .collect();
        let last_keys = sources.iter().map(|_| None).collect();
        let mut m = Self {
            sources,
            cursors,
            heap: BinaryHeap::new(),
            out_schema: Arc::new(sst_schema(&schema)),
            schema,
            cols: Vec::new(),
            seqs: Int64Builder::with_capacity(BATCH_ROWS),
            ops: Int8Builder::with_capacity(BATCH_ROWS),
            last_keys,
            buffered: 0,
            primed: false,
        };
        m.reset_builders();
        m
    }

    fn reset_builders(&mut self) {
        self.cols = self
            .schema
            .columns
            .iter()
            .map(|c| TypedBuilder::with_capacity(&c.data_type, BATCH_ROWS))
            .collect();
    }

    fn ensure_primed(&mut self) -> io::Result<()> {
        if self.primed {
            return Ok(());
        }
        self.primed = true;
        for src in 0..self.sources.len() {
            self.load_next_batch(src)?;
        }
        Ok(())
    }

    fn load_next_batch(&mut self, src: usize) -> io::Result<bool> {
        loop {
            match self.sources[src].next_batch()? {
                None => {
                    self.cursors[src] = SourceCursor {
                        batch: None,
                        keys: Vec::new(),
                    };
                    return Ok(false);
                }
                Some(batch) => {
                    let keys =
                        extract_keys(&self.schema, &batch, self.last_keys[src].as_ref(), src)?;
                    if keys.is_empty() {
                        continue;
                    }
                    self.last_keys[src] = keys.last().cloned();
                    let head = keys[0].clone();
                    self.cursors[src] = SourceCursor {
                        batch: Some(batch),
                        keys,
                    };
                    self.push_entry(src, head, 0);
                    return Ok(true);
                }
            }
        }
    }

    fn push_entry(&mut self, src: usize, key: CursorKey, row: usize) {
        self.heap.push(Reverse(CursorEntry { key, src, row }));
    }

    fn advance(&mut self, src: usize, row: usize) -> io::Result<()> {
        let next_row = row + 1;
        let in_batch = self.cursors[src]
            .batch
            .as_ref()
            .is_some_and(|b| next_row < b.num_rows());
        if in_batch {
            let key = self.cursors[src].keys[next_row].clone();
            self.push_entry(src, key, next_row);
            return Ok(());
        }
        self.load_next_batch(src)?;
        Ok(())
    }

    fn emit(&mut self, e: &CursorEntry) {
        let Some(cur) = self.cursors.get(e.src) else {
            return;
        };
        let Some(batch) = cur.batch.clone() else {
            return;
        };
        let nclos = self.schema.columns.len();
        for c in 0..nclos {
            self.cols[c].append_from(batch.column(c).as_ref(), e.row);
        }
        self.seqs.append_value(e.key.seq as i64);
        self.ops.append_value(e.key.op);
        self.buffered += 1;
    }

    fn take_output(&mut self) -> io::Result<RecordBatch> {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.cols.len() + 2);
        for b in self.cols.drain(..) {
            arrays.push(b.finish());
        }
        arrays.push(Arc::new(Int64Array::from(self.seqs.finish())));
        arrays.push(Arc::new(Int8Array::from(self.ops.finish())));
        self.buffered = 0;
        self.reset_builders();
        RecordBatch::try_new(self.out_schema.clone(), arrays)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    pub fn next_batch(&mut self) -> io::Result<Option<RecordBatch>> {
        self.ensure_primed()?;
        loop {
            let Some(Reverse(top)) = self.heap.pop() else {
                break;
            };
            loop {
                let same_key = matches!(self.heap.peek(), Some(Reverse(e)) if e.key.tags == top.key.tags && e.key.ts == top.key.ts);
                if !same_key {
                    break;
                }
                let Reverse(dup) = self.heap.pop().unwrap();
                self.advance(dup.src, dup.row)?;
            }
            if top.key.op != OP_DELETE {
                self.emit(&top);
            }
            self.advance(top.src, top.row)?;
            if self.buffered >= BATCH_ROWS {
                return Ok(Some(self.take_output()?));
            }
        }
        if self.buffered > 0 {
            return Ok(Some(self.take_output()?));
        }
        Ok(None)
    }
}

impl Iterator for MergeBatchIter {
    type Item = io::Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_batch().transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Key, Value,
        schema::SemanticType,
        sstable::sstable::{OP_PUT, internal_batch_from_rows, key_at, value_at},
    };

    fn default_schema() -> Arc<TableSchema> {
        Arc::new(TableSchema::default_table())
    }

    fn mem_source(schema: &TableSchema, rows: Vec<(Key, u64, Option<Value>)>) -> Source {
        Source::memtable(vec![Arc::new(
            internal_batch_from_rows(&rows, schema).unwrap(),
        )])
    }

    fn mem_sources_multi(
        schema: &TableSchema,
        batches: Vec<Vec<(Key, u64, Option<Value>)>>,
    ) -> Source {
        Source::memtable(
            batches
                .into_iter()
                .map(|rows| Arc::new(internal_batch_from_rows(&rows, schema).unwrap()))
                .collect(),
        )
    }

    fn merged_rows(m: MergeBatchIter, schema: &TableSchema) -> Vec<(Key, u64, i8, Option<Value>)> {
        let field_cols: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.semantic == SemanticType::Field)
            .map(|(c, _)| c)
            .collect();
        let mut out = Vec::new();
        for batch in m {
            let batch = batch.unwrap();
            let view = BatchView::new(&batch, schema);
            for i in 0..batch.num_rows() {
                out.push((
                    key_at(&view, schema, i),
                    view.seq_value(i) as u64,
                    view.op_type(i),
                    value_at(&view, schema, &field_cols, i),
                ));
            }
        }
        out
    }

    #[test]
    fn test_merge_dedup_newest_wins() {
        let schema = default_schema();
        let sources = vec![
            mem_source(&schema, vec![((vec![1], 50), 20, Some(b"new".to_vec()))]),
            mem_source(
                &schema,
                vec![
                    ((vec![1], 50), 10, Some(b"old".to_vec())),
                    ((vec![2], 10), 1, Some(b"x".to_vec())),
                ],
            ),
        ];
        let got = merged_rows(MergeBatchIter::new(sources, schema.clone()), &schema);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0], ((vec![1], 50), 20, OP_PUT, Some(b"new".to_vec())));
        assert_eq!(got[1], ((vec![2], 10), 1, OP_PUT, Some(b"x".to_vec())));
    }

    #[test]
    fn test_merge_drops_winning_tombstone() {
        let schema = default_schema();
        let sources = vec![
            mem_source(&schema, vec![((vec![1], 50), 20, None)]),
            mem_source(&schema, vec![((vec![1], 50), 10, Some(b"old".to_vec()))]),
        ];
        let got = merged_rows(MergeBatchIter::new(sources, schema.clone()), &schema);
        assert_eq!(got.len(), 0, "winning tombstone must not be materialized");
    }

    #[test]
    fn test_merge_seq_overrides_layer_priority() {
        let schema = default_schema();
        let sources = vec![
            mem_source(&schema, vec![((vec![1], 50), 3, Some(b"active".to_vec()))]),
            mem_source(&schema, vec![((vec![1], 50), 9, Some(b"sst".to_vec()))]),
        ];
        let got = merged_rows(MergeBatchIter::new(sources, schema.clone()), &schema);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0], ((vec![1], 50), 9, OP_PUT, Some(b"sst".to_vec())));
    }

    #[test]
    fn test_merge_flushes_multiple_batches_across_boundary() {
        let schema = default_schema();
        let mut rows = Vec::new();
        for i in 0..12_000i64 {
            rows.push((
                (vec![(i >> 8) as u8, (i & 0xff) as u8], i),
                i as u64 + 1,
                Some(i.to_le_bytes().to_vec()),
            ));
        }
        let sources = vec![mem_source(&schema, rows)];
        let got = merged_rows(MergeBatchIter::new(sources, schema.clone()), &schema);
        assert_eq!(got.len(), 12_000);
        assert_eq!(got[0].0, (vec![0, 0], 0));
        assert_eq!(got[11_999].0, (vec![46, 223], 11_999));
    }

    #[test]
    fn test_merge_rejects_unsorted_source_across_batches() {
        let schema = default_schema();
        let sources = vec![mem_sources_multi(
            &schema,
            vec![
                vec![((vec![2], 10), 1, Some(b"a".to_vec()))],
                vec![((vec![1], 10), 2, Some(b"b".to_vec()))],
            ],
        )];
        let result: io::Result<Vec<_>> = MergeBatchIter::new(sources, schema.clone()).collect();
        assert!(
            result.is_err(),
            "unsorted source must surface as InvalidInput"
        );
    }

    #[test]
    fn test_merge_rejects_duplicate_key_within_one_source() {
        let schema = default_schema();
        let sources = vec![mem_sources_multi(
            &schema,
            vec![
                vec![((vec![1], 10), 1, Some(b"a".to_vec()))],
                vec![((vec![1], 10), 2, Some(b"b".to_vec()))],
            ],
        )];
        let result: io::Result<Vec<_>> = MergeBatchIter::new(sources, schema.clone()).collect();
        assert!(
            result.is_err(),
            "intra-source duplicate keys violate the contract"
        );
    }

    #[test]
    fn test_merge_rejects_unsorted_rows_within_batch() {
        let schema = default_schema();
        let sources = vec![mem_source(
            &schema,
            vec![
                ((vec![2], 10), 1, Some(b"a".to_vec())),
                ((vec![1], 10), 2, Some(b"b".to_vec())),
            ],
        )];
        let result: io::Result<Vec<_>> = MergeBatchIter::new(sources, schema.clone()).collect();
        assert!(result.is_err());
    }
}
