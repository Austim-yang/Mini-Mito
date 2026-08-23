use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{
    array::{BooleanArray, Int8Array},
    compute::filter_record_batch,
};
use datafusion::{
    arrow::{
        array::{ArrayRef, RecordBatch},
        datatypes::SchemaRef,
    },
    error::DataFusionError,
    execution::RecordBatchStream,
};

use datafusion::error::Result as DataFusionResult;
use futures::Stream;

use crate::{
    memtable::memtable::Region,
    query::{merge::MergeBatchIter, predicate::TimeRange},
    schema::TableSchema,
    sstable::sstable::OP_DELETE,
};

pub struct LSMStream {
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    table_schema: Arc<TableSchema>,
    user_schema: SchemaRef,
    merge: MergeBatchIter,
    current: Option<RecordBatch>,
    emitted: usize,
    finished: bool,
}

impl RecordBatchStream for LSMStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for LSMStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(batch) = this.current.take() {
            return Poll::Ready(Some(Ok(batch)));
        }
        match this.refill() {
            Ok(true) => {
                let batch = this.current.take().expect("refill reported a batch");
                Poll::Ready(Some(Ok(batch)))
            }
            Ok(false) => Poll::Ready(None),
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}

impl LSMStream {
    pub fn new(
        region: Arc<Region>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
        time_range: TimeRange,
    ) -> io::Result<Self> {
        let table_schema = region.schema();
        let sources = match time_range.to_inclusive_bounds() {
            None => Vec::new(),
            Some(b) => region.snapshot_columnar_sources(Some(b))?,
        };
        let user_schema = Arc::new(table_schema.arrow_schema());
        let merge = MergeBatchIter::new(sources, table_schema.clone());
        Ok(Self {
            schema,
            projection,
            limit,
            table_schema,
            user_schema,
            merge,
            current: None,
            emitted: 0,
            finished: false,
        })
    }

    fn strip_internal(&self, batch: &RecordBatch) -> DataFusionResult<RecordBatch> {
        let ncols = self.table_schema.columns.len();
        let op = batch
            .column(ncols + 1)
            .as_any()
            .downcast_ref::<Int8Array>()
            .expect("__op_type must be Int8");
        let has_delete = op.iter().flatten().any(|v| v == OP_DELETE);
        let filtered = if has_delete {
            let mask: BooleanArray =
                BooleanArray::from_iter(op.iter().map(|v| v != Some(OP_DELETE)));
            filter_record_batch(batch, &mask)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?
        } else {
            batch.clone()
        };
        let arrays: Vec<ArrayRef> = (0..ncols).map(|c| filtered.column(c).clone()).collect();
        RecordBatch::try_new(self.user_schema.clone(), arrays)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
    }

    fn refill(&mut self) -> DataFusionResult<bool> {
        if self.finished || self.current.is_some() {
            return Ok(!self.finished && !self.current.is_some());
        }
        loop {
            let Some(result) = self.merge.next() else {
                return Ok(false);
            };
            let internal =
                result.map_err(|e| DataFusionError::Internal(format!("lsm merge failed: {e}")))?;
            let user = self.strip_internal(&internal)?;
            let projected = match &self.projection {
                Some(indices) => user
                    .project(indices)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
                None => user,
            };
            if projected.num_rows() == 0 {
                continue;
            }
            if let Some(lim) = self.limit {
                let remaining = lim - self.emitted;
                if projected.num_rows() >= remaining {
                    let sliced = projected.slice(0, remaining);
                    self.emitted += remaining;
                    self.current = Some(sliced);
                    self.finished = true;
                    return Ok(true);
                }
                self.emitted += projected.num_rows();
            }
            self.current = Some(projected);
            return Ok(true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::Region;
    use futures::StreamExt;
    use tempfile::tempdir;

    fn key(tag: u8, ts: i64) -> (Vec<u8>, i64) {
        (vec![tag], ts)
    }
    fn val(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[test]
    fn test_lsm_stream_merges_layers() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let mut region = Region::new(&path).unwrap();
        region.set_flush_threshold(2);
        region.write(key(1, 10), val("a")).unwrap();
        region.write(key(2, 10), val("b")).unwrap();
        region.write(key(1, 10), val("a2")).unwrap();
        region.write(key(3, 10), val("c")).unwrap();

        let region = Arc::new(region);
        let schema = Arc::new(region.schema().arrow_schema());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let stream = LSMStream::new(region, schema, None, None, TimeRange::unbounded()).unwrap();
        let batches: Vec<_> = rt.block_on(async { stream.collect::<Vec<_>>().await });
        let mut rows = Vec::new();
        for b in batches {
            let b = b.unwrap();
            for i in 0..b.num_rows() {
                rows.push((
                    b.column(0)
                        .as_any()
                        .downcast_ref::<arrow::array::BinaryArray>()
                        .unwrap()
                        .value(i)
                        .to_vec(),
                    b.column(1)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .unwrap()
                        .value(i),
                ));
            }
        }
        assert_eq!(rows, vec![(vec![1], 10), (vec![2], 10), (vec![3], 10)]);
    }

    #[test]
    fn test_lsm_stream_streams_multiple_batches() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let region = Region::new(&path).unwrap();
        for i in 0..25_000i64 {
            region.write(key(7, i), val("x")).unwrap();
        }
        region.flush().unwrap();

        let region = Arc::new(region);
        let schema = Arc::new(region.schema().arrow_schema());
        let rt = tokio::runtime::Runtime::new().unwrap();
        let stream = LSMStream::new(region, schema, None, None, TimeRange::unbounded()).unwrap();
        let batches: Vec<_> = rt.block_on(async { stream.collect::<Vec<_>>().await });

        let mut total = 0usize;
        let mut last_ts: Option<i64> = None;
        for b in batches {
            let b = b.unwrap();
            assert!(b.num_rows() <= 10_000);
            let col = b
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap();
            for i in 0..b.num_rows() {
                let ts = col.value(i);
                assert_eq!(ts, total as i64, "rows must arrive in order");
                last_ts = Some(ts);
                total += 1;
            }
        }
        assert_eq!(total, 25_000);
        assert_eq!(last_ts, Some(24_999));
    }
}
