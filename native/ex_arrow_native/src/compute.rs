//! Arrow compute kernel NIFs: filter, project, sort.
//!
//! All operations stay fully in native memory — the BEAM never sees column buffers.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch};
use arrow_ord::sort::sort_to_indices;
use arrow_schema::SortOptions;
use arrow_select::filter::filter_record_batch;
use arrow_select::take::take;
use rustler::resource::ResourceArc;
use rustler::{Env, Term};

use crate::resources::ExArrowRecordBatch;
use crate::util::{err_encode, ok_encode};

/// Filter rows from `batch` using the first column of `predicate_batch` (must be boolean).
///
/// Returns `{:ok, filtered_batch_ref}` or `{:error, msg}`.
#[rustler::nif]
pub fn compute_filter<'a>(
    env: Env<'a>,
    batch: ResourceArc<ExArrowRecordBatch>,
    predicate: ResourceArc<ExArrowRecordBatch>,
) -> Term<'a> {
    if predicate.batch.num_columns() == 0 {
        return err_encode(env, "predicate batch must have at least one column");
    }
    let bool_col = predicate.batch.column(0);
    let Some(bool_array) = bool_col.as_any().downcast_ref::<BooleanArray>() else {
        return err_encode(env, "predicate first column must be boolean");
    };
    match filter_record_batch(&batch.batch, bool_array) {
        Ok(filtered) => ok_encode(env, ResourceArc::new(ExArrowRecordBatch { batch: filtered })),
        Err(e) => err_encode(env, &e.to_string()),
    }
}

/// Project (select) a subset of columns from `batch` by name.
///
/// Returns `{:ok, projected_batch_ref}` or `{:error, msg}`.
#[rustler::nif]
pub fn compute_project<'a>(
    env: Env<'a>,
    batch: ResourceArc<ExArrowRecordBatch>,
    column_names: Vec<String>,
) -> Term<'a> {
    let schema = batch.batch.schema();
    let indices: Vec<usize> = match column_names
        .iter()
        .map(|name| {
            schema
                .index_of(name.as_str())
                .map_err(|_| format!("column '{}' not found", name))
        })
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(idx) => idx,
        Err(e) => return err_encode(env, &e),
    };
    match batch.batch.project(&indices) {
        Ok(projected) => ok_encode(env, ResourceArc::new(ExArrowRecordBatch { batch: projected })),
        Err(e) => err_encode(env, &e.to_string()),
    }
}

/// Sort `batch` by `column_name`.  `ascending = true` for ascending order.
///
/// Nulls are placed first regardless of sort direction.
/// Returns `{:ok, sorted_batch_ref}` or `{:error, msg}`.
#[rustler::nif]
pub fn compute_sort<'a>(
    env: Env<'a>,
    batch: ResourceArc<ExArrowRecordBatch>,
    column_name: String,
    ascending: bool,
) -> Term<'a> {
    let schema = batch.batch.schema();
    let col_idx = match schema.index_of(&column_name) {
        Ok(i) => i,
        Err(_) => return err_encode(env, &format!("column '{}' not found", column_name)),
    };
    let column: &Arc<dyn Array> = batch.batch.column(col_idx);
    let sort_opts = SortOptions { descending: !ascending, nulls_first: true };
    let indices = match sort_to_indices(column.as_ref(), Some(sort_opts), None) {
        Ok(i) => i,
        Err(e) => return err_encode(env, &e.to_string()),
    };
    let new_columns: Vec<ArrayRef> =
        match batch
            .batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices, None).map_err(|e| e.to_string()))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(cols) => cols,
            Err(e) => return err_encode(env, &e),
        };
    match RecordBatch::try_new(batch.batch.schema(), new_columns) {
        Ok(sorted) => ok_encode(env, ResourceArc::new(ExArrowRecordBatch { batch: sorted })),
        Err(e) => err_encode(env, &e.to_string()),
    }
}
