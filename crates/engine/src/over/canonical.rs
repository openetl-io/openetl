//! A value in buffers of its own, which [`super::row`] writes as Arrow IPC
//! for a type it doesn't compact. IPC writes a slice's buffers as they are:
//! a bitmap's bytes with the bits of the values beside it, the bytes under
//! nulls and in a null list's range, a dictionary's every value and a
//! view's every buffer. So a value sliced from two batches would have other
//! bytes, and its row another hash.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use arrow_array::builder::{BinaryViewBuilder, StringViewBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::{Int16Type, Int32Type, Int64Type, RunEndIndexType};
use arrow_array::{
    Array, ArrayRef, OffsetSizeTrait, PrimitiveArray, UInt64Array, UnionArray, make_array,
};
use arrow_buffer::{
    ArrowNativeType, BooleanBuffer, Buffer, MutableBuffer, NullBuffer, OffsetBuffer, ScalarBuffer,
};
use arrow_data::ArrayDataBuilder;
use arrow_schema::{ArrowError, DataType, UnionFields, UnionMode};

/// `a` in new buffers that depend on its values alone: no offset, offsets
/// from 0, zeros under nulls, null lists empty, bitmaps clear past their
/// end, a dictionary of the values its keys use in the order they first
/// do, and a run per value. A slot is null too where `mask` is false, as
/// under a null parent; a union, which has no nulls, and a run-end array
/// pass `mask` to their values.
pub(super) fn canonical(
    a: &dyn Array,
    mask: Option<&BooleanBuffer>,
) -> Result<ArrayRef, ArrowError> {
    let n = a.len();
    let t = a.data_type();
    let valid =
        BooleanBuffer::collect_bool(n, |i| a.is_valid(i) && mask.is_none_or(|m| m.value(i)));
    let nulls = (valid.count_set_bits() < n).then(|| NullBuffer::new(valid.clone()));
    let b = ArrayDataBuilder::new(t.clone()).len(n).nulls(nulls);
    if let Some(w) = t.primitive_width() {
        return Ok(make_array(b.add_buffer(fixed(a, w, &valid)).build()?));
    }
    let b = match t {
        DataType::Null => return Ok(arrow_array::new_null_array(t, n)),
        DataType::Boolean => {
            let v = a.as_boolean().values();
            let bits = BooleanBuffer::collect_bool(n, |i| valid.value(i) && v.value(i));
            b.add_buffer(bits.into_inner())
        }
        DataType::FixedSizeBinary(w) => b.add_buffer(fixed(a, width(*w)?, &valid)),
        DataType::Utf8 => {
            let s = a.as_string::<i32>();
            bytes(b, s.offsets(), s.values(), &valid)?
        }
        DataType::LargeUtf8 => {
            let s = a.as_string::<i64>();
            bytes(b, s.offsets(), s.values(), &valid)?
        }
        DataType::Binary => {
            let s = a.as_binary::<i32>();
            bytes(b, s.offsets(), s.values(), &valid)?
        }
        DataType::LargeBinary => {
            let s = a.as_binary::<i64>();
            bytes(b, s.offsets(), s.values(), &valid)?
        }
        DataType::Utf8View => {
            let mut out = StringViewBuilder::with_capacity(n);
            for (v, ok) in a.as_string_view().iter().zip(valid.iter()) {
                out.append_option(v.filter(|_| ok));
            }
            return Ok(Arc::new(out.finish()));
        }
        DataType::BinaryView => {
            let mut out = BinaryViewBuilder::with_capacity(n);
            for (v, ok) in a.as_binary_view().iter().zip(valid.iter()) {
                out.append_option(v.filter(|_| ok));
            }
            return Ok(Arc::new(out.finish()));
        }
        DataType::List(_) => {
            let l = a.as_list::<i32>();
            listed(b, l.offsets(), l.values(), &valid)?
        }
        DataType::LargeList(_) => {
            let l = a.as_list::<i64>();
            listed(b, l.offsets(), l.values(), &valid)?
        }
        DataType::Map(..) => {
            let m = a.as_map();
            let entries: ArrayRef = Arc::new(m.entries().clone());
            listed(b, m.offsets(), &entries, &valid)?
        }
        DataType::ListView(_) => {
            let l = a.as_list_view::<i32>();
            viewed(b, l.offsets(), l.sizes(), l.values(), &valid)?
        }
        DataType::LargeListView(_) => {
            let l = a.as_list_view::<i64>();
            viewed(b, l.offsets(), l.sizes(), l.values(), &valid)?
        }
        DataType::FixedSizeList(_, k) => {
            let k = width(*k)?;
            // Values under a null list are there all the same, and null.
            let under = BooleanBuffer::collect_bool(n * k, |j| valid.value(j / k));
            let values = a.as_fixed_size_list().values().slice(0, n * k);
            b.add_child_data(canonical(&values, Some(&under))?.to_data())
        }
        DataType::Struct(_) => {
            let mut b = b;
            for c in a.as_struct().columns() {
                b = b.add_child_data(canonical(c, Some(&valid))?.to_data());
            }
            b
        }
        DataType::Dictionary(keys, _) => return dictionary(a, keys, &valid),
        DataType::Union(fields, mode) => return union(a, fields, *mode, mask),
        DataType::RunEndEncoded(ends, _) => {
            return match ends.data_type() {
                DataType::Int16 => runs::<Int16Type>(a, mask),
                DataType::Int32 => runs::<Int32Type>(a, mask),
                DataType::Int64 => runs::<Int64Type>(a, mask),
                other => Err(unsupported(other)),
            };
        }
        other => return Err(unsupported(other)),
    };
    Ok(make_array(b.build()?))
}

fn unsupported(t: &DataType) -> ArrowError {
    ArrowError::NotYetImplemented(format!("a value of type {t} in a row"))
}

fn width(w: i32) -> Result<usize, ArrowError> {
    usize::try_from(w).map_err(|_| ArrowError::InvalidArgumentError(format!("a width of {w}")))
}

fn offset<O: OffsetSizeTrait>(n: usize) -> Result<O, ArrowError> {
    O::from_usize(n).ok_or_else(|| ArrowError::OffsetOverflowError(n))
}

/// Values of `w` bytes each, zeros where they're null.
fn fixed(a: &dyn Array, w: usize, valid: &BooleanBuffer) -> Buffer {
    let d = a.to_data();
    let from = &d.buffers()[0].as_slice()[d.offset() * w..];
    let mut out = MutableBuffer::from_len_zeroed(a.len() * w);
    for i in valid.set_indices() {
        out.as_slice_mut()[i * w..(i + 1) * w].copy_from_slice(&from[i * w..(i + 1) * w]);
    }
    out.into()
}

/// Strings or bytes: offsets from 0, and a null's empty.
fn bytes<O: OffsetSizeTrait>(
    b: ArrayDataBuilder,
    offsets: &OffsetBuffer<O>,
    values: &[u8],
    valid: &BooleanBuffer,
) -> Result<ArrayDataBuilder, ArrowError> {
    let mut ends = Vec::with_capacity(offsets.len());
    ends.push(O::usize_as(0));
    let mut out = Vec::new();
    for (i, w) in offsets.windows(2).enumerate() {
        if valid.value(i) {
            out.extend_from_slice(&values[w[0].as_usize()..w[1].as_usize()]);
        }
        ends.push(offset::<O>(out.len())?);
    }
    Ok(b.add_buffer(Buffer::from_vec(ends))
        .add_buffer(Buffer::from_vec(out)))
}

/// The values of `ranges` of `a`, in order.
fn gather(a: &ArrayRef, ranges: &[Range<usize>]) -> Result<ArrayRef, ArrowError> {
    let (Some(first), Some(last)) = (ranges.first(), ranges.last()) else {
        return Ok(a.slice(0, 0));
    };
    if ranges.windows(2).all(|w| w[0].end == w[1].start) {
        return Ok(a.slice(first.start, last.end - first.start));
    }
    let at: UInt64Array = ranges
        .iter()
        .flat_map(Clone::clone)
        .map(|i| Some(i as u64))
        .collect();
    arrow_select::take::take(a, &at, None)
}

/// Lists or a map: offsets from 0, and a null's empty, whatever its
/// offsets covered.
fn listed<O: OffsetSizeTrait>(
    b: ArrayDataBuilder,
    offsets: &OffsetBuffer<O>,
    values: &ArrayRef,
    valid: &BooleanBuffer,
) -> Result<ArrayDataBuilder, ArrowError> {
    let mut ends = Vec::with_capacity(offsets.len());
    ends.push(O::usize_as(0));
    let mut ranges = Vec::new();
    let mut total = 0;
    for (i, w) in offsets.windows(2).enumerate() {
        let (s, e) = (w[0].as_usize(), w[1].as_usize());
        if valid.value(i) && e > s {
            ranges.push(s..e);
            total += e - s;
        }
        ends.push(offset::<O>(total)?);
    }
    let values = canonical(&gather(values, &ranges)?, None)?;
    Ok(b.add_buffer(Buffer::from_vec(ends))
        .add_child_data(values.to_data()))
}

/// List views: each list's values after the one before's, and a null's
/// empty.
fn viewed<O: OffsetSizeTrait>(
    b: ArrayDataBuilder,
    offsets: &ScalarBuffer<O>,
    sizes: &ScalarBuffer<O>,
    values: &ArrayRef,
    valid: &BooleanBuffer,
) -> Result<ArrayDataBuilder, ArrowError> {
    let mut starts = Vec::with_capacity(offsets.len());
    let mut lens = Vec::with_capacity(offsets.len());
    let mut ranges = Vec::new();
    let mut total = 0;
    for (i, (s, len)) in offsets.iter().zip(sizes.iter()).enumerate() {
        let (s, len) = (s.as_usize(), len.as_usize());
        let len = if valid.value(i) { len } else { 0 };
        starts.push(offset::<O>(total)?);
        lens.push(offset::<O>(len)?);
        if len > 0 {
            ranges.push(s..s + len);
            total += len;
        }
    }
    let values = canonical(&gather(values, &ranges)?, None)?;
    Ok(b.add_buffer(Buffer::from_vec(starts))
        .add_buffer(Buffer::from_vec(lens))
        .add_child_data(values.to_data()))
}

/// A dictionary of the values its keys use, in the order they first do: a
/// key whose value is null is null.
fn dictionary(
    a: &dyn Array,
    keys: &DataType,
    valid: &BooleanBuffer,
) -> Result<ArrayRef, ArrowError> {
    let d = a.as_any_dictionary();
    let values = d.values();
    let n = a.len();
    let at = match values.is_empty() {
        true => vec![0; n],
        false => d.normalized_keys(),
    };
    let set = BooleanBuffer::collect_bool(n, |i| {
        valid.value(i) && !values.is_empty() && values.is_valid(at[i])
    });
    let w = keys.primitive_width().ok_or_else(|| unsupported(keys))?;
    let mut first: HashMap<usize, u64> = HashMap::new();
    let mut used = Vec::new();
    let mut out = MutableBuffer::from_len_zeroed(n * w);
    for i in set.set_indices() {
        let next = first.len() as u64;
        let k = *first.entry(at[i]).or_insert_with(|| {
            used.push(Some(at[i] as u64));
            next
        });
        // A key's little-endian bytes, of a number its type held already.
        out.as_slice_mut()[i * w..(i + 1) * w].copy_from_slice(&k.to_le_bytes()[..w]);
    }
    let used = arrow_select::take::take(values, &UInt64Array::from(used), None)?;
    let nulls = (set.count_set_bits() < n).then(|| NullBuffer::new(set));
    let data = ArrayDataBuilder::new(a.data_type().clone())
        .len(n)
        .nulls(nulls)
        .add_buffer(out.into())
        .add_child_data(canonical(&used, None)?.to_data())
        .build()?;
    Ok(make_array(data))
}

/// A union of each child's values of its slots alone, null where `mask`
/// is false: a sparse union's other slots null, a dense union's in the
/// order of its slots.
fn union(
    a: &dyn Array,
    fields: &UnionFields,
    mode: UnionMode,
    mask: Option<&BooleanBuffer>,
) -> Result<ArrayRef, ArrowError> {
    let u = a.as_union();
    let n = u.len();
    let ids: Vec<i8> = (0..n).map(|i| u.type_id(i)).collect();
    let on = |i: usize| mask.is_none_or(|m| m.value(i));
    let mut children = Vec::with_capacity(fields.len());
    let offsets = match mode {
        UnionMode::Sparse => {
            for (id, _) in fields.iter() {
                let mine = BooleanBuffer::collect_bool(n, |i| ids[i] == id && on(i));
                children.push(canonical(u.child(id), Some(&mine))?);
            }
            None
        }
        UnionMode::Dense => {
            let mut offsets = vec![0i32; n];
            for (id, _) in fields.iter() {
                let slots: Vec<usize> = (0..n).filter(|&i| ids[i] == id).collect();
                let mut ranges = Vec::with_capacity(slots.len());
                for (j, &i) in slots.iter().enumerate() {
                    offsets[i] = offset::<i32>(j)?;
                    let at = u.value_offset(i);
                    ranges.push(at..at + 1);
                }
                let kept = BooleanBuffer::collect_bool(slots.len(), |j| on(slots[j]));
                children.push(canonical(&gather(u.child(id), &ranges)?, Some(&kept))?);
            }
            Some(ScalarBuffer::from(offsets))
        }
    };
    let u = UnionArray::try_new(fields.clone(), ScalarBuffer::from(ids), offsets, children)?;
    Ok(Arc::new(u))
}

/// A run-end array of a run per value.
fn runs<R: RunEndIndexType>(
    a: &dyn Array,
    mask: Option<&BooleanBuffer>,
) -> Result<ArrayRef, ArrowError> {
    let r = a
        .as_run_opt::<R>()
        .ok_or_else(|| unsupported(a.data_type()))?;
    let n = r.len();
    let logical: Vec<u64> = (0..n as u64).collect();
    let at: UInt64Array = r
        .get_physical_indices(&logical)?
        .into_iter()
        .map(|i| Some(i as u64))
        .collect();
    let values = arrow_select::take::take(r.values(), &at, None)?;
    let values = canonical(&values, mask)?;
    let ends = (1..=n)
        .map(|e| R::Native::from_usize(e).ok_or(ArrowError::OffsetOverflowError(e)))
        .collect::<Result<Vec<_>, _>>()?;
    let ends = PrimitiveArray::<R>::new(ends.into(), None);
    // Under `a`'s own type: RunArray::try_new would give its fields Arrow's
    // default names and nullability.
    let data = ArrayDataBuilder::new(a.data_type().clone())
        .len(n)
        .add_child_data(ends.into_data())
        .add_child_data(values.to_data())
        .build()?;
    Ok(make_array(data))
}
