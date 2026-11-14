//! `ts AT TIME ZONE zone` and `timezone(zone, ts)`, as Postgres and DuckDB
//! both read them. A timestamp without a time zone is read as the wall-clock
//! time in the zone, and gives that instant, with a time zone. A timestamp
//! with a time zone gives the wall-clock time in the zone, without one.
//!
//! A zone is an IANA name, such as `Europe/Berlin`, in any case, or `UTC`.
//! Postgres reads an offset such as `'+05:30'` with its sign reversed, where
//! DuckDB refuses it, and reads an abbreviation such as `'CET'` as a fixed
//! offset, where DuckDB reads a zone with daylight saving, so neither is a
//! zone here. A wall-clock time that a change of offset skips or repeats is
//! read with the offset that gives the later instant: the offset before a
//! skip, and the one after a repeat, as both do.

use std::ops::ControlFlow;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::temporal_conversions::as_datetime;
use arrow_array::types::{
    ArrowTimestampType, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use arrow_array::{Array, ArrayRef, PrimitiveArray};
use arrow_schema::{DataType, TimeUnit};
use chrono::{LocalResult, NaiveDateTime, Offset, TimeDelta, TimeZone};
use chrono_tz::{TZ_VARIANTS, Tz};
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_sql::sqlparser::ast::{self, Spanned};
use datafusion_sql::sqlparser::tokenizer::Span;

const NAME: &str = "timezone";

pub(crate) fn function() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(Timezone {
        signature: Signature::user_defined(Volatility::Immutable),
    }))
}

/// Rewrites each `ts AT TIME ZONE zone` in a SELECT's list and WHERE as
/// `timezone(zone, ts)`, and gives where a literal zone, there or in a call
/// of `timezone`, isn't one, with why.
pub(crate) fn rewrite(s: &mut ast::Select) -> Vec<(Span, String)> {
    let mut refused = Vec::new();
    let mut visit = |e: &mut ast::Expr| {
        if let ast::Expr::AtTimeZone { .. } = e {
            let span = e.span();
            let taken = std::mem::replace(e, ast::Expr::value(ast::Value::Null));
            if let ast::Expr::AtTimeZone {
                timestamp,
                time_zone,
            } = taken
            {
                *e = call(span, *time_zone, *timestamp);
            }
        }
        if let ast::Expr::Function(f) = e
            && let Some(zone) = zone_argument(f)
            && let ast::Expr::Value(v) = zone
            && let Some(name) = v.value.clone().into_string()
            && find(&name).is_none()
        {
            refused.push((zone.span(), not_a_zone(&name)));
        }
        ControlFlow::<()>::Continue(())
    };
    let _ = ast::visit_expressions_mut(&mut s.projection, &mut visit);
    let _ = ast::visit_expressions_mut(&mut s.selection, &mut visit);
    refused
}

fn call(span: Span, zone: ast::Expr, ts: ast::Expr) -> ast::Expr {
    let arg = |e| ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e));
    ast::Expr::Function(ast::Function {
        name: ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident {
            value: NAME.into(),
            quote_style: None,
            span,
        })]),
        uses_odbc_syntax: false,
        parameters: ast::FunctionArguments::None,
        args: ast::FunctionArguments::List(ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![arg(zone), arg(ts)],
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    })
}

/// The zone of a call of `timezone` with two arguments.
fn zone_argument(f: &ast::Function) -> Option<&ast::Expr> {
    let name = f.name.0.last().and_then(|p| p.as_ident())?;
    if !name.value.eq_ignore_ascii_case(NAME) {
        return None;
    }
    let ast::FunctionArguments::List(list) = &f.args else {
        return None;
    };
    match list.args.as_slice() {
        [
            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(zone)),
            _,
        ] => Some(zone),
        _ => None,
    }
}

/// The zone of that name: an IANA name, in any case, or UTC.
pub(crate) fn find(name: &str) -> Option<Tz> {
    if name.eq_ignore_ascii_case("UTC") {
        return Some(Tz::UTC);
    }
    if !name.contains('/') {
        return None;
    }
    TZ_VARIANTS
        .iter()
        .copied()
        .find(|z| z.name().eq_ignore_ascii_case(name))
}

/// Why `name` isn't a zone, and what to write instead.
fn not_a_zone(name: &str) -> String {
    let upper = name.to_ascii_uppercase();
    let offset = name.starts_with(['+', '-'])
        || name.starts_with(|c: char| c.is_ascii_digit())
        || ["UTC", "GMT"].iter().any(|p| {
            upper
                .strip_prefix(p)
                .is_some_and(|r| r.starts_with(['+', '-']))
        });
    if offset {
        format!(
            "'{name}' is an offset, which Postgres reads with its sign reversed and DuckDB refuses: write a time zone's IANA name, such as 'Asia/Kolkata'"
        )
    } else if !name.contains('/') {
        format!(
            "'{name}' isn't a time zone's IANA name, and an abbreviation is a fixed offset in Postgres but a zone with daylight saving in DuckDB: write a name such as 'Europe/Berlin', or 'UTC'"
        )
    } else {
        format!(
            "'{name}' isn't a time zone transforms know: write an IANA name such as 'Europe/Berlin', or 'UTC'"
        )
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Timezone {
    signature: Signature,
}

impl ScalarUDFImpl for Timezone {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    /// Text and NULL zones as Utf8. Every other type is left as it is, for
    /// [`Self::return_type`] to refuse with a message of its own.
    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        Ok(args
            .iter()
            .enumerate()
            .map(|(i, t)| match (i, t) {
                (0, DataType::Utf8View | DataType::LargeUtf8 | DataType::Null) => DataType::Utf8,
                _ => t.clone(),
            })
            .collect())
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        use DataType as T;
        match args {
            [T::Utf8, T::Timestamp(unit, None)] => Ok(T::Timestamp(*unit, Some("UTC".into()))),
            [T::Utf8, T::Timestamp(unit, Some(_))] => Ok(T::Timestamp(*unit, None)),
            [T::Utf8, T::Date32 | T::Date64] => plan_err!(
                "AT TIME ZONE on a date is midnight in the zone in DuckDB but midnight UTC's wall-clock time in the zone in Postgres: CAST the date to TIMESTAMP for midnight in the zone, or to TIMESTAMPTZ for midnight UTC"
            ),
            [T::Utf8, T::Utf8 | T::Utf8View | T::LargeUtf8] => plan_err!(
                "AT TIME ZONE takes a timestamp, not text: write TIMESTAMP '…' for a wall-clock time or TIMESTAMPTZ '…' for an instant, or CAST the text to one"
            ),
            [T::Utf8, T::Null] => plan_err!(
                "AT TIME ZONE takes a timestamp, and NULL has no type: write CAST(NULL AS TIMESTAMP) or CAST(NULL AS TIMESTAMPTZ)"
            ),
            [T::Utf8, T::Time32(_) | T::Time64(_)] => plan_err!(
                "AT TIME ZONE takes a timestamp, and a time of day has no date to place it in a zone: add it to a date first"
            ),
            [T::Utf8, other] => plan_err!(
                "AT TIME ZONE takes a timestamp, with or without a time zone, not {other}"
            ),
            [zone, _] => plan_err!("a time zone is text, such as 'Europe/Berlin', not {zone}"),
            _ => plan_err!("timezone takes a time zone and a timestamp: timezone(zone, ts)"),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [zone, ts] = args.args.as_slice() else {
            return exec_err!("timezone takes two arguments");
        };
        let scalar = matches!(
            (zone, ts),
            (ColumnarValue::Scalar(_), ColumnarValue::Scalar(_))
        );
        let rows = if scalar { 1 } else { args.number_rows };
        let zones = zone.to_array(rows)?;
        let ts = ts.to_array(rows)?;
        let out = match ts.data_type() {
            DataType::Timestamp(TimeUnit::Second, tz) => {
                shift::<TimestampSecondType>(&ts, &zones, tz.is_none())
            }
            DataType::Timestamp(TimeUnit::Millisecond, tz) => {
                shift::<TimestampMillisecondType>(&ts, &zones, tz.is_none())
            }
            DataType::Timestamp(TimeUnit::Microsecond, tz) => {
                shift::<TimestampMicrosecondType>(&ts, &zones, tz.is_none())
            }
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
                shift::<TimestampNanosecondType>(&ts, &zones, tz.is_none())
            }
            other => exec_err!("timezone takes a timestamp, not {other}"),
        }?;
        match scalar {
            true => ScalarValue::try_from_array(&out, 0).map(ColumnarValue::Scalar),
            false => Ok(ColumnarValue::Array(out)),
        }
    }
}

/// Each timestamp in its row's zone: a wall-clock time's instant where
/// `wall_clock`, else an instant's wall-clock time.
fn shift<T: ArrowTimestampType>(
    ts: &ArrayRef,
    zones: &ArrayRef,
    wall_clock: bool,
) -> Result<ArrayRef> {
    let ts = ts.as_primitive::<T>();
    let Some(zones) = zones.as_string_opt::<i32>() else {
        return exec_err!("a time zone is text, not {}", zones.data_type());
    };
    let mut last: Option<(&str, Tz)> = None;
    let mut out = Vec::with_capacity(ts.len());
    for (v, name) in ts.iter().zip(zones.iter()) {
        let (Some(v), Some(name)) = (v, name) else {
            out.push(None);
            continue;
        };
        let tz = match last {
            Some((n, tz)) if n == name => tz,
            _ => match find(name) {
                Some(tz) => {
                    last = Some((name, tz));
                    tz
                }
                None => return exec_err!("{}", not_a_zone(name)),
            },
        };
        let shifted = as_datetime::<T>(v).and_then(|at| match wall_clock {
            true => instant(tz, at),
            false => at.checked_add_offset(tz.offset_from_utc_datetime(&at).fix()),
        });
        match shifted.and_then(|t| T::from_naive_datetime(t, None)) {
            Some(v) => out.push(Some(v)),
            None => {
                return exec_err!("a timestamp in '{name}' is outside the range a timestamp holds");
            }
        }
    }
    let out = PrimitiveArray::<T>::from_iter(out).with_timezone_opt(wall_clock.then_some("UTC"));
    Ok(Arc::new(out))
}

/// The instant, as UTC's wall-clock time, of a wall-clock time in a zone.
/// Where a change of offset skips or repeats it, the later of its two
/// readings.
fn instant(tz: Tz, local: NaiveDateTime) -> Option<NaiveDateTime> {
    let at = |o: chrono::FixedOffset| local.checked_sub_offset(o);
    match tz.offset_from_local_datetime(&local) {
        LocalResult::Single(o) => at(o.fix()),
        LocalResult::Ambiguous(a, b) => Some(at(a.fix())?.max(at(b.fix())?)),
        LocalResult::None => {
            let day = TimeDelta::days(1);
            let before = tz.offset_from_utc_datetime(&local.checked_sub_signed(day)?);
            let after = tz.offset_from_utc_datetime(&local.checked_add_signed(day)?);
            Some(at(before.fix())?.max(at(after.fix())?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap_or_else(|e| panic!("{s}: {e}"))
    }

    /// Postgres's and DuckDB's readings of skipped and repeated wall-clock
    /// times.
    #[test]
    fn skipped_and_repeated_times_read_as_the_later_instant() {
        let cases = [
            (
                "Europe/Berlin",
                "2024-01-15 12:00:00",
                "2024-01-15 11:00:00",
            ),
            (
                "Europe/Berlin",
                "2024-03-31 02:30:00",
                "2024-03-31 01:30:00",
            ),
            (
                "Europe/Berlin",
                "2024-10-27 02:30:00",
                "2024-10-27 01:30:00",
            ),
            (
                "America/New_York",
                "2024-03-10 02:30:00",
                "2024-03-10 07:30:00",
            ),
            (
                "America/New_York",
                "2024-11-03 01:30:00",
                "2024-11-03 06:30:00",
            ),
            (
                "Australia/Lord_Howe",
                "2024-10-06 02:15:00",
                "2024-10-05 15:45:00",
            ),
        ];
        for (zone, local, utc) in cases {
            let tz = find(zone).unwrap_or_else(|| panic!("{zone}"));
            assert_eq!(instant(tz, at(local)), Some(at(utc)), "{local} in {zone}");
        }
    }

    #[test]
    fn zones_are_iana_names_or_utc() {
        assert_eq!(find("europe/berlin"), Some(Tz::Europe__Berlin));
        assert_eq!(find("utc"), Some(Tz::UTC));
        assert_eq!(find("Etc/GMT+5"), Some(Tz::Etc__GMTPlus5));
        for name in ["CET", "PST", "EST", "+05:30", "UTC+5", "Nowhere/Land", ""] {
            assert_eq!(find(name), None, "{name}");
        }
        assert!(not_a_zone("+05:30").contains("offset"));
        assert!(not_a_zone("utc-3").contains("offset"));
        assert!(not_a_zone("CET").contains("abbreviation"));
        assert!(not_a_zone("Nowhere/Land").contains("know"));
    }
}
