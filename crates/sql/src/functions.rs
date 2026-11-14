//! The functions statements can call: a curated part of DataFusion's library,
//! and of our own `now()`, `current_timestamp`, `current_date` and the JSON
//! functions. Each is constructed by name here, so a binary links only these.
//! `functions.txt` records the list, and a test fails when the two differ.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_schema::{DataType, TimeUnit};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::planner::ExprPlanner;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

pub(crate) struct Functions {
    /// By name and by alias.
    pub(crate) scalars: HashMap<String, Arc<ScalarUDF>>,
    /// Syntax that plans to functions, such as `EXTRACT`, `POSITION`,
    /// `SUBSTRING`, `OVERLAY`, `a[i]`, `s['field']` and JSON's `->`. What it
    /// reaches must be in `scalars` too, or binding refuses it.
    pub(crate) planners: Vec<Arc<dyn ExprPlanner>>,
    /// Rewrites of calls, before type coercion, such as a cast of
    /// `json_get`'s value to the getter of that type.
    pub(crate) rewrites: Vec<Arc<dyn FunctionRewrite + Send + Sync>>,
}

impl Functions {
    pub(crate) fn get(&self, name: &str) -> Option<&Arc<ScalarUDF>> {
        self.scalars.get(name)
    }

    /// Whether a call by this name gives the same result for the same
    /// arguments, every time. Unknown names are.
    pub(crate) fn deterministic(&self, name: &str) -> bool {
        self.get(name)
            .is_none_or(|f| f.signature().volatility == Volatility::Immutable)
    }
}

pub(crate) static FUNCTIONS: LazyLock<Functions> = LazyLock::new(|| {
    let mut scalars = HashMap::new();
    for f in curated().into_iter().chain(crate::json::functions()) {
        for a in f.aliases() {
            scalars.insert(a.clone(), f.clone());
        }
        scalars.insert(f.name().to_owned(), f);
    }
    let planners: Vec<Arc<dyn ExprPlanner>> = vec![
        Arc::new(datafusion_functions::core::planner::CoreFunctionPlanner::default()),
        Arc::new(datafusion_functions_nested::planner::NestedFunctionPlanner),
        Arc::new(datafusion_functions_nested::planner::FieldAccessPlanner),
        Arc::new(datafusion_functions::datetime::planner::DatetimeFunctionPlanner),
        Arc::new(datafusion_functions::unicode::planner::UnicodeFunctionPlanner),
        Arc::new(crate::json::Operators),
    ];
    Functions {
        scalars,
        planners,
        rewrites: vec![Arc::new(crate::json::Rewrites)],
    }
});

fn curated() -> Vec<Arc<ScalarUDF>> {
    use datafusion_functions::{core, crypto, datetime, encoding, math, regex, string, unicode};
    use datafusion_functions_nested as nested;
    let config = crate::bind::options();
    vec![
        // Conditionals.
        core::coalesce(),
        core::greatest(),
        core::least(),
        core::nullif(),
        core::nvl(),
        core::nvl2(),
        // Structs.
        core::get_field(),
        core::named_struct(),
        core::r#struct(),
        // Strings.
        string::ascii(),
        string::bit_length(),
        string::btrim(),
        string::chr(),
        string::concat(),
        string::concat_ws(),
        string::contains(),
        string::ends_with(),
        string::levenshtein(),
        string::lower(),
        string::ltrim(),
        string::octet_length(),
        string::repeat(),
        string::replace(),
        string::rtrim(),
        string::split_part(),
        string::starts_with(),
        string::to_hex(),
        string::upper(),
        string::uuid(),
        unicode::character_length(),
        unicode::find_in_set(),
        unicode::initcap(),
        unicode::left(),
        unicode::lpad(),
        unicode::reverse(),
        unicode::right(),
        unicode::rpad(),
        unicode::strpos(),
        unicode::substr(),
        unicode::substr_index(),
        unicode::translate(),
        core::overlay(),
        regex::regexp_count(),
        regex::regexp_instr(),
        regex::regexp_like(),
        regex::regexp_match(),
        regex::regexp_replace(),
        // Numbers.
        math::abs(),
        math::acos(),
        math::acosh(),
        math::asin(),
        math::asinh(),
        math::atan(),
        math::atan2(),
        math::atanh(),
        math::cbrt(),
        math::ceil(),
        math::cos(),
        math::cosh(),
        math::cot(),
        math::degrees(),
        math::exp(),
        math::factorial(),
        math::floor(),
        math::gcd(),
        math::isnan(),
        math::iszero(),
        math::lcm(),
        math::ln(),
        math::log(),
        math::log10(),
        math::log2(),
        math::nanvl(),
        math::pi(),
        math::power(),
        math::radians(),
        math::random(),
        math::round(),
        math::signum(),
        math::sin(),
        math::sinh(),
        math::sqrt(),
        math::tan(),
        math::tanh(),
        math::trunc(),
        // Time.
        datetime::date_bin(),
        datetime::date_part(),
        datetime::date_trunc(),
        datetime::from_unixtime(),
        datetime::make_date(),
        datetime::to_char(),
        datetime::to_date(),
        datetime::to_timestamp(&config),
        datetime::to_timestamp_seconds(&config),
        datetime::to_timestamp_millis(&config),
        datetime::to_timestamp_micros(&config),
        datetime::to_timestamp_nanos(&config),
        datetime::to_unixtime(),
        now(),
        current_date(),
        // Hashes and encodings.
        crypto::digest(),
        crypto::md5(),
        crypto::sha224(),
        crypto::sha256(),
        crypto::sha384(),
        crypto::sha512(),
        encoding::encode(),
        encoding::decode(),
        // Lists.
        nested::make_array::make_array_udf(),
        nested::extract::array_element_udf(),
        nested::extract::array_slice_udf(),
        nested::length::array_length_udf(),
        nested::cardinality::cardinality_udf(),
        nested::array_has::array_has_udf(),
        nested::array_has::array_has_all_udf(),
        nested::array_has::array_has_any_udf(),
        nested::concat::array_append_udf(),
        nested::concat::array_prepend_udf(),
        nested::concat::array_concat_udf(),
        nested::position::array_position_udf(),
        nested::set_ops::array_distinct_udf(),
        nested::sort::array_sort_udf(),
        nested::reverse::array_reverse_udf(),
        nested::remove::array_remove_udf(),
        nested::flatten::flatten_udf(),
        nested::string::array_to_string_udf(),
        nested::string::string_to_array_udf(),
    ]
}

/// `now()` and `current_timestamp`: when the batch is evaluated, in UTC.
fn now() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(Now {
        signature: Signature::nullary(Volatility::Volatile),
        aliases: vec!["current_timestamp".into()],
    }))
}

/// `current_date`: the UTC date when the batch is evaluated.
fn current_date() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(CurrentDate {
        signature: Signature::nullary(Volatility::Volatile),
    }))
}

const UTC: &str = "UTC";

fn micros_now() -> Result<i64> {
    let since = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d,
        Err(e) => return exec_err!("the clock is before 1970: {e}"),
    };
    match i64::try_from(since.as_micros()) {
        Ok(m) => Ok(m),
        Err(_) => exec_err!("the clock is past what a timestamp holds"),
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Now {
    signature: Signature,
    aliases: Vec<String>,
}

impl ScalarUDFImpl for Now {
    fn name(&self) -> &str {
        "now"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
            Some(micros_now()?),
            Some(UTC.into()),
        )))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct CurrentDate {
    signature: Signature,
}

impl ScalarUDFImpl for CurrentDate {
    fn name(&self) -> &str {
        "current_date"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Date32)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let days = micros_now()?.div_euclid(86_400_000_000);
        match i32::try_from(days) {
            Ok(d) => Ok(ColumnarValue::Scalar(ScalarValue::Date32(Some(d)))),
            Err(_) => exec_err!("the clock is past what a date holds"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    /// The list `functions.txt` records: a line per function, by name, with its
    /// aliases and whether it's deterministic.
    fn recorded() -> String {
        let mut byname = BTreeMap::new();
        for f in FUNCTIONS.scalars.values() {
            byname.insert(f.name().to_owned(), f.clone());
        }
        let mut out = String::new();
        for (name, f) in byname {
            out.push_str(&name);
            let mut aliases = f.aliases().to_vec();
            aliases.sort();
            if !aliases.is_empty() {
                out.push_str(&format!(" ({})", aliases.join(", ")));
            }
            if f.signature().volatility != Volatility::Immutable {
                out.push_str(" not deterministic");
            }
            out.push('\n');
        }
        out
    }

    /// Run with `OPENETL_SQL_REWRITE=1` to write the file from the code.
    #[test]
    fn the_functions_are_recorded() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/functions.txt");
        let header = "# The functions SQL transforms can call, with their aliases. Written from\n\
                      # crates/sql/src/functions.rs by `OPENETL_SQL_REWRITE=1 cargo test -p openetl-sql`;\n\
                      # a test fails when the two differ.\n";
        let want = format!("{header}{}", recorded());
        if std::env::var_os("OPENETL_SQL_REWRITE").is_some() {
            if let Err(e) = std::fs::write(path, &want) {
                panic!("writing {path}: {e}");
            }
            return;
        }
        let got = std::fs::read_to_string(path).unwrap_or_default();
        let only = |a: &str, b: &str| -> Vec<String> {
            a.lines()
                .filter(|l| !b.lines().any(|m| m == *l))
                .map(str::to_owned)
                .collect()
        };
        assert!(
            got == want,
            "functions.txt doesn't match the functions in functions.rs: rewrite it with \
             OPENETL_SQL_REWRITE=1 cargo test -p openetl-sql and review the diff.\n\
             In the code only: {:?}\nIn the file only: {:?}",
            only(&want, &got),
            only(&got, &want)
        );
    }

    #[test]
    fn now_and_current_date_arent_deterministic() {
        for name in ["now", "current_timestamp", "current_date", "random", "uuid"] {
            assert!(!FUNCTIONS.deterministic(name), "{name}");
        }
        assert!(FUNCTIONS.deterministic("lower"));
    }
}
