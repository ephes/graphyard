//! InfluxDB v2: the Flux query text (`influx.py:_query_range_v2_flux`), the
//! request body `influxdb_client` 1.50.0 sends, and the decoding of the
//! annotated CSV response (`FluxCsvParser` in table mode, then the sample
//! loop of `_query_range_v2_flux`). PLAN.md 6.5.

use graphyard_core::pycompat::{
    PyDateTime, py_b64decode, py_bytes_repr, py_float, py_float_repr, py_fromisoformat, py_int,
    py_isoformat_utc, py_str_repr,
};
use graphyard_core::utc::UtcDateTime;
use graphyard_core::{MetricSample, Tags};
use indexmap::IndexMap;
use num_bigint::BigInt;

use crate::error::QueryError;
use crate::window::QueryRange;

/// `influx._flux_escape`: `\`, `"` and `${` escaped, in that order.
pub fn flux_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace("${", "\\${")
}

/// `influx._flux_record_access`: bracket access `r["<key>"]`.
fn record_access(key: &str) -> String {
    format!("r[\"{}\"]", flux_escape(key))
}

/// `influx._fmt_utc_query_ts`: `isoformat()` of the UTC time with `Z`
/// (`influx.query_range` normalizes the bounds to UTC first).
pub fn flux_timestamp(ts: &UtcDateTime) -> String {
    py_isoformat_utc(ts).replace("+00:00", "Z")
}

/// The Flux text of `_query_range_v2_flux` (byte-identical).
pub fn flux_query(range: &QueryRange, bucket: &str, measurement: &str) -> String {
    let mut clauses = vec![
        format!("r._measurement == \"{}\"", flux_escape(measurement)),
        "r._field == \"value\"".to_owned(),
        format!(
            "{} == \"{}\"",
            record_access("metric"),
            flux_escape(&range.metric)
        ),
    ];
    for (key, value) in range.dimension_filters() {
        clauses.push(format!(
            "{} == \"{}\"",
            record_access(key),
            flux_escape(value)
        ));
    }
    for (key, value) in &range.tags {
        clauses.push(format!(
            "{} == \"{}\"",
            record_access(key),
            flux_escape(value)
        ));
    }
    format!(
        "from(bucket: \"{}\")\n  |> range(start: {}, stop: {})\n  |> filter(fn: (r) => {})\n  |> sort(columns: [\"_time\"], desc: false)",
        flux_escape(bucket),
        flux_timestamp(&range.start),
        flux_timestamp(&range.stop),
        clauses.join(" and ")
    )
}

/// `json.dumps(text)` of a `str` (`ensure_ascii=True`): `"` and `\`
/// escaped, `\b \f \n \r \t` short, every other character outside
/// `0x20..0x7e` as `\uXXXX` (UTF-16 surrogate pairs above the BMP).
pub(crate) fn py_json_ascii_string(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            ' '..='~' => out.push(c),
            c => {
                let mut units = [0u16; 2];
                for unit in c.encode_utf16(&mut units) {
                    out.push_str(&format!("\\u{unit:04x}"));
                }
            }
        }
    }
    out.push('"');
    out
}

/// The JSON body `QueryApi.query` posts (`Query` with the default dialect
/// and an empty `extern` file), as `json.dumps` renders it.
pub fn flux_request_body(flux: &str) -> String {
    format!(
        "{{\"extern\": {{\"imports\": [], \"body\": []}}, \"query\": {}, \"dialect\": \
         {{\"header\": true, \"delimiter\": \",\", \"annotations\": [\"datatype\", \"group\", \
         \"default\"], \"commentPrefix\": \"#\", \"dateTimeFormat\": \"RFC3339\"}}}}",
        py_json_ascii_string(flux)
    )
}

/// A cell value as `FluxCsvParser._to_value` types it.
#[derive(Clone, Debug, PartialEq)]
pub enum FluxValue {
    None,
    Str(String),
    Bool(bool),
    Int(BigInt),
    Float(f64),
    Bytes(Vec<u8>),
    DateTime(PyDateTime),
}

impl FluxValue {
    /// `str(value)`.
    pub fn py_str(&self) -> String {
        match self {
            Self::None => "None".to_owned(),
            Self::Str(text) => text.clone(),
            Self::Bool(true) => "True".to_owned(),
            Self::Bool(false) => "False".to_owned(),
            Self::Int(int) => int.to_string(),
            Self::Float(float) => py_float_repr(*float),
            Self::Bytes(bytes) => py_bytes_repr(bytes),
            Self::DateTime(dt) => dt.to_string(),
        }
    }
}

/// A `FluxRecord`'s `values`: unique column labels in column order.
pub type FluxRecord = IndexMap<String, FluxValue>;

#[derive(Clone, Debug)]
struct FluxColumn {
    index: usize,
    data_type: String,
    label: Option<String>,
    default_value: Option<String>,
}

const ANNOTATIONS: [&str; 3] = ["#default", "#group", "#datatype"];

/// Python's `csv` module refuses a field longer than this many characters
/// (`csv.field_size_limit()` default).
const CSV_FIELD_LIMIT: usize = 131_072;
const MAX_EXPANDED_BYTES: usize = 16 * 1024 * 1024;
const MAX_RECORDS: usize = 100_000;
const MAX_CELLS: usize = 500_000;

/// `_FluxCsvParserMetadata` plus the parsed columns of the current table.
#[derive(Default)]
struct ParserState {
    columns: Vec<FluxColumn>,
    has_table: bool,
    table_id: Option<BigInt>,
    start_new_table: bool,
    groups: Vec<String>,
    parsing_state_error: bool,
    expanded_bytes: usize,
    records: usize,
    cells: usize,
}

/// `FluxCsvParser._to_value`.
fn to_value(text: &str, column: &FluxColumn) -> Result<FluxValue, QueryError> {
    if text.is_empty() {
        return match column.default_value.as_deref() {
            None | Some("") => Ok(FluxValue::None),
            Some(default) => to_value(default, column),
        };
    }
    Ok(match column.data_type.as_str() {
        "string" => FluxValue::Str(text.to_owned()),
        "boolean" => FluxValue::Bool(text == "true"),
        "unsignedLong" | "long" | "duration" => FluxValue::Int(py_int(text)?),
        "double" => FluxValue::Float(py_float(text)?),
        "base64Binary" => FluxValue::Bytes(py_b64decode(text)?),
        "dateTime:RFC3339" | "dateTime:RFC3339Nano" => FluxValue::DateTime(py_fromisoformat(text)?),
        _ => FluxValue::None,
    })
}

impl ParserState {
    /// `_parse_flux_response_row` for one CSV row; a data row yields a
    /// record.
    fn row(&mut self, csv: &[String]) -> Result<Option<FluxRecord>, QueryError> {
        if csv.is_empty() {
            return Ok(None);
        }
        let cell = |index: usize| csv.get(index).ok_or_else(QueryError::index_error);
        if cell(1)? == "error" && cell(2)? == "reference" {
            self.parsing_state_error = true;
            return Ok(None);
        }
        if self.parsing_state_error {
            // `FluxQueryException(message, reference)` does not call
            // `Exception.__init__`, so `str()` is the repr of both arguments.
            let message = cell(1)?;
            let reference = cell(2)?;
            return Err(QueryError::python(
                "FluxQueryException",
                format!("({}, {})", py_str_repr(message), py_str_repr(reference)),
            ));
        }
        let token = csv[0].as_str();
        if ANNOTATIONS.contains(&token) && !self.start_new_table {
            self.start_new_table = true;
            self.has_table = true;
            self.columns.clear();
            self.table_id = None;
        } else if !self.has_table {
            return Err(QueryError::python(
                "FluxCsvParserException",
                "Unable to parse CSV response. FluxTable definition was not found.",
            ));
        }
        match token {
            "#datatype" => {
                for (index, data_type) in csv.iter().enumerate().skip(1) {
                    self.columns.push(FluxColumn {
                        index: index - 1,
                        data_type: data_type.clone(),
                        label: None,
                        default_value: None,
                    });
                }
            }
            "#group" => self.groups = csv.to_vec(),
            "#default" => {
                for (i, column) in self.columns.iter_mut().enumerate() {
                    column.default_value = Some(cell(i + 1)?.clone());
                }
            }
            _ if self.start_new_table => {
                // `add_groups` only reads the flags here (IndexError when the
                // `#group` row is short or missing).
                if self.columns.len() >= self.groups.len() && !self.columns.is_empty() {
                    return Err(QueryError::index_error());
                }
                let mut seen = std::collections::HashSet::new();
                let duplicates = csv
                    .iter()
                    .skip(1)
                    .filter(|label| !seen.insert(*label))
                    .count();
                if duplicates > 0 {
                    log::warn!("InfluxDB Flux duplicate_columns count={duplicates}");
                    return Err(QueryError::config("InfluxDB Flux duplicate column labels"));
                }
                for (i, column) in self.columns.iter_mut().enumerate() {
                    column.label = Some(cell(i + 1)?.clone());
                }
                self.start_new_table = false;
            }
            _ => {
                let current_id = py_int(cell(2)?)?;
                match &self.table_id {
                    // A new table id starts a new table with the same columns.
                    Some(id) if *id == current_id => {}
                    _ => self.table_id = Some(current_id),
                }
                self.records += 1;
                if self.records > MAX_RECORDS {
                    return Err(crate::write::budget_error("Flux", "record limit", &[]).into());
                }
                let mut record = FluxRecord::new();
                for column in &self.columns {
                    let text = cell(column.index + 1)?;
                    let expanded = if text.is_empty() {
                        column.default_value.as_deref().unwrap_or("")
                    } else {
                        text
                    };
                    // Charge before materialization, including duplicated labels
                    // and a conservative allowance for each retained map cell.
                    self.expanded_bytes = self.expanded_bytes.saturating_add(
                        expanded.len() + column.label.as_deref().unwrap_or("None").len() + 64,
                    );
                    self.cells += 1;
                    if self.expanded_bytes > MAX_EXPANDED_BYTES || self.cells > MAX_CELLS {
                        return Err(
                            crate::write::budget_error("Flux", "expanded cell limit", &[]).into(),
                        );
                    }
                    let value = to_value(text, column)?;
                    let label = column.label.clone().unwrap_or_else(|| "None".to_owned());
                    record.insert(label, value);
                }
                return Ok(Some(record));
            }
        }
        Ok(None)
    }
}

/// Parse CSV text into rows with Python's `csv.reader` (excel dialect) rules
/// as far as the `csv` crate implements them, feeding each row to `on_row`
/// in order.
fn for_each_csv_row(
    text: &str,
    mut on_row: impl FnMut(&[String]) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(text.as_bytes());
    let mut record = csv::StringRecord::new();
    while let Some(row) = read_csv_row(&mut reader, &mut record)? {
        on_row(&row)?;
    }
    Ok(())
}

fn read_csv_row(
    reader: &mut csv::Reader<&[u8]>,
    record: &mut csv::StringRecord,
) -> Result<Option<Vec<String>>, QueryError> {
    if !reader
        .read_record(record)
        .map_err(|err| QueryError::python("Error", err.to_string()))?
    {
        return Ok(None);
    }
    if record
        .iter()
        .any(|field| field.chars().count() > CSV_FIELD_LIMIT)
    {
        return Err(QueryError::python(
            "Error",
            format!("field larger than field limit ({CSV_FIELD_LIMIT})"),
        ));
    }
    Ok(Some(record.iter().map(str::to_owned).collect()))
}

/// `str(UnicodeDecodeError)` of the UTF-8 decoder as `codecs.iterdecode`
/// meets it: positions count from the start of the response line (urllib3
/// iterates the body line by line).
fn unicode_decode_error(body: &[u8], err: &std::str::Utf8Error) -> QueryError {
    let at = err.valid_up_to();
    let line_start = body[..at]
        .iter()
        .rposition(|byte| *byte == b'\n')
        .map_or(0, |i| i + 1);
    let position = at - line_start;
    let message = match err.error_len() {
        Some(1) | None => {
            let reason = if err.error_len().is_none() {
                "unexpected end of data"
            } else if body[at] >= 0x80 && body[at] < 0xc2 || body[at] > 0xf4 {
                "invalid start byte"
            } else {
                "invalid continuation byte"
            };
            format!(
                "'utf-8' codec can't decode byte 0x{:02x} in position {position}: {reason}",
                body[at]
            )
        }
        Some(len) => format!(
            "'utf-8' codec can't decode bytes in position {position}-{}: invalid continuation byte",
            position + len - 1
        ),
    };
    QueryError::python("UnicodeDecodeError", message)
}

/// `FluxCsvParser` over a whole response body in table mode: the records of
/// every table, in order.
///
/// # Errors
/// The exceptions the parser raises, at the row where Python raises them
/// (`IndexError`, `ValueError` of the typed conversions, `binascii.Error`,
/// `FluxQueryException`, `FluxCsvParserException`, `UnicodeDecodeError`).
pub fn parse_annotated_csv(body: &[u8]) -> Result<Vec<FluxRecord>, QueryError> {
    let (text, decode_error) = decoded_csv_text(body);
    let mut state = ParserState::default();
    let mut records = Vec::new();
    for_each_csv_row(text, |row| {
        if let Some(record) = state.row(row)? {
            records.push(record);
        }
        Ok(())
    })?;
    match decode_error {
        Some(err) => Err(err),
        None => Ok(records),
    }
}

fn decoded_csv_text(body: &[u8]) -> (&str, Option<QueryError>) {
    match std::str::from_utf8(body) {
        Ok(text) => (text, None),
        Err(err) => {
            // The lines before the undecodable one are parsed first.
            let valid = &body[..err.valid_up_to()];
            let line_start = valid
                .iter()
                .rposition(|byte| *byte == b'\n')
                .map_or(0, |i| i + 1);
            let prefix = std::str::from_utf8(&body[..line_start]).unwrap_or_default();
            (prefix, Some(unicode_decode_error(body, &err)))
        }
    }
}

/// `influx._parse_query_value`: `float(str(value))`, finite.
pub(crate) fn sample_value(text: &str) -> Result<f64, QueryError> {
    match py_float(text) {
        Ok(value) if value.is_finite() => Ok(value),
        _ => Err(QueryError::config(
            "InfluxDB query sample must be a finite number",
        )),
    }
}

/// Columns `_query_range_v2_flux` does not turn into custom tags.
const NON_TAG_COLUMNS: [&str; 18] = [
    "result",
    "table",
    "_start",
    "_stop",
    "_time",
    "_value",
    "_field",
    "_measurement",
    "host",
    "service",
    "metric",
    "subject_type",
    "subject_id",
    "source_system",
    "source_instance",
    "source_entity_id",
    "collector_service",
    "collector_host",
];

/// The samples `_query_range_v2_flux` builds from the records.
///
/// # Errors
/// `InfluxConfigurationError` for missing or malformed row/time/value evidence.
pub fn flux_samples(
    records: &[FluxRecord],
    _metric_name: &str,
) -> Result<Vec<MetricSample>, QueryError> {
    let mut samples = Vec::new();
    for record in records {
        let invalid_row = || QueryError::config("Invalid InfluxDB query row");
        let value = record.get("_value").ok_or_else(invalid_row)?;
        if *value == FluxValue::None {
            return Err(invalid_row());
        }
        let time = record.get("_time").ok_or_else(invalid_row)?;
        let ts = match time {
            FluxValue::DateTime(dt) => dt
                .to_utc()
                .map_err(|_| QueryError::config("Invalid InfluxDB query timestamp"))?,
            _ => return Err(invalid_row()),
        };
        let value = sample_value(&value.py_str())?;
        let optional = |key: &str| match record.get(key) {
            None | Some(FluxValue::None) => None,
            Some(value) => Some(value.py_str()),
        };
        let tags: Tags = record
            .iter()
            .filter(|(key, _)| !NON_TAG_COLUMNS.contains(&key.as_str()))
            .map(|(key, value)| (key.clone(), value.py_str()))
            .collect();
        samples.push(MetricSample {
            ts,
            value,
            host: record
                .get("host")
                .map(FluxValue::py_str)
                .unwrap_or_default(),
            metric: record
                .get("metric")
                .map_or_else(String::new, FluxValue::py_str),
            service: optional("service"),
            subject_type: optional("subject_type"),
            subject_id: optional("subject_id"),
            source_system: optional("source_system"),
            source_instance: optional("source_instance"),
            source_entity_id: optional("source_entity_id"),
            collector_service: optional("collector_service"),
            collector_host: optional("collector_host"),
            tags,
        });
    }
    Ok(samples)
}

fn check_parse_deadline(deadline: tokio::time::Instant) -> Result<(), QueryError> {
    if tokio::time::Instant::now() >= deadline {
        return Err(crate::write::budget_error("Flux", "total deadline", &[]).into());
    }
    Ok(())
}

/// Parse bounded row batches with cooperative scheduling and no partial result.
pub async fn parse_annotated_csv_cooperative(
    body: &[u8],
    deadline: tokio::time::Instant,
) -> Result<Vec<FluxRecord>, QueryError> {
    check_parse_deadline(deadline)?;
    let (text, decode_error) = decoded_csv_text(body);
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(text.as_bytes());
    let mut csv_record = csv::StringRecord::new();
    let mut state = ParserState::default();
    let mut records = Vec::new();
    let mut rows = 0usize;
    let mut bytes = 0usize;
    while let Some(row) = read_csv_row(&mut reader, &mut csv_record)? {
        check_parse_deadline(deadline)?;
        let before = state.expanded_bytes;
        if let Some(record) = state.row(&row)? {
            records.push(record);
        }
        rows += 1;
        bytes += state.expanded_bytes - before + csv_record.as_slice().len();
        if rows >= 32 || bytes >= 64 * 1024 {
            tokio::task::yield_now().await;
            check_parse_deadline(deadline)?;
            rows = 0;
            bytes = 0;
        }
    }
    check_parse_deadline(deadline)?;
    match decode_error {
        Some(err) => Err(err),
        None => Ok(records),
    }
}

pub(crate) async fn flux_samples_cooperative(
    records: &[FluxRecord],
    metric: &str,
    deadline: tokio::time::Instant,
) -> Result<Vec<MetricSample>, QueryError> {
    let mut samples = Vec::new();
    for chunk in records.chunks(32) {
        check_parse_deadline(deadline)?;
        samples.extend(flux_samples(chunk, metric)?);
        tokio::task::yield_now().await;
    }
    check_parse_deadline(deadline)?;
    Ok(samples)
}
