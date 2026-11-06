//! Traces over OTLP, configured as openetld's are: with the standard OTEL_*
//! variables, which the runner passes on with OTEL_SERVICE_NAME set to
//! openetl-engine. Nothing is exported until an exporter or an endpoint is
//! set. The job's span continues the trace in StartJob's metadata.

use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{Context, KeyValue};
use opentelemetry_otlp::{SpanExporter, WithExportConfig as _};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider};
use tonic::metadata::{KeyRef, MetadataMap};
use tracing_opentelemetry::OpenTelemetryLayer;

/// What the engine's traces are filtered to: its own spans, batches
/// included, but not its libraries'.
pub const FILTER: &str = "openetl_engine=debug";

/// The exporting tracer provider, if OTEL_* configures one.
pub fn provider(version: &'static str) -> Result<Option<SdkTracerProvider>, String> {
    let var = |k: &str| std::env::var(k).unwrap_or_default();
    let configured = [
        "OTEL_TRACES_EXPORTER",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    ]
    .iter()
    .any(|k| !var(k).is_empty());
    if !configured || var("OTEL_SDK_DISABLED").eq_ignore_ascii_case("true") {
        return Ok(None);
    }
    match var("OTEL_TRACES_EXPORTER").as_str() {
        "" | "otlp" => {}
        "none" => return Ok(None),
        other => {
            return Err(format!(
                "OTEL_TRACES_EXPORTER {other:?}: only otlp and none"
            ));
        }
    }
    let protocol = Some(var("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"))
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| var("OTEL_EXPORTER_OTLP_PROTOCOL"));
    let exporter = match protocol.as_str() {
        "grpc" => SpanExporter::builder().with_tonic().build(),
        "" | "http/protobuf" => SpanExporter::builder()
            .with_http()
            .with_protocol(opentelemetry_otlp::Protocol::HttpBinary)
            .build(),
        other => {
            return Err(format!(
                "OTLP protocol {other:?}: only grpc and http/protobuf"
            ));
        }
    }
    .map_err(|e| format!("OTLP span exporter: {e}"))?;
    let resource = Resource::builder()
        .with_attributes([
            KeyValue::new("service.version", version),
            KeyValue::new("openetl.role", "engine"),
        ])
        .build();
    Ok(Some(
        SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource)
            .build(),
    ))
}

pub fn layer<S>(provider: &SdkTracerProvider) -> OpenTelemetryLayer<S, SdkTracer>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    tracing_opentelemetry::layer().with_tracer(provider.tracer("openetl-engine"))
}

/// The trace context a gRPC request carries, if any.
pub fn remote_context(metadata: &MetadataMap) -> Context {
    TraceContextPropagator::new().extract(&Metadata(metadata))
}

struct Metadata<'a>(&'a MetadataMap);

impl Extractor for Metadata<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .filter_map(|k| match k {
                KeyRef::Ascii(k) => Some(k.as_str()),
                KeyRef::Binary(_) => None,
            })
            .collect()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use opentelemetry::trace::TraceContextExt as _;

    use super::*;

    #[test]
    fn reads_the_trace_context_from_metadata() {
        let mut md = MetadataMap::new();
        md.insert(
            "traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
                .parse()
                .unwrap(),
        );
        let cx = remote_context(&md);
        let sc = cx.span().span_context().clone();
        assert!(sc.is_remote());
        assert_eq!(
            sc.trace_id().to_string(),
            "4bf92f3577b34da6a3ce929d0e0e4736"
        );
        assert_eq!(sc.span_id().to_string(), "00f067aa0ba902b7");
        assert!(!remote_context(&MetadataMap::new()).has_active_span());
    }
}
