//! OpenTelemetry setup and gRPC request instrumentation.
//!
//! OTLP targets are intentionally configured through the standard `OTEL_*`
//! environment variables so deployments can point traces and metrics at an APM
//! backend without adding pathlockd-specific config keys.

use std::future::Future;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::{global, KeyValue};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use tonic::metadata::{KeyRef, MetadataMap};
use tonic::{Code, Request, Response, Status};
use tracing::{field, Instrument, Span};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

const SERVICE_NAME: &str = "pathlockd";
const INSTRUMENTATION_NAME: &str = "pathlockd";

static METRICS: OnceLock<Metrics> = OnceLock::new();

#[derive(Default)]
pub struct TelemetryGuard {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
}

impl TelemetryGuard {
    pub fn traces_enabled(&self) -> bool {
        self.tracer_provider.is_some()
    }

    pub fn metrics_enabled(&self) -> bool {
        self.meter_provider.is_some()
    }

    pub fn shutdown(&self) -> anyhow::Result<()> {
        let mut errors = Vec::new();
        if let Some(provider) = &self.tracer_provider {
            if let Err(e) = provider.shutdown() {
                errors.push(format!("traces: {e}"));
            }
        }
        if let Some(provider) = &self.meter_provider {
            if let Err(e) = provider.shutdown() {
                errors.push(format!("metrics: {e}"));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            anyhow::bail!("OpenTelemetry shutdown failed: {}", errors.join("; "))
        }
    }
}

pub fn init(log_level: &str) -> anyhow::Result<TelemetryGuard> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(log_level.to_string()));
    let fmt_layer = tracing_subscriber::fmt::layer();

    if sdk_disabled() {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .try_init()?;
        return Ok(TelemetryGuard::default());
    }

    let traces_enabled =
        signal_enabled("OTEL_TRACES_EXPORTER", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT");
    let metrics_enabled = signal_enabled(
        "OTEL_METRICS_EXPORTER",
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    );
    let resource = resource();

    let tracer_provider = if traces_enabled {
        let provider = build_tracer_provider(resource.clone())?;
        global::set_text_map_propagator(TraceContextPropagator::new());
        global::set_tracer_provider(provider.clone());
        Some(provider)
    } else {
        None
    };

    let meter_provider = if metrics_enabled {
        let provider = build_meter_provider(resource)?;
        global::set_meter_provider(provider.clone());
        let _ = METRICS.set(Metrics::new());
        Some(provider)
    } else {
        None
    };

    if let Some(provider) = &tracer_provider {
        let tracer = provider.tracer(INSTRUMENTATION_NAME);
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .with(otel_layer)
            .try_init()?;
    } else {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .try_init()?;
    }

    Ok(TelemetryGuard {
        tracer_provider,
        meter_provider,
    })
}

pub async fn observe_rpc<Req, Resp, Fut, Handler>(
    service: &'static str,
    method: &'static str,
    request: Request<Req>,
    handler: Handler,
) -> Result<Response<Resp>, Status>
where
    Fut: Future<Output = Result<Response<Resp>, Status>>,
    Handler: FnOnce(Request<Req>) -> Fut,
{
    let parent = global::get_text_map_propagator(|propagator| {
        propagator.extract(&MetadataExtractor(request.metadata()))
    });
    let span = rpc_span(service, method);
    let _ = span.set_parent(parent);

    let started = Instant::now();
    let result = handler(request).instrument(span.clone()).await;
    let elapsed = started.elapsed();
    let (code, description) = match &result {
        Ok(_) => (Code::Ok, ""),
        Err(status) => (status.code(), status.message()),
    };

    record_span_status(&span, code, description);
    record_rpc_metrics(service, method, code, elapsed);
    result
}

pub fn record_gc_sweep(reclaimed: u64, elapsed: Duration, success: bool) {
    if let Some(metrics) = METRICS.get() {
        let attrs = [KeyValue::new("success", success)];
        metrics.gc_sweeps.add(1, &attrs);
        metrics
            .gc_duration_ms
            .record(elapsed.as_secs_f64() * 1000.0, &attrs);
        if reclaimed > 0 {
            metrics.gc_reclaimed.add(reclaimed, &attrs);
        }
    }
}

fn build_tracer_provider(resource: Resource) -> anyhow::Result<SdkTracerProvider> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .build()
        .map_err(|e| anyhow::anyhow!("building OTLP trace exporter: {e}"))?;
    Ok(SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter)
        .build())
}

fn build_meter_provider(resource: Resource) -> anyhow::Result<SdkMeterProvider> {
    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .build()
        .map_err(|e| anyhow::anyhow!("building OTLP metrics exporter: {e}"))?;
    Ok(SdkMeterProvider::builder()
        .with_resource(resource)
        .with_periodic_exporter(exporter)
        .build())
}

fn resource() -> Resource {
    let builder = Resource::builder();
    let builder = if env_string("OTEL_SERVICE_NAME").is_none() && !resource_has_service_name() {
        builder.with_service_name(SERVICE_NAME)
    } else {
        builder
    };
    builder.build()
}

fn signal_enabled(exporter_key: &str, endpoint_key: &str) -> bool {
    if exporter_is_none(exporter_key) {
        return false;
    }
    otlp_endpoint_configured(endpoint_key) || exporter_requests_otlp(exporter_key)
}

fn otlp_endpoint_configured(signal_endpoint_key: &str) -> bool {
    env_string("OTEL_EXPORTER_OTLP_ENDPOINT").is_some() || env_string(signal_endpoint_key).is_some()
}

fn exporter_requests_otlp(key: &str) -> bool {
    env_string(key)
        .map(|value| {
            value
                .split(',')
                .any(|part| part.trim().eq_ignore_ascii_case("otlp"))
        })
        .unwrap_or(false)
}

fn exporter_is_none(key: &str) -> bool {
    env_string(key)
        .map(|value| {
            value
                .split(',')
                .all(|part| part.trim().eq_ignore_ascii_case("none"))
        })
        .unwrap_or(false)
}

fn sdk_disabled() -> bool {
    env_string("OTEL_SDK_DISABLED")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn resource_has_service_name() -> bool {
    env_string("OTEL_RESOURCE_ATTRIBUTES")
        .map(|attrs| {
            attrs
                .split(',')
                .filter_map(|pair| pair.split_once('='))
                .any(|(key, _)| key.trim() == "service.name")
        })
        .unwrap_or(false)
}

fn env_string(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn rpc_span(service: &'static str, method: &'static str) -> Span {
    let otel_name = format!("{service}/{method}");
    tracing::info_span!(
        "grpc.request",
        "otel.name" = otel_name.as_str(),
        "otel.kind" = "server",
        "rpc.system" = "grpc",
        "rpc.service" = service,
        "rpc.method" = method,
        "grpc.status_code" = field::Empty,
        "grpc.status_text" = field::Empty,
        "otel.status_code" = field::Empty,
        "otel.status_description" = field::Empty,
    )
}

fn record_span_status(span: &Span, code: Code, description: &str) {
    span.record("grpc.status_code", code as i64);
    span.record("grpc.status_text", grpc_code_name(code));
    if code == Code::Ok {
        span.record("otel.status_code", "OK");
    } else {
        span.record("otel.status_code", "ERROR");
        if description.is_empty() {
            span.record("otel.status_description", code.description());
        } else {
            span.record("otel.status_description", description);
        }
    }
}

fn record_rpc_metrics(service: &'static str, method: &'static str, code: Code, elapsed: Duration) {
    if let Some(metrics) = METRICS.get() {
        let attrs = [
            KeyValue::new("rpc.system", "grpc"),
            KeyValue::new("rpc.service", service),
            KeyValue::new("rpc.method", method),
            KeyValue::new("grpc.status_code", code as i64),
            KeyValue::new("grpc.status_text", grpc_code_name(code)),
        ];
        metrics.rpc_requests.add(1, &attrs);
        metrics
            .rpc_duration_ms
            .record(elapsed.as_secs_f64() * 1000.0, &attrs);
        if code != Code::Ok {
            metrics.rpc_errors.add(1, &attrs);
        }
    }
}

fn grpc_code_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "OK",
        Code::Cancelled => "CANCELLED",
        Code::Unknown => "UNKNOWN",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Aborted => "ABORTED",
        Code::OutOfRange => "OUT_OF_RANGE",
        Code::Unimplemented => "UNIMPLEMENTED",
        Code::Internal => "INTERNAL",
        Code::Unavailable => "UNAVAILABLE",
        Code::DataLoss => "DATA_LOSS",
        Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

struct MetadataExtractor<'a>(&'a MetadataMap);

impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|key| match key {
                KeyRef::Ascii(key) => key.as_str(),
                KeyRef::Binary(key) => key.as_str(),
            })
            .collect()
    }
}

struct Metrics {
    rpc_requests: Counter<u64>,
    rpc_errors: Counter<u64>,
    rpc_duration_ms: Histogram<f64>,
    gc_sweeps: Counter<u64>,
    gc_reclaimed: Counter<u64>,
    gc_duration_ms: Histogram<f64>,
}

impl Metrics {
    fn new() -> Self {
        let meter = global::meter(INSTRUMENTATION_NAME);
        Self {
            rpc_requests: meter
                .u64_counter("pathlockd.grpc.server.requests")
                .with_description("Completed gRPC server requests.")
                .build(),
            rpc_errors: meter
                .u64_counter("pathlockd.grpc.server.errors")
                .with_description("Completed gRPC server requests with non-OK status.")
                .build(),
            rpc_duration_ms: meter
                .f64_histogram("pathlockd.grpc.server.duration")
                .with_description("gRPC server request duration.")
                .with_unit("ms")
                .build(),
            gc_sweeps: meter
                .u64_counter("pathlockd.gc.sweeps")
                .with_description("Completed storage GC sweeps.")
                .build(),
            gc_reclaimed: meter
                .u64_counter("pathlockd.gc.reclaimed")
                .with_description("Expired keys reclaimed by storage GC.")
                .build(),
            gc_duration_ms: meter
                .f64_histogram("pathlockd.gc.duration")
                .with_description("Storage GC sweep duration.")
                .with_unit("ms")
                .build(),
        }
    }
}
