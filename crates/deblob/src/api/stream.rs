//! `GET /api/v1/stream` — the live-stream tap (Stage L1): Server-Sent
//! Events of payload-free [`deblob_kafka::StreamEvent`]s, one per hot-path
//! record outcome. Authenticated exactly like every other `/api/v1/*`
//! route (`super::router`'s `route_layer`) — never reachable without the
//! same bearer token every other management-API endpoint requires.
//!
//! Best-effort, like the underlying `tokio::sync::broadcast` channel
//! itself: a slow subscriber that falls behind the channel's fixed
//! capacity (`crate::serve::STREAM_CHANNEL_CAPACITY`) silently misses the
//! events it lagged past (`BroadcastStreamRecvError::Lagged`) rather than
//! the connection erroring out — an SSE tap for a live dashboard is a
//! lossy multicast, never a delivery guarantee, and must never fail the
//! whole stream over one skipped event.

use std::convert::Infallible;

use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};

use super::ApiState;

/// Subscribes a fresh `Receiver` onto `state.stream_tx` for the lifetime of
/// this SSE connection and relays every successfully-received
/// `deblob_kafka::StreamEvent` as one `data:` JSON SSE event. A
/// lagged/dropped batch of events (this subscriber fell behind) is skipped
/// rather than surfaced as an SSE error; a `StreamEvent` that somehow fails
/// to serialize (never expected — it's a plain struct of ids/strings/counts,
/// see `deblob_kafka::stream`'s own docs) is skipped the same way, for the
/// same "never fail the whole stream over one event" reason.
pub async fn get_stream(
    State(state): State<ApiState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.stream_tx.subscribe();
    let events = BroadcastStream::new(rx).filter_map(|item| {
        let event = item.ok()?;
        let sse_event = Event::default().json_data(&event).ok()?;
        Some(Ok(sse_event))
    });
    Sse::new(events).keep_alive(KeepAlive::default())
}
