// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use anyhow::Context;
use async_graphql::dataloader::DataLoader;
use bytes::Bytes;
use futures::Stream;
use futures::StreamExt;
use prometheus::Registry;
use sui_rpc::proto::sui::rpc::v2 as grpc;
use sui_rpc::proto::sui::rpc::v2::ledger_service_client::LedgerServiceClient as V2LedgerServiceClient;
use sui_rpc::proto::sui::rpc::v2alpha as grpc_alpha;
use sui_rpc::proto::sui::rpc::v2alpha::ledger_service_client::LedgerServiceClient as V2alphaLedgerServiceClient;
use sui_types::effects::TransactionEffects;
use sui_types::event::Event;
use sui_types::messages_checkpoint::CheckpointSummary;
use sui_types::signature::GenericSignature;
use sui_types::transaction::TransactionData;
use tonic::transport::Channel;
use tonic::transport::ClientTlsConfig;
use tonic::transport::Uri;
use tower::Layer;
use tracing::warn;

use crate::metrics::GrpcMetricsLayer;
use crate::metrics::GrpcMetricsService;

const DEFAULT_MAX_DECODING_MESSAGE_SIZE: usize = 32 * 1024 * 1024;

#[derive(clap::Args, Debug, Clone, Default)]
pub struct LedgerGrpcArgs {
    /// Timeout for gRPC statements to the ledger service, in milliseconds.
    #[arg(long)]
    pub ledger_grpc_statement_timeout_ms: Option<u64>,

    /// Maximum gRPC decoding message size for Ledger service responses, in bytes.
    #[arg(long)]
    pub ledger_grpc_max_decoding_message_size: Option<usize>,

    /// Whether the configured ledger gRPC service has v2alpha experimental query APIs enabled (e.g.
    /// bitmap-backed `ListTransactions`). When unset, treated as `false`.
    #[arg(long)]
    pub enable_experimental_query_apis: Option<bool>,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Internal(#[from] anyhow::Error),

    #[error("Ledger gRPC v2alpha experimental query APIs not configured")]
    NotConfigured,
}

#[derive(Debug, Clone)]
pub struct CheckpointedTransaction {
    pub effects: Box<TransactionEffects>,
    pub events: Option<Vec<Event>>,
    pub transaction_data: Box<TransactionData>,
    pub signatures: Vec<GenericSignature>,
    pub timestamp_ms: Option<u64>,
    pub cp_sequence_number: Option<u64>,
    pub balance_changes: Vec<grpc::BalanceChange>,
}

/// A reader backed by gRPC LedgerService (sui-kv-rpc).
///
/// This connects to archival service that implements the same LedgerService gRPC interface
/// as fullnode, but is backed by Bigtable for serving historical data.
#[derive(Clone)]
pub struct LedgerGrpcReader {
    client: V2LedgerServiceClient<GrpcMetricsService<Channel>>,
    /// Client dedicated to experimental apis on `LedgerService`.
    alpha_client: Option<V2alphaLedgerServiceClient<GrpcMetricsService<Channel>>>,
    timeout: Option<Duration>,
}

/// A page drained from a stream consisting of the items in stream order, the latest watermark
/// cursor to continue paginating on, and why the stream stopped.
///
/// `end_cursor` may be beyond the last item in the collected page.
///
/// `end_reason` is `None` only when the stream terminated before a `QueryEnd` was received.
#[derive(Debug, Clone)]
pub struct StreamPage<I> {
    pub items: Vec<I>,
    pub end_cursor: Option<Bytes>,
    pub end_reason: Option<grpc_alpha::QueryEndReason>,
}

#[derive(Debug)]
pub enum FrameKind<I> {
    Item { item: I, cursor: Option<Bytes> },
    Watermark { cursor: Option<Bytes> },
    End { reason: i32 },
    Unknown,
}

impl LedgerGrpcArgs {
    pub fn statement_timeout(&self) -> Option<std::time::Duration> {
        self.ledger_grpc_statement_timeout_ms
            .map(Duration::from_millis)
    }
}

impl LedgerGrpcReader {
    pub async fn new(
        uri: Uri,
        args: LedgerGrpcArgs,
        prefix: Option<&str>,
        registry: &Registry,
    ) -> anyhow::Result<Self> {
        let mut endpoint = Channel::builder(uri.clone());
        if let Some(timeout) = args.statement_timeout() {
            endpoint = endpoint.timeout(timeout);
        }

        if uri.scheme_str() == Some("https") {
            let tls_config = ClientTlsConfig::new().with_native_roots();
            endpoint = endpoint.tls_config(tls_config)?;
        }

        let channel = endpoint.connect_lazy();
        let layered =
            GrpcMetricsLayer::new(prefix.unwrap_or("ledger_grpc"), registry).layer(channel);

        let timeout = args.statement_timeout();
        let max_decoding_message_size = args
            .ledger_grpc_max_decoding_message_size
            .unwrap_or(DEFAULT_MAX_DECODING_MESSAGE_SIZE);
        let client = V2LedgerServiceClient::new(layered.clone())
            .max_decoding_message_size(max_decoding_message_size);
        let alpha_client = if args.enable_experimental_query_apis.unwrap_or(false) {
            Some(
                V2alphaLedgerServiceClient::new(layered)
                    .max_decoding_message_size(max_decoding_message_size),
            )
        } else {
            None
        };

        Ok(Self {
            client,
            alpha_client,
            timeout,
        })
    }

    /// Whether this reader has v2alpha experimental query APIs configured.
    pub fn has_alpha(&self) -> bool {
        self.alpha_client.is_some()
    }

    pub fn as_data_loader(&self) -> DataLoader<Self> {
        DataLoader::new(self.clone(), tokio::spawn)
    }

    pub async fn checkpoint_watermark(&self) -> anyhow::Result<CheckpointSummary> {
        use grpc::GetCheckpointRequest;
        use prost_types::FieldMask;
        use sui_rpc::field::FieldMaskUtil;

        let request =
            GetCheckpointRequest::default().with_read_mask(FieldMask::from_paths(["summary.bcs"]));

        let response = self.get_checkpoint(request).await?;

        let checkpoint = response.checkpoint.context("No checkpoint returned")?;

        checkpoint
            .summary
            .as_ref()
            .and_then(|s| s.bcs.as_ref())
            .context("Missing summary.bcs")?
            .deserialize()
            .context("Failed to deserialize checkpoint summary")
    }

    /// Resolve a checkpoint digest to its sequence number via the ledger service. Returns `None`
    /// if no checkpoint with that digest is known.
    pub async fn checkpoint_seq_by_digest(
        &self,
        digest: sui_types::digests::CheckpointDigest,
    ) -> anyhow::Result<Option<u64>> {
        use grpc::GetCheckpointRequest;
        use prost_types::FieldMask;
        use sui_rpc::field::FieldMaskUtil;

        let sdk_digest = sui_sdk_types::Digest::new(digest.inner().to_owned());
        let request = GetCheckpointRequest::by_digest(&sdk_digest)
            .with_read_mask(FieldMask::from_paths(["sequence_number"]));

        match self.get_checkpoint(request).await {
            Ok(response) => {
                let checkpoint = response.checkpoint.context("No checkpoint returned")?;
                Ok(checkpoint.sequence_number)
            }
            Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
            Err(e) => Err(anyhow::anyhow!(e)),
        }
    }

    // Public wrapper methods for gRPC calls with metrics instrumentation

    pub async fn get_checkpoint(
        &self,
        request: grpc::GetCheckpointRequest,
    ) -> Result<grpc::GetCheckpointResponse, tonic::Status> {
        self.client
            .clone()
            .get_checkpoint(self.request(request))
            .await
            .map(|r| r.into_inner())
    }

    pub async fn batch_get_transactions(
        &self,
        request: grpc::BatchGetTransactionsRequest,
    ) -> Result<grpc::BatchGetTransactionsResponse, tonic::Status> {
        self.client
            .clone()
            .batch_get_transactions(self.request(request))
            .await
            .map(|r| r.into_inner())
    }

    pub async fn batch_get_objects(
        &self,
        request: grpc::BatchGetObjectsRequest,
    ) -> Result<grpc::BatchGetObjectsResponse, tonic::Status> {
        self.client
            .clone()
            .batch_get_objects(self.request(request))
            .await
            .map(|r| r.into_inner())
    }

    pub async fn get_transaction(
        &self,
        request: grpc::GetTransactionRequest,
    ) -> Result<grpc::GetTransactionResponse, tonic::Status> {
        self.client
            .clone()
            .get_transaction(self.request(request))
            .await
            .map(|r| r.into_inner())
    }

    /// Consumes the stream returned from a `list_transactions` request until server timeout or
    /// other terminal condition is met. The caller is responsible for resuming the next page from
    /// the `end_cursor` if there are more results to yield after the current page.
    pub async fn list_transactions(
        &self,
        request: grpc_alpha::ListTransactionsRequest,
    ) -> Result<StreamPage<grpc_alpha::TransactionItem>, Error> {
        let Some(mut alpha_client) = self.alpha_client.clone() else {
            return Err(Error::NotConfigured);
        };

        let stream = alpha_client
            .list_transactions(self.request(request))
            .await
            .map_err(|s| anyhow::anyhow!("ListTransactions stream open failed: {}", s.message()))?
            .into_inner();

        drain_list_stream("ListTransactions", stream).await
    }

    /// Create a gRPC request, optionally with the grpc-timeout header if configured.
    fn request<T>(&self, input: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(input);
        if let Some(timeout) = self.timeout {
            request.set_timeout(timeout);
        }
        request
    }
}

impl<I> StreamPage<I> {
    /// True while the server has not exhausted the requested range.
    pub fn has_more(&self) -> bool {
        !matches!(
            self.end_reason,
            Some(
                grpc_alpha::QueryEndReason::CheckpointBound
                    | grpc_alpha::QueryEndReason::CursorBound
                    | grpc_alpha::QueryEndReason::LedgerTip
            )
        )
    }

    /// The cursor to continue paginating, or `None` if the requested range has been exhausted and
    /// no further pagination is possible.
    ///
    /// Invariant: `has_more()` ⇒ `end_cursor.is_some()`. Enforced at the page boundary by
    /// `drain_list_stream` (returns `data_loss` on violation) and preserved by any caller that
    /// re-synthesizes a `StreamPage` from a previously-validated one's `end_reason` +
    /// `end_cursor` fields together.
    pub fn next_cursor(&self) -> Option<&Bytes> {
        self.has_more().then(|| {
            self.end_cursor
                .as_ref()
                .expect("invariant: has_more implies end_cursor is Some")
        })
    }

    /// Fold one frame into the page. The cursor is updated to the incoming item or standalone
    /// watermark.
    ///
    /// Returns `true` when the frame is the terminal `QueryEnd`.
    fn apply(&mut self, frame: FrameKind<I>) -> bool {
        match frame {
            FrameKind::Item { item, cursor } => {
                if cursor.is_some() {
                    self.end_cursor = cursor;
                }
                self.items.push(item);
            }
            FrameKind::Watermark { cursor } => {
                if cursor.is_some() {
                    self.end_cursor = cursor;
                }
            }
            FrameKind::End { reason } => {
                // Fold an unknown reason into `Unspecified` so `None` remains unambiguous shorthand
                // for "no End frame received" (i.e. the deadline cut the stream short).
                self.end_reason = match grpc_alpha::QueryEndReason::try_from(reason) {
                    Ok(decoded) => Some(decoded),
                    Err(_) => {
                        warn!(
                            reason_int = reason,
                            "list stream: server sent unknown QueryEndReason",
                        );
                        Some(grpc_alpha::QueryEndReason::Unspecified)
                    }
                };
                return true;
            }
            FrameKind::Unknown => {
                warn!("list stream: server sent empty or unrecognized Frame");
            }
        }
        false
    }
}

impl<I> Default for StreamPage<I> {
    fn default() -> Self {
        Self {
            items: Vec::new(),
            end_cursor: None,
            end_reason: None,
        }
    }
}

impl From<grpc_alpha::ListTransactionsResponse> for FrameKind<grpc_alpha::TransactionItem> {
    fn from(response: grpc_alpha::ListTransactionsResponse) -> Self {
        use grpc_alpha::list_transactions_response::Response;

        let Some(response) = response.response else {
            return FrameKind::Unknown;
        };
        match response {
            Response::Item(item) => {
                let cursor = item.watermark.as_ref().and_then(|w| w.cursor.clone());
                FrameKind::Item { item, cursor }
            }
            Response::Watermark(watermark) => FrameKind::Watermark {
                cursor: watermark.cursor,
            },
            Response::End(end) => FrameKind::End { reason: end.reason },
            _ => FrameKind::Unknown,
        }
    }
}

async fn drain_list_stream<R, I, S>(
    rpc_name: &'static str,
    stream: S,
) -> Result<StreamPage<I>, Error>
where
    R: Into<FrameKind<I>>,
    S: Stream<Item = Result<R, tonic::Status>>,
{
    futures::pin_mut!(stream);
    let mut page = StreamPage::default();
    loop {
        match stream.next().await {
            Some(Ok(response)) => {
                // Process and break on receiving `QueryEnd`.
                if page.apply(response.into()) {
                    break;
                }
            }
            // We expect the server to yield an `End` frame before reaching this branch.
            None => break,
            // `DeadlineExceeded`: server-side `grpc-timeout` header fired.
            // `Cancelled`: client-side channel timeout fired (or upstream cancel).
            // Both are timeout-shaped — preserve partial work if any progress was made;
            // propagate as error only if zero progress, so the caller can reshape.
            Some(Err(status))
                if matches!(
                    status.code(),
                    tonic::Code::DeadlineExceeded | tonic::Code::Cancelled
                ) =>
            {
                if page.items.is_empty() && page.end_cursor.is_none() {
                    return Err(anyhow::anyhow!(
                        "{rpc_name} stream {:?} with no progress: {}",
                        status.code(),
                        status.message()
                    )
                    .into());
                }
                break;
            }
            Some(Err(status)) => {
                return Err(
                    anyhow::anyhow!("{rpc_name} stream error: {}", status.message()).into(),
                );
            }
        }
    }

    // Pagination is considered unresumable if there is more server-side work, but no valid cursor
    // was yielded (either from a standalone `Watermark` or the last `Item`'s watermark.)
    if page.has_more() && page.end_cursor.is_none() {
        return Err(anyhow::anyhow!(
            "{rpc_name}: server reported more results but did not advance cursor — cannot resume"
        )
        .into());
    }

    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item_response(cursor: &[u8]) -> grpc_alpha::ListTransactionsResponse {
        let mut watermark = grpc_alpha::Watermark::default();
        watermark.cursor = Some(Bytes::copy_from_slice(cursor));
        let mut item = grpc_alpha::TransactionItem::default();
        item.watermark = Some(watermark);
        let mut response = grpc_alpha::ListTransactionsResponse::default();
        response.response = Some(grpc_alpha::list_transactions_response::Response::Item(item));
        response
    }

    fn watermark_response(cursor: &[u8]) -> grpc_alpha::ListTransactionsResponse {
        let mut watermark = grpc_alpha::Watermark::default();
        watermark.cursor = Some(Bytes::copy_from_slice(cursor));
        let mut response = grpc_alpha::ListTransactionsResponse::default();
        response.response = Some(grpc_alpha::list_transactions_response::Response::Watermark(
            watermark,
        ));
        response
    }

    fn end_response(reason: grpc_alpha::QueryEndReason) -> grpc_alpha::ListTransactionsResponse {
        let mut end = grpc_alpha::QueryEnd::default();
        end.reason = reason as i32;
        let mut response = grpc_alpha::ListTransactionsResponse::default();
        response.response = Some(grpc_alpha::list_transactions_response::Response::End(end));
        response
    }

    #[test]
    fn drains_items_tracking_latest_cursor_and_end_reason() {
        let mut page: StreamPage<grpc_alpha::TransactionItem> = StreamPage::default();
        assert!(!page.apply(item_response(b"c1").into()));
        assert!(!page.apply(watermark_response(b"c2").into()));
        assert!(!page.apply(item_response(b"c3").into()));
        // The terminal `QueryEnd` frame signals the caller to stop draining.
        assert!(page.apply(end_response(grpc_alpha::QueryEndReason::ItemLimit).into()));

        assert_eq!(page.items.len(), 2);
        // Latest cursor wins, including a standalone watermark between items.
        assert_eq!(page.end_cursor.as_deref(), Some(b"c3".as_ref()));
        assert_eq!(page.end_reason, Some(grpc_alpha::QueryEndReason::ItemLimit));
    }

    #[test]
    fn standalone_watermark_advances_cursor_without_items() {
        let mut page: StreamPage<grpc_alpha::TransactionItem> = StreamPage::default();
        assert!(!page.apply(watermark_response(b"w1").into()));
        assert!(page.apply(end_response(grpc_alpha::QueryEndReason::LedgerTip).into()));

        assert!(page.items.is_empty());
        assert_eq!(page.end_cursor.as_deref(), Some(b"w1".as_ref()));
        assert_eq!(page.end_reason, Some(grpc_alpha::QueryEndReason::LedgerTip));
    }

    #[test]
    fn has_more_true_when_truncated_or_timed_out() {
        // ITEM_LIMIT and SCAN_LIMIT both signal "we stopped short, resume here".
        for reason in [
            grpc_alpha::QueryEndReason::ItemLimit,
            grpc_alpha::QueryEndReason::ScanLimit,
        ] {
            let mut page: StreamPage<grpc_alpha::TransactionItem> = StreamPage::default();
            page.apply(end_response(reason).into());
            assert!(page.has_more(), "expected has_more for {reason:?}");
        }

        // `end_reason == None` covers both the deadline cut-short case (no terminal frame
        // received) and any unrecognized / future-added variant — defaulting to "may have more"
        // avoids silent truncation.
        let page: StreamPage<grpc_alpha::TransactionItem> = StreamPage::default();
        assert!(page.has_more());
    }

    #[test]
    fn has_more_false_when_range_exhausted() {
        for reason in [
            grpc_alpha::QueryEndReason::CheckpointBound,
            grpc_alpha::QueryEndReason::CursorBound,
            grpc_alpha::QueryEndReason::LedgerTip,
        ] {
            let mut page: StreamPage<grpc_alpha::TransactionItem> = StreamPage::default();
            page.apply(end_response(reason).into());
            assert!(!page.has_more(), "expected !has_more for {reason:?}");
        }
    }

    #[test]
    fn apply_end_with_unknown_reason_folds_to_unspecified() {
        // Reason int that doesn't decode to any known `QueryEndReason` variant — the SDK-skew
        // case. We log and degrade to `Unspecified` so callers still see a terminal reason.
        let mut end = grpc_alpha::QueryEnd::default();
        end.reason = i32::MAX;
        let mut response = grpc_alpha::ListTransactionsResponse::default();
        response.response = Some(grpc_alpha::list_transactions_response::Response::End(end));

        let mut page: StreamPage<grpc_alpha::TransactionItem> = StreamPage::default();
        assert!(page.apply(response.into()));
        assert_eq!(
            page.end_reason,
            Some(grpc_alpha::QueryEndReason::Unspecified)
        );
    }

    #[test]
    fn apply_unknown_frame_continues_draining() {
        // Outer message with no oneof set — classifies to `FrameKind::Unknown`. `apply` should
        // warn and continue (returns false) without mutating the page.
        let response = grpc_alpha::ListTransactionsResponse::default();

        let mut page: StreamPage<grpc_alpha::TransactionItem> = StreamPage::default();
        assert!(!page.apply(response.into()));
        assert!(page.items.is_empty());
        assert_eq!(page.end_cursor, None);
        assert_eq!(page.end_reason, None);
    }

    #[test]
    fn empty_response_classifies_as_unknown() {
        let response = grpc_alpha::ListTransactionsResponse::default();
        let kind: FrameKind<grpc_alpha::TransactionItem> = response.into();
        assert!(matches!(kind, FrameKind::Unknown));
    }

    async fn drain_iter(
        responses: Vec<Result<grpc_alpha::ListTransactionsResponse, tonic::Status>>,
    ) -> Result<StreamPage<grpc_alpha::TransactionItem>, Error> {
        drain_list_stream("ListTransactions", futures::stream::iter(responses)).await
    }

    #[tokio::test]
    async fn drain_preserves_partial_progress_on_timeout() {
        // Two items + a server-side deadline. We never saw `QueryEnd`, but `end_cursor` was
        // advanced — caller can resume from `c2`.
        let page = drain_iter(vec![
            Ok(item_response(b"c1")),
            Ok(item_response(b"c2")),
            Err(tonic::Status::deadline_exceeded("server budget")),
        ])
        .await
        .expect("partial progress should be preserved");

        assert_eq!(page.items.len(), 2);
        assert_eq!(page.end_cursor.as_deref(), Some(b"c2".as_ref()));
        assert_eq!(page.end_reason, None);
        assert!(page.has_more());
    }

    #[tokio::test]
    async fn drain_errors_on_zero_progress_half_close() {
        // Server opens the stream, sends nothing, half-closes. `has_more` defaults to true
        // (no End frame) and `end_cursor` is empty — the page is unresumable, so surface as
        // an error rather than silently dropping forward progress.
        let err = drain_iter(vec![])
            .await
            .expect_err("zero-progress half-close should error");
        let msg = format!("{err}");
        assert!(msg.contains("did not advance cursor"), "got: {msg}");
    }

    #[tokio::test]
    async fn drain_returns_page_on_half_close_after_progress() {
        // Server emitted one item, then half-closed without an End frame. The page is still
        // valid and resumable from the item's watermark.
        let page = drain_iter(vec![Ok(item_response(b"c1"))])
            .await
            .expect("partial-progress half-close should succeed");

        assert_eq!(page.items.len(), 1);
        assert_eq!(page.end_cursor.as_deref(), Some(b"c1".as_ref()));
        assert_eq!(page.end_reason, None);
    }
}
