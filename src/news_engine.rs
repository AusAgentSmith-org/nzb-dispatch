//! `NewsDispatchEngine` — `DispatchEngine` impl backed by the `nzb-news` crate.
//!
//! The layered news engine is a pure NNTP fetch layer: it takes per-article
//! work items and emits [`nzb_news::FetchOutcome`]s. This adapter bolts the
//! rest of the pipeline on top of it so it satisfies the contract the old
//! `WorkerPool` engine used to satisfy:
//!
//! 1. **Fetch** — delegated to `nzb_news::spawn_downloader`.
//! 2. **Decode** — `nzb_decode::decode_yenc` on each successful outcome.
//! 3. **Assemble** — `FileAssembler::assemble_article` writes the decoded
//!    bytes at the yEnc-declared offset.
//! 4. **Progress** — translates per-article outcomes into
//!    [`ProgressUpdate::ArticleComplete`] / [`ProgressUpdate::ArticleFailed`];
//!    drives job-level terminal via `JobContext::resolve_one`.
//!
//! Per-job lifecycle (pause/resume/cancel/abort) is tracked in this adapter
//! because the news engine is job-agnostic. We keep a `JobContext` per job
//! (same struct the old engine used — it owns the assembler, progress
//! channel, deobfuscation state, and terminal-emit logic).
//!
//! MVP limitations — marked with TODO comments:
//! - `pause_job` / `resume_job` are no-ops (work items are submitted
//!   eagerly; pause-gating is a follow-up).
//! - `reconcile_servers` is a no-op (nzb-news doesn't expose mid-flight
//!   server reconfiguration yet; requires a downloader rebuild).
//! - `set_max_worker_idle` / `eviction_count` are stubs (no idle-worker
//!   pool concept in nzb-news).

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::{Mutex, RwLock};
use tokio::sync::{Notify, mpsc};
use tracing::{debug, error, info, warn};

use nzb_core::models::NzbJob;
use nzb_nntp::config::ServerConfig;

use crate::article_failure::{ArticleFailure, ArticleFailureKind};
use crate::dispatch_engine::DispatchEngine;
use crate::download_engine::{JobContext, ProgressUpdate, build_job_submission};

// ---------------------------------------------------------------------------
// Tuning knobs
// ---------------------------------------------------------------------------

/// How many articles the nzb-news downloader will hold in-flight across all
/// servers at once. Matches the old engine's rough ceiling.
const DEFAULT_MAX_CONCURRENT_FETCHES: usize = 40;

/// Work channel depth inside nzb-news. Articles are buffered here between
/// `submit_job` enqueue and the per-server fan-out.
const DEFAULT_WORK_CHANNEL_CAPACITY: usize = 4096;

/// Outcome channel depth. Must be large enough that a momentary backlog in
/// the decode path doesn't block the fetch loop.
const DEFAULT_OUTCOME_CHANNEL_CAPACITY: usize = 4096;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Configuration for [`NewsDispatchEngine`]. Mirrors the knobs exposed by
/// the old engine so the swap is drop-in from the caller's perspective.
#[derive(Clone)]
pub struct NewsEngineConfig {
    pub servers: Vec<ServerConfig>,
    pub article_timeout: Duration,
    pub max_concurrent_fetches: usize,
    pub work_channel_capacity: usize,
    pub outcome_channel_capacity: usize,
    /// Optional backup-server probe policy. Forwarded verbatim to
    /// `nzb_news::DownloaderConfig::probe_policy`.
    ///
    /// `None` disables probing (every cascade article tries every server).
    /// `Some(_)` enables fast-fail on a backup server when the probed
    /// hit-rate falls below the threshold for that job.
    ///
    /// Defaults to `Some(ServerProbePolicy::default())` which matches the
    /// nzb-news default (probe 10 articles, require >=10% hits).
    pub probe_policy: Option<nzb_news::ServerProbePolicy>,
}

impl NewsEngineConfig {
    pub fn new(servers: Vec<ServerConfig>, article_timeout: Duration) -> Self {
        Self {
            servers,
            article_timeout,
            max_concurrent_fetches: DEFAULT_MAX_CONCURRENT_FETCHES,
            work_channel_capacity: DEFAULT_WORK_CHANNEL_CAPACITY,
            outcome_channel_capacity: DEFAULT_OUTCOME_CHANNEL_CAPACITY,
            probe_policy: Some(nzb_news::ServerProbePolicy::default()),
        }
    }
}

// ---------------------------------------------------------------------------
// Adapter state
// ---------------------------------------------------------------------------

/// Shared state held behind an `Arc` so the outcome-dispatcher task can
/// access jobs while the engine is owned by the caller.
struct Inner {
    config: NewsEngineConfig,
    /// Populated by `start()`. Holds the downloader's work-submission sender.
    handle: RwLock<Option<nzb_news::DownloaderHandle>>,
    /// Job map: `job_id` → per-job state. Cloned into the outcome task.
    jobs: RwLock<HashMap<String, Arc<JobEntry>>>,
    /// Monotonic tag issued for every `WorkItem` submitted to the downloader.
    /// Also serves as the routing key back to the originating article when
    /// outcomes come out the other side.
    next_tag: AtomicU64,
    /// `tag` → in-flight article metadata. We remove on outcome; this is the
    /// only place that holds the file_id / segment_number for an article
    /// mid-flight. Cleared on job cancel to free memory fast.
    in_flight: RwLock<HashMap<u64, InFlight>>,
}

/// Per-job state owned by the adapter.
struct JobEntry {
    /// Reused from the old engine — owns the assembler, progress channel,
    /// deobfuscation state, and terminal emit logic. Everything the adapter
    /// needs on the success/failure path is already a field here.
    context: Arc<JobContext>,
    /// When true, the pump task holds items in `pending` instead of
    /// forwarding them to the downloader. In-flight articles still
    /// complete — pause gates the *next* work only.
    paused: AtomicBool,
    /// Set by `cancel_job`. The pump exits and drains the queue.
    cancelled: AtomicBool,
    /// Work items waiting to be handed to the downloader. `submit_job`
    /// pushes all items here; the pump task drains on start and on
    /// resume.
    pending: Mutex<VecDeque<nzb_news::WorkItem>>,
    /// Wake-up signal for the pump task. Notified when `submit_job` adds
    /// items or when `resume_job` / `cancel_job` changes the gate.
    pump_wake: Notify,
}

/// Metadata recorded when a `WorkItem` is dispatched so we can route the
/// outcome back to the right job / file / segment.
#[derive(Clone)]
struct InFlight {
    job_id: String,
    file_id: String,
    segment_number: u32,
}

// ---------------------------------------------------------------------------
// NewsDispatchEngine
// ---------------------------------------------------------------------------

/// `DispatchEngine` impl backed by the layered nzb-news fetch engine.
pub struct NewsDispatchEngine {
    inner: Arc<Inner>,
}

impl NewsDispatchEngine {
    /// Construct the engine. Does **not** spawn the downloader —
    /// [`DispatchEngine::start`] does that.
    pub fn new(config: NewsEngineConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                config,
                handle: RwLock::new(None),
                jobs: RwLock::new(HashMap::new()),
                next_tag: AtomicU64::new(1),
                in_flight: RwLock::new(HashMap::new()),
            }),
        }
    }
}

#[async_trait::async_trait]
impl DispatchEngine for NewsDispatchEngine {
    fn start(&self) {
        let mut slot = self.inner.handle.write();
        if slot.is_some() {
            return; // idempotent
        }

        let cfg = &self.inner.config;
        let dl_config = nzb_news::DownloaderConfig {
            servers: cfg.servers.clone(),
            max_concurrent_fetches: cfg.max_concurrent_fetches,
            article_timeout: cfg.article_timeout,
            work_channel_capacity: cfg.work_channel_capacity,
            outcome_channel_capacity: cfg.outcome_channel_capacity,
            probe_policy: cfg.probe_policy.clone(),
        };
        let (handle, outcomes) = nzb_news::spawn_downloader(dl_config);

        // Spawn the outcome dispatcher. It lives as long as the downloader's
        // outcome channel is open — shutdown closes it and the loop exits.
        let inner = Arc::clone(&self.inner);
        tokio::spawn(outcome_dispatcher(inner, outcomes));

        *slot = Some(handle);
        info!(
            servers = self.inner.config.servers.len(),
            "NewsDispatchEngine started"
        );
    }

    fn submit_job(&self, job: &NzbJob, progress_tx: mpsc::Sender<ProgressUpdate>) {
        // Reuse the old engine's job-submission builder: creates the
        // FileAssembler, registers files, and filters out already-downloaded
        // articles. We only use the returned `JobContext` — the WorkItem
        // vec it produces is in the old engine's format; we build nzb-news
        // work items fresh below.
        let (ctx, legacy_items) = build_job_submission(job, progress_tx);

        // Build nzb-news wrapper types. One NzbObject for the job, one
        // NzbFile per file, and one Article per work item.
        let news_files: Vec<Arc<nzb_news::NzbFile>> = job
            .files
            .iter()
            .map(|f| {
                Arc::new(nzb_news::NzbFile::new(
                    &f.id,
                    &job.id,
                    &f.filename,
                    f.articles.len() as u32,
                ))
            })
            .collect();
        let news_files_by_id: HashMap<String, Arc<nzb_news::NzbFile>> = news_files
            .iter()
            .map(|nf| (nf.id.clone(), Arc::clone(nf)))
            .collect();
        let total_articles = legacy_items.len() as u64;
        let news_job = Arc::new(nzb_news::NzbObject::new(
            &job.id,
            &job.name,
            total_articles,
            job.total_bytes,
            news_files.clone(),
        ));

        // Convert each legacy WorkItem into an nzb-news WorkItem, recording
        // the routing metadata into in_flight and the item itself into the
        // job's pending queue. The pump task forwards from pending to the
        // downloader, gated by the `paused` flag.
        let mut pending = VecDeque::with_capacity(legacy_items.len());
        let tag_counter = &self.inner.next_tag;
        for item in legacy_items {
            let tag = tag_counter.fetch_add(1, Ordering::Relaxed);
            let file = match news_files_by_id.get(&item.file_id) {
                Some(f) => Arc::clone(f),
                None => continue, // shouldn't happen — file_id came from the same job
            };
            self.inner.in_flight.write().insert(
                tag,
                InFlight {
                    job_id: item.job_id.clone(),
                    file_id: item.file_id.clone(),
                    segment_number: item.segment_number,
                },
            );
            let article = Arc::new(nzb_news::Article::new(
                item.message_id.clone(),
                item.file_id.clone(),
                item.job_id.clone(),
                0,
                item.segment_number,
                tag,
            ));
            pending.push_back(nzb_news::WorkItem {
                tag,
                article,
                file,
                job: Arc::clone(&news_job),
            });
        }

        let entry = Arc::new(JobEntry {
            context: Arc::clone(&ctx),
            paused: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            pending: Mutex::new(pending),
            pump_wake: Notify::new(),
        });
        self.inner
            .jobs
            .write()
            .insert(ctx.job_id.clone(), Arc::clone(&entry));

        // Get a handle sender before spawning the pump. If start() wasn't
        // called, we bail here — the caller's contract is to start() first.
        let sender = match self.inner.handle.read().as_ref() {
            Some(h) => h.clone_tx(),
            None => {
                error!(
                    job_id = %job.id,
                    "submit_job called before start(); work will not be dispatched"
                );
                return;
            }
        };

        let job_id = job.id.clone();
        tokio::spawn(pump_loop(entry, sender, job_id));
    }

    fn pause_job(&self, job_id: &str) {
        if let Some(entry) = self.inner.jobs.read().get(job_id) {
            // Local gate — stops the pump from handing new items to nzb-news.
            entry.paused.store(true, Ordering::SeqCst);
        }
        // Scheduler-level gate — holds already-submitted articles in
        // nzb-news's own pending queue. Without this, anything already
        // accepted into `work_channel_capacity` (default 4096) would still
        // route to servers despite the local gate.
        if let Some(h) = self.inner.handle.read().as_ref() {
            h.pause_job(job_id);
        }
        debug!(job_id, "paused");
    }

    fn resume_job(&self, job_id: &str) {
        if let Some(entry) = self.inner.jobs.read().get(job_id) {
            entry.paused.store(false, Ordering::SeqCst);
            entry.pump_wake.notify_waiters();
        }
        if let Some(h) = self.inner.handle.read().as_ref() {
            h.resume_job(job_id);
        }
        debug!(job_id, "resumed");
    }

    fn cancel_job(&self, job_id: &str) {
        let entry = self.inner.jobs.write().remove(job_id);
        if let Some(entry) = entry {
            // Signal pump to drain + exit.
            entry.cancelled.store(true, Ordering::SeqCst);
            entry.pump_wake.notify_waiters();
            // Drop any not-yet-dispatched items so the pump sees an empty
            // queue and exits promptly.
            entry.pending.lock().clear();
            // Clear in-flight entries for this job so stale outcomes are
            // dropped silently by the dispatcher (unknown-tag path).
            self.inner
                .in_flight
                .write()
                .retain(|_, m| m.job_id != job_id);
            // Purge nzb-news scheduler-level state: items already accepted
            // into the downloader's work_channel or pending list get emitted
            // as Cancelled outcomes and removed. Without this, a cancelled
            // job would keep routing its buffered articles to servers.
            if let Some(h) = self.inner.handle.read().as_ref() {
                h.purge_job(job_id);
            }
            debug!(job_id, "cancelled");
        }
    }

    fn abort_job(&self, job_id: &str, reason: String) {
        // Emit terminal via the existing JobContext machinery — same path
        // the old engine uses. `emit_terminal` is idempotent; cancel_job
        // later is safe.
        let entry = self.inner.jobs.read().get(job_id).cloned();
        if let Some(entry) = entry {
            *entry.context.abort_reason.lock() = Some(reason);
            entry.context.emit_terminal_public();
        }
        self.cancel_job(job_id);
    }

    fn has_job(&self, job_id: &str) -> bool {
        self.inner.jobs.read().contains_key(job_id)
    }

    fn reconcile_servers(&self) {
        // TODO: nzb-news doesn't expose dynamic server reconfiguration; a
        // full reconcile requires a downloader restart. Defer until we have
        // a concrete need from the queue manager tests.
    }

    fn set_max_worker_idle(&self, _d: Duration) {
        // No per-worker idle concept in nzb-news; workers are persistent
        // until the downloader shuts down.
    }

    fn eviction_count(&self) -> u64 {
        0
    }

    fn server_stats_snapshot(&self) -> Vec<(String, crate::dispatch_engine::ServerAttemptStats)> {
        let guard = self.inner.handle.read();
        let Some(h) = guard.as_ref() else {
            return Vec::new();
        };
        h.server_stats_snapshot()
            .into_iter()
            .map(|(id, s)| {
                (
                    id,
                    crate::dispatch_engine::ServerAttemptStats {
                        attempted: s.attempted,
                        succeeded: s.succeeded,
                        not_found: s.not_found,
                        transient_failed: s.transient_failed,
                    },
                )
            })
            .collect()
    }

    async fn shutdown(&self) {
        let handle = self.inner.handle.write().take();
        if let Some(h) = handle {
            h.shutdown();
            h.join().await;
        }
    }
}

// ---------------------------------------------------------------------------
// Helper trait: pull the work-submission sender out of DownloaderHandle
// ---------------------------------------------------------------------------

trait HandleExt {
    fn clone_tx(&self) -> mpsc::Sender<nzb_news::WorkItem>;
}
impl HandleExt for nzb_news::DownloaderHandle {
    fn clone_tx(&self) -> mpsc::Sender<nzb_news::WorkItem> {
        self.sender()
    }
}

// ---------------------------------------------------------------------------
// Outcome dispatcher
// ---------------------------------------------------------------------------

/// Main loop: consume `FetchOutcome`s from nzb-news and translate each into
/// a `ProgressUpdate`, doing decode + assembly inline on success. Runs until
/// the outcome channel is closed (downloader shutdown).
async fn outcome_dispatcher(
    inner: Arc<Inner>,
    mut outcomes: mpsc::Receiver<nzb_news::FetchOutcome>,
) {
    while let Some(outcome) = outcomes.recv().await {
        match outcome {
            nzb_news::FetchOutcome::Success {
                tag,
                server_id,
                bytes,
                article_bytes: _,
            } => {
                // Spawn each success so decode+assemble runs in parallel.
                // The old engine got this for free because every worker did
                // its own fetch+decode+assemble — centralising here would
                // serialise all post-fetch work to a single task.
                let inner2 = Arc::clone(&inner);
                tokio::spawn(async move {
                    process_success(inner2, tag, server_id, bytes).await;
                });
            }
            nzb_news::FetchOutcome::Failed { tag, last_error } => {
                process_failure(&inner, tag, last_error);
            }
            nzb_news::FetchOutcome::Cancelled { tag } => {
                // Treat as benign discard — caller (queue manager) will
                // observe JobAborted separately via abort_job.
                inner.in_flight.write().remove(&tag);
            }
        }
    }
    debug!("outcome_dispatcher exiting: channel closed");
}

async fn process_success(inner: Arc<Inner>, tag: u64, server_id: String, raw: Vec<u8>) {
    let meta = inner.in_flight.write().remove(&tag);
    let Some(meta) = meta else {
        return; // stale / cancelled
    };

    let entry = inner.jobs.read().get(&meta.job_id).cloned();
    let Some(entry) = entry else {
        return; // job cancelled after submit
    };
    let ctx = &entry.context;

    // Decode (CPU-bound; SIMD is fast but not free).
    let decode_start = Instant::now();
    let decoded = match nzb_decode::decode_yenc(&raw) {
        Ok(d) => d,
        Err(e) => {
            let failure = ArticleFailure::decode_error(server_id, format!("yEnc decode: {e}"));
            emit_failed(ctx, &meta, failure);
            return;
        }
    };
    let decode_us = decode_start.elapsed().as_micros() as u64;

    // Record yEnc filename for deobfuscation.
    if let Some(ref fname) = decoded.filename
        && !fname.is_empty()
    {
        ctx.yenc_names
            .lock()
            .insert(meta.file_id.clone(), fname.clone());
    }

    let data_begin = decoded.part_begin.unwrap_or(0);

    // Assemble.
    let assemble_start = Instant::now();
    let file_complete = match ctx.assembler.assemble_article(
        &meta.job_id,
        &meta.file_id,
        meta.segment_number,
        data_begin,
        &decoded.data,
    ) {
        Ok(b) => b,
        Err(e) => {
            let failure = ArticleFailure::decode_error(server_id, format!("assembly: {e}"));
            emit_failed(ctx, &meta, failure);
            return;
        }
    };
    let assemble_us = assemble_start.elapsed().as_micros() as u64;

    // Timing stats.
    ctx.total_decode_us.fetch_add(decode_us, Ordering::Relaxed);
    ctx.total_assemble_us
        .fetch_add(assemble_us, Ordering::Relaxed);
    ctx.total_articles_decoded.fetch_add(1, Ordering::Relaxed);

    // Emit progress.
    let decoded_bytes = decoded.data.len() as u64;
    let _ = ctx.progress_tx.try_send(ProgressUpdate::ArticleComplete {
        job_id: meta.job_id.clone(),
        file_id: meta.file_id.clone(),
        segment_number: meta.segment_number,
        decoded_bytes,
        file_complete,
        server_id: Some(server_id),
    });

    ctx.resolve_one_public();
}

fn process_failure(inner: &Inner, tag: u64, last_error: Option<String>) {
    let meta = inner.in_flight.write().remove(&tag);
    let Some(meta) = meta else {
        return;
    };
    let entry = inner.jobs.read().get(&meta.job_id).cloned();
    let Some(entry) = entry else {
        return;
    };
    let msg = last_error.unwrap_or_else(|| "all servers exhausted".into());
    // nzb-news doesn't expose per-server failure classification at the
    // outcome layer; it only tells us an article was given up on. Map to
    // `NotFound` (the closest analogue the hopeless-tracker cares about)
    // with the error string for diagnostics. server_id is empty because
    // the outcome aggregates across every tried server.
    let failure = ArticleFailure {
        kind: ArticleFailureKind::NotFound,
        server_id: String::new(),
        message: msg,
    };
    emit_failed(&entry.context, &meta, failure);
}

fn emit_failed(ctx: &JobContext, meta: &InFlight, failure: ArticleFailure) {
    ctx.articles_failed.fetch_add(1, Ordering::Relaxed);
    let _ = ctx.progress_tx.try_send(ProgressUpdate::ArticleFailed {
        job_id: meta.job_id.clone(),
        file_id: meta.file_id.clone(),
        segment_number: meta.segment_number,
        failure,
    });
    ctx.resolve_one_public();
}

// ---------------------------------------------------------------------------
// Per-job pump task
// ---------------------------------------------------------------------------

/// Drains a job's `pending` queue into the downloader's work channel,
/// respecting the `paused` gate and exiting on `cancelled`.
///
/// The pump parks on `pump_wake` when `pending` is empty or when
/// `paused` is true. `submit_job` / `resume_job` notify to wake it.
async fn pump_loop(entry: Arc<JobEntry>, sender: mpsc::Sender<nzb_news::WorkItem>, job_id: String) {
    loop {
        if entry.cancelled.load(Ordering::SeqCst) {
            debug!(job_id, "pump exiting: cancelled");
            return;
        }
        if entry.paused.load(Ordering::SeqCst) {
            // Wait for resume or cancel.
            entry.pump_wake.notified().await;
            continue;
        }
        let next = entry.pending.lock().pop_front();
        let Some(item) = next else {
            // Queue empty. For the current adapter, submit_job enqueues
            // every article up-front, so an empty queue means we're done.
            // Leave the pump waiting in case a future enhancement appends
            // more work (e.g. dynamic NZB growth); cancel will still wake
            // it. This costs one idle task per running job.
            entry.pump_wake.notified().await;
            continue;
        };
        if sender.send(item).await.is_err() {
            warn!(job_id, "downloader sender closed; pump exiting");
            return;
        }
    }
}
