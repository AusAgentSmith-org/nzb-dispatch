//! Layered NNTP download primitives.
//!
//! The stack is split into five modules, each owning one concern:
//!
//! | Concern                                      | Module                          |
//! |----------------------------------------------|---------------------------------|
//! | One NNTP connection — raw I/O, reset         | [`news_wrapper::NewsWrapper`]   |
//! | Per-server pool of wrappers + health state   | [`server::Server`]              |
//! | Per-article/file/job try-list                | [`trylist::TryList`]            |
//! | Article lifecycle + retry routing            | [`article`] (Article/File/Job)  |
//! | Top-level orchestrator + multiplexed driver  | [`downloader::Downloader`]      |
//!
//! Design rules (applied in priority order):
//!
//! 1. **No silent stalls.** Every path has a bounded timeout, retry limit,
//!    or explicit penalty; a slow or dead server must not block the rest.
//! 2. **Strict layering.** Raw NNTP I/O is confined to `NewsWrapper`; article
//!    lifecycle is in `Article`; server orchestration is in `Server`; the
//!    top-level `Downloader` is coordination only (no protocol knowledge).
//! 3. **Priority-aware failover.** Article dispatch and retry obey server
//!    priority: a lower-priority server only gets an article after every
//!    enabled higher-priority server has failed on it.
//! 4. **Explicit try-lists.** Every `Article`, `NzbFile`, and `NzbObject`
//!    owns a try-list mutex; failed servers are recorded at all three levels
//!    so retry logic is never ambiguous.
//! 5. **Multiplexed I/O.** A single tokio driver polls all busy sockets with
//!    `FuturesUnordered`; a slow `recv` on one socket cannot block any other.

pub mod article;
pub mod dispatch;
pub mod downloader;
pub mod news_wrapper;
pub mod penalty;
pub mod server;
pub mod trylist;
