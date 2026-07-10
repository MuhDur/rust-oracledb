use std::borrow::Cow;
use std::future::Future;
use std::num::NonZeroU32;
use std::time::Duration;

use asupersync::{time, Cx};
use oracledb_protocol::thin::{
    BindValue, ExecuteOptions, TNS_FETCH_ORIENTATION_ABSOLUTE, TNS_FETCH_ORIENTATION_CURRENT,
    TNS_FETCH_ORIENTATION_FIRST, TNS_FETCH_ORIENTATION_LAST, TNS_FETCH_ORIENTATION_NEXT,
    TNS_FETCH_ORIENTATION_PRIOR, TNS_FETCH_ORIENTATION_RELATIVE,
};

use crate::{BindError, Error, Params, Result};

const DEFAULT_QUERY_ARRAYSIZE: u32 = 100;

fn default_query_arraysize() -> NonZeroU32 {
    NonZeroU32::new(DEFAULT_QUERY_ARRAYSIZE).expect("default query arraysize is non-zero")
}

fn duration_to_nanos_saturating(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

fn duration_to_millis_saturating(duration: Duration) -> u32 {
    duration.as_millis().min(u128::from(u32::MAX)) as u32
}

/// Scroll target for [`Rows::scroll`](crate::Rows::scroll).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Scroll {
    Current,
    Next,
    Prior,
    First,
    Last,
    Absolute(u32),
    Relative(u32),
}

impl Scroll {
    pub(crate) fn into_wire_parts(self) -> (u32, u32) {
        match self {
            Scroll::Current => (TNS_FETCH_ORIENTATION_CURRENT, 0),
            Scroll::Next => (TNS_FETCH_ORIENTATION_NEXT, 0),
            Scroll::Prior => (TNS_FETCH_ORIENTATION_PRIOR, 0),
            Scroll::First => (TNS_FETCH_ORIENTATION_FIRST, 0),
            Scroll::Last => (TNS_FETCH_ORIENTATION_LAST, 0),
            Scroll::Absolute(pos) => (TNS_FETCH_ORIENTATION_ABSOLUTE, pos),
            Scroll::Relative(pos) => (TNS_FETCH_ORIENTATION_RELATIVE, pos),
        }
    }
}

/// Query builder for the high-level row API.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Query<'a> {
    pub(crate) sql: Cow<'a, str>,
    pub(crate) params: Params<'a>,
    pub(crate) arraysize: NonZeroU32,
    pub(crate) prefetch: u32,
    pub(crate) prefetch_set: bool,
    pub(crate) materialize_lobs: bool,
    pub(crate) scrollable: bool,
    pub(crate) timeout: Option<Duration>,
}

impl<'a> Query<'a> {
    pub fn new(sql: &'a str) -> Self {
        let arraysize = default_query_arraysize();
        Self {
            sql: Cow::Borrowed(sql),
            params: Params::None,
            arraysize,
            prefetch: arraysize.get(),
            prefetch_set: false,
            materialize_lobs: true,
            scrollable: false,
            timeout: None,
        }
    }

    pub(crate) fn owned_sql(sql: String) -> Self {
        let arraysize = default_query_arraysize();
        Self {
            sql: Cow::Owned(sql),
            params: Params::None,
            arraysize,
            prefetch: arraysize.get(),
            prefetch_set: false,
            materialize_lobs: true,
            scrollable: false,
            timeout: None,
        }
    }

    pub fn bind(mut self, params: impl Into<Params<'a>>) -> Self {
        self.params = params.into();
        self
    }

    pub fn arraysize(mut self, n: NonZeroU32) -> Self {
        self.arraysize = n;
        if !self.prefetch_set {
            self.prefetch = n.get();
        }
        self
    }

    pub fn prefetch(mut self, n: u32) -> Self {
        self.prefetch = n;
        self.prefetch_set = true;
        self
    }

    pub fn stream_lobs(mut self) -> Self {
        self.materialize_lobs = false;
        self
    }

    pub fn scrollable(mut self) -> Self {
        self.scrollable = true;
        self
    }

    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    pub fn sql(&self) -> &str {
        self.sql.as_ref()
    }

    pub fn params(&self) -> &Params<'a> {
        &self.params
    }

    pub fn arraysize_value(&self) -> NonZeroU32 {
        self.arraysize
    }

    pub fn prefetch_rows(&self) -> u32 {
        self.prefetch
    }

    pub fn materialize_lobs(&self) -> bool {
        self.materialize_lobs
    }

    pub fn is_scrollable(&self) -> bool {
        self.scrollable
    }

    pub fn timeout_duration(&self) -> Option<Duration> {
        self.timeout
    }
}

/// Execute builder for DML, DDL, and PL/SQL operations that use at most one
/// bind row.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Execute<'a> {
    pub(crate) sql: Cow<'a, str>,
    pub(crate) params: Params<'a>,
    pub(crate) timeout: Option<Duration>,
    pub(crate) options: ExecuteOptions,
}

impl<'a> Execute<'a> {
    pub fn new(sql: &'a str) -> Self {
        Self {
            sql: Cow::Borrowed(sql),
            params: Params::None,
            timeout: None,
            options: ExecuteOptions::default(),
        }
    }

    pub(crate) fn owned_sql(sql: String) -> Self {
        Self {
            sql: Cow::Owned(sql),
            params: Params::None,
            timeout: None,
            options: ExecuteOptions::default(),
        }
    }

    pub fn bind(mut self, params: impl Into<Params<'a>>) -> Self {
        self.params = params.into();
        self
    }

    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    pub fn parse_only(mut self) -> Self {
        self.options = self.options.with_parse_only(true);
        self
    }

    pub fn raw_options(mut self, options: ExecuteOptions) -> Self {
        self.options = options;
        self
    }

    pub fn sql(&self) -> &str {
        self.sql.as_ref()
    }

    pub fn params(&self) -> &Params<'a> {
        &self.params
    }

    pub fn timeout_duration(&self) -> Option<Duration> {
        self.timeout
    }

    pub fn options(&self) -> ExecuteOptions {
        self.options
    }
}

/// Bind rows for [`Connection::execute_many`](crate::Connection::execute_many).
/// Each inner `Vec<BindValue>` is one execution of the statement.
#[derive(Clone, Debug, PartialEq)]
pub enum BatchRows<'a> {
    Borrowed(&'a [Vec<BindValue>]),
    Owned(Vec<Vec<BindValue>>),
}

impl<'a> BatchRows<'a> {
    pub(crate) fn as_slice(&self) -> &[Vec<BindValue>] {
        match self {
            Self::Borrowed(rows) => rows,
            Self::Owned(rows) => rows.as_slice(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }

    fn bind_width(&self) -> Option<usize> {
        self.as_slice().first().map(Vec::len)
    }

    pub(crate) fn validate_rectangular(&self) -> Result<()> {
        let Some(expected) = self.bind_width() else {
            return Ok(());
        };
        for (row_index, row) in self.as_slice().iter().enumerate().skip(1) {
            if row.len() != expected {
                return Err(Error::Bind(BindError::BatchRowWidthMismatch {
                    row_index,
                    expected,
                    actual: row.len(),
                }));
            }
        }
        Ok(())
    }
}

impl<'a> From<&'a [Vec<BindValue>]> for BatchRows<'a> {
    fn from(rows: &'a [Vec<BindValue>]) -> Self {
        Self::Borrowed(rows)
    }
}

impl<'a> From<&'a Vec<Vec<BindValue>>> for BatchRows<'a> {
    fn from(rows: &'a Vec<Vec<BindValue>>) -> Self {
        Self::Borrowed(rows.as_slice())
    }
}

impl<'a, const N: usize> From<&'a [Vec<BindValue>; N]> for BatchRows<'a> {
    fn from(rows: &'a [Vec<BindValue>; N]) -> Self {
        Self::Borrowed(rows.as_slice())
    }
}

impl<'a> From<Vec<Vec<BindValue>>> for BatchRows<'a> {
    fn from(rows: Vec<Vec<BindValue>>) -> Self {
        Self::Owned(rows)
    }
}

/// Execute-many builder for array DML.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct Batch<'a> {
    pub(crate) sql: Cow<'a, str>,
    pub(crate) rows: BatchRows<'a>,
    pub(crate) timeout: Option<Duration>,
    pub(crate) options: ExecuteOptions,
}

impl<'a> Batch<'a> {
    pub fn new(sql: &'a str, rows: impl Into<BatchRows<'a>>) -> Self {
        Self {
            sql: Cow::Borrowed(sql),
            rows: rows.into(),
            timeout: None,
            options: ExecuteOptions::default(),
        }
    }

    pub(crate) fn owned_sql(sql: String, rows: impl Into<BatchRows<'a>>) -> Self {
        Self {
            sql: Cow::Owned(sql),
            rows: rows.into(),
            timeout: None,
            options: ExecuteOptions::default(),
        }
    }

    pub fn collect_errors(mut self) -> Self {
        self.options = self.options.with_batcherrors(true);
        self
    }

    pub fn row_counts(mut self) -> Self {
        self.options = self.options.with_arraydmlrowcounts(true);
        self
    }

    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    pub fn raw_options(mut self, options: ExecuteOptions) -> Self {
        self.options = options;
        self
    }

    pub fn sql(&self) -> &str {
        self.sql.as_ref()
    }

    pub fn rows(&self) -> &BatchRows<'a> {
        &self.rows
    }

    pub fn timeout_duration(&self) -> Option<Duration> {
        self.timeout
    }

    pub fn options(&self) -> ExecuteOptions {
        self.options
    }
}

/// Registered-query builder for Continuous Query Notification (CQN).
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct Registration<'a> {
    pub(crate) sql: Cow<'a, str>,
    pub(crate) params: Params<'a>,
    pub(crate) registration_id: u64,
    pub(crate) timeout: Option<Duration>,
}

impl<'a> Registration<'a> {
    pub fn new(sql: &'a str, registration_id: u64) -> Self {
        Self {
            sql: Cow::Borrowed(sql),
            params: Params::None,
            registration_id,
            timeout: None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn owned_sql(sql: String, registration_id: u64) -> Self {
        Self {
            sql: Cow::Owned(sql),
            params: Params::None,
            registration_id,
            timeout: None,
        }
    }

    pub fn bind(mut self, params: impl Into<Params<'a>>) -> Self {
        self.params = params.into();
        self
    }

    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    pub fn sql(&self) -> &str {
        self.sql.as_ref()
    }

    pub fn params(&self) -> &Params<'a> {
        &self.params
    }

    pub fn registration_id(&self) -> u64 {
        self.registration_id
    }

    pub fn timeout_duration(&self) -> Option<Duration> {
        self.timeout
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct QueryDeadline {
    deadline: Option<asupersync::types::Time>,
    timeout_ms: u32,
}

/// How a [`QueryDeadline`] elapsed, which determines whether wire recovery is
/// required. A future rejected before its first poll cannot have sent bytes;
/// once the operation future has actually been polled, callers must
/// conservatively recover.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeadlineExpiry {
    BeforeStart,
    InFlight,
}

impl QueryDeadline {
    pub(crate) fn from_timeout(timeout: Duration) -> Self {
        let now = time::wall_now();
        Self {
            deadline: Some(now.saturating_add_nanos(duration_to_nanos_saturating(timeout))),
            timeout_ms: duration_to_millis_saturating(timeout),
        }
    }

    pub(crate) fn new(cx: &Cx, timeout: Option<Duration>) -> Self {
        let now = time::wall_now();
        Self::from_budget(now, cx.budget(), timeout)
    }

    pub(crate) fn from_budget(
        now: asupersync::types::Time,
        budget: asupersync::types::Budget,
        timeout: Option<Duration>,
    ) -> Self {
        let query_deadline = timeout
            .map(|duration| now.saturating_add_nanos(duration_to_nanos_saturating(duration)));
        let cx_deadline = budget.deadline;
        let deadline = match (query_deadline, cx_deadline) {
            (Some(query), Some(cx)) => Some(query.min(cx)),
            (Some(query), None) => Some(query),
            (None, Some(cx)) => Some(cx),
            (None, None) => None,
        };
        let timeout_ms = timeout
            .map(duration_to_millis_saturating)
            .or_else(|| {
                budget
                    .remaining_time(now)
                    .map(duration_to_millis_saturating)
            })
            .unwrap_or(0);
        Self {
            deadline,
            timeout_ms,
        }
    }

    pub(crate) fn timeout_ms(self) -> u32 {
        self.timeout_ms
    }

    pub(crate) async fn run<T, F>(self, future: F) -> std::result::Result<Result<T>, DeadlineExpiry>
    where
        F: Future<Output = Result<T>>,
    {
        let Some(deadline) = self.deadline else {
            return Ok(future.await);
        };
        let now = time::wall_now();
        if now >= deadline {
            return Err(DeadlineExpiry::BeforeStart);
        }
        self.run_after_precheck(now, deadline, future).await
    }

    async fn run_after_precheck<T, F>(
        self,
        now: asupersync::types::Time,
        deadline: asupersync::types::Time,
        future: F,
    ) -> std::result::Result<Result<T>, DeadlineExpiry>
    where
        F: Future<Output = Result<T>>,
    {
        debug_assert!(now < deadline, "precheck must reject an elapsed deadline");
        let remaining = Duration::from_nanos(deadline.as_nanos().saturating_sub(now.as_nanos()));
        // The timeout wrapper is allowed to observe an elapsed timer before it
        // polls its inner future. Track that first poll explicitly: only an
        // actually-polled database future can have emitted wire bytes and need
        // BREAK/drain recovery.
        let polled = std::sync::atomic::AtomicBool::new(false);
        let mut future = std::pin::pin!(future);
        let tracked = std::future::poll_fn(|cx| {
            polled.store(true, std::sync::atomic::Ordering::Relaxed);
            future.as_mut().poll(cx)
        });
        let result = time::timeout(now, remaining, tracked).await;
        match result {
            Ok(result) => Ok(result),
            Err(_) if polled.load(std::sync::atomic::Ordering::Relaxed) => {
                Err(DeadlineExpiry::InFlight)
            }
            Err(_) => Err(DeadlineExpiry::BeforeStart),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use asupersync::types::{Budget, Time};
    use asupersync::Cx;
    use oracledb_protocol::thin::{BindValue, ExecuteOptions, TNS_FETCH_ORIENTATION_ABSOLUTE};

    use super::*;
    use crate::{BindError, Error, Params};

    #[test]
    fn query_arraysize_updates_default_prefetch_until_overridden() {
        let seven = NonZeroU32::new(7).expect("non-zero");
        let eleven = NonZeroU32::new(11).expect("non-zero");

        let query = Query::new("select * from dual").arraysize(seven);
        assert_eq!(query.arraysize, seven);
        assert_eq!(
            query.prefetch, 7,
            "default prefetch follows arraysize when not explicitly set"
        );

        let query = Query::new("select * from dual")
            .prefetch(3)
            .arraysize(eleven);
        assert_eq!(query.arraysize, eleven);
        assert_eq!(query.prefetch, 3, "explicit prefetch must be stable");
    }

    #[test]
    fn query_deadline_captures_one_absolute_query_timeout() {
        let runtime = crate::build_io_runtime().expect("runtime");
        runtime.block_on(async {
            let cx = Cx::current().expect("runtime installs Cx");

            let deadline = QueryDeadline::new(&cx, Some(Duration::from_secs(5)));
            let captured = deadline.deadline.expect("query timeout sets deadline");

            assert_eq!(deadline.timeout_ms(), 5_000);
            assert_eq!(
                deadline.deadline,
                Some(captured),
                "the deadline is captured once and then carried by value"
            );
        });
    }

    #[test]
    fn preexpired_query_deadline_is_classified_before_start_without_polling() {
        let runtime = crate::build_io_runtime().expect("runtime");
        runtime.block_on(async {
            let cx = Cx::current().expect("runtime installs Cx");
            let deadline = QueryDeadline::new(&cx, Some(Duration::ZERO));
            let polled = Arc::new(AtomicBool::new(false));
            let observed = Arc::clone(&polled);

            let result = deadline
                .run(async move {
                    observed.store(true, Ordering::SeqCst);
                    Ok::<_, Error>(())
                })
                .await;

            assert!(matches!(result, Err(DeadlineExpiry::BeforeStart)));
            assert!(
                !polled.load(Ordering::SeqCst),
                "before-start expiry must not poll the operation future"
            );
        });
    }

    #[test]
    fn expired_ambient_deadline_is_before_start_without_request_timeout() {
        let runtime = crate::build_io_runtime().expect("runtime");
        runtime.block_on(async {
            let deadline = QueryDeadline::from_budget(
                asupersync::time::wall_now(),
                Budget::new().with_deadline(Time::ZERO),
                None,
            );
            let polled = Arc::new(AtomicBool::new(false));
            let observed = Arc::clone(&polled);

            let result = deadline
                .run(async move {
                    observed.store(true, Ordering::SeqCst);
                    Ok::<_, Error>(())
                })
                .await;

            assert!(matches!(result, Err(DeadlineExpiry::BeforeStart)));
            assert!(
                !polled.load(Ordering::SeqCst),
                "an expired ambient budget must reject before polling the operation"
            );
        });
    }

    #[test]
    fn armed_query_deadline_is_classified_in_flight_after_polling() {
        let runtime = crate::build_io_runtime().expect("runtime");
        runtime.block_on(async {
            let cx = Cx::current().expect("runtime installs Cx");
            let deadline = QueryDeadline::new(&cx, Some(Duration::from_millis(100)));
            let polled = Arc::new(AtomicBool::new(false));
            let observed = Arc::clone(&polled);

            let result = deadline
                .run(async move {
                    observed.store(true, Ordering::SeqCst);
                    std::future::pending::<Result<()>>().await
                })
                .await;

            assert!(matches!(result, Err(DeadlineExpiry::InFlight)));
            assert!(
                polled.load(Ordering::SeqCst),
                "in-flight expiry must mean the operation future was armed"
            );
        });
    }

    #[test]
    fn timeout_wrapper_expiry_before_inner_poll_is_classified_before_start() {
        let runtime = crate::build_io_runtime().expect("runtime");
        runtime.block_on(async {
            let deadline = QueryDeadline {
                deadline: Some(Time::from_nanos(1)),
                timeout_ms: 0,
            };
            let inner_polled = Arc::new(AtomicBool::new(false));
            let observed = Arc::clone(&inner_polled);

            // Supply a synthetic precheck time just before the deadline while
            // the runtime's ambient timer is already far past it. Asupersync's
            // timeout wrapper wins before polling its inner future.
            let result = deadline
                .run_after_precheck(Time::ZERO, Time::from_nanos(1), async move {
                    observed.store(true, Ordering::SeqCst);
                    Ok::<_, Error>(())
                })
                .await;

            assert!(matches!(result, Err(DeadlineExpiry::BeforeStart)));
            assert!(
                !inner_polled.load(Ordering::SeqCst),
                "timeout-before-inner-poll cannot have emitted request bytes"
            );
        });
    }

    #[test]
    fn execute_raw_options_preserves_full_escape_hatch() {
        let options = ExecuteOptions::default()
            .with_batcherrors(true)
            .with_arraydmlrowcounts(true)
            .with_parse_only(true)
            .with_token_num(7)
            .with_cursor_id(11)
            .with_cache_statement(false)
            .with_scrollable(true)
            .with_fetch_orientation(TNS_FETCH_ORIENTATION_ABSOLUTE)
            .with_fetch_pos(3)
            .with_scroll_operation(true)
            .with_suspend_on_success(true)
            .with_no_prefetch(true)
            .with_registration_id(13)
            .with_max_string_size(4_000);

        let execute = Execute::new("begin null; end;").raw_options(options);

        assert_eq!(execute.options, options);
    }

    #[test]
    fn batch_builder_sets_batch_execution_flags() {
        let rows = vec![
            vec![BindValue::Number("1".to_string())],
            vec![BindValue::Number("2".to_string())],
        ];

        let batch = Batch::new("delete from t where id = :1", &rows)
            .collect_errors()
            .row_counts()
            .timeout(Duration::from_secs(3));

        assert!(matches!(batch.rows, BatchRows::Borrowed(_)));
        assert!(batch.options.batcherrors());
        assert!(batch.options.arraydmlrowcounts());
        assert_eq!(batch.timeout, Some(Duration::from_secs(3)));
    }

    #[test]
    fn batch_raw_options_preserves_escape_hatch() {
        let rows = vec![vec![BindValue::Number("1".to_string())]];
        let options = ExecuteOptions::default()
            .with_batcherrors(true)
            .with_arraydmlrowcounts(true)
            .with_parse_only(true)
            .with_token_num(9)
            .with_cursor_id(17)
            .with_cache_statement(false)
            .with_scrollable(true)
            .with_fetch_orientation(TNS_FETCH_ORIENTATION_ABSOLUTE)
            .with_fetch_pos(4)
            .with_scroll_operation(true)
            .with_suspend_on_success(true)
            .with_no_prefetch(true)
            .with_registration_id(21)
            .with_max_string_size(4_000);

        let batch = Batch::new("begin null; end;", rows).raw_options(options);

        assert_eq!(batch.options, options);
    }

    #[test]
    fn batch_rows_reject_ragged_bind_shapes() {
        let rows = vec![
            vec![
                BindValue::Number("1".to_string()),
                BindValue::Text("a".into()),
            ],
            vec![BindValue::Number("2".to_string())],
        ];
        let batch = Batch::new("insert into t values (:1, :2)", &rows);

        let err = batch.rows.validate_rectangular().unwrap_err();

        assert!(matches!(
            err,
            Error::Bind(BindError::BatchRowWidthMismatch {
                row_index: 1,
                expected: 2,
                actual: 1
            })
        ));
    }

    #[test]
    fn registration_builder_carries_params_subscription_and_timeout(
    ) -> std::result::Result<(), String> {
        let registration = Registration::new("select * from rust_cqn_t where id = :id", 42)
            .bind(vec![(
                ":id".to_string(),
                BindValue::Number("7".to_string()),
            )])
            .timeout(Duration::from_secs(9));

        assert_eq!(registration.registration_id, 42);
        assert_eq!(registration.timeout, Some(Duration::from_secs(9)));
        let Params::Named(values) = registration.params else {
            return Err("expected named params".to_string());
        };
        assert_eq!(values[0].0, ":id");
        assert_eq!(values[0].1, BindValue::Number("7".to_string()));
        Ok(())
    }
}
