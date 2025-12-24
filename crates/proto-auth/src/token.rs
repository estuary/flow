/// TokenStream is a watch of Tokens which are periodically refreshed.
#[derive(Clone)]
pub struct TokenStream<Token>(pub tokio::sync::watch::Receiver<Option<tonic::Result<Token>>>);

/// TokenSource produces an associated Token and its validity duration on demand.
pub trait TokenSource: Send + Sync + Sized + 'static {
    type Token: Send + Sync + 'static;

    /// Refresh a Token from the TokenSource.
    ///
    /// `started` is the time when the overall refresh operation began,
    /// and is held constant across retries.
    ///
    /// Refresh returns a future that resolves to:
    /// - Ok(Ok((Token, valid_for))) if the refresh was successful,
    ///   where `valid_for` is the remaining lifetime of the yielded Token.
    /// - Ok(Err(retry_after)) if the refresh result was indeterminate,
    ///   but should be retried after `retry_after`.
    /// - Err(err) if a non-retriable error occurred.
    fn refresh(
        &mut self,
        started: std::time::SystemTime,
    ) -> impl std::future::Future<
        Output = tonic::Result<Result<(Self::Token, std::time::Duration), std::time::Duration>>,
    > + Send
    + Sync;

    /// Start a Stream by fetching tokens from Source as a spawned task.
    /// Tokens are periodically refreshed ahead of their expiry.
    /// The spawned task stops when all Stream clones are dropped.
    fn start_stream(mut self) -> TokenStream<Self::Token> {
        let (tx, mut rx) = tokio::sync::watch::channel::<Option<tonic::Result<Self::Token>>>(None);

        // Mark the initial state as observed, so that changed() of this Receiver
        // (and all clones thereof) will yield only after the first refresh().
        rx.mark_unchanged();

        tokio::spawn(async move {
            let mut backoff = std::time::Duration::ZERO;
            let mut maybe_started = None;

            loop {
                tokio::select! {
                    _ = tx.closed() => return,
                    _ = tokio::time::sleep(backoff) => (),
                }

                let started = if let Some(started) = maybe_started {
                    started
                } else {
                    let now = std::time::SystemTime::now();
                    maybe_started = Some(now);
                    now
                };

                match self.refresh(started).await {
                    Ok(Ok((token, valid_for))) => {
                        backoff = valid_for.saturating_sub(crate::MINUTE).max(crate::MINUTE);
                        maybe_started = None;
                        let _ = tx.send(Some(Ok(token.into())));
                    }
                    Ok(Err(retry_after)) => {
                        backoff = retry_after;
                    }
                    Err(err) => {
                        // Re-attempt after a random backoff centered around 1 minute.
                        backoff =
                            std::time::Duration::from_millis(rand::random_range(45_000..75_000));
                        maybe_started = None;
                        let _ = tx.send(Some(Err(err)));
                    }
                }
            }
        });

        TokenStream(rx)
    }
}

impl<Token> TokenStream<Token> {
    /// Map the current Token into a tonic::Result<Output>.
    ///
    /// This routine only blocks if the TokenStream is awaiting its very first Token.
    /// As Extract is run on every token access, it should be lightweight and non-blocking.
    pub async fn map_current<Extract, Output>(&self, f: Extract) -> tonic::Result<Output>
    where
        Extract: FnMut(&Token) -> tonic::Result<Output>,
    {
        // Fast path: a token is already available.
        if let Some(result) = self.0.borrow().as_ref() {
            return result.as_ref().map_err(Clone::clone).and_then(f);
        }

        // Initialization case: we must await a first token.
        () = self
            .0
            .clone()
            .changed()
            .await
            .expect("TokenStream Sender is not dropped until Receivers all close");

        self.0
            .borrow()
            .as_ref()
            .expect("TokenStream must never yield None after first value")
            .as_ref()
            .map_err(Clone::clone)
            .and_then(f)
    }

    /// Map changes of this TokenStream into a Stream of tonic::Result<Output>.
    ///
    /// The returned Stream will yield no more than once for every change of the
    /// underlying TokenStream.
    ///
    /// This routine is recommended for more-expensive transformations, where
    /// significant computation / allocation / validation must be done atop
    /// underlying Tokens that can be be re-used until a next refresh. The
    /// recommended usage is to await a first Output, and to thereafter poll
    /// on-demand to check for updated Outputs or errors.
    ///
    /// The returned Stream may also be boxed(), making it useful for building
    /// type-erased streams of Outputs built in terms of a boxed closure.
    pub fn map_changes<Extract, Output>(
        self,
        extract: Extract,
    ) -> impl futures::Stream<Item = tonic::Result<Output>>
    where
        Extract: FnMut(&Token) -> tonic::Result<Output>,
    {
        futures::stream::unfold((self, extract), |(mut tokens, mut extract)| async move {
            () = tokens
                .0
                .changed()
                .await
                .expect("TokenStream Sender is not dropped until Receivers all close");

            let guard = tokens.0.borrow_and_update();

            let result = guard
                .as_ref()
                .expect("TokenStream must never yield None after first value")
                .as_ref()
                .map_err(Clone::clone)
                .and_then(&mut extract);

            std::mem::drop(guard);

            Some((result, (tokens, extract)))
        })
    }
}

/// FixedSource is a TokenSource that returns a Token fixture of infinite validity.
pub struct FixedSource<Token>(pub Token);

impl<Token> TokenSource for FixedSource<Token>
where
    Token: Send + Sync + 'static + Clone,
{
    type Token = Token;

    async fn refresh(
        &mut self,
        _started: std::time::SystemTime,
    ) -> tonic::Result<Result<(Self::Token, std::time::Duration), std::time::Duration>> {
        let token = self.0.clone();
        Ok(Ok((token, std::time::Duration::from_secs(u64::MAX))))
    }
}

/// RestSource is a trait fulfilling TokenSource via REST API requests and responses.
pub trait RestSource: Send + Sync {
    /// Model is the deserialized response type from the REST API.
    type Model: for<'de> serde::Deserialize<'de> + Send + Sync + 'static;
    /// Token type extracted from the Model. Often identical to Model.
    type Token: Send + Sync + 'static;

    /// Build an API request whose 200 OK response is a JSON serialization of Self::Model.
    /// A server-side (5XX) error is logged and retried, but not surfaced.
    /// All other error statuses are mapped and surfaced as tonic::Status.
    fn build_request(
        &mut self,
        started: std::time::SystemTime,
    ) -> impl std::future::Future<Output = Result<reqwest::RequestBuilder, tonic::Status>> + Send + Sync;

    /// Extract a Token from a response Model. Returns:
    /// - Ok(Ok((token, valid_for))) if the token is ready for use and valid for the returned Duration.
    /// - Ok(Err(retry_after)) if the response model represents a server-directed client retry.
    /// - Err(status) if the response model is invalid.
    fn extract(
        response: Self::Model,
    ) -> Result<Result<(Self::Token, std::time::Duration), std::time::Duration>, tonic::Status>;
}

impl<R> TokenSource for R
where
    R: RestSource + 'static,
{
    type Token = R::Token;

    async fn refresh(
        &mut self,
        started: std::time::SystemTime,
    ) -> tonic::Result<Result<(Self::Token, std::time::Duration), std::time::Duration>> {
        let request = self.build_request(started).await?;

        let response = request
            .send()
            .await
            .map_err(crate::reqwest_error_to_tonic_status)?;
        let status = response.status();

        // Server errors (5XX) are logged and retried, but aren't surfaced.
        if status.is_server_error() {
            let retry = std::time::Duration::from_millis(rand::random_range(250..5000));
            tracing::warn!(?status, ?retry, "REST token fetch failed (will retry)");
            return Ok(Err(retry));
        }

        let body = response
            .error_for_status()
            .map_err(crate::reqwest_error_to_tonic_status)?
            .bytes()
            .await
            .map_err(crate::reqwest_error_to_tonic_status)?;

        let response: R::Model = serde_json::from_slice(&body).map_err(|err| {
            tonic::Status::unknown(format!(
                "failed to deserialize token response (error {err}) from response body {}",
                String::from_utf8_lossy(&body[..(body.len().min(500))]) // Bounded prefix of body.
            ))
        })?;

        Self::extract(response)
    }
}
