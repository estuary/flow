use std::marker::PhantomData;

use page_turner::PageTurner;
use page_turner::PageTurnerOutput;
use page_turner::TurnedPage;
use tonic::async_trait;

use crate::api_exec;

/// A simple wrapper around [`postgrest::Builder`] that lets us keep track of which page
/// it's currently on. Used in [`page_turner::PageTurner`] to implement pagination.
pub struct PaginationRequest {
    builder: postgrest::Builder,
    page: usize,
    page_size: usize,
}

impl PaginationRequest {
    pub fn new(builder: postgrest::Builder) -> Self {
        Self {
            builder,
            page: 0,
            page_size: 1000,
        }
    }

    fn set_page(mut self, page: usize) -> Self {
        self.page = page;
        self.builder = self
            .builder
            .range(page * self.page_size, (page + 1) * self.page_size);

        self
    }
}

/// A placeholder struct onto which we can implement [`page_turner::PageTurner`].
/// Normally this would be the API client responsible for actually executing the requests
/// defined in [`PaginationRequest`], but since a [`postgrest::Builder`] already has
/// its own client and is responsible for making its own requests, this is empty.
pub struct PaginationClient<Item>
where
    for<'de> Item: serde::Deserialize<'de> + Send + Sync,
{
    phantom: PhantomData<fn() -> Item>,
}

impl<T> PaginationClient<T>
where
    for<'de> T: serde::Deserialize<'de> + Send + Sync,
{
    pub fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<Item> PageTurner<PaginationRequest> for PaginationClient<Item>
where
    for<'de> Item: serde::Deserialize<'de> + Send + Sync,
{
    type PageItem = Item;
    type PageError = anyhow::Error;

    async fn turn_page(
        &self,
        request: PaginationRequest,
    ) -> PageTurnerOutput<Self, PaginationRequest> {
        let resp: Vec<Item> = api_exec::<Vec<Item>>(request.builder.clone()).await?;

        if resp.len() == request.page_size {
            let current_page = request.page;
            Ok(TurnedPage::next(resp, request.set_page(current_page + 1)))
        } else {
            Ok(TurnedPage::last(resp))
        }
    }
}
