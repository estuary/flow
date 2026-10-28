use crate::device;
use crate::journal;
use tokio::sync::mpsc;

/// An attachment standing by, which replays its journal into the image its device
/// will serve.
pub(super) struct Standby {
    device: device::Standby,
    content_tx: mpsc::Sender<(i64, bytes::Bytes)>,
    client: gazette::journal::Client,
    journal: String,
    read: journal::Read,
}

impl Standby {
    /// Open `journal` and a standby device, and replay the journal from its
    /// recovery floor to the head it held at open.
    pub async fn open(
        client: gazette::journal::Client,
        image_dir: &std::path::Path,
        horizon_policy: device::Policy,
        journal: String,
        blocks: u32,
    ) -> anyhow::Result<Self> {
        let (read, span) = journal::open(&client, &journal).await?;
        let (device, content_tx) = device::Standby::open(image_dir, blocks, horizon_policy)?;

        let mut standby = Self {
            device,
            content_tx,
            client,
            journal,
            read,
        };

        if span.is_empty() {
            return Ok(standby);
        }

        match standby
            .read(Some(span.end), std::future::pending::<()>())
            .await
        {
            Ok(_) => Ok(standby),
            Err(err) => Err(match standby.discard().await {
                Ok(()) => err,
                Err(discard_err) => discard_err,
            }),
        }
    }

    /// Tail the journal until `until` resolves.
    pub async fn tail<T>(mut self, until: impl Future<Output = T>) -> anyhow::Result<(Self, T)> {
        match self.read(None, until).await {
            Ok(Some(out)) => Ok((self, out)),
            Ok(None) => unreachable!("a tailing read never ends"),
            Err(err) => Err(match self.discard().await {
                Ok(()) => err,
                Err(discard_err) => discard_err,
            }),
        }
    }

    /// Fence the previous primary, re-acknowledging `ack_intent` in the fence's
    /// append, and replay through the head the broker then confirms. Then begin
    /// starting the device.
    pub async fn promote(
        mut self,
        mount_dir: std::path::PathBuf,
        peer: (u32, u32),
        ack_intent: Option<bytes::Bytes>,
    ) -> anyhow::Result<super::promoted::Promoted> {
        let (writer, head) = journal::Writer::fence(
            self.client.clone(),
            self.journal.clone(),
            ack_intent.as_ref(),
        )
        .await?;

        // The fence appended past everything read so far, so the read yields at
        // least the fence's record before it reaches `head`.
        if let Err(err) = self.read(Some(head), std::future::pending::<()>()).await {
            return Err(match self.discard().await {
                Ok(()) => err,
                Err(discard_err) => discard_err,
            });
        }

        let Self {
            device, content_tx, ..
        } = self;

        Ok(super::promoted::Promoted::start(
            device,
            content_tx,
            writer,
            mount_dir,
            peer,
            ack_intent.is_some(),
        ))
    }

    /// Discard the standby and the device it created.
    async fn discard(self) -> anyhow::Result<()> {
        let Self {
            device, content_tx, ..
        } = self;

        drop(content_tx);
        tokio::task::spawn_blocking(move || device.discard()).await?
    }

    /// Read until a response reaches `through`, or until `until` resolves,
    /// sending everything read to the device's rebuilder.
    async fn read<T>(
        &mut self,
        through: Option<i64>,
        until: impl Future<Output = T>,
    ) -> anyhow::Result<Option<T>> {
        tokio::pin!(until);

        loop {
            let (response, offset) = tokio::select! {
                out = &mut until => return Ok(Some(out)),
                () = self.content_tx.closed() => {
                    anyhow::bail!("the disk's device failed to rebuild its image")
                }
                response = futures::StreamExt::next(&mut self.read) => {
                    response.expect("a blocking read does not end")?
                }
            };
            if self
                .content_tx
                .send((response.offset, response.content))
                .await
                .is_err()
            {
                anyhow::bail!("the disk's device failed to rebuild its image");
            }
            if through.is_some_and(|through| offset >= through) {
                return Ok(None);
            }
        }
    }
}
