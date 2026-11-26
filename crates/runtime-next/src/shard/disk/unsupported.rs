pub struct Attached(std::convert::Infallible);

impl Attached {
    pub async fn new(
        _uds_path: &std::path::Path,
        _journal: &str,
        _ack_intent: Option<bytes::Bytes>,
    ) -> anyhow::Result<Self> {
        anyhow::bail!("persistent disks need Linux")
    }

    pub fn persist_dir(&self) -> std::path::PathBuf {
        match self.0 {}
    }

    pub async fn prepare(
        &mut self,
    ) -> anyhow::Result<std::collections::BTreeMap<String, bytes::Bytes>> {
        match self.0 {}
    }

    pub fn acknowledge(&mut self) {
        match self.0 {}
    }

    pub async fn close(self) -> anyhow::Result<()> {
        match self.0 {}
    }
}
