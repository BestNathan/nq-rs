use anyhow::Result;
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use tracing::trace;

#[async_trait]
pub trait Runner: Send + Sync {
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    async fn run(&self, canceltoken: CancellationToken) -> Result<()> {
        canceltoken.cancelled().await;
        trace!(runner = self.name(), "runner recv cancelling");
        Ok(())
    }
}
