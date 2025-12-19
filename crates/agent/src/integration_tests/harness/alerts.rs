use crate::alerts::EmailSender;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct TestSender(Arc<Mutex<TestSenderInner>>);
impl TestSender {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(TestSenderInner {
            fail_on: usize::MAX,
            sent: Vec::new(),
        })))
    }

    pub async fn take_sent(&self) -> Vec<notifications::NotificationEmail> {
        let mut lock = self.0.lock().await;
        std::mem::take(&mut lock.sent)
    }

    pub async fn set_fail_after(&self, successful_sends: usize) {
        let mut lock = self.0.lock().await;
        lock.fail_on = successful_sends;
    }
}

#[derive(Debug)]
struct TestSenderInner {
    fail_on: usize,
    sent: Vec<notifications::NotificationEmail>,
}

impl EmailSender for TestSender {
    async fn send<'s>(
        &'s self,
        notification: notifications::NotificationEmail,
    ) -> anyhow::Result<()> {
        let mut lock = self.0.lock().await;
        if lock.fail_on <= lock.sent.len() {
            anyhow::bail!(
                "mock error sending alert email '{}'",
                notification.idempotency_key
            );
        }
        lock.sent.push(notification);
        Ok(())
    }
}
