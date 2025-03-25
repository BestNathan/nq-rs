use std::{cell::RefCell, sync::Arc, time::Duration};

use runner::Runner;
use tokio::{
    select, signal,
    sync::{mpsc, oneshot},
    time::timeout,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, warn};

pub mod runner;

pub struct Application {
    task_tracker: TaskTracker,
    runners: RefCell<Vec<Arc<dyn Runner>>>,
}

impl Application {
    pub fn new() -> Self {
        Self {
            runners: Vec::new().into(),
            task_tracker: TaskTracker::new(),
        }
    }

    pub fn add_runner(&self, r: Arc<dyn Runner>) {
        self.runners.borrow_mut().push(r);
    }

    pub async fn run(&self, canceltoken: CancellationToken) {
        debug!("application start running");
        let tt = self.task_tracker.clone();
        let (errtx, mut errrx) = mpsc::channel(1);

        for runner in self.runners.borrow().iter() {
            let canceltoken = canceltoken.clone();
            let runner = Arc::clone(runner);
            let runner_name = runner.name();
            let tx = errtx.clone();

            debug!("application spawn runner: {runner_name:}");

            tt.spawn(async move {
                match runner.run(canceltoken).await {
                    Err(error) => {
                        warn!(runner = runner_name, ?error, "runner error");
                        tx.send(()).await.unwrap_or_default();
                    }
                    Ok(()) => {}
                };
            });
        }

        tt.close();

        debug!("application is running");

        select! {
            _ = errrx.recv() => {
                debug!("application recv runner error");
            }
            _ = canceltoken.cancelled() => {
                debug!("application recv cancelling");
            }
            _ = signal::ctrl_c() => {
                debug!("application recv ctrl-c");
            }
            _ = tt.wait() => {
                debug!("application recv all tasks done");
            }
        }

        if !canceltoken.is_cancelled() {
            canceltoken.cancel();
        }

        debug!("application waiting for tasks done");

        let (tx, rx) = oneshot::channel::<()>();

        select! {
            _ = tt.wait() => {
                debug!("application all task done");
                tx.send(()).unwrap_or_default();
            }
            _ = timeout(Duration::from_secs(3), rx) => {
                debug!("application waiting for all tasks timeout");
            }
        }

        debug!("application done");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use async_trait::async_trait;
    use std::time::Duration;
    use tokio::time::{sleep, Instant};
    use tracing::info;

    #[tokio::test]
    async fn test_for_timeout() {
        std::env::set_var("RUST_LOG", "trace");
        tracing_subscriber::fmt::try_init().unwrap_or_default();

        struct TestRunner;

        #[async_trait]
        impl Runner for TestRunner {
            async fn run(&self, canceltoken: CancellationToken) -> Result<()> {
                loop {
                    let (_, rx) = oneshot::channel::<()>();
                    select! {
                        _ = canceltoken.cancelled() => {
                            debug!(runner = self.name(), "runner recv cancelling");
                            break;
                        }
                        _ = rx => {}
                    }
                }

                debug!(runner = self.name(), "runner cleaning...");
                sleep(Duration::from_secs(5)).await;
                debug!(runner = self.name(), "runner done");
                Ok(())
            }
        }

        let app = Application::new();
        let canceltoken = CancellationToken::new();
        app.add_runner(Arc::new(TestRunner));

        {
            let canceltoken = canceltoken.clone();
            tokio::spawn(async move {
                sleep(Duration::from_secs(1)).await;
                info!("cancel application after 1 second");
                canceltoken.cancel();
            });
        }

        let start = Instant::now();
        app.run(canceltoken).await;
        let end = Instant::now();

        assert!(end - start >= Duration::from_secs(3));
    }

    #[tokio::test]
    async fn test_for_normal() {
        std::env::set_var("RUST_LOG", "trace");
        tracing_subscriber::fmt::try_init().unwrap_or_default();

        struct TestRunner;

        #[async_trait]
        impl Runner for TestRunner {
            async fn run(&self, canceltoken: CancellationToken) -> Result<()> {
                loop {
                    let (_, rx) = oneshot::channel::<()>();
                    select! {
                        _ = canceltoken.cancelled() => {
                            debug!(runner = self.name(), "runner recv cancelling");
                            break;
                        }
                        _ = rx => {}
                    }
                }

                debug!(runner = self.name(), "runner cleaning...");
                sleep(Duration::from_secs(1)).await;
                debug!(runner = self.name(), "runner done");
                Ok(())
            }
        }

        let app = Application::new();
        let canceltoken = CancellationToken::new();
        app.add_runner(Arc::new(TestRunner));

        {
            let canceltoken = canceltoken.clone();
            tokio::spawn(async move {
                sleep(Duration::from_secs(1)).await;
                info!("cancel application after 1 second");
                canceltoken.cancel();
            });
        }

        let start = Instant::now();
        app.run(canceltoken).await;
        let end = Instant::now();

        assert!(end - start <= Duration::from_secs(3));
    }

    #[tokio::test]
    async fn test_for_all_task_done() {
        std::env::set_var("RUST_LOG", "trace");
        tracing_subscriber::fmt::try_init().unwrap_or_default();

        struct TestRunner;

        #[async_trait]
        impl Runner for TestRunner {
            async fn run(&self, _canceltoken: CancellationToken) -> Result<()> {
                sleep(Duration::from_secs(1)).await;
                debug!(runner = self.name(), "runner done");
                Ok(())
            }
        }

        let app = Application::new();
        let canceltoken = CancellationToken::new();
        app.add_runner(Arc::new(TestRunner));

        let start = Instant::now();
        app.run(canceltoken).await;
        let end = Instant::now();

        assert!(end - start <= Duration::from_secs(3));
    }
}
