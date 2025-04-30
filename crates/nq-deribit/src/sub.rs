use flume::{Receiver, RecvError};
use futures_util::Stream;

#[derive(Clone)]
pub struct DeribitSubscriptionClient {
    rx: Receiver<String>,
}

impl DeribitSubscriptionClient {
    pub(crate) fn new(rx: Receiver<String>) -> Self {
        Self { rx }
    }

    pub fn recv(&self) -> impl Future<Output=Result<String, RecvError>> {
        self.rx.recv_async()
    }

    pub fn stream(&self) -> impl Stream<Item=String> {
        self.rx.stream()
    }
}
