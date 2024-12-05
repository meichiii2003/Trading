// communication.rs
use crate::models::PriceUpdate;
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct CommunicationChannels {
    pub price_tx: broadcast::Sender<PriceUpdate>,
}

impl CommunicationChannels {
    pub fn new(buffer_size: usize) -> Self {
        let (price_tx, _) = broadcast::channel(buffer_size);
        Self { price_tx }
    }
}
