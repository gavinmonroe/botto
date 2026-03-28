// ---------------------------------------------------------------------------
// MessageBus — in-process broadcast for channel adapter messages.
//
// Dual-channel design: one broadcast for inbound (parsed input from any
// channel), one for outbound (responses to deliver). Uses tokio::broadcast
// so multiple subscribers (router, output adapters, audit) can all receive.
// ---------------------------------------------------------------------------

use tokio::sync::broadcast;
use tracing::trace;

use super::types::{InboundMessage, OutboundMessage};

const INBOUND_CAPACITY: usize = 512;
const OUTBOUND_CAPACITY: usize = 512;

#[derive(Clone)]
pub struct MessageBus {
    inbound_tx: broadcast::Sender<InboundMessage>,
    outbound_tx: broadcast::Sender<OutboundMessage>,
}

impl MessageBus {
    pub fn new() -> Self {
        let (inbound_tx, _) = broadcast::channel(INBOUND_CAPACITY);
        let (outbound_tx, _) = broadcast::channel(OUTBOUND_CAPACITY);
        Self {
            inbound_tx,
            outbound_tx,
        }
    }

    /// Publish a parsed inbound message. Returns receiver count.
    pub fn publish_inbound(&self, message: InboundMessage) -> usize {
        trace!(
            id = %message.id,
            channel = %message.context.channel,
            action = %message.action.action_name(),
            "publishing inbound message"
        );
        self.inbound_tx.send(message).unwrap_or(0)
    }

    /// Subscribe to inbound messages.
    pub fn subscribe_inbound(&self) -> broadcast::Receiver<InboundMessage> {
        self.inbound_tx.subscribe()
    }

    /// Publish an outbound message for delivery. Returns receiver count.
    pub fn publish_outbound(&self, message: OutboundMessage) -> usize {
        trace!(
            id = %message.id,
            channel = %message.reply_context.channel,
            msg_type = %message.message_type.type_name(),
            "publishing outbound message"
        );
        self.outbound_tx.send(message).unwrap_or(0)
    }

    /// Subscribe to outbound messages.
    pub fn subscribe_outbound(&self) -> broadcast::Receiver<OutboundMessage> {
        self.outbound_tx.subscribe()
    }
}
