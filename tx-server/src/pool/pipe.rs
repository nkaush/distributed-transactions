use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

#[derive(Debug)]
pub struct UnboundedPipe<I, O> {
    rcv: UnboundedReceiver<I>,
    snd: UnboundedSender<O>
}

pub fn unbounded_pipe<I, O>() -> (UnboundedPipe<I, O>, UnboundedPipe<O, I>) {
    let (s1, r1) = unbounded_channel();
    let (s2, r2) = unbounded_channel();

    let p1 = UnboundedPipe { snd: s1, rcv: r2 };
    let p2 = UnboundedPipe { snd: s2, rcv: r1 };

    (p1, p2)
}

#[derive(Debug)]
pub struct PipeError {}

impl<I, O> UnboundedPipe<I, O> {
    pub fn send(&self, item: O) -> Result<(), PipeError>  {
        self.snd.send(item).map_err(|_| PipeError { })
    }

    pub async fn recv(&mut self) -> Option<I> {
        self.rcv.recv().await
    }
}