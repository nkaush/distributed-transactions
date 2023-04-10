use tokio::{sync::mpsc::{UnboundedSender, error::SendError, UnboundedReceiver}, net::TcpStream};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use futures::{SinkExt, StreamExt};
use serde::{de::DeserializeOwned, Serialize};

pub(super) type FramedStream = Framed<TcpStream, LengthDelimitedCodec>;

#[derive(Debug)]
pub enum StreamError<T> {
    RemoteIoError(std::io::Error),
    BincodeError(Box<bincode::ErrorKind>),
    LocalSendError(SendError<T>),
    InvalidRawRecv,
    InvalidRawSend
}

impl<T> From<SendError<T>> for StreamError<T> {
    fn from(err: SendError<T>) -> Self {
        StreamError::LocalSendError(err)
    }
}

impl<T> From<std::io::Error> for StreamError<T> {
    fn from(err: std::io::Error) -> Self {
        StreamError::RemoteIoError(err)
    }
}

impl<T> From<Box<bincode::ErrorKind>> for StreamError<T> {
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        StreamError::BincodeError(err)
    }
}

#[derive(Debug)]
pub enum MessageStream<T> {
    Remote(FramedStream),
    Local((UnboundedSender<T>, UnboundedReceiver<T>))
}

impl<T> MessageStream<T> {
    pub fn from_tcp_stream(stream: TcpStream) -> Self {
        let stream = LengthDelimitedCodec::builder()
            .length_field_type::<u32>()
            .new_framed(stream);

        Self::Remote(stream)
    }

    // pub fn from_local(sender: UnboundedSender<T>) -> Self {
    //     Self::Local(sender)
    // }

    pub async fn send(&mut self, message: T) -> Result<(), StreamError<T>> where T: Serialize {
        match self {
            Self::Remote(stream) => {
                let bytes = bincode::serialize(&message)?;
                Ok(stream.send(bytes.into()).await?)
            },
            Self::Local((sender, _)) => Ok(sender.send(message)?)
        }
    }

    pub async fn unchecked_send<R>(&mut self, message: R) -> Result<(), StreamError<R>> where R: Serialize {
        match self {
            Self::Remote(stream) => {
                let bytes = bincode::serialize(&message)?;
                Ok(stream.send(bytes.into()).await?)
            },
            Self::Local(_) => Err(StreamError::InvalidRawSend)
        }
    }

    pub async fn recv(&mut self) -> Option<Result<T, StreamError<T>>> where T: DeserializeOwned {
        match self {
            Self::Remote(stream) => match stream.next().await {
                Some(Ok(bytes)) => {
                    match bincode::deserialize(&bytes) {
                        Ok(item) => Some(Ok(item)),
                        Err(e) => Some(Err(StreamError::BincodeError(e)))
                    }
                },
                Some(Err(e)) => Some(Err(StreamError::RemoteIoError(e))),
                None => None
            },
            Self::Local((_, receiver)) => receiver.recv().await.map(|item| Ok(item))
        }
    }

    pub async fn unchecked_recv<R>(&mut self) -> Option<Result<R, StreamError<R>>> where R: DeserializeOwned {
        match self {
            Self::Remote(stream) => match stream.next().await {
                Some(Ok(bytes)) => {
                    match bincode::deserialize(&bytes) {
                        Ok(item) => Some(Ok(item)),
                        Err(e) => Some(Err(StreamError::BincodeError(e)))
                    }
                },
                Some(Err(e)) => Some(Err(StreamError::RemoteIoError(e))),
                None => None
            },
            Self::Local(_) => Some(Err(StreamError::InvalidRawRecv))
        }
    }
}