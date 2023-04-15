use tokio_util::codec::{Framed, LengthDelimitedCodec};
use serde::{de::DeserializeOwned, Serialize};
use super::pipe::{UnboundedPipe, PipeError};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;

pub(super) type FramedStream = Framed<TcpStream, LengthDelimitedCodec>;

#[derive(Debug)]
pub enum StreamError {
    RemoteIoError(std::io::Error),
    BincodeError(Box<bincode::ErrorKind>),
    PipeError(PipeError),
    InvalidRawRecv,
    InvalidRawSend
}

impl From<std::io::Error> for StreamError {
    fn from(err: std::io::Error) -> Self {
        StreamError::RemoteIoError(err)
    }
}

impl From<Box<bincode::ErrorKind>> for StreamError {
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        StreamError::BincodeError(err)
    }
}

impl From<PipeError> for StreamError {
    fn from(err: PipeError) -> Self {
        StreamError::PipeError(err)
    }
}

#[derive(Debug)]
pub enum MessageStream<I, O> {
    Remote(FramedStream),
    Local(UnboundedPipe<I, O>)
}

impl<I, O> MessageStream<I, O> {
    pub fn from_tcp_stream(stream: TcpStream) -> Self {
        let stream = LengthDelimitedCodec::builder()
            .length_field_type::<u32>()
            .new_framed(stream);

        Self::Remote(stream)
    }

    pub fn from_local(sender: UnboundedPipe<I, O>) -> Self {
        Self::Local(sender)
    }

    pub async fn send(&mut self, message: O) -> Result<(), StreamError> where O: Serialize {
        match self {
            Self::Remote(stream) => {
                let bytes = bincode::serialize(&message)?;
                Ok(stream.send(bytes.into()).await?)
            },
            Self::Local(pipe) => Ok(pipe.send(message)?)
        }
    }

    pub async fn untyped_send<R>(&mut self, message: R) -> Result<(), StreamError> where R: Serialize {
        match self {
            Self::Remote(stream) => {
                let bytes = bincode::serialize(&message)?;
                Ok(stream.send(bytes.into()).await?)
            },
            Self::Local(_) => Err(StreamError::InvalidRawSend)
        }
    }

    pub async fn recv(&mut self) -> Option<Result<I, StreamError>> where I: DeserializeOwned {
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
            Self::Local(pipe) => pipe.recv().await.map(|item| Ok(item))
        }
    }

    pub async fn untyped_recv<R>(&mut self) -> Option<Result<R, StreamError>> where R: DeserializeOwned {
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