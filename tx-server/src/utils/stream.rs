use tokio_util::codec::{Framed, LengthDelimitedCodec};
use serde::{de::DeserializeOwned, Serialize};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;

pub(super) type FramedStream = Framed<TcpStream, LengthDelimitedCodec>;

#[derive(Debug)]
pub enum StreamError {
    RemoteIoError(std::io::Error),
    BincodeError(Box<bincode::ErrorKind>)
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

#[derive(Debug)]
pub struct MessageStream {
    stream: FramedStream
}

impl MessageStream {
    pub fn from_tcp_stream(stream: TcpStream) -> Self {
        let stream = LengthDelimitedCodec::builder()
            .length_field_type::<u32>()
            .new_framed(stream);

        Self { stream }
    }

    pub async fn send<O>(&mut self, message: O) -> Result<(), StreamError> where O: Serialize {
        let bytes = bincode::serialize(&message)?;
        Ok(self.stream.send(bytes.into()).await?)
    }

    pub async fn recv<I>(&mut self) -> Option<Result<I, StreamError>> where I: DeserializeOwned {
        match self.stream.next().await {
            Some(Ok(bytes)) => {
                match bincode::deserialize(&bytes) {
                    Ok(item) => Some(Ok(item)),
                    Err(e) => Some(Err(StreamError::BincodeError(e)))
                }
            },
            Some(Err(e)) => Some(Err(StreamError::RemoteIoError(e))),
            None => None
        }
    }
}