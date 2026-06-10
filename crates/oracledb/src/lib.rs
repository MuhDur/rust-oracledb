#![forbid(unsafe_code)]

use asupersync::{runtime::RuntimeBuilder, Cx};
use oracledb_protocol::{net::EasyConnect, ClientIdentity};

pub use oracledb_protocol as protocol;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Protocol(#[from] oracledb_protocol::ProtocolError),
    #[error("asupersync runtime error: {0}")]
    Runtime(String),
    #[error("Oracle thin protocol connect is not implemented yet; M1 owns this path")]
    NotImplemented,
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug)]
pub struct ConnectOptions {
    pub connect_string: String,
    pub user: String,
    pub password: String,
    pub identity: ClientIdentity,
}

impl ConnectOptions {
    pub fn new(
        connect_string: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
        identity: ClientIdentity,
    ) -> Self {
        Self {
            connect_string: connect_string.into(),
            user: user.into(),
            password: password.into(),
            identity,
        }
    }
}

#[derive(Debug)]
pub struct Connection {
    descriptor: EasyConnect,
    identity: ClientIdentity,
}

impl Connection {
    pub async fn connect(cx: &Cx, options: ConnectOptions) -> Result<Self> {
        cx.checkpoint()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        let descriptor = EasyConnect::parse(&options.connect_string)?;
        let identity = options.identity;
        let _ = (options.user, options.password);
        let _pending_connection = Self {
            descriptor,
            identity,
        };
        Err(Error::NotImplemented)
    }

    pub fn descriptor(&self) -> &EasyConnect {
        &self.descriptor
    }

    pub fn identity(&self) -> &ClientIdentity {
        &self.identity
    }
}

pub struct BlockingConnection;

impl BlockingConnection {
    pub fn connect(options: ConnectOptions) -> Result<Connection> {
        let runtime = RuntimeBuilder::current_thread()
            .build()
            .map_err(|err| Error::Runtime(err.to_string()))?;
        runtime.block_on(async {
            let cx = Cx::current()
                .ok_or_else(|| Error::Runtime("asupersync did not install an ambient Cx".into()))?;
            Connection::connect(&cx, options).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> ClientIdentity {
        ClientIdentity::new("program", "machine", "osuser", "terminal", "driver")
            .expect("test identity should be valid")
    }

    #[test]
    fn blocking_facade_enters_asupersync_runtime() {
        let options = ConnectOptions::new("localhost/FREEPDB1", "user", "password", identity());
        let err = BlockingConnection::connect(options)
            .expect_err("M0 blocking facade should stop at unimplemented protocol");
        assert!(matches!(err, Error::NotImplemented));
    }
}
