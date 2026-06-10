use asupersync::{runtime::RuntimeBuilder, Cx};
use oracledb::{ConnectOptions, Connection, Error};
use oracledb_protocol::ClientIdentity;

#[test]
fn connect_seam_is_cx_first_and_cancel_checkpointed() {
    let runtime = RuntimeBuilder::current_thread()
        .build()
        .expect("current-thread Asupersync runtime should build");

    let result = runtime.block_on(async {
        let cx = Cx::current().expect("Runtime::block_on should install an ambient Cx");
        let identity = ClientIdentity::new("program", "machine", "osuser", "terminal", "driver")
            .expect("test identity should be valid");
        let options = ConnectOptions::new("localhost/FREEPDB1", "user", "password", identity);
        Connection::connect(&cx, options).await
    });

    assert!(matches!(result, Err(Error::NotImplemented)));
}
