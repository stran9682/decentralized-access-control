use iroh::protocol::ProtocolHandler;

#[derive(Debug)]
struct AccessControl;

impl ProtocolHandler for AccessControl {
    async fn accept(
        &self,
        connection: iroh::endpoint::Connection,
    ) -> Result<(), iroh::protocol::AcceptError> {
        todo!()
    }
}
