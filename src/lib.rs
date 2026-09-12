pub mod access_list;
pub mod discovery;
pub mod iroh;
pub mod protocol;
pub mod store;

pub const ALPN: &[u8] = b"gate";

#[repr(u8)]
#[derive(Debug)]
pub enum Status {
    Denied,
    Allowed,
    FileNotFound,
    ResourceNotFound,
    UnknownError,
}

impl TryFrom<u8> for Status {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Status::Denied),
            1 => Ok(Status::Allowed),
            2 => Ok(Status::FileNotFound),
            3 => Ok(Status::ResourceNotFound),
            _ => Ok(Status::UnknownError),
        }
    }
}
