pub mod access_list;
pub mod iroh;
pub mod protocol;
pub mod store;

pub const ALPN: &[u8] = b"gate";

#[repr(u8)]
pub enum Status {
    Denied,
    Allowed,
    FileNotFound,
    ResourceNotFound,
}
