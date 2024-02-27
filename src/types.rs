pub trait ByteHandler {
    fn get_bytes(&self) -> Vec<u8>;
    fn from_bytes(a: Vec<u8>) -> Self;
}