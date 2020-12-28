use crate::service::{self, Channel};
use derive::derive_api::API;

#[no_mangle]
pub extern "C" fn derive_create() -> *mut Channel {
    service::create::<API>()
}
#[no_mangle]
pub extern "C" fn derive_invoke1(ch: *mut Channel, i: service::In1) {
    service::invoke::<API, _>(ch, i)
}
#[no_mangle]
pub extern "C" fn derive_invoke4(ch: *mut Channel, i: service::In4) {
    service::invoke::<API, _>(ch, i)
}
#[no_mangle]
pub extern "C" fn derive_invoke16(ch: *mut Channel, i: service::In16) {
    service::invoke::<API, _>(ch, i)
}
#[no_mangle]
pub extern "C" fn derive_drop(ch: *mut Channel) {
    service::drop::<API>(ch)
}
