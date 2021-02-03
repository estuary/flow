use crate::service::{self, Channel};
use derive::schema_api::API;

#[no_mangle]
pub extern "C" fn schema_create() -> *mut Channel {
    service::create::<API>()
}
#[no_mangle]
pub extern "C" fn schema_invoke1(ch: *mut Channel, i: service::In1) {
    service::invoke::<API, _>(ch, i)
}
#[no_mangle]
pub extern "C" fn schema_invoke4(ch: *mut Channel, i: service::In4) {
    service::invoke::<API, _>(ch, i)
}
#[no_mangle]
pub extern "C" fn schema_invoke16(ch: *mut Channel, i: service::In16) {
    service::invoke::<API, _>(ch, i)
}
#[no_mangle]
pub extern "C" fn schema_drop(ch: *mut Channel) {
    service::drop::<API>(ch)
}
