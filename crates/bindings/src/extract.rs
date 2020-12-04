use crate::service::{self, Channel};
use derive::extract_api::Extractor;

#[no_mangle]
pub extern "C" fn extractor_create() -> *mut Channel {
    service::create::<Extractor>()
}
#[no_mangle]
pub extern "C" fn extractor_invoke1(ch: *mut Channel, i: service::In1) {
    service::invoke::<Extractor, _>(ch, i)
}
#[no_mangle]
pub extern "C" fn extractor_invoke4(ch: *mut Channel, i: service::In4) {
    service::invoke::<Extractor, _>(ch, i)
}
#[no_mangle]
pub extern "C" fn extractor_invoke16(ch: *mut Channel, i: service::In16) {
    service::invoke::<Extractor, _>(ch, i)
}
#[no_mangle]
pub extern "C" fn extractor_drop(ch: *mut Channel) {
    service::drop::<Extractor>(ch)
}
