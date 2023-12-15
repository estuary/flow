use crate::{capture, derive, materialize, runtime};
use prost::Message;

macro_rules! impl_internal {
    ($msg_type:ty , $ext_type:ty) => {
        impl $msg_type {
            /// Get the internal field, decoded into its corresponding extension type.
            pub fn get_internal(&self) -> Result<$ext_type, prost::DecodeError> {
                prost::Message::decode(self.internal.clone())
            }

            /// Set and inspect the internal field via a callback.
            /// Modifications made by the callback are re-encoded into the
            /// internal Any message.
            pub fn set_internal<F>(&mut self, cb: F)
            where
                F: FnOnce(&mut $ext_type),
            {
                self.set_internal_buf(&mut bytes::BytesMut::new(), cb)
            }

            /// Set and inspect the internal field via a callback.
            /// Uses an owned buffer for required allocations.
            pub fn set_internal_buf<F>(&mut self, buf: &mut bytes::BytesMut, cb: F)
            where
                F: FnOnce(&mut $ext_type),
            {
                let mut internal = self.get_internal().expect("internal is valid");
                cb(&mut internal);

                buf.reserve(internal.encoded_len());
                internal.encode(buf).unwrap();

                self.internal = buf.split().freeze();
            }

            /// Set and inspect the internal field via callback,
            /// returning Self.
            pub fn with_internal<F>(mut self, cb: F) -> Self
            where
                F: FnOnce(&mut $ext_type),
            {
                self.set_internal_buf(&mut bytes::BytesMut::new(), cb);
                self
            }

            /// Set and inspect the internal field via callback,
            /// returning Self and using the provided buffer.
            pub fn with_internal_buf<F>(mut self, buf: &mut bytes::BytesMut, cb: F) -> Self
            where
                F: FnOnce(&mut $ext_type),
            {
                self.set_internal_buf(buf, cb);
                self
            }
        }
    };
}

impl_internal!(capture::Request, runtime::CaptureRequestExt);
impl_internal!(capture::Response, runtime::CaptureResponseExt);
impl_internal!(derive::Request, runtime::DeriveRequestExt);
impl_internal!(derive::Response, runtime::DeriveResponseExt);
impl_internal!(materialize::Request, runtime::MaterializeRequestExt);
impl_internal!(materialize::Response, runtime::MaterializeResponseExt);
