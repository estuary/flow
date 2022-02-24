use std::marker::PhantomData;

use hmac::digest::MacError;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::config::settings;

pub struct MessageVerifier<M> {
    key: Vec<u8>,
    _message_type: PhantomData<M>,
}

impl<M> MessageVerifier<M> {
    pub fn with_key_base(key_base: &[u8]) -> Self {
        let key = [key_base, std::any::type_name::<M>().as_bytes()].concat();
        Self {
            key,
            _message_type: PhantomData,
        }
    }

    pub fn sign(&self, input: &[u8]) -> Vec<u8> {
        let mut hmac = self.new_hmac();
        hmac.update(input);
        hmac.finalize().into_bytes().to_vec()
    }

    pub fn verify(&self, input: &[u8], mac: &[u8]) -> Result<(), MacError> {
        let mut hmac = self.new_hmac();
        hmac.update(input);
        hmac.verify_slice(&mac)
    }

    fn new_hmac(&self) -> Hmac<Sha256> {
        Hmac::<Sha256>::new_from_slice(&self.key)
            .expect("HMAC can take a key of any size so this cannot fail")
    }
}

impl<M> Default for MessageVerifier<M> {
    fn default() -> Self {
        Self::with_key_base(&settings().application.secret_key_base)
    }
}
