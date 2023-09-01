use super::{capture, derive, flow, materialize};
use zeroize::Zeroize;

impl Drop for capture::request::Spec {
    fn drop(&mut self) {
        self.config_json.zeroize();
    }
}
impl Drop for capture::request::Discover {
    fn drop(&mut self) {
        self.config_json.zeroize();
    }
}
impl Drop for capture::request::Validate {
    fn drop(&mut self) {
        self.config_json.zeroize();
    }
}
impl Drop for flow::CaptureSpec {
    fn drop(&mut self) {
        self.config_json.zeroize();
    }
}

impl Drop for derive::request::Spec {
    fn drop(&mut self) {
        self.config_json.zeroize();
    }
}
impl Drop for derive::request::Validate {
    fn drop(&mut self) {
        self.config_json.zeroize();
    }
}
impl Drop for flow::collection_spec::Derivation {
    fn drop(&mut self) {
        self.config_json.zeroize();
    }
}

impl Drop for materialize::request::Spec {
    fn drop(&mut self) {
        self.config_json.zeroize();
    }
}
impl Drop for materialize::request::Validate {
    fn drop(&mut self) {
        self.config_json.zeroize();
    }
}
impl Drop for flow::MaterializationSpec {
    fn drop(&mut self) {
        self.config_json.zeroize();
    }
}
