pub trait IntoMessages {
    type Message: serde::Serialize + Sized;

    fn into_messages(self) -> Vec<Self::Message>;
}

pub trait FromMessage: Sized {
    type Message: for<'de> serde::Deserialize<'de> + Sized;

    fn from_message(msg: Self::Message, out: &mut Vec<Self>) -> anyhow::Result<()>;
}

// Convert is an internal trait similar to Into, which plays a role similar to From,
// but is defined over types of external crates.
trait Convert {
    type Target;
    #[must_use]
    fn convert(self: Self) -> Self::Target;
}

pub mod capture;
pub mod materialize;
pub mod test;
