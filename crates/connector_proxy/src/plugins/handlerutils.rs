#[macro_export]
macro_rules! plugin_handlers{
    ($($plugins:expr, $message:ty, $fn:path)?) => {{
        $(
            let mut handlers: Vec<Box<dyn Fn(&mut $message) -> Result<(), Error>>> = Vec::new();
            for plugin in $plugins {
                handlers.push(Box::new(move |r| $fn(&*plugin, r)));
            }
            handlers
         )?
    }}
}
