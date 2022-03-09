/// Returns the full name of the function where it is invoked. This includes the module path to the function.
///
/// Ex. `"acme::anvils::drop_from_a_great_height"`
// Directly pulled from https://github.com/mitsuhiko/insta/blob/e8f3f2782e24b4eb5f256f94bbd98048d4a716ba/src/macros.rs#L1-L17
// Apache Licensed from https://github.com/mitsuhiko/insta/blob/e8f3f2782e24b4eb5f256f94bbd98048d4a716ba/LICENSE
macro_rules! function_name {
    () => {{
        fn f() {}
        fn type_name_of_val<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let mut name = type_name_of_val(f).strip_suffix("::f").unwrap_or("");
        while let Some(rest) = name.strip_suffix("::{{closure}}") {
            name = rest;
        }
        name
    }};
}

pub(crate) use function_name;
