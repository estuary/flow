use doc::shape::location;
use itertools::Itertools;
use serde_json::Value;
use std::{
    fmt::{self, Display},
    io::Read,
};

#[derive(Debug, clap::Args)]
pub struct Args {
    /// Exclude the row with the given JSON pointer from the generated table.
    ///
    /// Passing a pointer to a JSON object does _not_ automatically exclude child properties of
    /// that object. The root document can be excluded by passing `--exclude ''`.
    #[clap(short = 'e', long)]
    pub exclude: Vec<String>,
}

pub fn run(args: Args) -> anyhow::Result<()> {
    let mut schema = Vec::new();
    let _ = std::io::stdin().read_to_end(&mut schema)?;

    let schema = doc::validation::build_bundle(&schema)?;
    let validator = doc::Validator::new(schema)?;
    let shape = doc::Shape::infer(validator.schema(), validator.schema_index());

    println!("| Property | Title | Description | Type | Required/Default |");
    println!("|---|---|---|---|---|");

    for (ptr, pattern, shape, exists) in shape.locations() {
        let ptr_str = ptr.to_string();
        if args.exclude.contains(&ptr_str) {
            continue;
        }
        let formatted_ptr = surround_if(
            exists.cannot(),
            "~~",
            surround_if(
                exists.must(),
                "**",
                surround_if(pattern, "_", Code(ptr_str.as_str())),
            ),
        );

        let title = shape.title.as_deref().unwrap_or("");
        let desc = shape.description.as_deref().unwrap_or("");
        let type_ = shape.type_.to_vec().join(", ");
        let def = shape.default.as_ref().map(|def| &def.0);

        println!(
            "| {} | {} | {} | {} | {} |",
            formatted_ptr,
            md_escape(title),
            md_escape(desc),
            type_,
            RequiredAndDefault(exists, def),
        );
    }

    Ok(())
}

struct RequiredAndDefault<'a>(location::Exists, Option<&'a Value>);
impl<'a> Display for RequiredAndDefault<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let req = match self.0 {
            location::Exists::Must => "Required",
            location::Exists::Cannot => "Cannot exist",
            _ => "",
        };
        let def = self.1.map(ToString::to_string).unwrap_or_default();

        match (req, def.as_str()) {
            ("", "") => Ok(()),
            ("", default_val) => Code(default_val).fmt(f),
            (_, "") => f.write_str(req),
            (_, default_val) => write!(f, "{}, {}", req, Code(default_val)),
        }
    }
}

/// Conditionally surround `inner` with `with` if `surround` is true.
fn surround_if<T: Display>(surround: bool, with: &'static str, inner: T) -> Surround<Repeat, T> {
    let n = if surround { 1 } else { 0 };
    Surround(Repeat(with, n), inner)
}

/// Wrapper around a string to be formatted as a markdown code block
struct Code<'a>(&'a str);
impl<'a> Display for Code<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // An empty pair of backtics ends up rendering as an empty pair of backtics, which looks
        // pretty weird and confusing. So just don't render anything.
        if self.0.is_empty() {
            return Ok(());
        }

        // If this code contains any backtics, then we may need to surround it with more than one
        // backtic. We need to determine the length of the longest continuous sequence of backtic
        // characters within the code block (n), and then surround the entire code with (n+1) backtics.
        let sequential_backtics: usize = self
            .0
            .chars()
            .dedup_with_count()
            .filter_map(|(count, c)| if c == '`' { Some(count) } else { None })
            .max()
            .unwrap_or_default();

        let quote = Repeat("`", sequential_backtics + 1);
        Surround(quote, self.0).fmt(f)
    }
}

#[derive(Clone)]
struct Surround<S, M>(S, M);
impl<S: Display, M: Display> Display for Surround<S, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)?;
        self.1.fmt(f)?;
        self.0.fmt(f)
    }
}

#[derive(Clone, Copy)]
struct Repeat(&'static str, usize);
impl Display for Repeat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for _ in 0..self.1 {
            f.write_str(self.0)?;
        }
        Ok(())
    }
}

// md_escape aggressively escapes any characters which have
// significance for either Markdown or HTML.
fn md_escape(s: &str) -> String {
    let mut out = String::new();
    for c in s.chars() {
        match c {
            '"' => out.push_str("&quot;"),
            '$' => out.push_str("&#x24;"),
            '&' => out.push_str("&amp;"),
            '*' => out.push_str("&#x2A;"),
            '/' => out.push_str("&#x2F;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '[' => out.push_str("&#x5B;"),
            '\'' => out.push_str("&#x27;"),
            '\\' => out.push_str("&#x5C;"),
            '\n' => out.push_str("<br>"),
            '\r' => {}
            ']' => out.push_str("&#x5D;"),
            '_' => out.push_str("&#x5F;"),
            '`' => out.push_str("&#x60;"),
            '|' => out.push_str("&#x7C;"),
            '~' => out.push_str("&#x7E;"),
            _ => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn code_containing_backtics_is_surrounded_with_more_backtics() {
        assert_code_format(r#"foo with ` a backtic"#, r#"``foo with ` a backtic``"#);
        assert_code_format(r#"foo with no backtic"#, r#"`foo with no backtic`"#);
        assert_code_format(
            r#"foo `` with ``````` many backtic`s"#,
            r#"````````foo `` with ``````` many backtic`s````````"#,
        );
    }

    fn assert_code_format(input: &str, expected: &str) {
        let actual = Code(input).to_string();
        assert_eq!(expected, &actual);
    }
}
