/// Prints the database url gathered from the config settings files.
///
/// Useful for:
/// - export DATABASE_URL=$(cargo run --bin db_url)
/// or
/// - sqlx database setup --database-url $(cargo run --bin db_url)
fn main() {
    let settings = control::config::settings();
    print!("{}", settings.database.url());
}
