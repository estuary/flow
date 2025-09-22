use std::fs;
use std::path::Path;

fn main() {
    // Get the GraphQL schema SDL from control-plane-api
    let schema_sdl = control_plane_api::graphql_schema_sdl();
    
    // Write the schema to a file
    let out_dir = env!("CARGO_MANIFEST_DIR");
    let schema_path = Path::new(out_dir).join("control-plane-api.graphql");
    
    fs::write(&schema_path, schema_sdl)
        .expect("Failed to write GraphQL schema file");
    
    println!("cargo:rerun-if-changed=build.rs");
}