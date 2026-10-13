use std::{env, fs, path::Path};

fn main() -> anyhow::Result<()> {
    let json = tackt_core_api::openapi::json();
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("openapi.json");
    match env::args().nth(1).as_deref() {
        Some("--write") => fs::write(path, json)?,
        Some("--check") => anyhow::ensure!(
            fs::read_to_string(path)? == json,
            "Core API openapi.json is stale. Run: cargo run --locked -p tackt-core-api --example export_openapi -- --write"
        ),
        None => print!("{json}"),
        _ => anyhow::bail!("usage: export_openapi [--write|--check]"),
    }
    Ok(())
}
