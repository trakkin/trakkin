use std::error::Error;

#[cfg(feature = "embedded-database")]
use std::{env, fs, path::PathBuf};

#[cfg(feature = "embedded-database")]
use postgresql_archive::{VersionReq, blocking::get_archive, configuration::zonky};

#[cfg(feature = "embedded-database")]
const POSTGRESQL_VERSION: &str = "18.2.0";

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=frontend/build");

    #[cfg(feature = "embedded-database")]
    embed_postgresql()?;

    Ok(())
}

#[cfg(feature = "embedded-database")]
fn embed_postgresql() -> Result<(), Box<dyn Error>> {
    let version = VersionReq::parse(&format!("={POSTGRESQL_VERSION}"))?;
    let (_, archive) = get_archive(zonky::URL, &version)?;
    fs::write(
        PathBuf::from(env::var("OUT_DIR")?).join("postgresql.jar"),
        archive,
    )?;
    println!("cargo:rustc-env=TRAKKIN_POSTGRESQL_VERSION={POSTGRESQL_VERSION}");
    Ok(())
}
