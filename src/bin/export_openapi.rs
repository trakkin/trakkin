use std::{env, fs, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args_os().skip(1);
    let output = arguments
        .next()
        .map(PathBuf::from)
        .ok_or("usage: export_openapi <output-path>")?;
    if arguments.next().is_some() {
        return Err("usage: export_openapi <output-path>".into());
    }
    if let Some(parent) = output.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    let mut contract = serde_json::to_string_pretty(&trakkin::http::openapi())?;
    contract.push('\n');
    fs::write(output, contract)?;
    Ok(())
}
