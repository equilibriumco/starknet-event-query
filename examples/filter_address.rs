use clap::Parser;
use eyre::anyhow;
use regex::Regex;
use serde_json::json;
use tracing_subscriber::filter::LevelFilter;

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

use starknet_event_query::util::{parse_event, start_logger};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[arg(
        long,
        short = 'r',
        value_name = "n",
        long_help = "Repeat threshold",
        default_value = "2"
    )]
    pub repeat: u32,
    #[arg(
        long,
        value_name = "fixtures",
        long_help = "Path to fixture directory",
        default_value = "ground"
    )]
    pub fixture_dir: PathBuf,
    #[arg(
        long,
        short = 'm',
        long_help = "Multi-address mode",
        default_value = "false"
    )]
    pub multi: bool,
}

fn cond_refract(cli: &Cli, unfiltered_rx: &Regex, fixture: PathBuf) -> eyre::Result<()> {
    let os_stem = fixture
        .file_stem()
        .ok_or_else(|| anyhow!("invalid fixture path: {:?}", fixture))?;
    let stem = os_stem
        .to_str()
        .ok_or_else(|| anyhow!("invalid fixture name: {:?}", fixture))?;
    if !unfiltered_rx.is_match(stem) {
        return Ok(());
    }

    let mut known_addresses = HashMap::new();
    let mut opt_events = if !cli.multi { Some(Vec::new()) } else { None };
    let source = fs::File::open(&fixture)?;
    let reader = BufReader::new(source);
    for line in reader.lines() {
        let event = parse_event(&line?)?;
        let serde_json::Value::String(ref addr) = event["from_address"] else {
            return Err(anyhow!("unexpected address type"));
        };

        let count = known_addresses.entry(addr.clone()).or_insert(0);
        *count += 1;

        if let Some(ref mut events) = opt_events {
            events.push(event);
        }
    }

    let mut known_addresses: Vec<String> = known_addresses
        .into_iter()
        .filter(|(_, c)| *c >= cli.repeat)
        .map(|(a, _)| a)
        .collect();
    known_addresses.sort();
    tracing::debug!(
        "fixture has events from {} repeated addresses",
        known_addresses.len()
    );

    if let Some(events) = opt_events {
        for (index, addr) in known_addresses.into_iter().enumerate() {
            let filter_no = index + 1;
            let filter_name = format!("{}f{}.json", stem, filter_no);
            let filter_path = cli.fixture_dir.join(filter_name);
            let filter_json = json!({
                "address": addr.clone()
            });
            fs::write(filter_path, filter_json.to_string())?;

            let output_name = format!("{}w{}.jsonl", stem, filter_no);
            let output_path = cli.fixture_dir.join(output_name);
            let mut output_file = fs::File::create(&output_path)?;
            let address = serde_json::Value::String(addr.clone());
            for event in events.iter() {
                if event["from_address"] == address {
                    writeln!(&mut output_file, "{}", event)?;
                }
            }
        }
    } else {
        let filter_name = format!("{}f0.json", stem);
        let filter_path = cli.fixture_dir.join(filter_name);
        let filter_json = json!({
            "address": known_addresses
        });
        fs::write(filter_path, filter_json.to_string())?;

        let output_name = format!("{}w0.jsonl", stem);
        let output_path = cli.fixture_dir.join(output_name);
        fs::copy(fixture, output_path)?;
    }

    Ok(())
}

fn main() -> eyre::Result<()> {
    start_logger(LevelFilter::INFO);
    let cli = Cli::parse();

    let mask_path = cli.fixture_dir.join("*.jsonl");
    let path_str = mask_path
        .to_str()
        .ok_or_else(|| anyhow!("invalid fixture dir: {:?}", cli.fixture_dir))?;
    let unfiltered_rx = Regex::new("^([0-9]+)(?:[+]([1-9][0-9]*))?$").unwrap();
    for entry in glob::glob(path_str)? {
        cond_refract(&cli, &unfiltered_rx, entry?)?;
    }

    Ok(())
}
