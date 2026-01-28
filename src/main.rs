use clap::Parser;
use eyre::anyhow;
use pretty_assertions_sorted::assert_eq;
use serde_json::json;
use starknet_rust::{
    core::types::{BlockId, ConfirmedBlockId, EventFilter, L2TransactionFinalityStatus},
    providers::{
        Provider, Url,
        jsonrpc::{HttpTransport, JsonRpcClient},
    },
};
use starknet_rust_tokio_tungstenite::{EventSubscriptionOptions, EventsUpdate, TungsteniteStream};
use tracing_subscriber::filter::LevelFilter;

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use starknet_event_query::{
    config::Cli,
    filter_seed::FilterSeed,
    util::{parse_event, start_logger},
};

fn check_received_data(fixture: PathBuf, mut destination: fs::File) -> eyre::Result<()> {
    destination.seek(SeekFrom::Start(0))?;
    let actual_reader = BufReader::new(destination);
    let source = fs::File::open(fixture)?;
    let expected_reader = BufReader::new(source);
    for (actual_line, expected_line) in
        itertools::zip_eq(actual_reader.lines(), expected_reader.lines())
    {
        let actual_event = parse_event(&actual_line?)?;
        let expected_event = parse_event(&expected_line?)?;
        assert_eq!(actual_event, expected_event);
    }

    Ok(())
}

fn make_actual_path(fixture: &Path) -> eyre::Result<PathBuf> {
    let fixture_dir = fixture
        .parent()
        .ok_or_else(|| anyhow!("fixture without path: {:?}", fixture))?;
    let os_name = fixture
        .file_name()
        .ok_or_else(|| anyhow!("invalid fixture path: {:?}", fixture))?;
    let name = os_name
        .to_str()
        .ok_or_else(|| anyhow!("invalid fixture name: {:?}", fixture))?;
    let actual_name = format!("a{name}");
    Ok(fixture_dir.join(actual_name))
}

async fn check_rpc_fixture(
    cli: &Cli,
    provider: &impl Provider,
    fixture: PathBuf,
) -> eyre::Result<()> {
    let filter_seed = FilterSeed::load(&fixture)?;
    let (address, keys) = filter_seed.get_filter_address_and_keys(&fixture)?;
    let filter = EventFilter {
        from_block: Some(BlockId::Number(filter_seed.from_block)),
        to_block: Some(BlockId::Number(filter_seed.to_block)),
        address,
        keys,
    };
    let mut token = None;
    let mut destination = if cli.persist {
        let destination_path = make_actual_path(&fixture)?;
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(destination_path)?
    } else {
        tempfile::tempfile()?
    };
    let mut actual_count = 0;
    let mut page_count = 0;
    loop {
        let page = provider.get_events(filter.clone(), token, 1024).await?;
        page_count += 1;
        for event in page.events {
            let raw_string = serde_json::to_string(&event)?;
            let mut event_map: HashMap<String, serde_json::Value> =
                serde_json::from_str(&raw_string)?;
            for extra in ["block_hash", "event_index", "transaction_index"] {
                event_map.remove(extra);
            }

            let s = serde_json::to_string(&event_map)?;
            let v: serde_json::Value = serde_json::from_str(&s)?;
            writeln!(&mut destination, "{}", v)?;
            actual_count += 1;
        }

        token = page.continuation_token;
        if token.is_none() {
            break;
        }
    }

    tracing::debug!("retrieved {} events in {} pages", actual_count, page_count);
    check_received_data(fixture, destination)
}

async fn check_ws_fixture(ws_url: &Url, fixture: PathBuf) -> eyre::Result<()> {
    let filter_seed = FilterSeed::load(&fixture)?;
    let (address, keys) = filter_seed.get_filter_address_and_keys(&fixture)?;
    let stream = TungsteniteStream::connect(ws_url, Duration::from_secs(5))
        .await
        .expect("WebSocket connection failed");
    let mut options = EventSubscriptionOptions::new()
        .with_block_id(ConfirmedBlockId::Number(filter_seed.from_block));
    options.from_address = address;
    options.keys = keys;
    // requires JSON-RPC API >= v09
    options.finality_status = L2TransactionFinalityStatus::AcceptedOnL2;
    let mut subscription = stream.subscribe_events(options).await.unwrap();
    let mut actual_count = 0;
    let source = fs::File::open(fixture)?;
    let expected_reader = BufReader::new(source);
    let mut expected_iter = expected_reader.lines();
    let Some(expected_res) = expected_iter.next() else {
        return Err(anyhow!("empty results not supported in subscribe mode"));
    };
    let mut expected_line = expected_res?;
    loop {
        match subscription.recv().await {
            Ok(EventsUpdate::Event(event)) => {
                if let Some(block_number) = event.emitted_event.block_number {
                    if block_number > filter_seed.to_block {
                        return Err(anyhow!("missing expected values"));
                    }

                    let actual_json = json!({
                        "block_number": block_number,
                        "data": event.emitted_event.data,
                        "from_address": event.emitted_event.from_address,
                        "keys": event.emitted_event.keys,
                        "transaction_hash": event.emitted_event.transaction_hash,
                    });
                    let expected_json = parse_event(&expected_line)?;
                    assert_eq!(actual_json, expected_json);
                    actual_count += 1;
                    if let Some(expected_res) = expected_iter.next() {
                        expected_line = expected_res?;
                    } else {
                        break;
                    }
                } else {
                    return Err(anyhow!("got event w/o block number"));
                }
            }
            Ok(EventsUpdate::Reorg(reorg)) => {
                // we only test stable historical data
                return Err(anyhow!(
                    "encountered reorg {} -> {}",
                    reorg.starting_block_number,
                    reorg.ending_block_number
                ));
            }
            Err(err) => {
                return Err(err.into());
            }
        }
    }

    subscription.unsubscribe().await?;
    tracing::debug!("retrieved {} events", actual_count);
    Ok(())
}

async fn run_rpc(cli: Cli, mask_path_str: &str) -> eyre::Result<()> {
    let rpc_url: Url = cli.pathfinder_rpc_url.parse()?;
    let provider = JsonRpcClient::new(HttpTransport::new(rpc_url));
    for entry in glob::glob(mask_path_str)? {
        check_rpc_fixture(&cli, &provider, entry?).await?;
    }

    Ok(())
}

async fn run_ws(ws_url: Url, mask_path_str: &str) -> eyre::Result<()> {
    for entry in glob::glob(mask_path_str)? {
        check_ws_fixture(&ws_url, entry?).await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    start_logger(LevelFilter::INFO);

    let cli = Cli::parse();
    let mask_path = cli.fixture_dir.join("*.jsonl");
    let path_str = mask_path
        .to_str()
        .ok_or_else(|| anyhow!("invalid fixture dir: {:?}", cli.fixture_dir))?;
    if !cli.subscribe {
        run_rpc(cli, path_str).await
    } else {
        let ws_url: Url = cli.pathfinder_ws_url.parse()?;
        run_ws(ws_url, path_str).await
    }
}
