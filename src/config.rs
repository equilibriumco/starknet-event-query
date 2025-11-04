use clap::Parser;

use std::path::PathBuf;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[arg(
        long,
        value_name = "url",
        long_help = "Server URL",
        default_value = "http://127.0.0.1:9545"
    )]
    pub pathfinder_rpc_url: String,
    #[arg(
        long,
        value_name = "url",
        long_help = "Server websocket URL",
        default_value = "ws://localhost:9545/rpc/v0_9"
    )]
    pub pathfinder_ws_url: String,
    #[arg(
        long,
        value_name = "fixtures",
        long_help = "Path to fixture directory",
        default_value = "ground"
    )]
    pub fixture_dir: PathBuf,
    #[arg(
        long,
        short = 's',
        long_help = "Subscribe mode",
        default_value = "false"
    )]
    pub subscribe: bool,
}
