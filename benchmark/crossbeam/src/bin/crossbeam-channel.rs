//! Times one crossbeam benchmark workload on a crossbeam-channel bounded channel,
//! and prints one result line.
//!
//! Usage: crossbeam-channel <workload>

use crossbeam_bench::{crossbeam_channel, MESSAGES};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    std::process::exit(crossbeam_channel::main(
        &args,
        MESSAGES,
        "Rust crossbeam-channel",
    ));
}
