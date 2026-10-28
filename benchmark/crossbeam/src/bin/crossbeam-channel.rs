//! Times one crossbeam benchmark workload on a crossbeam-channel bounded channel,
//! and prints one result line.
//!
//! Usage: crossbeam-channel <workload>

use std::io::Write;

use crossbeam_bench::{crossbeam_channel, MESSAGES};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut stdout = std::io::stdout().lock();
    let status = crossbeam_channel::main(
        &args,
        MESSAGES,
        "Rust crossbeam-channel",
        &mut stdout,
        &mut std::io::stderr(),
    );
    // process::exit does not run destructors, and stdout is documented to flush line by line
    // only for a terminal, so the result line is flushed here before exiting.
    // A program that cannot write its output has nowhere to report that.
    let _ = stdout.flush();
    std::process::exit(status);
}
