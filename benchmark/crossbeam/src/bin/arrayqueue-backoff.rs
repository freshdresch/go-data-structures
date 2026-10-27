//! Times one crossbeam benchmark workload on an ArrayQueue that waits with Backoff::snooze,
//! and prints one result line.
//!
//! Usage: arrayqueue-backoff <workload>

use crossbeam_bench::{
    arrayqueue::{self, Snooze},
    MESSAGES,
};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    std::process::exit(arrayqueue::main::<Snooze>(
        &args,
        MESSAGES,
        "Rust arrayqueue-backoff",
    ));
}
