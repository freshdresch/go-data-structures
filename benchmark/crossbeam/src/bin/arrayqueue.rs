//! Times one crossbeam benchmark workload on an ArrayQueue that yields while it waits,
//! and prints one result line.
//!
//! Usage: arrayqueue <workload>

use crossbeam_bench::{
    arrayqueue::{self, Yield},
    MESSAGES,
};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    std::process::exit(arrayqueue::main::<Yield>(
        &args,
        MESSAGES,
        "Rust arrayqueue",
        &mut std::io::stdout().lock(),
        &mut std::io::stderr(),
    ));
}
