// Derived from crossbeam-rs/crossbeam crossbeam-channel/benchmarks/crossbeam-channel.rs at commit
// 099d046, which is licensed under the MIT license. Copyright (c) 2019 The Crossbeam Project
// Developers. The changes are: the topologies take the sender, the receiver, and the message
// count, the message count and thread count come from the library target, select_both,
// select_rx, the unbounded variants, and the run! macro are dropped, the program times one
// workload per run, the channel is built and warmed before the timer starts and dropped after it
// stops, the capacities are 1, 2^10, 2^18, and 2^23 in place of 0, 1, 5,000,000, and unbounded,
// seconds print with six decimals, and bad arguments print usage to stderr and exit with
// status 2.

//! The `crossbeam-channel` program: a bounded crossbeam-channel channel.

use std::io::Write;
use std::time::Instant;

use ::crossbeam_channel::{bounded, Receiver, Sender};

use crate::message::{self, Message};
use crate::{check_messages, parse_args, print, print_usage, Topology, THREADS};

/// Returns the channel that every workload with `capacity` runs on.
pub(crate) fn new_channel(capacity: usize) -> (Sender<Message>, Receiver<Message>) {
    bounded(capacity)
}

fn seq(tx: &Sender<Message>, rx: &Receiver<Message>, messages: usize) {
    for i in 0..messages {
        tx.send(message::new(i)).unwrap();
    }

    for _ in 0..messages {
        rx.recv().unwrap();
    }
}

fn spsc(tx: &Sender<Message>, rx: &Receiver<Message>, messages: usize) {
    crossbeam::scope(|scope| {
        scope.spawn(|_| {
            for i in 0..messages {
                tx.send(message::new(i)).unwrap();
            }
        });

        for _ in 0..messages {
            rx.recv().unwrap();
        }
    })
    .unwrap();
}

fn mpsc(tx: &Sender<Message>, rx: &Receiver<Message>, messages: usize) {
    crossbeam::scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|_| {
                for i in 0..messages / THREADS {
                    tx.send(message::new(i)).unwrap();
                }
            });
        }

        for _ in 0..messages {
            rx.recv().unwrap();
        }
    })
    .unwrap();
}

fn mpmc(tx: &Sender<Message>, rx: &Receiver<Message>, messages: usize) {
    crossbeam::scope(|scope| {
        for _ in 0..THREADS {
            scope.spawn(|_| {
                for i in 0..messages / THREADS {
                    tx.send(message::new(i)).unwrap();
                }
            });
        }

        for _ in 0..THREADS {
            scope.spawn(|_| {
                for _ in 0..messages / THREADS {
                    rx.recv().unwrap();
                }
            });
        }
    })
    .unwrap();
}

/// Runs `topology` on the channel with `messages` in total. `messages` must be a positive
/// multiple of `THREADS`. For `Seq`, it must not exceed the channel's capacity, because one
/// thread sends everything before receiving and a larger count blocks forever.
pub(crate) fn run_topology(
    tx: &Sender<Message>,
    rx: &Receiver<Message>,
    topology: Topology,
    messages: usize,
) {
    match topology {
        Topology::Seq => seq(tx, rx, messages),
        Topology::Spsc => spsc(tx, rx, messages),
        Topology::Mpsc => mpsc(tx, rx, messages),
        Topology::Mpmc => mpmc(tx, rx, messages),
    }
}

/// Sends and receives `capacity` messages on the calling thread, so the timed run starts with
/// every slot touched.
fn warm(tx: &Sender<Message>, rx: &Receiver<Message>, capacity: usize) {
    for i in 0..capacity {
        tx.send(message::new(i)).unwrap();
    }
    for _ in 0..capacity {
        rx.recv().unwrap();
    }
}

/// Runs the workload that `args` name with `messages` in total, writes one result line labeled
/// `kind` to `stdout`, and returns the exit status: 0 on success, and 2 for bad arguments or a
/// bad message count, which it reports on `stderr`.
pub fn main(
    args: &[String],
    messages: usize,
    kind: &str,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> i32 {
    let Some((name, capacity, topology)) = parse_args(args) else {
        print_usage(stderr, kind);
        return 2;
    };
    if let Err(line) = check_messages(kind, messages) {
        // A program that cannot write to stderr has nowhere to report that.
        let _ = writeln!(stderr, "{line}");
        return 2;
    }

    let (tx, rx) = new_channel(capacity);
    warm(&tx, &rx, capacity);

    let start = Instant::now();
    run_topology(&tx, &rx, topology, messages);
    let elapsed = start.elapsed();
    print(stdout, &name, kind, elapsed);

    // Freeing a large channel takes milliseconds, so it happens after the timer stops.
    drop((tx, rx));
    0
}
