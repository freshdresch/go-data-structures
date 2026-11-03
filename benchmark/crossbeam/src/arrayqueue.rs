// Modeled on crossbeam-rs/crossbeam crossbeam-channel/benchmarks/segqueue.rs at commit 099d046,
// which is licensed under the MIT license. Copyright (c) 2019 The Crossbeam Project Developers.
// The changes are: the queue is a bounded ArrayQueue in place of a SegQueue, so a push that
// finds the queue full waits and tries again, the waiting is a type parameter, either upstream's
// thread::yield_now or Backoff::snooze with a reset after each success, seq waits the same way
// as the other topologies, the topologies take the queue and the message count, the message
// count and thread count come from the library target, receivers keep what they got when
// recording is on, the program times one workload per run, the queue is built and warmed before
// the timer starts and dropped after it stops, the capacities are 1, 2^10, 2^18, and 2^23 in
// place of unbounded, seconds print with six decimals, and bad arguments print usage to stderr
// and exit with status 2.

//! The `arrayqueue` and `arrayqueue-backoff` programs: crossbeam's `ArrayQueue`, polled with
//! one of two waiting strategies.

use std::io::Write;
use std::thread;
use std::time::Instant;

use crossbeam_queue::ArrayQueue;
use crossbeam_utils::Backoff;

use crate::message::Message;
use crate::{
    check_messages, message, parse_args, print, print_usage, Topology, RECORDING, THREADS,
};

/// How a sender or receiver waits after a push or pop that failed.
pub trait Wait {
    /// Returns a waiter for one thread.
    fn new() -> Self;
    /// Waits before the next attempt.
    fn wait(&mut self);
    /// Starts over after an attempt that succeeded.
    fn reset(&mut self);
}

/// Waits with `thread::yield_now`, as upstream's `segqueue` does.
pub struct Yield;

impl Wait for Yield {
    fn new() -> Self {
        Yield
    }

    fn wait(&mut self) {
        thread::yield_now();
    }

    fn reset(&mut self) {}
}

/// Waits with `Backoff::snooze`, which spins before it yields, as crossbeam-channel starts its
/// blocking waits.
pub struct Snooze(Backoff);

impl Wait for Snooze {
    fn new() -> Self {
        Snooze(Backoff::new())
    }

    fn wait(&mut self) {
        self.0.snooze();
    }

    fn reset(&mut self) {
        self.0.reset();
    }
}

/// Returns the queue that every workload with `capacity` runs on.
pub(crate) fn new_queue(capacity: usize) -> ArrayQueue<Message> {
    ArrayQueue::new(capacity)
}

/// Pushes `count` messages as `sender`.
fn send<W: Wait>(q: &ArrayQueue<Message>, sender: usize, count: usize) {
    let mut w = W::new();
    for i in 0..count {
        let mut m = message(sender, i);
        while let Err(rejected) = q.push(m) {
            m = rejected;
            w.wait();
        }
        w.reset();
    }
}

/// Pops `count` messages. When recording is on, it appends their values to `got`.
fn recv<W: Wait>(q: &ArrayQueue<Message>, count: usize, got: &mut Vec<usize>) {
    let mut w = W::new();
    for _ in 0..count {
        let m = loop {
            if let Some(m) = q.pop() {
                break m;
            }
            w.wait();
        };
        w.reset();
        if RECORDING {
            got.push(m.value());
        }
    }
}

/// Runs `topology` on `q` with `messages` in total and returns what each receiver got. The
/// vectors are empty unless recording is on. `messages` must be a positive multiple of
/// `THREADS`. For `Seq`, it must not exceed the capacity of `q`, because one thread sends
/// everything before receiving and a larger count waits forever.
pub(crate) fn run_topology<W: Wait>(
    q: &ArrayQueue<Message>,
    topology: Topology,
    messages: usize,
) -> Vec<Vec<usize>> {
    match topology {
        Topology::Seq => run_seq::<W>(q, messages),
        Topology::Spsc => run_concurrent::<W>(q, messages, 1, 1),
        Topology::Mpsc => run_concurrent::<W>(q, messages, THREADS, 1),
        Topology::Mpmc => run_concurrent::<W>(q, messages, THREADS, THREADS),
    }
}

/// Sends every message and then receives them on the calling thread.
fn run_seq<W: Wait>(q: &ArrayQueue<Message>, messages: usize) -> Vec<Vec<usize>> {
    let mut got = vec![Vec::new()];
    send::<W>(q, 0, messages);
    recv::<W>(q, messages, &mut got[0]);
    got
}

/// Starts `senders` sender threads and `receivers` receiver threads and splits the messages
/// evenly between them. A single receiver runs on the calling thread, as upstream's does.
fn run_concurrent<W: Wait>(
    q: &ArrayQueue<Message>,
    messages: usize,
    senders: usize,
    receivers: usize,
) -> Vec<Vec<usize>> {
    let mut got = vec![Vec::new(); receivers];
    crossbeam::scope(|scope| {
        for sender in 0..senders {
            scope.spawn(move |_| send::<W>(q, sender, messages / senders));
        }
        if receivers == 1 {
            recv::<W>(q, messages, &mut got[0]);
        } else {
            for slot in got.iter_mut() {
                scope.spawn(move |_| recv::<W>(q, messages / receivers, slot));
            }
        }
    })
    .unwrap();
    got
}

/// Pushes and pops `q.capacity()` messages on the calling thread, so the timed run starts with
/// every slot touched.
pub(crate) fn warm<W: Wait>(q: &ArrayQueue<Message>) {
    let mut discard = Vec::new();
    send::<W>(q, 0, q.capacity());
    recv::<W>(q, q.capacity(), &mut discard);
}

/// Runs the workload that `args` name with `messages` in total, waiting with `W`, writes one
/// result line labeled `kind` to `stdout`, and returns the exit status: 0 on success, and 2 for
/// bad arguments or a bad message count, which it reports on `stderr`.
pub fn main<W: Wait>(
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

    let q = new_queue(capacity);
    warm::<W>(&q);

    let start = Instant::now();
    let received = run_topology::<W>(&q, topology, messages);
    let elapsed = start.elapsed();
    print(stdout, &name, kind, elapsed);

    // Freeing a large queue takes milliseconds, so it happens after the timer stops.
    drop(received);
    drop(q);
    0
}
