//! What the Rust programs of the crossbeam benchmark suite share: the capacity table, the four
//! topologies, argument handling, and the output format. Each program is a thin binary that
//! calls the `main` of one module here.

use std::io::Write;
use std::time::Duration;

use message::Message;

pub mod arrayqueue;
pub mod crossbeam_channel;
mod message;

/// The number of messages in a timed run, upstream's `MESSAGES`.
pub const MESSAGES: usize = 5_000_000;

/// The number of senders in the mpsc and mpmc topologies, and of receivers in mpmc.
pub const THREADS: usize = 4;

/// Whether senders tag each message with their index and receivers keep what they got. Tests
/// turn it on to check delivery. Timed builds leave it off, so they send upstream's values with
/// no extra work per message.
pub(crate) const RECORDING: bool = cfg!(test);

/// How many threads send and receive, and whether sending and receiving overlap.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Topology {
    /// Sends every message and then receives them all, on one thread.
    Seq,
    /// One sender and one receiver.
    Spsc,
    /// `THREADS` senders and one receiver.
    Mpsc,
    /// `THREADS` senders and `THREADS` receivers.
    Mpmc,
}

impl Topology {
    /// Returns the lowercase name used in workload names.
    fn name(self) -> &'static str {
        match self {
            Topology::Seq => "seq",
            Topology::Spsc => "spsc",
            Topology::Mpsc => "mpsc",
            Topology::Mpmc => "mpmc",
        }
    }
}

/// The capacity table: each workload-name prefix and its queue capacity, smallest first.
const PREFIXES: [(&str, usize); 4] = [
    ("bounded1", 1),
    ("bounded1024", 1 << 10),
    ("bounded2p18", 1 << 18),
    ("bounded2p23", 1 << 23),
];

/// Returns the 13 workloads as name, capacity, and topology: `spsc`, `mpsc`, and `mpmc` for each
/// prefix, then `bounded2p23_seq`.
fn workloads() -> Vec<(String, usize, Topology)> {
    let mut all = Vec::new();
    for (prefix, capacity) in PREFIXES {
        for topology in [Topology::Spsc, Topology::Mpsc, Topology::Mpmc] {
            all.push((format!("{prefix}_{}", topology.name()), capacity, topology));
        }
    }
    // seq sends every message before receiving any, so it runs only where every message fits.
    let (prefix, capacity) = PREFIXES[PREFIXES.len() - 1];
    all.push((format!("{prefix}_seq"), capacity, Topology::Seq));
    all
}

/// Returns the capacity and topology of the workload with the given name, or `None` if it is
/// not one of the 13 workloads.
pub(crate) fn lookup(name: &str) -> Option<(usize, Topology)> {
    workloads()
        .into_iter()
        .find(|(workload, _, _)| workload == name)
        .map(|(_, capacity, topology)| (capacity, topology))
}

/// Returns the `i`-th message that `sender` sends. Recording tags it with the sender, so the
/// tests can check each sender's order. Timed builds send `i`, as upstream does.
pub(crate) fn message(sender: usize, i: usize) -> Message {
    if RECORDING {
        message::new(sender << 32 | i)
    } else {
        message::new(i)
    }
}

/// Returns the result line that `print` writes, in the format plot.py parses.
fn result_line(name: &str, kind: &str, elapsed: Duration) -> String {
    format!("{:25} {:15} {:7.6} sec", name, kind, elapsed.as_secs_f64())
}

/// Writes one result line to `stdout`. A program that cannot write its output has nowhere to
/// report that, so a write error is discarded.
pub(crate) fn print(stdout: &mut impl Write, name: &str, kind: &str, elapsed: Duration) {
    let _ = writeln!(stdout, "{}", result_line(name, kind, elapsed));
}

/// Returns the workload name, capacity, and topology, or `None` unless `args` is exactly one
/// name that `lookup` accepts.
pub(crate) fn parse_args(args: &[String]) -> Option<(String, usize, Topology)> {
    let [name] = args else {
        return None;
    };
    let (capacity, topology) = lookup(name)?;
    Some((name.clone(), capacity, topology))
}

/// Returns the program name in `kind`, such as `arrayqueue` for `Rust arrayqueue`.
fn program_name(kind: &str) -> &str {
    kind.strip_prefix("Rust ").unwrap_or(kind)
}

/// Writes the usage text of the program that `kind` names to `stderr`. A program that cannot
/// write to stderr has nowhere to report that, so write errors are discarded.
pub(crate) fn print_usage(stderr: &mut impl Write, kind: &str) {
    let names: Vec<String> = workloads().into_iter().map(|(name, _, _)| name).collect();
    let _ = writeln!(stderr, "usage: {} <workload>", program_name(kind));
    let _ = writeln!(stderr, "workloads: {}", names.join(", "));
}

/// Returns the one-line error that the program `kind` names prints if `messages` is not a
/// positive multiple of `THREADS`.
pub(crate) fn check_messages(kind: &str, messages: usize) -> Result<(), String> {
    if messages > 0 && messages.is_multiple_of(THREADS) {
        return Ok(());
    }
    Err(format!(
        "{}: {messages} messages is not a positive multiple of {THREADS}",
        program_name(kind)
    ))
}

// These tests run with RECORDING on, so senders tag each value with their index and receivers
// keep what they got. A queue that loses a message leaves its workload waiting forever, so every
// test that runs a workload does it through `within`.
#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::mpsc;
    use std::thread;

    use super::*;
    use crate::arrayqueue::{Snooze, Wait, Yield};

    const TOPOLOGIES: [Topology; 4] = [
        Topology::Seq,
        Topology::Spsc,
        Topology::Mpsc,
        Topology::Mpmc,
    ];

    /// Runs `f` on a detached thread and returns its result. It panics if `f` does not finish
    /// within 60 s. The thread is detached because joining it would wait for a stuck workload.
    /// The test process then exits and ends the thread.
    fn within<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            // The receiver is gone only after a timeout, when nobody wants the result.
            let _ = tx.send(f());
        });
        match rx.recv_timeout(Duration::from_secs(60)) {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => panic!("workload did not finish within 60 s"),
            Err(mpsc::RecvTimeoutError::Disconnected) => panic!("workload panicked"),
        }
    }

    #[test]
    fn prefix_capacities() {
        let expected = [
            ("bounded1", 1),
            ("bounded1024", 1024),
            ("bounded2p18", 262_144),
            ("bounded2p23", 8_388_608),
        ];
        assert_eq!(PREFIXES, expected);
        for (name, capacity, _) in workloads() {
            let (prefix, _) = name.rsplit_once('_').unwrap();
            let (_, want) = expected.iter().find(|(p, _)| *p == prefix).unwrap();
            assert_eq!(capacity, *want, "{name}");
        }
    }

    #[test]
    fn workload_names_and_lookup() {
        let expected = [
            "bounded1_spsc",
            "bounded1_mpsc",
            "bounded1_mpmc",
            "bounded1024_spsc",
            "bounded1024_mpsc",
            "bounded1024_mpmc",
            "bounded2p18_spsc",
            "bounded2p18_mpsc",
            "bounded2p18_mpmc",
            "bounded2p23_spsc",
            "bounded2p23_mpsc",
            "bounded2p23_mpmc",
            "bounded2p23_seq",
        ];
        let all = workloads();
        let names: Vec<&str> = all.iter().map(|(name, _, _)| name.as_str()).collect();
        assert_eq!(names, expected);
        for (name, capacity, topology) in &all {
            assert_eq!(lookup(name), Some((*capacity, *topology)), "{name}");
            assert!(name.ends_with(&format!("_{}", topology.name())), "{name}");
        }
        for name in [
            "",
            "bounded1024",
            "bounded1024_seq",
            "bounded1000_mpmc",
            "unbounded_mpmc",
        ] {
            assert_eq!(lookup(name), None, "{name}");
        }
    }

    #[test]
    fn result_line_format() {
        let elapsed = Duration::from_nanos(1_234_567_891);
        assert_eq!(
            result_line("bounded1024_mpmc", "Rust arrayqueue", elapsed),
            "bounded1024_mpmc          Rust arrayqueue 1.234568 sec"
        );
        assert_eq!(
            result_line(
                "bounded2p23_seq",
                "Rust arrayqueue-backoff",
                Duration::from_micros(1)
            ),
            "bounded2p23_seq           Rust arrayqueue-backoff 0.000001 sec"
        );
    }

    #[test]
    fn queue_capacities() {
        for (name, capacity, _) in workloads() {
            assert_eq!(
                arrayqueue::new_queue(capacity).capacity(),
                capacity,
                "{name}"
            );
            let (tx, _rx) = crossbeam_channel::new_channel(capacity);
            assert_eq!(tx.capacity(), Some(capacity), "{name}");
        }
    }

    #[test]
    fn delivery_yield() {
        delivery::<Yield>();
    }

    #[test]
    fn delivery_snooze() {
        delivery::<Snooze>();
    }

    /// Runs every topology on an ArrayQueue that waits with `W`, warm-up included, and checks
    /// what arrived. Capacity 1 makes every push wait for a pop, so a receiver that stops early
    /// leaves a sender waiting and the test times out.
    fn delivery<W: Wait + 'static>() {
        const MESSAGES: usize = 100_000;
        for topology in TOPOLOGIES {
            // seq sends everything before receiving, so it runs only where every message fits.
            let capacities: &[usize] = match topology {
                Topology::Seq => &[1 << 18],
                _ => &[1, 1 << 10, 1 << 18],
            };
            for &capacity in capacities {
                let result = within(move || {
                    let q = arrayqueue::new_queue(capacity);
                    arrayqueue::warm::<W>(&q);
                    let got = arrayqueue::run_topology::<W>(&q, topology, MESSAGES);
                    check_delivery(topology, &got, MESSAGES)
                });
                assert_eq!(result, Ok(()), "{topology:?} at capacity {capacity}");
            }
        }
    }

    /// Returns an error unless `got` is what the senders of `topology` sent with `messages` in
    /// total: every value exactly once, and each sender's values in increasing order within each
    /// receiver. The expected values come from the topology, never from what arrived.
    fn check_delivery(
        topology: Topology,
        got: &[Vec<usize>],
        messages: usize,
    ) -> Result<(), String> {
        let (senders, receivers) = match topology {
            Topology::Seq | Topology::Spsc => (1, 1),
            Topology::Mpsc => (THREADS, 1),
            Topology::Mpmc => (THREADS, THREADS),
        };
        if got.len() != receivers {
            return Err(format!(
                "got {} receiver slices, want {receivers}",
                got.len()
            ));
        }
        let per_sender = messages / senders;
        let mut seen = HashSet::with_capacity(messages);
        for (r, values) in got.iter().enumerate() {
            let mut next = vec![0; senders];
            for &v in values {
                let sender = v >> 32;
                let i = v & 0xffff_ffff;
                if sender >= senders || i >= per_sender {
                    return Err(format!("receiver {r} got {v:#x}, which no sender sends"));
                }
                if !seen.insert(v) {
                    return Err(format!("receiver {r} got {v:#x} twice"));
                }
                if i < next[sender] {
                    return Err(format!("receiver {r} got {v:#x} out of order"));
                }
                next[sender] = i + 1;
            }
        }
        if seen.len() != senders * per_sender {
            return Err(format!(
                "got {} distinct values, want {}",
                seen.len(),
                senders * per_sender
            ));
        }
        Ok(())
    }

    /// Runs each crossbeam-channel topology and checks that the receivers drained the channel.
    /// Capacity 1 makes every send wait for a receive, so a receiver loop that stops early
    /// leaves a sender blocked and the test times out.
    #[test]
    fn channel_topologies_drain() {
        let cases = [
            (Topology::Spsc, 1),
            (Topology::Mpsc, 1),
            (Topology::Mpmc, 1),
            (Topology::Seq, 1024),
        ];
        for (topology, capacity) in cases {
            let drained = within(move || {
                let (tx, rx) = crossbeam_channel::new_channel(capacity);
                crossbeam_channel::run_topology(&tx, &rx, topology, 1_000);
                rx.is_empty()
            });
            assert!(drained, "{topology:?}");
        }
    }

    fn smoke_workloads() -> Vec<String> {
        workloads()
            .into_iter()
            .map(|(name, _, _)| name)
            .filter(|name| name.starts_with("bounded1024_") || name.starts_with("bounded2p18_"))
            .collect()
    }

    /// Asserts that a run wrote nothing to `stderr` and exactly one result line for `name` and
    /// `kind` to `stdout`.
    fn assert_result_line(stdout: Vec<u8>, stderr: Vec<u8>, name: &str, kind: &str) {
        let stderr = String::from_utf8(stderr).unwrap();
        assert_eq!(stderr, "", "{kind} {name}");
        let stdout = String::from_utf8(stdout).unwrap();
        assert!(
            is_result_line(&stdout, name, kind),
            "{kind} {name}: {stdout:?}"
        );
    }

    /// Returns whether `stdout` is exactly `<name> +<kind> +<digits>.<six digits> sec\n`.
    fn is_result_line(stdout: &str, name: &str, kind: &str) -> bool {
        let Some(line) = stdout.strip_suffix(" sec\n") else {
            return false;
        };
        let Some((labels, seconds)) = line.rsplit_once(' ') else {
            return false;
        };
        let Some(padded_kind) = labels.strip_prefix(name) else {
            return false;
        };
        padded_kind.starts_with(' ') && padded_kind.trim_matches(' ') == kind && is_seconds(seconds)
    }

    /// Returns whether `seconds` is one or more digits, a point, and six digits.
    fn is_seconds(seconds: &str) -> bool {
        let Some((whole, fraction)) = seconds.split_once('.') else {
            return false;
        };
        let all_digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
        !whole.is_empty() && all_digits(whole) && fraction.len() == 6 && all_digits(fraction)
    }

    /// Runs `crossbeam_channel::main` and returns its exit status, stdout, and stderr.
    fn run_channel(args: &[String], messages: usize) -> (i32, Vec<u8>, Vec<u8>) {
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let status = crossbeam_channel::main(
            args,
            messages,
            "Rust crossbeam-channel",
            &mut stdout,
            &mut stderr,
        );
        (status, stdout, stderr)
    }

    /// Runs `arrayqueue::main` waiting with `W` and returns its exit status, stdout, and stderr.
    fn run_arrayqueue<W: Wait>(
        args: &[String],
        messages: usize,
        kind: &str,
    ) -> (i32, Vec<u8>, Vec<u8>) {
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let status = arrayqueue::main::<W>(args, messages, kind, &mut stdout, &mut stderr);
        (status, stdout, stderr)
    }

    #[test]
    fn smoke() {
        let names = smoke_workloads();
        assert_eq!(names.len(), 6);
        for name in names {
            let (status, stdout, stderr) = within({
                let name = name.clone();
                move || run_channel(&[name], 1_000)
            });
            assert_eq!(status, 0, "crossbeam-channel {name}");
            assert_result_line(stdout, stderr, &name, "Rust crossbeam-channel");
            let (status, stdout, stderr) = within({
                let name = name.clone();
                move || run_arrayqueue::<Yield>(&[name], 1_000, "Rust arrayqueue")
            });
            assert_eq!(status, 0, "arrayqueue {name}");
            assert_result_line(stdout, stderr, &name, "Rust arrayqueue");
            let (status, stdout, stderr) = within({
                let name = name.clone();
                move || run_arrayqueue::<Snooze>(&[name], 1_000, "Rust arrayqueue-backoff")
            });
            assert_eq!(status, 0, "arrayqueue-backoff {name}");
            assert_result_line(stdout, stderr, &name, "Rust arrayqueue-backoff");
        }
    }

    /// Asserts that a run rejected its arguments: exit status 2, nothing on stdout, and the usage
    /// text on stderr.
    fn assert_usage((status, stdout, stderr): (i32, Vec<u8>, Vec<u8>), args: &[String]) {
        assert_eq!(status, 2, "{args:?}");
        assert!(stdout.is_empty(), "{args:?}");
        let stderr = String::from_utf8(stderr).unwrap();
        assert!(stderr.contains("usage:"), "{args:?}: {stderr:?}");
    }

    /// Asserts that a run rejected its message count: exit status 2, nothing on stdout, and the
    /// message-count error on stderr without the usage text.
    fn assert_message_count_error((status, stdout, stderr): (i32, Vec<u8>, Vec<u8>)) {
        assert_eq!(status, 2);
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).unwrap();
        assert!(stderr.contains("not a positive multiple"), "{stderr:?}");
        assert!(!stderr.contains("usage:"), "{stderr:?}");
    }

    #[test]
    fn bad_arguments() {
        let cases: [&[&str]; 5] = [
            &[],
            &["bounded1024_mpmc", "extra"],
            &["bounded1000_mpmc"],
            &["bounded1024_seq"],
            &["bounded2p23_seq "],
        ];
        for case in cases {
            let args: Vec<String> = case.iter().map(|arg| arg.to_string()).collect();
            assert_usage(run_channel(&args, 1_000), &args);
            assert_usage(
                run_arrayqueue::<Yield>(&args, 1_000, "Rust arrayqueue"),
                &args,
            );
        }
        let args = ["bounded1024_mpmc".to_string()];
        for messages in [0, 1, THREADS + 1] {
            assert_message_count_error(run_channel(&args, messages));
            assert_message_count_error(run_arrayqueue::<Yield>(&args, messages, "Rust arrayqueue"));
        }
    }

    #[test]
    fn message_count_error() {
        assert_eq!(check_messages("Rust arrayqueue", 1_000), Ok(()));
        for messages in [0, 1, THREADS + 1] {
            let line = check_messages("Rust arrayqueue", messages).unwrap_err();
            assert_eq!(
                line,
                format!("arrayqueue: {messages} messages is not a positive multiple of 4")
            );
        }
    }
}
