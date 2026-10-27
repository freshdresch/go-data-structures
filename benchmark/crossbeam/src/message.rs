// Derived from crossbeam-rs/crossbeam crossbeam-channel/benchmarks/message.rs at commit 099d046,
// which is licensed under the MIT license. Copyright (c) 2019 The Crossbeam Project Developers.
// The changes are: value returns the stored number, and the #[allow(dead_code)] that value makes
// unneeded is dropped.

use std::fmt;

const LEN: usize = 1;

#[derive(Clone, Copy)]
pub(crate) struct Message([usize; LEN]);

impl Message {
    /// Returns the number the message was made from.
    pub(crate) fn value(&self) -> usize {
        self.0[0]
    }
}

impl fmt::Debug for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad("Message")
    }
}

#[inline]
pub(crate) fn new(num: usize) -> Message {
    Message([num; LEN])
}
