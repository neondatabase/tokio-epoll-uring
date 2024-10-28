// Copyright (c) 2021 Carl Lerche
//
// Permission is hereby granted, free of charge, to any
// person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the
// Software without restriction, including without
// limitation the rights to use, copy, modify, merge,
// publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software
// is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice
// shall be included in all copies or substantial portions
// of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF
// ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
// TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
// PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
// SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
// CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
// IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.
//
//
// Based on tokio-uring.git:d5e90539bd6d1c518e848298564a098c300866bc

use super::{BoundedBuf, BoundedBufMut, IoBuf, IoBufMut};

use std::cmp;
use std::ops;

/// An owned view into a contiguous sequence of bytes.
///
/// This is similar to Rust slices (`&buf[..]`) but owns the underlying buffer.
/// This type is useful for performing io-uring read and write operations using
/// a subset of a buffer.
///
/// Slices are created using [`BoundedBuf::slice`].
pub struct Slice<T> {
    buf: T,
    begin: usize,
    end: usize,
}

impl<T> Slice<T> {
    pub(crate) fn new(buf: T, begin: usize, end: usize) -> Slice<T> {
        Slice { buf, begin, end }
    }

    /// Offset in the underlying buffer at which this slice starts.
    pub fn begin(&self) -> usize {
        self.begin
    }

    /// Ofset in the underlying buffer at which this slice ends.
    pub fn end(&self) -> usize {
        self.end
    }

    /// Gets a reference to the underlying buffer.
    ///
    /// This method escapes the slice's view.
    pub fn get_ref(&self) -> &T {
        &self.buf
    }

    /// Gets a mutable reference to the underlying buffer.
    ///
    /// This method escapes the slice's view.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.buf
    }

    /// Unwraps this `Slice`, returning the underlying buffer.
    pub fn into_inner(self) -> T {
        self.buf
    }
}

impl<T: IoBuf> ops::Deref for Slice<T> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        let buf_bytes = super::deref(&self.buf);
        let end = cmp::min(self.end, buf_bytes.len());
        &buf_bytes[self.begin..end]
    }
}

impl<T: IoBufMut> ops::DerefMut for Slice<T> {
    fn deref_mut(&mut self) -> &mut [u8] {
        let buf_bytes = super::deref_mut(&mut self.buf);
        let end = cmp::min(self.end, buf_bytes.len());
        &mut buf_bytes[self.begin..end]
    }
}

impl<T: IoBuf> BoundedBuf for Slice<T> {
    type Buf = T;
    type Bounds = ops::Range<usize>;

    fn slice(self, range: impl ops::RangeBounds<usize>) -> Slice<T> {
        use ops::Bound;

        let begin = match range.start_bound() {
            Bound::Included(&n) => self.begin.checked_add(n).expect("out of range"),
            Bound::Excluded(&n) => self
                .begin
                .checked_add(n)
                .and_then(|x| x.checked_add(1))
                .expect("out of range"),
            Bound::Unbounded => self.begin,
        };

        assert!(begin <= self.end);

        let end = match range.end_bound() {
            Bound::Included(&n) => self
                .begin
                .checked_add(n)
                .and_then(|x| x.checked_add(1))
                .expect("out of range"),
            Bound::Excluded(&n) => self.begin.checked_add(n).expect("out of range"),
            Bound::Unbounded => self.end,
        };

        assert!(end <= self.end);
        assert!(begin <= self.buf.bytes_init());

        Slice::new(self.buf, begin, end)
    }

    fn slice_full(self) -> Slice<T> {
        self
    }

    fn get_buf(&self) -> &T {
        &self.buf
    }

    fn bounds(&self) -> Self::Bounds {
        self.begin..self.end
    }

    fn from_buf_bounds(buf: T, bounds: Self::Bounds) -> Self {
        assert!(bounds.start <= buf.bytes_init());
        assert!(bounds.end <= buf.bytes_total());
        Slice::new(buf, bounds.start, bounds.end)
    }

    fn stable_ptr(&self) -> *const u8 {
        super::deref(&self.buf)[self.begin..].as_ptr()
    }

    fn bytes_init(&self) -> usize {
        ops::Deref::deref(self).len()
    }

    fn bytes_total(&self) -> usize {
        self.end - self.begin
    }
}

impl<T: IoBufMut> BoundedBufMut for Slice<T> {
    type BufMut = T;

    fn stable_mut_ptr(&mut self) -> *mut u8 {
        super::deref_mut(&mut self.buf)[self.begin..].as_mut_ptr()
    }

    unsafe fn set_init(&mut self, pos: usize) {
        self.buf.set_init(self.begin + pos);
    }
}
