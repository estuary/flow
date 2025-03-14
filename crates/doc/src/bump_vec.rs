use std::ptr::NonNull;
use std::{cmp, fmt, mem, ops, ptr};

/// BumpVec is an optimized bump-allocator vector representation
/// that fits in 8 bytes. It achieves this by storing the length
/// and capacity as leading u32s of a pointed-to allocated memory region.
/// This is unlike Rust's Vec<> which includes the length and
/// capacity within the struct, or -- worse -- bumpalo's Vec which
/// *also* embeds a reference to the owning bump allocator.
///
/// As trade-off, we restrict BumpVec to u32 elements and require
/// that the allocator be presented to any operation which may
/// require growing the backing memory region.
///
/// BumpVec *does not Drop* its held elements and should not be
/// used with types that implement custom Drop behaviors.
pub struct BumpVec<'alloc, T> {
    ptr: Option<NonNull<u8>>,
    marker: std::marker::PhantomData<&'alloc [T]>,
}

// RawVec is allocated within the Bump.
// We rely on C field ordering.
#[repr(C)]
struct RawVec<T> {
    cap: u32,
    len: u32,
    data: [T],
}

impl<'alloc, T> BumpVec<'alloc, T> {
    pub fn new() -> Self {
        Self {
            ptr: None,
            marker: Default::default(),
        }
    }

    #[inline]
    pub fn with_capacity_in(capacity: usize, alloc: &'alloc bumpalo::Bump) -> Self {
        if capacity == 0 {
            return Self::new(); // Don't allocate an empty array.
        }
        Self {
            ptr: Some(Self::allocate_inline(capacity, alloc).0),
            marker: Default::default(),
        }
    }

    pub fn with_contents<I>(alloc: &'alloc bumpalo::Bump, iter: I) -> Self
    where
        I: ExactSizeIterator<Item = T>,
    {
        if iter.len() == 0 {
            return Self::new();
        }
        let (ptr, raw) = Self::allocate_inline(iter.len(), alloc);

        for value in iter {
            // Safety: we just allocated this memory to the exact size of this iterator.
            unsafe {
                let end = raw.data.as_mut_ptr().add(raw.len as usize);
                ptr::write(end, value);
                raw.len += 1;
            }
        }

        Self {
            ptr: Some(ptr),
            marker: Default::default(),
        }
    }

    #[inline]
    pub unsafe fn set_len(&mut self, len: usize) {
        #[cold]
        #[inline(never)]
        #[track_caller]
        fn assert_failed(len: usize, cap: usize) -> ! {
            panic!("cannot set_len({len}) for BumpVec of capacity {cap}");
        }

        match self.raw() {
            Some(raw) if raw.cap as usize >= len => raw.len = len as u32,
            None if len == 0 => (),
            Some(raw) => assert_failed(len, raw.cap as usize),
            None => assert_failed(len, 0),
        }
    }

    pub fn len(&self) -> usize {
        match self.raw() {
            Some(raw) => raw.len as usize,
            None => 0,
        }
    }

    pub fn cap(&self) -> usize {
        match self.raw() {
            Some(raw) => raw.cap as usize,
            None => 0,
        }
    }

    pub fn as_slice(&self) -> &'alloc [T] {
        match self.raw() {
            Some(raw) => &raw.data[..raw.len as usize],
            None => &[],
        }
    }

    pub fn push(&mut self, value: T, alloc: &'alloc bumpalo::Bump) {
        let raw = match self.raw() {
            Some(raw) if raw.len != raw.cap => raw,
            None | Some(_) => self.grow(1, alloc),
        };

        unsafe {
            let end = raw.data.as_mut_ptr().add(raw.len as usize);
            ptr::write(end, value);
            raw.len += 1;
        }
    }

    pub fn insert(&mut self, index: usize, value: T, alloc: &'alloc bumpalo::Bump) {
        #[cold]
        #[inline(never)]
        #[track_caller]
        fn assert_failed(index: usize, len: u32) -> ! {
            panic!("insertion index (is {index}) should be <= len (is {len})");
        }

        let raw = match self.raw() {
            Some(raw) if (raw.len as usize) < index => assert_failed(index, raw.len),
            Some(raw) if raw.len == raw.cap => self.grow(1, alloc),
            Some(raw) => raw,
            None => self.grow(1, alloc), // Initialize new allocated array.
        };
        let len = raw.len as usize;

        unsafe {
            // The place we are inserting into.
            let ptr = raw.data.as_mut_ptr().add(index);
            // Shift everything up to open the spot.
            ptr::copy(ptr, ptr.add(1), len - index);
            // Copy in the inserted value.
            ptr::write(ptr, value);
        }
        raw.len += 1;
    }

    pub fn remove(&mut self, index: usize) -> T {
        #[cold]
        #[inline(never)]
        #[track_caller]
        fn assert_failed(index: usize, len: u32) -> ! {
            panic!("removal index (is {index}) should be < len (is {len})");
        }

        let raw = match self.raw() {
            Some(raw) if (raw.len as usize) <= index => assert_failed(index, raw.len),
            Some(raw) => raw,
            None => assert_failed(index, 0),
        };
        let len = raw.len as usize;

        let ret;
        unsafe {
            // The place we are taking from.
            let ptr = raw.data.as_mut_ptr().add(index);
            // Copy it out, unsafely having a copy of the value on
            // the stack and in the vector at the same time.
            ret = ptr::read(ptr);
            // Shift everything down to fill in that spot.
            ptr::copy(ptr.add(1), ptr, len - index - 1);
        }
        raw.len -= 1;
        ret
    }

    pub fn extend<I: Iterator<Item = T>>(&mut self, it: I, alloc: &'alloc bumpalo::Bump) {
        for value in it {
            self.push(value, alloc)
        }
    }

    // Allocate space for a RawVec with the correct size and alignment.
    fn allocate(
        capacity: usize,
        alloc: &'alloc bumpalo::Bump,
    ) -> (NonNull<u8>, &'alloc mut RawVec<T>) {
        Self::allocate_inline(capacity, alloc)
    }

    #[inline(always)]
    fn allocate_inline(
        capacity: usize,
        alloc: &'alloc bumpalo::Bump,
    ) -> (NonNull<u8>, &'alloc mut RawVec<T>) {
        let cap = u32::try_from(capacity).expect("capacity is too large");
        let (size_of_elem, size_of_header, align) = Self::sizes();
        let size = size_of_header + capacity * size_of_elem; // Cannot overflow (cap is u32).

        // Safety: we know `align` is a non-zero power of two, and capacity fits within u32.
        // Using unchecked has a small, persistent effect in benchmarks.
        let layout = unsafe { std::alloc::Layout::from_size_align_unchecked(size, align) };
        let ptr = alloc.alloc_layout(layout);

        // Initialize the allocated RawVec by building a "fat" reference to the !Sized RawVec.
        // Fat references include the length of the pointed-to slice.
        let raw =
            unsafe { std::mem::transmute::<(NonNull<u8>, usize), &mut RawVec<T>>((ptr, capacity)) };

        raw.cap = cap;
        raw.len = 0;
        // raw.data is left uninitialized.

        (ptr, raw)
    }

    fn raw(&self) -> Option<&'alloc mut RawVec<T>> {
        let Some(ptr) = self.ptr else {
            return None;
        };
        unsafe {
            // We know that the allocated slice capacity is the first u32 of RawVec.
            let cap = std::mem::transmute::<NonNull<u8>, &u32>(ptr);
            // Recover a "fat" reference to the !Sized RawVec which includes its capacity.
            let raw = std::mem::transmute::<(NonNull<u8>, usize), &'alloc mut RawVec<T>>((
                ptr,
                *cap as usize,
            ));
            Some(raw)
        }
    }

    fn grow(&mut self, additional: u32, alloc: &'alloc bumpalo::Bump) -> &mut RawVec<T> {
        let Some(src) = self.raw() else {
            let (ptr, raw) = Self::allocate(cmp::max(additional, 4) as usize, alloc);
            self.ptr = Some(ptr);
            return raw;
        };

        let (ptr, dst) = Self::allocate(cmp::max(additional, 2 * src.cap) as usize, alloc);
        self.ptr = Some(ptr);

        unsafe {
            ptr::copy(
                src.data.as_mut_ptr(),
                dst.data.as_mut_ptr(),
                src.len as usize,
            );
        }
        dst.len = src.len;

        dst
    }

    const fn sizes() -> (usize, usize, usize) {
        // Use a placeholder struct to detect layout for T.
        #[repr(C)]
        struct One<T> {
            _cap: u32,
            _len: u32,
            // Padding here?
            _one: T,
        }
        let size_of_elem = mem::size_of::<T>();
        let size_of_header = if size_of_elem > mem::size_of::<u32>() {
            mem::size_of::<One<T>>() - size_of_elem
        } else {
            2 * mem::size_of::<u32>() // `cap` + `len`.
        };

        // Note that if T is u8 or bool then u32 will dominate its alignment.
        // If T is, say, a u64 then T will.

        (size_of_elem, size_of_header, mem::align_of::<One<T>>())
    }
}

impl<'alloc, T> ops::Deref for BumpVec<'alloc, T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        self.as_slice()
    }
}

impl<'alloc, T> ops::DerefMut for BumpVec<'alloc, T> {
    fn deref_mut(&mut self) -> &mut [T] {
        match self.raw() {
            Some(raw) => &mut raw.data[..raw.len as usize],
            None => &mut [],
        }
    }
}

impl<'alloc, T: Copy> BumpVec<'alloc, T> {
    pub fn from_slice(slice: &[T], alloc: &'alloc bumpalo::Bump) -> Self {
        let (ptr, raw) = Self::allocate_inline(slice.len(), alloc);
        raw.data.copy_from_slice(slice);
        raw.len = raw.cap;

        Self {
            ptr: Some(ptr),
            marker: Default::default(),
        }
    }
}

impl<'alloc, T: fmt::Debug> fmt::Debug for BumpVec<'alloc, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_slice().fmt(f)
    }
}

#[cfg(test)]
mod test {
    use super::BumpVec;

    #[test]
    fn test_operations() {
        let alloc = bumpalo::Bump::new();
        let mut b = BumpVec::<u32>::new();

        assert_eq!(b.len(), 0);
        assert_eq!(b.cap(), 0);

        b.push(8, &alloc);
        assert_eq!(b.len(), 1);
        assert_eq!(b.cap(), 4);

        b.extend([6, 7, 5].into_iter(), &alloc);
        assert_eq!(b.len(), 4);
        assert_eq!(b.cap(), 4);

        b.push(3, &alloc);
        assert_eq!(b.len(), 5);
        assert_eq!(b.cap(), 8);

        b.extend([0, 9].into_iter(), &alloc);
        assert_eq!(b.len(), 7);
        assert_eq!(b.cap(), 8);

        // Acts as &[T]
        assert_eq!(b.get(2), Some(&7));
        assert_eq!(b.as_slice(), &[8, 6, 7, 5, 3, 0, 9]);

        b.insert(4, 11, &alloc);
        assert_eq!(b.cap(), 8);
        b.insert(2, 13, &alloc); // Re-allocs.
        assert_eq!(b.cap(), 16);
        b.insert(9, 99, &alloc); // Insert at end.
        b.insert(0, 0, &alloc); // And beginning.

        assert_eq!(b.as_slice(), &[0, 8, 6, 13, 7, 5, 11, 3, 0, 9, 99]);

        b.remove(3); // 13
        b.remove(0); // 0
        b.remove(4); // 11
        b.remove(7); // 99

        assert_eq!(b.as_slice(), &[8, 6, 7, 5, 3, 0, 9]);

        b.reverse(); // Derefs as &mut [T].
        assert_eq!(b.as_slice(), &[9, 0, 3, 5, 7, 6, 8]);

        // We can convert BumpVec into an Iterator.
        let v = b.iter().copied().collect::<Vec<_>>();
        assert_eq!(v.as_slice(), &[9, 0, 3, 5, 7, 6, 8]);
    }

    // This test fails on m1 macs, so just restrict it to x86_64 systems for now
    #[test]
    #[cfg_attr(not(target_arch = "x86_64"), ignore)]
    fn test_various_t() {
        // Returns (size_of_elem, size_of_header, alignment).
        assert_eq!((1, 8, 4), BumpVec::<u8>::sizes());
        assert_eq!((2, 8, 4), BumpVec::<u16>::sizes());
        assert_eq!((4, 8, 4), BumpVec::<u32>::sizes());
        assert_eq!((8, 8, 8), BumpVec::<u64>::sizes());
        assert_eq!((16, 16, 16), BumpVec::<u128>::sizes());

        let alloc = bumpalo::Bump::new();

        let mut b = BumpVec::<u8>::new();
        b.extend([1, 2, 3, 4].into_iter(), &alloc);
        assert_eq!(b.as_slice(), &[1, 2, 3, 4]);

        let mut b = BumpVec::<u64>::new();
        b.extend([1, 2, 3, 4].into_iter(), &alloc);
        assert_eq!(b.as_slice(), &[1, 2, 3, 4]);

        let mut b = BumpVec::<u128>::new();
        b.extend([1, 2, 3, 4].into_iter(), &alloc);
        assert_eq!(b.as_slice(), &[1, 2, 3, 4]);
    }
}
