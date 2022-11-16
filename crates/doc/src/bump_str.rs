use std::{fmt, mem, ops, ptr::NonNull};

/// BumpStr is an optimized bump-allocator string representation
/// that fits in 8 bytes. It achieves this by storing the length
/// as a leading u32 of a pointed-to allocated memory region.
/// This is unlike Rust's "fat" &str reference which includes the
/// length within the reference handle (see test_sizes()).
pub struct BumpStr<'alloc> {
    ptr: NonNull<u8>,
    marker: std::marker::PhantomData<&'alloc str>,
}

// RawStr is allocated within the Bump.
// We rely on C field ordering and padding.
#[repr(C)]
struct RawStr {
    len: u32,
    data: [u8],
}

impl<'alloc> BumpStr<'alloc> {
    pub fn from_str(s: &str, alloc: &'alloc bumpalo::Bump) -> Self {
        let size_of_header = 4; // u32.

        // Allocate space for a RawStr with the correct size and alignment.
        let size = size_of_header + s.len(); // `len` header plus string length in bytes.
        let align = mem::align_of::<u32>(); // Must align to u32 of RawStr::len
        let layout = unsafe { std::alloc::Layout::from_size_align_unchecked(size, align) };
        let ptr = alloc.alloc_layout(layout);

        // Initialize the allocated RawStr.
        let raw =
            unsafe { std::mem::transmute::<(NonNull<u8>, usize), &mut RawStr>((ptr, s.len())) };

        raw.len = u32::try_from(s.len()).expect("string is too large");
        raw.data.copy_from_slice(s.as_bytes());

        Self {
            ptr,
            marker: Default::default(),
        }
    }

    #[inline]
    fn raw(&self) -> &RawStr {
        unsafe {
            // We know that the allocated slice length is a leading u32.
            let len = std::mem::transmute::<NonNull<u8>, &u32>(self.ptr);
            // Construct a "fat" pointer using the "thin" pointer and length.
            std::mem::transmute::<(NonNull<u8>, usize), &RawStr>((self.ptr, *len as usize))
        }
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        unsafe {
            // We know that `data` was directly constructed from an input string.
            std::str::from_utf8_unchecked(&self.raw().data)
        }
    }
}

impl ops::Deref for BumpStr<'_> {
    type Target = str;

    #[inline]
    fn deref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<str> for BumpStr<'_> {
    #[inline]
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for BumpStr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

#[cfg(test)]
mod test {
    use super::BumpStr;

    #[test]
    fn test_str() {
        let alloc = bumpalo::Bump::new();
        let b = BumpStr::from_str("hello\0world!", &alloc);

        assert_eq!(b.raw().len, 12);
        assert_eq!(
            b.raw().data,
            [104, 101, 108, 108, 111, 0, 119, 111, 114, 108, 100, 33]
        );
        assert_eq!(b.as_str(), "hello\0world!");

        // Acts as &str.
        assert_eq!(b.len(), 12);
        assert_eq!(b.to_uppercase().as_str(), "HELLO\0WORLD!")
    }
}
