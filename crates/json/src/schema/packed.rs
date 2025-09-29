// PackedSlice is like Box<[T]>, but stores its pointer using two u32s on 64-bit platforms.
// It's padded size is 12 bytes (instead of 16 bytes), which means its
// embedded size within an enum is 16 bytes instead of 24 bytes.
// On 32-bit platforms, we only need one u32 for the pointer.
pub struct PackedSlice<T> {
    marker: std::marker::PhantomData<T>,
    ptr_l: u32,
    #[cfg(target_pointer_width = "64")]
    ptr_h: u32,
    len: u32,
}

// PackedStr is like PackedSlice, but specifically for UTF-8 strings.
pub struct PackedStr(PackedSlice<u8>);

impl<T> PackedSlice<T> {
    pub fn new(v: Vec<T>) -> Self {
        let mut boxed: Box<[T]> = v.into_boxed_slice();
        let len = boxed.len();
        let ptr = boxed.as_mut_ptr();
        assert!(len <= u32::MAX as usize);
        std::mem::forget(boxed);

        #[cfg(target_pointer_width = "32")]
        {
            Self {
                marker: std::marker::PhantomData,
                ptr_l: ptr as u32,
                len: len as u32,
            }
        }
        #[cfg(target_pointer_width = "64")]
        {
            Self {
                marker: std::marker::PhantomData,
                ptr_l: (ptr as usize & 0xFFFF_FFFF) as u32,
                ptr_h: ((ptr as usize >> 32) & 0xFFFF_FFFF) as u32,
                len: len as u32,
            }
        }
    }

    #[inline(always)]
    pub fn ptr(&self) -> *mut T {
        #[cfg(target_pointer_width = "32")]
        {
            self.ptr_l as *mut T
        }
        #[cfg(target_pointer_width = "64")]
        {
            let ptr = ((self.ptr_h as usize) << 32) | (self.ptr_l as usize);
            ptr as *mut T
        }
    }
}

impl PackedStr {
    pub fn new(s: String) -> Self {
        Self(PackedSlice::new(s.into_bytes()))
    }
}

impl<T> From<Vec<T>> for PackedSlice<T> {
    fn from(v: Vec<T>) -> Self {
        Self::new(v)
    }
}
impl From<String> for PackedStr {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl<T> std::ops::Deref for PackedSlice<T> {
    type Target = [T];

    #[inline(always)]
    fn deref(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr(), self.len as usize) }
    }
}

impl std::ops::Deref for PackedStr {
    type Target = str;

    #[inline(always)]
    fn deref(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(self.0.deref()) }
    }
}

impl<'alloc, T> std::ops::DerefMut for PackedSlice<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr(), self.len as usize) }
    }
}

impl<T> Drop for PackedSlice<T> {
    fn drop(&mut self) {
        use std::ops::DerefMut;
        unsafe {
            let _boxed = Box::<[T]>::from_raw(self.deref_mut());
        }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for PackedSlice<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, f)
    }
}

impl std::fmt::Debug for PackedStr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, f)
    }
}
