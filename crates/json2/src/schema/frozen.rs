pub struct FrozenSlice<T> {
    marker: std::marker::PhantomData<T>,
    ptr_l: u32,
    ptr_h: u32,
    tag_len: u32,
}

impl<T> FrozenSlice<T> {
    pub fn new(v: Vec<T>, tag: bool) -> Self {
        let mut boxed: Box<[T]> = v.into_boxed_slice();
        let len = boxed.len();
        let ptr = boxed.as_mut_ptr();
        assert!(len <= u32::MAX as usize);
        std::mem::forget(boxed);

        Self {
            marker: std::marker::PhantomData,
            ptr_l: (ptr as usize & 0xFFFF_FFFF) as u32,
            ptr_h: ((ptr as usize >> 32) & 0xFFFF_FFFF) as u32,
            tag_len: len as u32 | if tag { 0x8000_0000 } else { 0x0000_0000 },
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        (self.tag_len & 0xEFFF_FFFF) as usize
    }

    #[inline(always)]
    pub fn tag(&self) -> bool {
        (self.tag_len & 0x8000_0000) != 0
    }

    #[inline(always)]
    pub fn ptr(&self) -> *mut T {
        let ptr = ((self.ptr_h as usize) << 32) | (self.ptr_l as usize);
        ptr as *mut T
    }
}

impl<T> std::ops::Deref for FrozenSlice<T> {
    type Target = [T];

    #[inline(always)]
    fn deref(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr(), self.len()) }
    }
}

impl<'alloc, T> std::ops::DerefMut for FrozenSlice<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr(), self.len()) }
    }
}

impl<T> Drop for FrozenSlice<T> {
    fn drop(&mut self) {
        use std::ops::DerefMut;
        unsafe {
            let _boxed = Box::<[T]>::from_raw(self.deref_mut());
        }
    }
}

pub struct FrozenString(FrozenSlice<u8>);

impl FrozenString {
    pub fn new(s: String, tag: bool) -> Self {
        Self(FrozenSlice::new(s.into_bytes(), tag))
    }
    #[inline(always)]
    pub fn tag(&self) -> bool {
        self.0.tag()
    }
}

impl std::ops::Deref for FrozenString {
    type Target = str;

    #[inline(always)]
    fn deref(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(self.0.deref()) }
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for FrozenSlice<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.tag() {
            std::fmt::Debug::fmt(&(&**self, "tagged"), f)
        } else {
            std::fmt::Debug::fmt(&**self, f)
        }
    }
}

impl std::fmt::Debug for FrozenString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.tag() {
            std::fmt::Debug::fmt(&(&**self, "tagged"), f)
        } else {
            std::fmt::Debug::fmt(&**self, f)
        }
    }
}
