pub struct FrozenSlice<T> {
    marker: std::marker::PhantomData<T>,
    ptr_l: u32,
    ptr_h: u32,
    len: u32,
}

pub struct FrozenString(FrozenSlice<u8>);

impl<T> FrozenSlice<T> {
    pub fn new(v: Vec<T>) -> Self {
        let mut boxed: Box<[T]> = v.into_boxed_slice();
        let len = boxed.len();
        let ptr = boxed.as_mut_ptr();
        assert!(len <= u32::MAX as usize);
        std::mem::forget(boxed);

        Self {
            marker: std::marker::PhantomData,
            ptr_l: (ptr as usize & 0xFFFF_FFFF) as u32,
            ptr_h: ((ptr as usize >> 32) & 0xFFFF_FFFF) as u32,
            len: len as u32,
        }
    }

    #[inline(always)]
    pub fn ptr(&self) -> *mut T {
        let ptr = ((self.ptr_h as usize) << 32) | (self.ptr_l as usize);
        ptr as *mut T
    }
}

impl FrozenString {
    pub fn new(s: String) -> Self {
        Self(FrozenSlice::new(s.into_bytes()))
    }
}

impl<T> From<Vec<T>> for FrozenSlice<T> {
    fn from(v: Vec<T>) -> Self {
        Self::new(v)
    }
}
impl From<String> for FrozenString {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl<T> std::ops::Deref for FrozenSlice<T> {
    type Target = [T];

    #[inline(always)]
    fn deref(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.ptr(), self.len as usize) }
    }
}

impl std::ops::Deref for FrozenString {
    type Target = str;

    #[inline(always)]
    fn deref(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(self.0.deref()) }
    }
}

impl<'alloc, T> std::ops::DerefMut for FrozenSlice<T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut [T] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr(), self.len as usize) }
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

impl<T: std::fmt::Debug> std::fmt::Debug for FrozenSlice<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, f)
    }
}

impl std::fmt::Debug for FrozenString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, f)
    }
}
