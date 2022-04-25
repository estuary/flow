use bit_set::BitSet;
use fxhash::FxHashMap as HashMap;
use thiserror;

/// Set of interned strings.
pub type Set = BitSet;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("intern table is full")]
    Overflow,
}

#[derive(Debug)]
pub struct Table {
    m: HashMap<String, Set>,
}

impl Table {
    /// New returns a new, empty Table.
    pub fn new() -> Table {
        Table {
            m: HashMap::default(),
        }
    }

    /// Intern a str into a corresponding Set having exactly one bit set.
    /// After the first intern of a str, all future interns will return
    /// the same Set value.
    ///
    /// An Error is returned only if the str overflows Set's capacity to
    /// represent all interned strings (determined by the number of bits
    /// of the Set type).
    pub fn intern(&mut self, s: &str) -> Result<&Set, Error> {
        let l = self.m.len();
        if l == MAX_TABLE_SIZE {
            return Err(Error::Overflow);
        }
        let mut id = BitSet::new();
        id.insert(l);
        Ok(self.m.entry(s.to_owned()).or_insert(id.clone()))
    }

    /// Freeze the table, indicating no further strings will be interned.
    pub fn freeze(&mut self) {
        self.m.shrink_to_fit()
    }

    /// Lookup a string in the table. If found, a corresponding Set having
    /// exactly one bit set will be returned. Otherwise, the returned Set
    /// is zero-valued.
    pub fn lookup(&self, s: &str) -> Set {
        match self.m.get(s) {
            Some(v) => v.clone(),
            None => BitSet::new(),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_intern_and_lookup_with_fixtures() {
        let mut t = Table::new();

        assert_eq!(
            t.intern("hello").unwrap(),
            &BitSet::from_bytes(&[0b1_000_0000])
        );
        assert_eq!(
            t.intern("world").unwrap(),
            &BitSet::from_bytes(&[0b0_100_0000])
        );
        assert_eq!(
            t.intern("hello").unwrap(),
            &BitSet::from_bytes(&[0b1_000_0000])
        );
        assert_eq!(
            t.intern("there").unwrap(),
            &BitSet::from_bytes(&[0b0_010_0000])
        );
        assert_eq!(
            t.intern("now").unwrap(),
            &BitSet::from_bytes(&[0b0_001_0000])
        );

        assert_eq!(t.lookup("there"), BitSet::from_bytes(&[0b0_010_0000]));
        assert_eq!(t.lookup("world"), BitSet::from_bytes(&[0b0_100_0000]));
        assert_eq!(t.lookup("not found"), BitSet::from_bytes(&[0b0]));
    }
}

const MAX_TABLE_SIZE: usize = 256;
