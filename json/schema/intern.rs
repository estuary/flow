use error_chain;
use fxhash::FxHashMap as HashMap;

error_chain::error_chain! {}

/// Set of interned strings.
pub type Set = u64;

const MAX_TABLE_SIZE: usize = std::mem::size_of::<Set>() * 8;

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
    pub fn intern(&mut self, s: &str) -> Result<Set> {
        let id = match self.m.len() {
            MAX_TABLE_SIZE => error_chain::bail!("table overflow"),
            l => (1 as Set) << (l as Set),
        };
        Ok(*self.m.entry(s.to_owned()).or_insert(id))
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
            Some(&v) => v,
            None => 0,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_intern_and_lookup_with_fixtures() {
        let mut t = Table::new();

        assert_eq!(t.intern("hello").unwrap(), 0b0001 as Set);
        assert_eq!(t.intern("world").unwrap(), 0b0010 as Set);
        assert_eq!(t.intern("hello").unwrap(), 0b0001 as Set);
        assert_eq!(t.intern("there").unwrap(), 0b0100 as Set);
        assert_eq!(t.intern("now").unwrap(), 0b1000 as Set);

        assert_eq!(t.lookup("there"), 0b0100 as Set);
        assert_eq!(t.lookup("world"), 0b0010 as Set);
        assert_eq!(t.lookup("not found"), 0b0 as Set);
    }
}
