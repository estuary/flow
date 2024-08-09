pub use gazette::uuid::{build, parse, Clock, Flags, Producer};
use proto_flow::flow;

/// Build a flow::UuidParts from a Producer, Clock, and Flags.
pub fn build_uuid_parts(p: Producer, c: Clock, f: Flags) -> flow::UuidParts {
    flow::UuidParts {
        clock: c.to_g1582_ns100(),
        node: (p.0[0] as u64) << 56
            | (p.0[1] as u64) << 48
            | (p.0[2] as u64) << 40
            | (p.0[3] as u64) << 32
            | (p.0[4] as u64) << 24
            | (p.0[5] as u64) << 16
            | (f.0 as u64),
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_and_build_with_fixture() {
        let u1 = uuid::Uuid::parse_str("9f2952f3-c6a3-11ea-8802-080607050309").unwrap();
        let (producer, clock, flags) = gazette::uuid::parse(u1).unwrap();

        assert_eq!(producer.as_bytes(), &[8, 6, 7, 5, 3, 9]);
        assert_eq!(clock.to_g1582_ns100(), 0x1eac6a39f2952f32);
        assert_eq!(clock.to_unix(), (1594821664, 47589108));
        assert_eq!(flags.0, 0x02);

        let u2 = gazette::uuid::build(producer, clock, flags);
        assert_eq!(u1, u2);

        assert_eq!(
            build_uuid_parts(producer, clock, flags),
            flow::UuidParts {
                node: 0x0806070503090000 + 0x02,
                clock: 0x1eac6a39f2952f32,
            }
        );
    }
}
