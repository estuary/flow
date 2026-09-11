//! The two halves of the sandbox's egress control: the nftables ruleset that
//! the policy compiles to, and the DNS forwarder that feeds the one set the
//! ruleset leaves empty.
//!
//! Both binaries need the same three things - the policy, the baseline
//! denylist, and the helper's own subnets - so they share this library rather
//! than agreeing twice on what "denied" means.

pub mod cidr;
pub mod dns;
pub mod ifaddrs;
pub mod policy;
pub mod ruleset;
