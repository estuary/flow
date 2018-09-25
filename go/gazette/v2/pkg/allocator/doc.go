// Package v3_allocator implements a distributed algorithm for assigning a number
// of "Items" across a number of "Members", where each Member runs an instance of
// the Allocator. Items and Members may come and go over time; each may have
// constraints on desired replication and assignment limits which must be
// satisfied, and replicas may be placed across distinct failure Zones.
// Allocator coordinates through Etcd, and uses a greedy, incremental maximum-
// flow solver to quickly determine minimal re-Assignments which best balance
// Items across Members (subject to constraints).
package v3_allocator
