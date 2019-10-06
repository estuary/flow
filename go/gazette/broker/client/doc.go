// Package client implements a Go client for interacting with the gRPC Journal
// service of Gazette brokers. It concerns itself with operations over journal
// specifications, fragments and byte-streams. See package message for an
// implementation of messages layered atop journal streams.
//
// The package provides Reader and RetryReader, which adapt the broker Read
// RPC to the io.Reader interface. Reader will utilize just one Read RPC,
// while RetryReader will silently restart Read RPCs as needed:
//
//      // Copy a journal byte range to os.Stdout.
//      io.Copy(os.Stdout, NewReader(ctx, client, pb.ReadRequest{
//          Journal: "a/journal/name",
//          Offset:  1234,
//          EndOffset: 5678,
//      }))
//
// It provides Appender, which adapts the Append RPC to a io.WriteCloser:
//
//      // Copy os.Stdin to the journal.
//      var a = NewAppender(ctx, client, pb.AppendRequest{
//          Journal: "a/journal/name",
//      })
//      if err = io.Copy(a, os.Stdin); err == nil {
//          err = a.Close() // Commit the append.
//      }
//
// Gazette appends are atomic and linearizable. An Appender streams content to
// brokers as its written, but no content of an Appender will be visible to any
// reader until Close is called and succeeds. An implication of this is that
// once brokers have begun to sequence an append into a journal, they expect the
// remaining content and Close of that Appender to be forthcoming, and will
// quickly time it out if it stalls. Uses of Appender should thus be limited
// to cases where its full content is readily available.
//
// Most clients should instead use an AppendService. It offers automatic retries,
// an asynchronous API, and supports constraints on the ordering of appends with
// respect to other ongoing operations (ie, "append to journal Foo, but not
// before this append to journal Bar completes"). It also dynamically batches
// many co-occurring small writes into larger ones for efficiency.
//
//      var as = NewAppendService(ctx, client)
//      var op = as.StartAppend(pb.AppendRequest{
//          Journal: "a/journal/name",
//      }, myOtherOpsWhichMustCompleteFirst)
//
//      // Produce content to append into the AsyncAppend's Writer.
//      // We hold an exclusive lock over it until Release.
//      op.Writer().Write("hello, ")
//      op.Writer().Write("gazette: ")
//      op.Require(os.Copy(op.Writer(), os.Stdin))
//
//      // If os.Copy error'd, it aborts the append and is returned by Release.
//      if err = op.Release(); err == nil {
//          err = op.Err() // Blocks until operation completes.
//      }
//
// The package offers functions for listing Fragments & JournalSpecs and applying
// JournalSpecs, while accounting for pagination details. Also notable is
// PolledList, which is an important building-block for applications scaling to
// multiple journals.
package client
