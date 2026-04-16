control message invariant: a capture checkpoint with a control message must have
only that control message, and nothing else. no docs, SourcedSchema, etc. just a
final checkpoint.

we must not combine a control-bearing checkpoint with any other checkpoint.

high-level capture runtime strategy (see /Users/wbaker/estuary/flow/crates/runtime/src/capture/*.rs):

> process checkpoints normally until you read a checkpoint that is a control
  message. when you read a control message, finish the transaction that was
  accumulated prior to that, and then do a standalone transaction just for the
  control message.

generally:

- when a control message checkpoint is read, store that on the Transaction
  struct, as a new field control: Option<ControlSignal>
- this signals `read_transaction` to finish
- `serve_session` then drains the accumulator and writes the runtime transaction for the prior batch of checkpoints
- now, there's that present control signal: emit that control doc, and then the checkpoint that immediately follows that is still on the stream
