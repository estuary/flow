package flow

import (
	"fmt"
)

type OpFutureWithState interface {
	OpFuture
	// State emitted by OpFuture to be sent as part of the Acknowledge response
	State() *ConnectorState
}

type AsyncOperationWithState struct {
	op    *AsyncOperation
	state *ConnectorState
}

func (o AsyncOperationWithState) Done() <-chan struct{} {
	return o.op.Done()
}

func (o AsyncOperationWithState) Err() error {
	return o.op.Err()
}

func (o AsyncOperationWithState) State() *ConnectorState {
	return o.state
}

func RunAsyncOperationWithState(fn func() (ConnectorState, error)) OpFutureWithState {
	var op = NewAsyncOperation()
	var s ConnectorState = ConnectorState{}
	var withState = AsyncOperationWithState{op: op, state: &s}

	go func(op *AsyncOperation) {
		var resolved = false
		defer func() {
			if !resolved {
				op.Resolve(fmt.Errorf("operation had an internal panic"))
			}
		}()

		var err error
		s, err = fn()
		op.Resolve(err)
		resolved = true
	}(op)

	return withState
}
