// Package recoverylog specifies a finite-state machine for recording
// and replaying observed filesystem operations into a Gazette journal.

//go:generate protoc -I . -I ../../vendor  --gogo_out=plugins=grpc:. recorded_op.proto
package recoverylog
