package runtime

import (
	proto "github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
)

func ToAny[
	Message interface {
		proto.Message
		Marshal() ([]byte, error)
	},
](m Message) *types.Any {
	var b, err = m.Marshal()
	if err != nil {
		panic(err)
	}
	return &types.Any{Value: b}
}

func FromAny[
	Message any,
	MessagePtr interface {
		*Message
		proto.Message
		Unmarshal([]byte) error
	},
](any *types.Any) *Message {
	var msg = new(Message)
	var err = MessagePtr(msg).Unmarshal(any.Value)
	if err != nil {
		panic(err)
	}
	return msg
}
