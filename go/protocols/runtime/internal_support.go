package runtime

import (
	proto "github.com/gogo/protobuf/proto"
)

func ToInternal[
	Message interface {
		proto.Message
		Marshal() ([]byte, error)
	},
](m Message) []byte {
	var b, err = m.Marshal()
	if err != nil {
		panic(err)
	}
	return b
}

func FromInternal[
	Message any,
	MessagePtr interface {
		*Message
		proto.Message
		Unmarshal([]byte) error
	},
](b []byte) *Message {
	var msg = new(Message)
	var err = MessagePtr(msg).Unmarshal(b)
	if err != nil {
		panic(err)
	}
	return msg
}
