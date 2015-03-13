package gazette

type Topic struct {
	Name string

	// Serializes |message| to []byte, returning error.
	Serialize func(message interface{}) ([]byte, error)

	// Extracts a serialized message from []byte, returning error.
	Parse func(buffer []byte) (interface{}, error)

	// Extracts a routing key to be used for the message.
	// TODO(johnny).
	//RoutingKey func(message interface{}) string

	// Defines message ordering. Allows the service to intelligently order
	// messages arriving from multiple concurrent stream subscriptions.
	// TODO(johnny).
	// Less func(lhs, rhs interface{})
}
