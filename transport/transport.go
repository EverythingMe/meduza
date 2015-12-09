package transport

type MessageType string

const (
	// query and respon

	UpdateMessage         MessageType = "UPDATE"
	UpdateResponseMessage MessageType = "RUPDATE"

	GetMessage         MessageType = "GET"
	GetResponseMessage MessageType = "RGET"

	PutMessage         MessageType = "PUT"
	PutResponseMessage MessageType = "RPUT"

	DelMessage         MessageType = "DEL"
	DelResponseMessage MessageType = "RDEL"

	PingMessage         MessageType = "PING"
	PingResponseMessage MessageType = "PONG"
)

// Frame represents the raw view of a serialized message over the protocol
type Message struct {
	Type MessageType
	Body []byte
}

// Transport is responsible for receiving and sending serialized messages as frames
type Transport interface {
	ReadMessage() (Message, error)
	WriteMessage(Message) error
}
