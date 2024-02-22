package codec

type Message struct {
	Header Header
	Body interface{}
}

type Header struct {
	ServiceMethod string // format "Service.Method"
	Seq           uint64 // sequence number chosen by client
	Error         string
}

type Codec interface {
	Decode([]byte, *Message) error
	Encode(*Header, interface{}) ([]byte, error)
}

type NewCodecFunc func() Codec

type Type string

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json" // not implemented
)

var NewCodecFuncMap map[Type]NewCodecFunc

func init() {
	NewCodecFuncMap = make(map[Type]NewCodecFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
}



